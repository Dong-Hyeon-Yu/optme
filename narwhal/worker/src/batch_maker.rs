// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::metrics::WorkerMetrics;
use config::WorkerId;
use ethers_core::{
    types::transaction::eip2718::TypedTransaction,
    utils::rlp::Rlp,
};
use fastcrypto::hash::Hash;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use mysten_metrics::metered_channel::{Receiver, Sender};
use mysten_metrics::{monitored_scope, spawn_logged_monitored_task};
use rayon::prelude::*;
use network::{client::PrimaryNetworkClient, ReportBatchToPrimary};
use std::sync::Arc;
use store::{rocks::DBMap, Map};
use sui_protocol_config::ProtocolConfig;
use tokio::{
    task::JoinHandle,
    time::{sleep, Duration, Instant},
};
use tracing::{error, warn};
use types::{
    error::DagError, now, Batch, BatchAPI, BatchDigest, ConditionalBroadcastReceiver, MetadataAPI,
    Transaction, TxResponse, WorkerOurBatchMessage, WorkerOwnBatchMessage,
};

#[cfg(feature = "trace_transaction")]
use byteorder::{BigEndian, ReadBytesExt};
#[cfg(feature = "benchmark")]
use std::convert::TryInto;

// The number of batches to store / transmit in parallel.
pub const MAX_PARALLEL_BATCH: usize = 100;

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    // Our worker's id.
    id: WorkerId,
    /// The preferred batch size (in bytes).
    batch_size_limit: usize,
    /// The maximum delay after which to seal the batch.
    max_batch_delay: Duration,
    /// Receiver for shutdown.
    rx_shutdown: ConditionalBroadcastReceiver,
    /// Channel to receive transactions from the network.
    rx_batch_maker: Receiver<(Transaction, TxResponse)>,
    /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_quorum_waiter: Sender<(Batch, tokio::sync::oneshot::Sender<()>)>,
    /// Metrics handler
    node_metrics: Arc<WorkerMetrics>,
    /// The timestamp of the batch creation.
    /// Average resident time in the batch would be ~ (batch seal time - creation time) / 2
    batch_start_timestamp: Instant,
    /// The network client to send our batches to the primary.
    client: PrimaryNetworkClient,
    /// The batch store to store our own batches.
    store: DBMap<BatchDigest, Batch>,
    protocol_config: ProtocolConfig,
}

impl BatchMaker {
    #[must_use]
    pub fn spawn(
        id: WorkerId,
        batch_size_limit: usize,
        max_batch_delay: Duration,
        rx_shutdown: ConditionalBroadcastReceiver,
        rx_batch_maker: Receiver<(Transaction, TxResponse)>,
        tx_quorum_waiter: Sender<(Batch, tokio::sync::oneshot::Sender<()>)>,
        node_metrics: Arc<WorkerMetrics>,
        client: PrimaryNetworkClient,
        store: DBMap<BatchDigest, Batch>,
        protocol_config: ProtocolConfig,
    ) -> JoinHandle<()> {
        spawn_logged_monitored_task!(
            async move {
                Self {
                    id,
                    batch_size_limit,
                    max_batch_delay,
                    rx_shutdown,
                    rx_batch_maker,
                    tx_quorum_waiter,
                    batch_start_timestamp: Instant::now(),
                    node_metrics,
                    client,
                    store,
                    protocol_config,
                }
                .run()
                .await;
            },
            "BatchMakerTask"
        )
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(self.max_batch_delay);
        tokio::pin!(timer);

        let mut current_batch = Batch::new(vec![], &self.protocol_config);
        let mut current_responses = Vec::new();
        let mut current_batch_size = 0;

        let mut batch_pipeline = FuturesUnordered::new();

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                // Note that transactions are only consumed when the number of batches
                // 'in-flight' are below a certain number (MAX_PARALLEL_BATCH). This
                // condition will be met eventually if the store and network are functioning.
                Some((transaction, response_sender)) = self.rx_batch_maker.recv(), if batch_pipeline.len() < MAX_PARALLEL_BATCH => {
                    let _scope = monitored_scope("BatchMaker::recv");
                    current_batch_size += transaction.len();
                    current_batch.transactions_mut().push(transaction);
                    current_responses.push(response_sender);
                    if current_batch_size >= self.batch_size_limit {
                        if let Some(seal) = self.seal(false, current_batch, current_batch_size, current_responses).await{
                            batch_pipeline.push(seal);
                        }
                        self.node_metrics.parallel_worker_batches.set(batch_pipeline.len() as i64);

                        current_batch = Batch::new(vec![], &self.protocol_config);
                        current_responses = Vec::new();
                        current_batch_size = 0;

                        timer.as_mut().reset(Instant::now() + self.max_batch_delay);
                        self.batch_start_timestamp = Instant::now();

                        // Yield once per size threshold to allow other tasks to run.
                        tokio::task::yield_now().await;
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    let _scope = monitored_scope("BatchMaker::timer");
                    if !current_batch.transactions().is_empty() {
                        if let Some(seal) = self.seal(true, current_batch, current_batch_size, current_responses).await {
                            batch_pipeline.push(seal);
                        }
                        self.node_metrics.parallel_worker_batches.set(batch_pipeline.len() as i64);

                        current_batch = Batch::new(vec![], &self.protocol_config);
                        current_responses = Vec::new();
                        current_batch_size = 0;
                    }
                    timer.as_mut().reset(Instant::now() + self.max_batch_delay);
                    self.batch_start_timestamp = Instant::now();
                }

                _ = self.rx_shutdown.receiver.recv() => {
                    return
                }

                // Process the pipeline of batches, this consumes items in the `batch_pipeline`
                // list, and ensures the main loop in run will always be able to make progress
                // by lowering it until condition batch_pipeline.len() < MAX_PARALLEL_BATCH is met.
                _ = batch_pipeline.next(), if !batch_pipeline.is_empty() => {
                    self.node_metrics.parallel_worker_batches.set(batch_pipeline.len() as i64);
                }

            }
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal<'a>(
        &self,
        timeout: bool,
        mut batch: Batch,
        size: usize,
        responses: Vec<TxResponse>,
    ) -> Option<BoxFuture<'a, ()>> {

        // TODO: only for benchmarking. 
        // Look for sample txs (they all start with 0) and gather their txs id (the next 8 bytes).
        let _tx_ids = batch
            .transactions()
            .iter()
            .filter_map(|tx| tx[2..10].try_into().ok())
            .collect::<Vec<[u8; 8]>>();

        let mut batch = tokio::task::spawn_blocking(move || {
            batch
                .transactions_mut()
                .into_par_iter() 
                .for_each(|tx| {
                    let _tx: Vec<u8> = tx.clone();
                    let rlp = Rlp::new(&_tx);
                    tx.clear();

                    let (rlp_decoded_tx, _) = TypedTransaction::decode_signed(&rlp)
                        .expect("validation for rlp decoding must be done once receiving the tx from clients at TxServer");
                    let se = serde_json::to_vec(&rlp_decoded_tx).unwrap();
                    tx.extend(se);
                });
            batch
        }).await.expect("Failed to spawn a thread for decoding transactions.");
        

        #[cfg(feature = "benchmark")]
        {
            let digest = batch.digest();

            for id in _tx_ids.clone() {
                // NOTE: This log entry is used to compute performance.
                tracing::info!(
                    "Batch {:?} contains sample tx {}",
                    digest,
                    u64::from_be_bytes(id)
                );
            }

            #[cfg(feature = "trace_transaction")]
            {
                // The first 8 bytes of each transaction message is reserved for an identifier
                // that's useful for debugging and tracking the lifetime of messages between
                // Narwhal and clients.
                let tracking_ids: Vec<_> = batch
                    .transactions()
                    .iter()
                    .map(|tx| {
                        let len = tx.len();
                        if len >= 8 {
                            (&tx[0..8]).read_u64::<BigEndian>().unwrap_or_default()
                        } else {
                            0
                        }
                    })
                    .collect();
                tracing::debug!(
                    "Tracking IDs of transactions in the Batch {:?}: {:?}",
                    digest,
                    tracking_ids
                );
            }

            // NOTE: This log entry is used to compute performance.
            tracing::info!("Batch {:?} contains {} B with {} tx", digest, size, _tx_ids.len());
        }

        let reason = if timeout { "timeout" } else { "size_reached" };

        self.node_metrics
            .created_batch_size
            .with_label_values(&[reason])
            .observe(size as f64);

        // Send the batch through the deliver channel for further processing.
        let (notify_done, done_sending) = tokio::sync::oneshot::channel();
        if self
            .tx_quorum_waiter
            .send((batch.clone(), notify_done))
            .await
            .is_err()
        {
            tracing::debug!("{}", DagError::ShuttingDown);
            return None;
        }

        let batch_creation_duration = self.batch_start_timestamp.elapsed().as_secs_f64();

        tracing::debug!(
            "Batch {:?} took {} seconds to create due to {}",
            batch.digest(),
            batch_creation_duration,
            reason
        );

        // we are deliberately measuring this after the sending to the downstream
        // channel tx_quorum_waiter as the operation is blocking and affects any further
        // batch creation.
        self.node_metrics
            .created_batch_latency
            .with_label_values(&[reason])
            .observe(batch_creation_duration);

        // Clone things to not capture self
        let client = self.client.clone();
        let store = self.store.clone();
        let worker_id = self.id;

        // TODO: Remove once we have upgraded to protocol version 12.
        if self.protocol_config.narwhal_versioned_metadata() {
            // The batch has been sealed so we can officially set its creation time
            // for latency calculations.
            batch.versioned_metadata_mut().set_created_at(now());
            let metadata = batch.versioned_metadata().clone();

            Some(Box::pin(async move {
                // Now save it to disk
                let digest = batch.digest();

                if let Err(e) = store.insert(&digest, &batch) {
                    error!("Store failed with error: {:?}", e);
                    return;
                }

                // Also wait for sending to be done here
                //
                // TODO: Here if we get back Err it means that potentially this was not send
                //       to a quorum. However, if that happens we can still proceed on the basis
                //       that an other authority will request the batch from us, and we will deliver
                //       it since it is now stored. So ignore the error for the moment.
                let _ = done_sending.await;

                // Send the batch to the primary.
                let message = WorkerOwnBatchMessage {
                    digest,
                    worker_id,
                    metadata,
                };
                if let Err(e) = client.report_own_batch(message).await {
                    warn!("Failed to report our batch: {}", e);
                    // Drop all response handers to signal error, since we
                    // cannot ensure the primary has actually signaled the
                    // batch will eventually be sent.
                    // The transaction submitter will see the error and retry.
                    return;
                }

                // We now signal back to the transaction sender that the transaction is in a
                // batch and also the digest of the batch.
                for response in responses {
                    let _ = response.send(digest);
                }
            }))
        } else {
            // The batch has been sealed so we can officially set its creation time
            // for latency calculations.
            batch.metadata_mut().created_at = now();
            let metadata = batch.metadata().clone();

            Some(Box::pin(async move {
                // Now save it to disk
                let digest = batch.digest();

                if let Err(e) = store.insert(&digest, &batch) {
                    error!("Store failed with error: {:?}", e);
                    return;
                }

                // Also wait for sending to be done here
                //
                // TODO: Here if we get back Err it means that potentially this was not send
                //       to a quorum. However, if that happens we can still proceed on the basis
                //       that an other authority will request the batch from us, and we will deliver
                //       it since it is now stored. So ignore the error for the moment.
                let _ = done_sending.await;

                // Send the batch to the primary.
                let message = WorkerOurBatchMessage {
                    digest,
                    worker_id,
                    metadata,
                };
                if let Err(e) = client.report_our_batch(message).await {
                    warn!("Failed to report our batch: {}", e);
                    // Drop all response handers to signal error, since we
                    // cannot ensure the primary has actually signaled the
                    // batch will eventually be sent.
                    // The transaction submitter will see the error and retry.
                    return;
                }

                // We now signal back to the transaction sender that the transaction is in a
                // batch and also the digest of the batch.
                for response in responses {
                    let _ = response.send(digest);
                }
            }))
        }
    }
}
