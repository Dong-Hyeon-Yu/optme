use ethers_core::types::H256;
use itertools::Itertools;
use narwhal_types::BatchDigest;
use rayon::prelude::*;
use sslab_execution::{
    evm_storage::{backend::ExecutionBackend, ConcurrentEVMStorage},
    executor::Executable,
    types::{ExecutableEthereumBatch, ExecutionResult, IndexedEthereumTransaction},
};
use std::sync::Arc;
use tracing::warn;

use crate::{
    address_based_conflict_graph::FastHashMap,
    types::{
        is_disjoint, AbortedTransaction, FinalizedTransaction, ReExecutedTransaction,
        ScheduledTransaction,
    },
    AddressBasedConflictGraph, SimulationResult,
};

use super::{address_based_conflict_graph::Transaction, types::SimulatedTransaction};

#[async_trait::async_trait]
impl Executable for OptME {
    async fn execute(&self, consensus_output: Vec<ExecutableEthereumBatch>) {
        let _ = self.inner.prepare_execution(consensus_output).await;
    }
}

pub struct OptME {
    inner: ConcurrencyLevelManager,
}

impl OptME {
    pub fn new(global_state: ConcurrentEVMStorage, concurrency_level: usize) -> Self {
        Self {
            inner: ConcurrencyLevelManager::new(global_state, concurrency_level),
        }
    }
}

pub struct ConcurrencyLevelManager {
    concurrency_level: usize,
    global_state: Arc<ConcurrentEVMStorage>,
}

impl ConcurrencyLevelManager {
    pub fn new(global_state: ConcurrentEVMStorage, concurrency_level: usize) -> Self {
        Self {
            global_state: Arc::new(global_state),
            concurrency_level,
        }
    }

    async fn prepare_execution(
        &self,
        consensus_output: Vec<ExecutableEthereumBatch>,
    ) -> ExecutionResult {
        let mut result = vec![];
        let mut target = consensus_output;

        while !target.is_empty() {
            let split_idx = std::cmp::min(self.concurrency_level, target.len());
            let remains: Vec<ExecutableEthereumBatch> = target.split_off(split_idx);

            result.extend(self._execute(target).await);

            target = remains;
        }

        ExecutionResult::new(result)
    }

    async fn _unpack_batches(
        consensus_output: Vec<ExecutableEthereumBatch>,
    ) -> (Vec<BatchDigest>, Vec<IndexedEthereumTransaction>) {
        let (send, recv) = tokio::sync::oneshot::channel();

        rayon::spawn(move || {
            let (digests, batches): (Vec<_>, Vec<_>) = consensus_output
                .par_iter()
                .map(|batch| (batch.digest().to_owned(), batch.data().to_owned()))
                .unzip();

            let tx_list = batches
                .into_iter()
                .flatten()
                .enumerate()
                .map(|(id, tx)| IndexedEthereumTransaction::new(tx, id as u64))
                .collect::<Vec<_>>();

            let _ = send.send((digests, tx_list)).unwrap();
        });

        recv.await.unwrap()
    }

    pub async fn _execute(
        &self,
        consensus_output: Vec<ExecutableEthereumBatch>,
    ) -> Vec<BatchDigest> {
        let (digests, tx_list) = Self::_unpack_batches(consensus_output).await;

        let scheduled_aborted_txs: Vec<Vec<AbortedTransaction>>;

        // 1st execution
        {
            let rw_sets = self._simulate(tx_list).await;

            let ScheduledInfo {
                scheduled_txs,
                aborted_txs,
            } = AddressBasedConflictGraph::par_construct(rw_sets)
                .await
                .hierarchcial_sort()
                .reorder()
                .par_extract_schedule()
                .await;

            self._concurrent_commit(scheduled_txs).await;

            scheduled_aborted_txs = aborted_txs;
        }

        for tx_list_to_re_execute in scheduled_aborted_txs.into_iter() {
            // 2nd execution
            //  (1) re-simulation  ----------------> (rw-sets are changed ??)  -------yes-------> (2') invalidate (or, fallback)
            //                                                 |
            //                                                no
            //                                                 |
            //                                          (2) commit

            let rw_sets = self
                ._re_execute(
                    tx_list_to_re_execute
                        .into_iter()
                        .map(|tx| tx.into_raw_tx())
                        .collect(),
                )
                .await;

            match self._validate_optimistic_assumption(rw_sets).await {
                None => {}
                Some(invalid_txs) => {
                    //* invalidate */
                    tracing::debug!("invalidated txs: {:?}", invalid_txs);

                    //* fallback */
                    // let ScheduledInfo {scheduled_txs, aborted_txs } = AddressBasedConflictGraph::par_construct(rw_sets).await
                    //     .hierarchcial_sort()
                    //     .reorder()
                    //     .par_extract_schedule().await;

                    // self._concurrent_commit(scheduled_txs).await;

                    //* 3rd execution (serial) for complex transactions */
                    // let snapshot = self.global_state.clone();
                    // tokio::task::spawn_blocking(move || {
                    //     aborted_txs.into_iter()
                    //         .flatten()
                    //         .for_each(|tx| {
                    //             match evm_utils::simulate_tx(tx.raw_tx(), snapshot.as_ref()) {
                    //                 Ok(Some((effect, _, _))) => {
                    //                     snapshot.apply_local_effect(effect);
                    //                 },
                    //                 _ => {
                    //                     warn!("fail to execute a transaction {}", tx.id());
                    //                 }
                    //             }
                    //         });
                    // }).await.expect("fail to spawn a task for serial execution of aborted txs");
                }
            }
        }

        digests
    }

    pub async fn simulate(
        &self,
        consensus_output: Vec<ExecutableEthereumBatch>,
    ) -> SimulationResult {
        let (digests, tx_list) = Self::_unpack_batches(consensus_output).await;
        let rw_sets = self._simulate(tx_list).await;

        SimulationResult { digests, rw_sets }
    }

    async fn _simulate(
        &self,
        tx_list: Vec<IndexedEthereumTransaction>,
    ) -> Vec<SimulatedTransaction> {
        let snapshot = self.global_state.clone();

        // Parallel simulation requires heavy cpu usages.
        // CPU-bound jobs would make the I/O-bound tokio threads starve.
        // To this end, a separated thread pool need to be used for cpu-bound jobs.
        // a new thread is created, and a new thread pool is created on the thread. (specifically, rayon's thread pool is created)
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let result = tx_list
                .into_par_iter()
                .filter_map(|tx| {
                    match crate::evm_utils::simulate_tx(tx.data(), snapshot.as_ref()) {
                        Ok(Some((effect, log, rw_set))) => {
                            Some(SimulatedTransaction::new(rw_set, effect, log, tx))
                        }
                        _ => {
                            warn!("fail to execute a transaction {}", tx.digest_u64());
                            None
                        }
                    }
                })
                .collect();

            let _ = send.send(result).unwrap();
        });

        match recv.await {
            Ok(rw_sets) => rw_sets,
            Err(e) => {
                panic!(
                    "fail to receive simulation result from the worker thread. {:?}",
                    e
                );
            }
        }
    }

    async fn _re_execute(
        &self,
        tx_list: Vec<IndexedEthereumTransaction>,
    ) -> Vec<ReExecutedTransaction> {
        let snapshot = self.global_state.clone();

        // Parallel simulation requires heavy cpu usages.
        // CPU-bound jobs would make the I/O-bound tokio threads starve.
        // To this end, a separated thread pool need to be used for cpu-bound jobs.
        // a new thread is created, and a new thread pool is created on the thread. (specifically, rayon's thread pool is created)
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let result = tx_list
                .into_par_iter()
                .filter_map(|tx| {
                    match crate::evm_utils::simulate_tx(tx.data(), snapshot.as_ref()) {
                        Ok(Some((effect, log, rw_set))) => {
                            Some(ReExecutedTransaction::build_from(tx, effect, log, rw_set))
                        }
                        _ => {
                            warn!("fail to execute a transaction {}", tx.digest_u64());
                            None
                        }
                    }
                })
                .collect();

            let _ = send.send(result).unwrap();
        });

        match recv.await {
            Ok(rw_sets) => rw_sets,
            Err(e) => {
                panic!(
                    "fail to receive simulation result from the worker thread. {:?}",
                    e
                );
            }
        }
    }

    //TODO: (optimization) commit the last write of each key
    #[cfg(not(feature = "latency"))]
    pub async fn _concurrent_commit(&self, scheduled_txs: Vec<Vec<FinalizedTransaction>>) {
        let storage = self.global_state.clone();

        // Parallel simulation requires heavy cpu usages.
        // CPU-bound jobs would make the I/O-bound tokio threads starve.
        // To this end, a separated thread pool need to be used for cpu-bound jobs.
        // a new thread is created, and a new thread pool is created on the thread. (specifically, rayon's thread pool is created)
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let _storage = &storage;
            for txs_to_commit in scheduled_txs {
                txs_to_commit.into_par_iter().for_each(|tx| {
                    let effect = tx.extract();
                    _storage.apply_local_effect(effect)
                })
            }
            let _ = send.send(());
        });

        let _ = recv.await;
    }

    #[cfg(feature = "latency")]
    pub async fn _concurrent_commit(&self, scheduled_txs: Vec<Vec<FinalizedTransaction>>) -> u128 {
        let storage = self.global_state.clone();

        // Parallel simulation requires heavy cpu usages.
        // CPU-bound jobs would make the I/O-bound tokio threads starve.
        // To this end, a separated thread pool need to be used for cpu-bound jobs.
        // a new thread is created, and a new thread pool is created on the thread. (specifically, rayon's thread pool is created)
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let _storage = &storage;

            let mut latency = 0u128;
            let clock = std::time::Instant::now();
            for txs_to_commit in scheduled_txs {
                let tx_len = txs_to_commit.len() as u128;
                txs_to_commit.into_par_iter().for_each(|tx| {
                    let effect = tx.extract();
                    _storage.apply_local_effect(effect)
                });
                latency += tx_len * clock.elapsed().as_micros();
            }
            let _ = send.send(latency);
        });

        recv.await.unwrap()
    }

    async fn _validate_optimistic_assumption(
        &self,
        rw_set: Vec<ReExecutedTransaction>,
    ) -> Option<Vec<ReExecutedTransaction>> {
        if rw_set.len() == 1 {
            self._concurrent_commit_2(rw_set).await;
            return None;
        }

        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let mut valid_txs = vec![];
            let mut invalid_txs = vec![];

            let mut write_set = hashbrown::HashSet::<H256>::new();
            for tx in rw_set.into_iter() {
                let set = tx.write_set();

                if is_disjoint(&set, &write_set) {
                    write_set.extend(set);
                    valid_txs.push(tx);
                } else {
                    invalid_txs.push(tx);
                }
            }

            if invalid_txs.is_empty() {
                let _ = send.send((valid_txs, None));
            } else {
                let _ = send.send((valid_txs, Some(invalid_txs)));
            }
        });

        let (valid_txs, invalid_txs) = recv.await.unwrap();

        self._concurrent_commit_2(valid_txs).await;

        invalid_txs
    }

    pub async fn _concurrent_commit_2(&self, scheduled_txs: Vec<ReExecutedTransaction>) {
        let scheduled_txs = vec![scheduled_txs //TODO: compare to into_par_iter()
            .into_iter()
            .map(FinalizedTransaction::from)
            .collect_vec()];

        self._concurrent_commit(scheduled_txs).await;
    }
}
// #[cfg(feature = "latency")]
use tokio::time::Instant;

// #[cfg(feature = "latency")]
#[async_trait::async_trait]
pub trait LatencyBenchmark {
    async fn _execute_and_return_latency(
        &self,
        consensus_output: Vec<ExecutableEthereumBatch>,
    ) -> (u128, u128, u128, u128, u128, u128, f64);

    async fn _validate_optimistic_assumption_and_return_latency(
        &self,
        rw_set: Vec<ReExecutedTransaction>,
    ) -> (Option<Vec<ReExecutedTransaction>>, u128, u128);
}

// #[cfg(feature = "latency")]
#[async_trait::async_trait]
impl LatencyBenchmark for ConcurrencyLevelManager {
    async fn _execute_and_return_latency(
        &self,
        consensus_output: Vec<ExecutableEthereumBatch>,
    ) -> (u128, u128, u128, u128, u128, u128, f64) {
        let (_, tx_list) = Self::_unpack_batches(consensus_output).await;
        let total_tx_len = tx_list.len();

        let scheduled_aborted_txs: Vec<Vec<AbortedTransaction>>;

        let mut simulation_latency = 0;
        let mut scheduling_latency = 0;
        let mut v_val_latency = 0;
        let mut v_exec_latency = 0;
        let mut commit_latency = 0;

        let total_latency = Instant::now();
        let mut tx_latency = 0u128;
        // 1st execution
        {
            let latency = Instant::now();
            let rw_sets = self._simulate(tx_list).await;
            simulation_latency += latency.elapsed().as_micros();

            let latency = Instant::now();
            let ScheduledInfo {
                scheduled_txs,
                aborted_txs,
            } = AddressBasedConflictGraph::par_construct(rw_sets)
                .await
                .hierarchcial_sort()
                .reorder()
                .par_extract_schedule()
                .await;
            scheduling_latency += latency.elapsed().as_micros();

            let tx_len = scheduled_txs.len() as u128;
            let latency = Instant::now();
            tx_latency += total_latency.elapsed().as_micros() * tx_len
                + self._concurrent_commit(scheduled_txs).await;
            commit_latency += latency.elapsed().as_micros();

            scheduled_aborted_txs = aborted_txs;
        }

        for tx_list_to_re_execute in scheduled_aborted_txs.into_iter() {
            // 2nd execution
            //  (1) re-simulation  ----------------> (rw-sets are changed ??)  -------yes-------> (2') invalidate (or, fallback)
            //                                                 |
            //                                                no
            //                                                 |
            //                                          (2) commit
            let txss: Vec<IndexedEthereumTransaction> = tx_list_to_re_execute
                .into_par_iter()
                .map(|tx| tx.into_raw_tx())
                .collect();
            let tx_len = txss.len() as u128;

            let latency = Instant::now();
            let rw_sets = self._re_execute(txss).await;
            v_exec_latency += latency.elapsed().as_micros();

            match self
                ._validate_optimistic_assumption_and_return_latency(rw_sets)
                .await
            {
                (None, v, c) => {
                    commit_latency += c;
                    v_val_latency += v;
                }
                (Some(invalid_txs), v, c) => {
                    commit_latency += c;
                    v_val_latency += v;

                    //* invalidate */
                    tracing::debug!("invalidated txs: {:?}", invalid_txs);
                }
            }

            tx_latency += total_latency.elapsed().as_micros() * tx_len;
        }

        (
            total_latency.elapsed().as_micros(),
            simulation_latency,
            scheduling_latency,
            v_exec_latency,
            v_val_latency,
            commit_latency,
            tx_latency as f64 / total_tx_len as f64,
        )
    }

    async fn _validate_optimistic_assumption_and_return_latency(
        &self,
        rw_set: Vec<ReExecutedTransaction>,
    ) -> (Option<Vec<ReExecutedTransaction>>, u128, u128) {
        if rw_set.len() == 1 {
            let latency = Instant::now();
            self._concurrent_commit_2(rw_set).await;

            return (None, 0, latency.elapsed().as_micros());
        }

        let (send, recv) = tokio::sync::oneshot::channel();

        let latency = Instant::now();
        rayon::spawn(move || {
            let mut valid_txs = vec![];
            let mut invalid_txs = vec![];

            let mut write_set = hashbrown::HashSet::<H256>::new();
            for tx in rw_set.into_iter() {
                let set = tx.write_set();

                if is_disjoint(&set, &write_set) {
                    write_set.extend(set);
                    valid_txs.push(tx);
                } else {
                    invalid_txs.push(tx);
                }
            }

            if invalid_txs.is_empty() {
                let _ = send.send((valid_txs, None));
            } else {
                let _ = send.send((valid_txs, Some(invalid_txs)));
            }
        });

        let (valid_txs, invalid_txs) = recv.await.unwrap();
        let validation_latency = latency.elapsed().as_micros();

        let commit_latency = Instant::now();
        self._concurrent_commit_2(valid_txs).await;

        (
            invalid_txs,
            validation_latency,
            commit_latency.elapsed().as_micros(),
        )
    }
}

#[cfg(all(feature = "parallelism-analysis", feature = "disable-early-detection"))]
#[async_trait::async_trait]
pub trait Benchmark {
    /// when the 'last-committer-wins' feature is activated, this function measures the parallelism of LCW,
    /// otherwise, first-committer-wins rule is applied.
    async fn _analysis_parallelism_of_vanilla(
        &self,
        consensus_output: Vec<ExecutableEthereumBatch>,
    ) -> (f64, f64, f64, f64, f64, u32);

    async fn _analysis_parallelism_of_optme(
        &self,
        consensus_output: Vec<ExecutableEthereumBatch>,
    ) -> (f64, f64, f64, f64, f64, u32);
}
#[cfg(all(feature = "parallelism-analysis", feature = "disable-early-detection"))]
use crate::address_based_conflict_graph::Benchmark as _;
#[cfg(all(feature = "parallelism-analysis", feature = "disable-early-detection"))]
use incr_stats::incr::Stats;

#[cfg(all(feature = "parallelism-analysis", feature = "disable-early-detection"))]
#[async_trait::async_trait]
impl Benchmark for ConcurrencyLevelManager {
    async fn _analysis_parallelism_of_vanilla(
        &self,
        consensus_output: Vec<ExecutableEthereumBatch>,
    ) -> (f64, f64, f64, f64, f64, u32) {
        let (_, tx_list) = Self::_unpack_batches(consensus_output).await;
        let rw_sets = self._simulate(tx_list).await;

        let ScheduledInfo {
            scheduled_txs,
            aborted_txs: _,
        } = AddressBasedConflictGraph::construct_without_early_detection(rw_sets)
            .hierarchcial_sort()
            .reorder()
            .par_extract_schedule()
            .await;

        let mut stat = Stats::new();
        scheduled_txs.iter().for_each(|seq| {
            stat.update(seq.len() as f64).ok();
        });

        let metric = (
            stat.sum().unwrap_or_default(),
            stat.mean().unwrap_or_default(),
            stat.population_standard_deviation().unwrap_or_default(),
            stat.population_skewness().unwrap_or_default(),
            stat.max().unwrap_or_default(),
            stat.count(),
        );

        metric
    }

    async fn _analysis_parallelism_of_optme(
        &self,
        consensus_output: Vec<ExecutableEthereumBatch>,
    ) -> (f64, f64, f64, f64, f64, u32) {
        let (_, tx_list) = Self::_unpack_batches(consensus_output).await;
        let rw_sets = self._simulate(tx_list).await;

        let ScheduledInfo {
            scheduled_txs,
            aborted_txs,
        } = AddressBasedConflictGraph::par_construct(rw_sets)
            .await
            .hierarchcial_sort()
            .reorder()
            .par_extract_schedule()
            .await;

        let mut stat = Stats::new();
        scheduled_txs.iter().for_each(|seq| {
            stat.update(seq.len() as f64).ok();
        });

        aborted_txs.iter().for_each(|seq| {
            stat.update(seq.len() as f64).ok();
        });

        let metric = (
            stat.sum().unwrap_or_default(),
            stat.mean().unwrap_or_default(),
            stat.population_standard_deviation().unwrap_or_default(),
            stat.population_skewness().unwrap_or_default(),
            stat.max().unwrap_or_default(),
            stat.count(),
        );

        metric
    }
}

pub struct ScheduledInfo {
    pub scheduled_txs: Vec<Vec<FinalizedTransaction>>,
    pub aborted_txs: Vec<Vec<AbortedTransaction>>,
}

impl ScheduledInfo {
    pub fn from(
        tx_list: FastHashMap<u64, Arc<Transaction>>,
        aborted_txs: Vec<Arc<Transaction>>,
    ) -> Self {
        let aborted_txs = Self::_schedule_aborted_txs(aborted_txs, false);
        let scheduled_txs = Self::_schedule_sorted_txs(tx_list, false);

        Self {
            scheduled_txs,
            aborted_txs,
        }
    }

    pub fn par_from(
        tx_list: FastHashMap<u64, Arc<Transaction>>,
        aborted_txs: Vec<Arc<Transaction>>,
    ) -> Self {
        let aborted_txs = Self::_schedule_aborted_txs(aborted_txs, true);
        let scheduled_txs = Self::_schedule_sorted_txs(tx_list, true);

        Self {
            scheduled_txs,
            aborted_txs,
        }
    }

    fn _unwrap(tx: Arc<Transaction>) -> Transaction {
        match Arc::try_unwrap(tx) {
            Ok(tx) => tx,
            Err(tx) => {
                panic!(
                    "fail to unwrap transaction. (strong:{}, weak:{}): {:?}",
                    Arc::strong_count(&tx),
                    Arc::weak_count(&tx),
                    tx
                );
            }
        }
    }

    fn _schedule_sorted_txs(
        tx_list: FastHashMap<u64, Arc<Transaction>>,
        rayon: bool,
    ) -> Vec<Vec<FinalizedTransaction>> {
        let mut list = if rayon {
            tx_list
                .par_iter()
                .for_each(|(_, tx)| tx.clear_write_units());

            tx_list
                .into_par_iter()
                .map(|(_, tx)| ScheduledTransaction::from(tx))
                .collect::<Vec<ScheduledTransaction>>()
        } else {
            tx_list.iter().for_each(|(_, tx)| tx.clear_write_units());

            tx_list
                .into_iter()
                .map(|(_, tx)| ScheduledTransaction::from(tx))
                .collect::<Vec<ScheduledTransaction>>()
        };

        // sort groups by sequence.
        list.sort_unstable_by_key(|tx| tx.seq());
        let mut scheduled_txs = Vec::<Vec<FinalizedTransaction>>::new();
        for (_key, txns) in &list.into_iter().group_by(|tx| tx.seq()) {
            scheduled_txs.push(
                txns.into_iter()
                    .map(FinalizedTransaction::from)
                    .collect_vec(),
            );
        }

        scheduled_txs
    }

    fn _schedule_aborted_txs(
        txs: Vec<Arc<Transaction>>,
        rayon: bool,
    ) -> Vec<Vec<AbortedTransaction>> {
        let mut aborted_txs;
        if rayon {
            txs.par_iter().for_each(|tx| {
                tx.clear_write_units();
                tx.init();
            });
            aborted_txs = txs
                .into_par_iter()
                .map(AbortedTransaction::from)
                .collect::<Vec<_>>();
        } else {
            txs.iter().for_each(|tx| {
                tx.clear_write_units();
                tx.init();
            });
            aborted_txs = txs
                .into_iter()
                .map(AbortedTransaction::from)
                .collect::<Vec<_>>();
        };

        // determine minimum #epoch in which tx have no conflicts with others --> by binary-search over a map (#epoch, writeset)
        let mut epoch_map: Vec<hashbrown::HashSet<H256>> = vec![]; // (epoch, write set)

        // store final schedule information
        let mut schedule: Vec<Vec<AbortedTransaction>> = vec![];

        if cfg!(not(feature = "disable-rescheduling")) {
            aborted_txs.sort_unstable_by_key(|tx| tx.id());

            for tx in aborted_txs.iter() {
                let read_keys = tx.read_keys();
                let write_keys = tx.write_keys();

                let epoch = Self::_find_minimun_epoch_with_no_conflicts(
                    &read_keys,
                    &write_keys,
                    &epoch_map,
                );

                // update epoch_map & schedule
                match epoch_map.get_mut(epoch) {
                    Some(w_map) => {
                        w_map.extend(write_keys);
                        schedule[epoch].push(tx.clone());
                    }
                    None => {
                        epoch_map.push(write_keys.clone());
                        schedule.push(vec![tx.clone()]);
                    }
                };
            }
        }

        schedule
    }

    fn _find_minimun_epoch_with_no_conflicts(
        read_keys_of_tx: &hashbrown::HashSet<H256>,
        write_keys_of_tx: &hashbrown::HashSet<H256>,
        epoch_map: &Vec<hashbrown::HashSet<H256>>,
    ) -> usize {
        // 1) ww dependencies are occured when the keys which are both read and written by latter tx are overlapped with the rw keys of the previous txs in the same epoch.
        //   for simplicity, only single write is allowed for each key in the same epoch.

        // 2) anti-rw dependencies are occured when the read keys of latter tx are overlapped with the write keys of the previous txs in the same epoch.
        let keys_of_tx = read_keys_of_tx
            .union(write_keys_of_tx)
            .cloned()
            .collect::<hashbrown::HashSet<_>>();

        let mut epoch = 0;
        while epoch_map.len() > epoch && !keys_of_tx.is_disjoint(&epoch_map[epoch]) {
            epoch += 1;
        }

        epoch
    }

    pub fn scheduled_txs_len(&self) -> usize {
        self.scheduled_txs.iter().map(|vec| vec.len()).sum()
    }

    pub fn aborted_txs_len(&self) -> usize {
        self.aborted_txs.iter().map(|vec| vec.len()).sum()
    }

    pub fn parallism_metric(&self) -> (usize, f64, f64, usize, usize) {
        let total_tx = self.scheduled_txs_len() + self.aborted_txs_len();
        let max_width = self
            .scheduled_txs
            .iter()
            .map(|vec| vec.len())
            .max()
            .unwrap_or(0);
        let depth = self.scheduled_txs.len();
        let average_width = self
            .scheduled_txs
            .iter()
            .map(|vec| vec.len())
            .sum::<usize>() as f64
            / depth as f64;
        let var_width = self
            .scheduled_txs
            .iter()
            .map(|vec| vec.len())
            .fold(0.0, |acc, len| acc + (len as f64 - average_width).powi(2))
            / depth as f64;
        let std_width = var_width.sqrt();
        (total_tx, average_width, std_width, max_width, depth)
    }
}
