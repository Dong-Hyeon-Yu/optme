use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use ethers_providers::{MockProvider, Provider};
use parking_lot::RwLock;
use sslab_execution::{
    types::ExecutableEthereumBatch,
    utils::smallbank_contract_benchmark::concurrent_evm_storage,
    utils::test_utils::{SmallBankTransactionHandler, DEFAULT_CHAIN_ID},
};

use sslab_execution_optme::{
    address_based_conflict_graph::Benchmark as _,
    optme_core::{Benchmark, ScheduledInfo},
    AddressBasedConflictGraph, ConcurrencyLevelManager, SimulatedTransaction, SimulationResult,
};

const DEFAULT_BATCH_SIZE: usize = 200;
const DEFAULT_ACCOUNT_NUM: u64 = 100_000;

fn _get_smallbank_handler() -> SmallBankTransactionHandler {
    let provider = Provider::<MockProvider>::new(MockProvider::default());
    SmallBankTransactionHandler::new(provider, DEFAULT_CHAIN_ID)
}

fn _get_optme_executor(clevel: usize) -> ConcurrencyLevelManager {
    ConcurrencyLevelManager::new(concurrent_evm_storage(), clevel)
}

fn _create_random_smallbank_workload(
    skewness: f32,
    batch_size: usize,
    block_concurrency: usize,
    account_num: u64,
) -> Vec<ExecutableEthereumBatch> {
    let handler = _get_smallbank_handler();

    handler.create_batches(batch_size, block_concurrency, skewness, account_num)
}

fn _get_rw_sets(
    optme: std::sync::Arc<ConcurrencyLevelManager>,
    consensus_output: Vec<ExecutableEthereumBatch>,
) -> Vec<SimulatedTransaction> {
    let (tx, rx) = std::sync::mpsc::channel();
    let _ = tokio::runtime::Handle::current().spawn(async move {
        let SimulationResult { rw_sets, .. } = optme.simulate(consensus_output).await;
        tx.send(rw_sets).unwrap();
    });
    rx.recv().unwrap()
}

///*  must activate features=vanilla-kdg-fcw
fn parallelism_of_first_committer_wins_rule(c: &mut Criterion) {
    let s = [0.0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
    let param = 80..81;
    let mut group = c.benchmark_group("Vanilla");

    for i in param {
        for zipfian in s {
            let parallelism_metrics = std::sync::Arc::new(RwLock::new(Vec::new()));

            group.bench_with_input(
                criterion::BenchmarkId::new(
                    "FCW-parallelism",
                    format!("(zipfian: {}, block_concurrency: {})", zipfian, i),
                ),
                &(i, parallelism_metrics.clone()),
                |b, (i, metrics)| {
                    b.to_async(tokio::runtime::Runtime::new().unwrap())
                        .iter_batched(
                            || {
                                let consensus_output = _create_random_smallbank_workload(
                                    zipfian,
                                    DEFAULT_BATCH_SIZE,
                                    *i,
                                    DEFAULT_ACCOUNT_NUM,
                                );
                                let optme = _get_optme_executor(*i);
                                (optme, consensus_output)
                            },
                            |(optme, consensus_output)| async move {
                                metrics.write().push(
                                    optme
                                        ._analysis_parallelism_of_vanilla(consensus_output)
                                        .await,
                                );
                            },
                            BatchSize::SmallInput,
                        );
                },
            );

            let len = parallelism_metrics.read().len();

            if len == 0 {
                continue;
            }

            let (
                // mut total_tx,
                mut average_height,
                // mut std_height,
                // mut skewness_height,
                // mut max_height,
                mut depth,
            ) = (0 as f64, 0 as u32);

            for (_a1, a2, _a3, _a4, _a5, a6) in parallelism_metrics.read().iter() {
                average_height += a2;
                depth += a6;
            }
            println!(
                "average_height: {:.2}, depth: {:.2}",
                average_height / len as f64,
                depth as f64 / len as f64
            )
        }
    }
}

/// must activate features=parallelism
fn parallelism_of_optme(c: &mut Criterion) {
    let account_num = 400;
    let s = [0.0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
    let param = 1..2;
    let mut group = c.benchmark_group("OptME");

    for i in param {
        for zipfian in s {
            let parallelism_metrics = std::sync::Arc::new(RwLock::new(Vec::new()));

            group.bench_with_input(
                criterion::BenchmarkId::new(
                    "optme",
                    format!("(zipfian: {}, block_concurrency: {})", zipfian, i),
                ),
                &(i, parallelism_metrics.clone()),
                |b, (i, metrics)| {
                    b.to_async(tokio::runtime::Runtime::new().unwrap())
                        .iter_batched(
                            || {
                                let consensus_output = _create_random_smallbank_workload(
                                    zipfian,
                                    DEFAULT_BATCH_SIZE,
                                    *i,
                                    account_num,
                                );
                                let optme = _get_optme_executor(*i);
                                (optme, consensus_output)
                            },
                            |(optme, consensus_output)| async move {
                                metrics.write().push(
                                    optme._analysis_parallelism_of_optme(consensus_output).await,
                                );
                            },
                            BatchSize::SmallInput,
                        );
                },
            );

            let len = parallelism_metrics.read().len();

            if len == 0 {
                continue;
            }

            let (
                // mut total_tx,
                mut average_height,
                // mut std_height,
                // mut skewness_height,
                // mut max_height,
                mut depth,
            ) = (0 as f64, 0 as u32);

            for (_a1, a2, _a3, _a4, _a5, a6) in parallelism_metrics.read().iter() {
                average_height += a2;
                depth += a6;
            }
            println!(
                "average_height: {:.2}, depth: {:.2}",
                average_height / len as f64,
                depth as f64 / len as f64
            )
        }
    }
}

fn tps_of_first_committer_wins_rule(c: &mut Criterion) {
    let s = [0.0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
    let param = 80..81;
    let mut group = c.benchmark_group("Vanilla");

    for i in param {
        for zipfian in s {
            let throughput_metrics = std::sync::Arc::new(RwLock::new(Vec::new()));

            group.bench_with_input(
                criterion::BenchmarkId::new(
                    "FCW-tps",
                    format!("(zipfian: {}, block_size: {})", zipfian, i),
                ),
                &(i, throughput_metrics.clone()),
                |b, (i, metrics)| {
                    b.to_async(tokio::runtime::Runtime::new().unwrap())
                        .iter_batched(
                            || {
                                let consensus_output = _create_random_smallbank_workload(
                                    zipfian,
                                    DEFAULT_BATCH_SIZE,
                                    *i,
                                    DEFAULT_ACCOUNT_NUM,
                                );
                                let optme = std::sync::Arc::new(_get_optme_executor(*i));
                                (optme, consensus_output)
                            },
                            |(optme, consensus_output)| async move {
                                let now = tokio::time::Instant::now();
                                let result = optme.simulate(consensus_output).await;
                                let ScheduledInfo {
                                    scheduled_txs,
                                    aborted_txs: _,
                                } = AddressBasedConflictGraph::construct_without_early_detection(
                                    result.rw_sets,
                                )
                                .hierarchcial_sort()
                                .reorder()
                                .par_extract_schedule()
                                .await;
                                let commit_len =
                                    scheduled_txs.iter().map(|txs| txs.len()).sum::<usize>() as f64;
                                let c_latency = tokio::time::Instant::now();
                                optme._concurrent_commit(scheduled_txs).await;
                                let c_latency = c_latency.elapsed().as_micros() as f64;
                                let latency = now.elapsed().as_micros() as f64;

                                let expected_num_of_trials =
                                    DEFAULT_BATCH_SIZE as f64 * *i as f64 / commit_len;
                                let ktps = commit_len / (latency * expected_num_of_trials);
                                metrics.write().push((ktps, c_latency));
                            },
                            BatchSize::SmallInput,
                        );
                },
            );

            let (mut ktps, mut c_latency) = (0 as f64, 0 as f64);
            if throughput_metrics.read().is_empty() {
                continue;
            }
            let len = throughput_metrics.read().len() as f64;

            for (a1, c1) in throughput_metrics.read().iter() {
                ktps += a1;
                c_latency += c1;
            }

            println!("Ktps: {:.4}", (ktps / len) * 1000f64);
            println!("commit latency: {:.4} ms", (c_latency / len) / 1000f64)
        }
    }
}

criterion_group!(
    benches,
    parallelism_of_optme,
    parallelism_of_first_committer_wins_rule,
    tps_of_first_committer_wins_rule
);
criterion_main!(benches);
