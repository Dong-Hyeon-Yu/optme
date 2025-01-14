use criterion::Throughput;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use ethers_providers::{MockProvider, Provider};
use sslab_execution::{
    types::ExecutableEthereumBatch,
    utils::smallbank_contract_benchmark::concurrent_evm_storage,
    utils::test_utils::{SmallBankTransactionHandler, DEFAULT_CHAIN_ID},
};

use sslab_execution_optme::{ConcurrencyLevelManager, SimulatedTransaction, SimulationResult};

const DEFAULT_BATCH_SIZE: usize = 200;

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
) -> Vec<ExecutableEthereumBatch> {
    let handler = _get_smallbank_handler();

    handler.create_batches(batch_size, block_concurrency, skewness, 100_000)
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

fn optme(c: &mut Criterion) {
    let s = [0.0];
    let param = 1..81;
    let mut group = c.benchmark_group("OptME");

    for skewness in s {
        for i in param.clone() {
            group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE * i) as u64));
            group.bench_with_input(
                criterion::BenchmarkId::new(
                    "blocksize",
                    format!("(zipfian: {}, block_concurrency: {})", skewness, i),
                ),
                &i,
                |b, i| {
                    b.to_async(tokio::runtime::Runtime::new().unwrap())
                        .iter_batched(
                            || {
                                let consensus_output = _create_random_smallbank_workload(
                                    skewness,
                                    DEFAULT_BATCH_SIZE,
                                    *i,
                                );
                                let optme = _get_optme_executor(*i);
                                (optme, consensus_output)
                            },
                            |(optme, consensus_output)| async move {
                                optme._execute(consensus_output).await
                            },
                            BatchSize::SmallInput,
                        );
                },
            );
        }
    }
}

fn optme_skewness(c: &mut Criterion) {
    let s = [0.0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
    let param = 80..81;
    let mut group = c.benchmark_group("OptME");

    for skewness in s {
        for i in param.clone() {
            group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE * i) as u64));
            group.bench_with_input(
                criterion::BenchmarkId::new(
                    "skewness",
                    format!("(zipfian: {}, block_concurrency: {})", skewness, i),
                ),
                &i,
                |b, i| {
                    b.to_async(tokio::runtime::Runtime::new().unwrap())
                        .iter_batched(
                            || {
                                let consensus_output = _create_random_smallbank_workload(
                                    skewness,
                                    DEFAULT_BATCH_SIZE,
                                    *i,
                                );
                                let optme = _get_optme_executor(*i);
                                (optme, consensus_output)
                            },
                            |(optme, consensus_output)| async move {
                                optme._execute(consensus_output).await
                            },
                            BatchSize::SmallInput,
                        );
                },
            );
        }
    }
}

criterion_group!(benches, optme, optme_skewness);
criterion_main!(benches);
