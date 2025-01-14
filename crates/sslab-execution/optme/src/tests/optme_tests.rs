use std::str::FromStr;

use ethers_core::types::{H160, H256};
use evm::executor::stack::{RwSet, Simulatable};
use hashbrown::HashSet;
use itertools::Itertools;
use sslab_execution::types::{EthereumTransaction, IndexedEthereumTransaction};

use crate::{
    address_based_conflict_graph::AddressBasedConflictGraph, optme_core::ScheduledInfo,
    types::SimulatedTransaction,
};

const CONTRACT_ADDR: u64 = 0x1;

fn transaction_with_rw(tx_id: u64, read_addr: u64, write_addr: u64) -> SimulatedTransaction {
    let mut set = RwSet::new();
    set.record_read_key(
        H160::from_low_u64_be(CONTRACT_ADDR),
        H256::from_low_u64_be(read_addr),
        H256::from_low_u64_be(1),
    );
    set.record_write_key(
        H160::from_low_u64_be(CONTRACT_ADDR),
        H256::from_low_u64_be(write_addr),
        H256::from_low_u64_be(1),
    );
    SimulatedTransaction::new(
        set,
        Vec::new(),
        Vec::new(),
        IndexedEthereumTransaction::new(EthereumTransaction::default(), tx_id),
    )
}

fn transaction_with_multiple_rw(
    tx_id: u64,
    read_addr: Vec<u64>,
    write_addr: Vec<u64>,
) -> SimulatedTransaction {
    let mut set = RwSet::new();
    read_addr.iter().for_each(|addr| {
        set.record_read_key(
            H160::from_low_u64_be(CONTRACT_ADDR),
            H256::from_low_u64_be(*addr),
            H256::from_low_u64_be(1),
        );
    });
    write_addr.iter().for_each(|addr| {
        set.record_write_key(
            H160::from_low_u64_be(CONTRACT_ADDR),
            H256::from_low_u64_be(*addr),
            H256::from_low_u64_be(1),
        );
    });
    SimulatedTransaction::new(
        set,
        Vec::new(),
        Vec::new(),
        IndexedEthereumTransaction::new(EthereumTransaction::default(), tx_id),
    )
}

fn transaction_with_multiple_rw_str(
    tx_id: u64,
    read_addr: Vec<&str>,
    write_addr: Vec<&str>,
) -> SimulatedTransaction {
    let mut set = RwSet::new();
    read_addr.into_iter().for_each(|addr| {
        set.record_read_key(
            H160::from_low_u64_be(CONTRACT_ADDR),
            H256::from_str(addr).unwrap(),
            H256::from_low_u64_be(1),
        );
    });
    write_addr.into_iter().for_each(|addr| {
        set.record_write_key(
            H160::from_low_u64_be(CONTRACT_ADDR),
            H256::from_str(addr).unwrap(),
            H256::from_low_u64_be(1),
        );
    });
    SimulatedTransaction::new(
        set,
        Vec::new(),
        Vec::new(),
        IndexedEthereumTransaction::new(EthereumTransaction::default(), tx_id),
    )
}

fn optme_test(
    input_txs: Vec<SimulatedTransaction>,
    answer: (Vec<Vec<u64>>, Vec<Vec<u64>>),
    print_result: bool,
) {
    let ScheduledInfo {
        scheduled_txs,
        aborted_txs,
    } = AddressBasedConflictGraph::construct(input_txs.clone())
        .hierarchcial_sort()
        .reorder()
        .extract_schedule();

    if print_result {
        println!("Scheduled Transactions:");
        scheduled_txs.iter().for_each(|txs| {
            txs.iter().for_each(|tx| {
                print!("{} ", tx.id());
            });
            print!("\n");
        });

        println!("Aborted Transactions:");
        aborted_txs.iter().for_each(|txs| {
            txs.iter().for_each(|tx| {
                print!("{} ", tx.id());
            });
            print!("\n");
        });
    }

    let (s_ans, a_ans) = answer;

    scheduled_txs
        .iter()
        .map(|tx| tx.iter().map(|tx| tx.id()).collect_vec())
        .zip(s_ans)
        .for_each(|(txs, idx)| {
            assert_eq!(txs.len(), idx.len());
            assert_eq!(txs, idx);
        });

    aborted_txs
        .iter()
        .map(|tx| tx.iter().map(|tx| tx.id()).collect_vec())
        .zip(a_ans)
        .for_each(|(txs, idx)| {
            assert_eq!(txs.len(), idx.len());
            assert_eq!(txs, idx);
        });
}

async fn optme_par_test(
    input_txs: Vec<SimulatedTransaction>,
    answer: (Vec<Vec<u64>>, Vec<Vec<u64>>),
    print_result: bool,
) {
    let ScheduledInfo {
        scheduled_txs,
        aborted_txs,
    } = AddressBasedConflictGraph::par_construct(input_txs.clone())
        .await
        .hierarchcial_sort()
        .reorder()
        .par_extract_schedule()
        .await;

    if print_result {
        println!("Scheduled Transactions:");
        scheduled_txs.iter().for_each(|txs| {
            txs.iter().for_each(|tx| {
                print!("{} ", tx.id());
            });
            print!("\n");
        });
        println!("Aborted Transactions:");
        aborted_txs.iter().for_each(|txs| {
            txs.iter().for_each(|tx| {
                print!("{} ", tx.id());
            });
            print!("\n");
        });
    }

    let (s_ans, a_ans) = answer;

    scheduled_txs
        .iter()
        .zip(s_ans.iter())
        .for_each(|(txs, idx)| {
            // println!("output: {:#?}", txs);
            // println!("answer: {:#?}", idx);
            assert_eq!(txs.len(), idx.len());
            let answer_set: HashSet<&u64> = idx.iter().collect();
            assert!(txs.iter().all(|tx| answer_set.contains(&tx.id())))
        });

    aborted_txs.iter().zip(a_ans.iter()).for_each(|(txs, idx)| {
        assert_eq!(txs.len(), idx.len());
        let answer_set: HashSet<&u64> = idx.iter().collect();
        assert!(txs.iter().all(|tx| answer_set.contains(&tx.id())))
    });
}

#[tokio::test]
async fn test_scenario_1() {
    let txs = vec![
        transaction_with_rw(1, 2, 1),
        transaction_with_rw(2, 3, 2),
        transaction_with_rw(3, 4, 2),
        transaction_with_rw(4, 4, 3),
        transaction_with_rw(5, 4, 4),
        transaction_with_rw(6, 1, 3),
    ];

    let first_scheduled = vec![vec![2], vec![3, 4], vec![5, 6]];

    let second_scheduled = vec![vec![1]];

    optme_test(
        txs.clone(),
        (first_scheduled.clone(), second_scheduled.clone()),
        false,
    );
    optme_par_test(txs.clone(), (first_scheduled, second_scheduled), false).await;
}

#[tokio::test]
async fn test_scenario_2() {
    let txs = vec![
        transaction_with_rw(1, 2, 1),
        transaction_with_rw(3, 4, 2),
        transaction_with_rw(2, 3, 2),
        transaction_with_rw(4, 4, 3),
        transaction_with_rw(5, 4, 4),
        transaction_with_rw(6, 1, 3),
    ];

    let first_scheduled = vec![vec![3], vec![2], vec![4], vec![5, 6]];

    let second_scheduled = vec![vec![1]];

    optme_test(
        txs.clone(),
        (first_scheduled.clone(), second_scheduled.clone()),
        false,
    );
    optme_par_test(txs.clone(), (first_scheduled, second_scheduled), false).await;
}

#[tokio::test]
async fn test_scenario_3() {
    let txs = vec![
        transaction_with_rw(1, 2, 1),
        transaction_with_rw(2, 3, 2),
        transaction_with_rw(3, 4, 2),
        transaction_with_rw(6, 1, 3),
        transaction_with_rw(5, 4, 4),
        transaction_with_rw(4, 4, 3),
    ];

    let first_scheduled = vec![vec![2], vec![3, 6], vec![4], vec![5]];

    let second_scheduled = vec![vec![1]];

    optme_test(
        txs.clone(),
        (first_scheduled.clone(), second_scheduled.clone()),
        false,
    );
    optme_par_test(txs.clone(), (first_scheduled, second_scheduled), false).await;
}

#[tokio::test]
async fn test_scenario_4() {
    let txs = vec![
        transaction_with_rw(1, 2, 1),
        transaction_with_rw(2, 3, 2),
        transaction_with_rw(3, 4, 2),
        transaction_with_rw(4, 4, 4),
        transaction_with_rw(5, 4, 4),
        transaction_with_rw(6, 1, 3),
    ];

    let first_scheduled = vec![vec![1], vec![2], vec![3], vec![4]];

    let second_scheduled = vec![vec![5, 6]];

    optme_test(
        txs.clone(),
        (first_scheduled.clone(), second_scheduled.clone()),
        false,
    );
    optme_par_test(txs.clone(), (first_scheduled, second_scheduled), false).await;
}

#[tokio::test]
async fn test_scenario_5() {
    let txs = vec![
        transaction_with_rw(1, 2, 1),
        transaction_with_rw(2, 3, 2),
        transaction_with_rw(3, 4, 2),
        transaction_with_rw(4, 4, 4),
        transaction_with_rw(5, 4, 4),
        transaction_with_rw(6, 1, 3),
        transaction_with_rw(7, 4, 4),
    ];

    let first_scheduled = vec![vec![1], vec![2], vec![3], vec![4]];

    let second_scheduled = vec![vec![5, 6], vec![7]];

    optme_test(
        txs.clone(),
        (first_scheduled.clone(), second_scheduled.clone()),
        false,
    );
    optme_par_test(txs.clone(), (first_scheduled, second_scheduled), false).await;
}

#[tokio::test]
async fn test_reordering() {
    let txs = vec![
        transaction_with_multiple_rw(1, vec![], vec![1, 2]),
        transaction_with_rw(2, 2, 1),
    ];

    let first_scheduled = vec![vec![2], vec![1]];

    let second_scheduled = vec![];

    optme_test(
        txs.clone(),
        (first_scheduled.clone(), second_scheduled.clone()),
        false,
    );
    optme_par_test(txs.clone(), (first_scheduled, second_scheduled), false).await;
}

#[tokio::test]
async fn test_scenario_6() {
    let txs = vec![
        transaction_with_multiple_rw_str(
            1,
            vec![
                "0x48c8d13a49dbf1c93484ba997be20d9cae319d82960232db3544bb8bf65d4ac0",
                "0xe3ea58be4f1efa6db4e24abc274fb1bccd82dfcd49c8f508a08c911f0357c19d",
            ],
            vec![
                "0x48c8d13a49dbf1c93484ba997be20d9cae319d82960232db3544bb8bf65d4ac0",
                "0xe3ea58be4f1efa6db4e24abc274fb1bccd82dfcd49c8f508a08c911f0357c19d",
            ],
        ),
        transaction_with_multiple_rw_str(
            2,
            vec![
                "0x7b6a909101d770fd973075a9dbcef6c7ae894d77f3f89dcacb997ab3178cd44e",
                "0xb955ea50cf68e45358af8183015c9694f0e9401fee45e367d90c462108f102bd",
            ],
            vec![
                "0x7b6a909101d770fd973075a9dbcef6c7ae894d77f3f89dcacb997ab3178cd44e",
                "0xb955ea50cf68e45358af8183015c9694f0e9401fee45e367d90c462108f102bd",
            ],
        ),
        transaction_with_multiple_rw_str(
            3,
            vec![
                "0x7b6a909101d770fd973075a9dbcef6c7ae894d77f3f89dcacb997ab3178cd44e",
                "0xe3ea58be4f1efa6db4e24abc274fb1bccd82dfcd49c8f508a08c911f0357c19d",
            ],
            vec![
                "0x7b6a909101d770fd973075a9dbcef6c7ae894d77f3f89dcacb997ab3178cd44e",
                "0xe3ea58be4f1efa6db4e24abc274fb1bccd82dfcd49c8f508a08c911f0357c19d",
            ],
        ),
    ];

    let first_scheduled = vec![vec![1, 2]];

    let second_scheduled = vec![vec![3]];

    optme_test(
        txs.clone(),
        (first_scheduled.clone(), second_scheduled.clone()),
        false,
    );
    optme_par_test(txs.clone(), (first_scheduled, second_scheduled), false).await;
}
