use core::panic;

use ethers_core::types::H256;
use evm::{
    backend::{Apply, Log},
    executor::stack::RwSet,
};

use narwhal_types::BatchDigest;
use sslab_execution::types::{EthereumTransaction, IndexedEthereumTransaction};

use crate::address_based_conflict_graph::Transaction;

// SimulcationResult includes the batch digests and rw sets of each transctions in a ConsensusOutput.
#[derive(Clone, Debug, Default)]
pub struct SimulationResult {
    pub digests: Vec<BatchDigest>,
    pub rw_sets: Vec<SimulatedTransaction>,
}

#[derive(Clone, Debug, Default)]
pub struct SimulatedTransaction {
    tx_id: u64,
    read_set: hashbrown::HashSet<H256>,
    write_set: hashbrown::HashSet<H256>,
    rw_set: RwSet,
    effects: Vec<Apply>,
    logs: Vec<Log>,
    raw_tx: IndexedEthereumTransaction,
}

impl SimulatedTransaction {
    pub fn new(
        rw_set: RwSet,
        effects: Vec<Apply>,
        logs: Vec<Log>,
        raw_tx: IndexedEthereumTransaction,
    ) -> Self {
        /* mitigation for the across-contract calls: hash(contract addr + key) */
        // let mut hasher = Sha256::new();
        // hasher.update(address.as_bytes());
        // hasher.update(key.as_bytes());
        // let key = H256::from_slice(hasher.finalize().as_ref())
        let read_set = extract_read_set(&rw_set);
        let write_set = extract_write_set(&rw_set);

        Self {
            tx_id: raw_tx.id,
            read_set,
            write_set,
            rw_set,
            effects,
            logs,
            raw_tx,
        }
    }

    #[inline]
    pub fn id(&self) -> u64 {
        self.tx_id
    }

    #[inline]
    pub fn deconstruct(self) -> (u64, RwSet, Vec<Apply>, Vec<Log>, IndexedEthereumTransaction) {
        (
            self.raw_tx.id,
            self.rw_set,
            self.effects,
            self.logs,
            self.raw_tx,
        )
    }

    #[inline]
    pub fn write_set(&self) -> &hashbrown::HashSet<H256> {
        &self.write_set
    }

    #[inline]
    pub fn read_set(&self) -> &hashbrown::HashSet<H256> {
        &self.read_set
    }

    #[inline]
    pub fn raw_tx(&self) -> &IndexedEthereumTransaction {
        &self.raw_tx
    }
}

#[derive(Clone, Debug)]
pub struct AbortedTransaction {
    raw_tx: IndexedEthereumTransaction,
    prev_write_keys: hashbrown::HashSet<H256>,
    prev_read_keys: hashbrown::HashSet<H256>,
}

impl AbortedTransaction {
    #[inline]
    pub(crate) fn id(&self) -> u64 {
        self.raw_tx.id
    }

    #[inline]
    pub(crate) fn write_keys(&self) -> &hashbrown::HashSet<H256> {
        &self.prev_write_keys
    }

    #[inline]
    pub(crate) fn read_keys(&self) -> &hashbrown::HashSet<H256> {
        &self.prev_read_keys
    }

    #[inline]
    pub(crate) fn raw_tx(&self) -> &IndexedEthereumTransaction {
        &self.raw_tx
    }
}

// #[derive(Clone, Debug)]
// pub struct AbortedTransaction {
//     optimistic_info: OptimisticInfo,
//     raw_tx: IndexedEthereumTransaction,
// }

impl From<std::sync::Arc<Transaction>> for AbortedTransaction {
    fn from(value: std::sync::Arc<Transaction>) -> Self {
        let Transaction {
            raw_tx, abort_info, ..
        } = _unwrap_arc(value);
        let ainfo = abort_info.read();
        let prev_write_keys = ainfo.write_keys();
        let prev_read_keys = ainfo.read_keys();

        Self {
            raw_tx,
            prev_write_keys,
            prev_read_keys,
        }
    }
}

#[inline]
fn _unwrap_arc<T>(data: std::sync::Arc<T>) -> T {
    match std::sync::Arc::into_inner(data) {
        Some(inner) => inner,
        None => {
            panic!("fail to unwrap Arc! please check if the data has only one strong reference!")
        }
    }
}

#[derive(Debug)]
pub struct ScheduledTransaction {
    pub seq: u32,
    pub tx_id: u64,
    pub effect: Vec<Apply>,
    pub log: Vec<Log>,
}
impl Ord for ScheduledTransaction {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.seq > other.seq {
            std::cmp::Ordering::Greater
        } else if self.seq < other.seq {
            std::cmp::Ordering::Less
        } else {
            if self.tx_id < other.tx_id {
                std::cmp::Ordering::Less
            } else if self.tx_id > other.tx_id {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        }
    }
}
impl PartialOrd for ScheduledTransaction {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for ScheduledTransaction {
    fn eq(&self, other: &Self) -> bool {
        self.tx_id == other.tx_id
    }
}
impl Eq for ScheduledTransaction {}

impl ScheduledTransaction {
    #[inline]
    pub fn seq(&self) -> u32 {
        self.seq
    }

    #[inline]
    pub fn extract(&self) -> Vec<Apply> {
        self.effect.clone()
    }

    #[inline]
    #[allow(dead_code)] // this function is used in unit tests.
    pub(crate) fn id(&self) -> u64 {
        self.tx_id
    }
}

impl From<std::sync::Arc<Transaction>> for ScheduledTransaction {
    fn from(tx: std::sync::Arc<Transaction>) -> Self {
        let (tx_id, seq, effect, log) = _unwrap_arc(tx).deconstruct();

        Self {
            seq,
            tx_id,
            effect,
            log,
        }
    }
}

impl From<Transaction> for ScheduledTransaction {
    fn from(tx: Transaction) -> Self {
        let (tx_id, seq, effect, log) = tx.deconstruct();

        Self {
            seq,
            tx_id,
            effect,
            log,
        }
    }
}

#[derive(Debug)]
pub struct ReExecutedTransaction {
    tx: IndexedEthereumTransaction,
    effect: Vec<Apply>,
    log: Vec<Log>,
    rw_set: RwSet,
}

impl ReExecutedTransaction {
    #[inline]
    pub fn build_from(
        tx: IndexedEthereumTransaction,
        effect: Vec<Apply>,
        log: Vec<Log>,
        rw_set: RwSet,
    ) -> Self {
        Self {
            tx,
            effect,
            log,
            rw_set,
        }
    }

    #[inline]
    pub fn write_set(&self) -> hashbrown::HashSet<H256> {
        extract_write_set(&self.rw_set)
    }

    #[inline]
    pub fn raw_tx(&self) -> &EthereumTransaction {
        &self.tx.tx
    }
}

pub struct FinalizedTransaction {
    id: u64,
    effect: Vec<Apply>,
    // log: Vec<Log>,
}

impl FinalizedTransaction {
    #[inline]
    pub fn extract(self) -> Vec<Apply> {
        self.effect
    }

    #[inline]
    pub fn id(&self) -> u64 {
        self.id
    }
}

impl From<ReExecutedTransaction> for FinalizedTransaction {
    fn from(value: ReExecutedTransaction) -> Self {
        let ReExecutedTransaction {
            effect,
            log: _log,
            tx,
            ..
        } = value;
        Self { effect, id: tx.id }
    }
}

impl From<ScheduledTransaction> for FinalizedTransaction {
    fn from(value: ScheduledTransaction) -> Self {
        let ScheduledTransaction {
            effect: effects,
            log: _log,
            tx_id,
            ..
        } = value;
        Self {
            effect: effects,
            id: tx_id,
        }
    }
}

#[inline]
fn extract_read_set(rw_set: &RwSet) -> hashbrown::HashSet<H256> {
    rw_set
        .reads()
        .into_iter()
        .flat_map(|(_, state)| state.keys().cloned())
        .collect()
}

#[inline]
fn extract_write_set(rw_set: &RwSet) -> hashbrown::HashSet<H256> {
    rw_set
        .writes()
        .into_iter()
        .flat_map(|(_, state)| state.keys().cloned())
        .collect()
}

#[inline]
pub(crate) fn is_disjoint<K>(left: &hashbrown::HashSet<K>, right: &hashbrown::HashSet<K>) -> bool
where
    K: std::cmp::Eq,
    K: std::hash::Hash,
{
    (left.len() <= right.len() && left.is_disjoint(right))
        || (left.len() > right.len() && right.is_disjoint(left))
}
