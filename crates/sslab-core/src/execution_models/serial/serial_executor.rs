use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::mpsc::Sender;
use tracing::{warn, info, trace};

use crate::{executor::{Executable, EvmExecutionUtils}, types::{ExecutionResult, ExecutableEthereumBatch}, execution_storage::{MemoryStorage, ExecutionBackend}};


#[async_trait::async_trait]
impl Executable for SerialExecutor {
    async fn execute(&mut self, consensus_output: Vec<ExecutableEthereumBatch>, tx_execute_notification: &mut Sender<ExecutionResult>) {

        for batch in consensus_output {
            let result = self._execute(batch);
            let _ = tx_execute_notification.send(result).await;
        }
    }
}


pub struct SerialExecutor {
    global_state: Arc<RwLock<MemoryStorage>>,
}

impl SerialExecutor {
    pub fn new(global_state: Arc<RwLock<MemoryStorage>>) -> Self {
        info!("Execution mode: 'serial'");
        Self {
            global_state
        }
    }

    pub fn _execute(&mut self, batch: ExecutableEthereumBatch) -> ExecutionResult {

        let mut state = self.global_state.write();
        let snapshot = & state.snapshot();

        for tx in batch.data() {
            match EvmExecutionUtils::execute_tx(tx, snapshot) {
                Ok(Some((effect, log))) 
                    => state.apply_local_effect(effect, log),
                Ok(None) 
                    => trace!("{:?} may be reverted.", tx.id()),
                Err(e) 
                    => warn!("fail to execute a transaction {:?}", e)
            }
        }

        ExecutionResult::new(vec![batch.digest().clone()])
    }
}

