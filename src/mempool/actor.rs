use std::collections::HashMap;

use tokio::sync::mpsc;

use crate::transaction::Transaction;

use super::commands::{MempoolCommand, MempoolError, MempoolStats};

pub struct MempoolActor {
    transactions: HashMap<u64, Transaction>,
}

impl MempoolActor {
    pub fn new() -> Self {
        Self {
            transactions: HashMap::new(),
        }
    }

    pub async fn run(mut self, mut rx: mpsc::Receiver<MempoolCommand>) {
        while let Some(command) = rx.recv().await {
            match command {
                MempoolCommand::Insert {
                    transaction,
                    response,
                } => {
                    let id = transaction.id;

                    let result = if self.transactions.contains_key(&id) {
                        Err(MempoolError::DuplicateTransaction(id))
                    } else {
                        self.transactions.insert(id, transaction);
                        Ok(())
                    };

                    let _ = response.send(result);
                }

                MempoolCommand::Remove { id, response } => {
                    let removed = self.transactions.remove(&id).is_some();
                    let _ = response.send(removed);
                }

                MempoolCommand::Stats { response } => {
                    let stats = MempoolStats {
                        transaction_count: self.transactions.len(),
                    };

                    let _ = response.send(stats);
                }

                MempoolCommand::Shutdown { response } => {
                    let _ = response.send(());
                    break;
                }
            }
        }
    }
}
