use tokio::sync::oneshot;

use crate::transaction::Transaction;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MempoolError {
    DuplicateTransaction(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MempoolStats {
    pub transaction_count: usize,
}

pub enum MempoolCommand {
    Insert {
        transaction: Transaction,
        response: oneshot::Sender<Result<(), MempoolError>>,
    },
    Remove {
        id: u64,
        response: oneshot::Sender<bool>,
    },
    Stats {
        response: oneshot::Sender<MempoolStats>,
    },
    Shutdown {
        response: oneshot::Sender<()>,
    },
}
