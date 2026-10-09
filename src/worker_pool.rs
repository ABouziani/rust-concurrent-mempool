use std::fmt;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::mempool::actor::MempoolActor;
use crate::mempool::commands::{MempoolCommand, MempoolError, MempoolStats};
use crate::transaction::Transaction;
use crate::validation::{self, ValidationError};

enum Request {
    Submit {
        transaction: Transaction,
        response: oneshot::Sender<Result<(), SubmitError>>,
    },
    Shutdown,
}

enum WorkerMessage {
    Validate {
        transaction: Transaction,
        response: oneshot::Sender<Result<(), SubmitError>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitError {
    Validation(ValidationError),
    Mempool(MempoolError),
    ServiceClosed,
}

impl fmt::Display for SubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for SubmitError {}

#[derive(Clone)]
pub struct MempoolHandle {
    ingress_tx: mpsc::Sender<Request>,
    actor_tx: mpsc::Sender<MempoolCommand>,
}

impl MempoolHandle {
    pub async fn submit(&self, transaction: Transaction) -> Result<(), SubmitError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.ingress_tx
            .send(Request::Submit {
                transaction,
                response: response_tx,
            })
            .await
            .map_err(|_| SubmitError::ServiceClosed)?;

        response_rx.await.map_err(|_| SubmitError::ServiceClosed)?
    }

    pub async fn remove(&self, id: u64) -> Result<bool, SubmitError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.actor_tx
            .send(MempoolCommand::Remove {
                id,
                response: response_tx,
            })
            .await
            .map_err(|_| SubmitError::ServiceClosed)?;

        response_rx.await.map_err(|_| SubmitError::ServiceClosed)
    }

    pub async fn stats(&self) -> Result<MempoolStats, SubmitError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.actor_tx
            .send(MempoolCommand::Stats {
                response: response_tx,
            })
            .await
            .map_err(|_| SubmitError::ServiceClosed)?;

        response_rx.await.map_err(|_| SubmitError::ServiceClosed)
    }
}

pub struct MempoolNode {
    handle: MempoolHandle,
    dispatcher: JoinHandle<()>,
    workers: Vec<JoinHandle<()>>,
    actor: JoinHandle<()>,
}

impl MempoolNode {
    pub fn start(worker_count: usize, ingress_capacity: usize, worker_capacity: usize) -> Self {
        assert!(worker_count > 0, "worker_count must be positive");
        assert!(ingress_capacity > 0, "ingress_capacity must be positive");
        assert!(worker_capacity > 0, "worker_capacity must be positive");

        let (actor_tx, actor_rx) = mpsc::channel(64);

        let actor = tokio::spawn(MempoolActor::new().run(actor_rx));

        let (ingress_tx, ingress_rx) = mpsc::channel(ingress_capacity);

        let mut worker_senders = Vec::with_capacity(worker_count);
        let mut workers = Vec::with_capacity(worker_count);

        for _ in 0..worker_count {
            let (worker_tx, worker_rx) = mpsc::channel(worker_capacity);

            worker_senders.push(worker_tx);

            let actor_tx = actor_tx.clone();

            workers.push(tokio::spawn(async move {
                worker_loop(worker_rx, actor_tx).await;
            }));
        }

        let dispatcher = tokio::spawn(async move {
            dispatch_loop(ingress_rx, worker_senders).await;
        });

        let handle = MempoolHandle {
            ingress_tx,
            actor_tx,
        };

        Self {
            handle,
            dispatcher,
            workers,
            actor,
        }
    }

    pub fn handle(&self) -> MempoolHandle {
        self.handle.clone()
    }

    pub async fn shutdown(self) {
        let Self {
            handle,
            dispatcher,
            workers,
            actor,
        } = self;

        // Stop accepting new requests and let the dispatcher finish
        // requests already ahead of the shutdown message.
        let _ = handle.ingress_tx.send(Request::Shutdown).await;

        let _ = dispatcher.await;

        // Workers drain their queues after the dispatcher drops
        // their senders.
        for worker in workers {
            let _ = worker.await;
        }

        // All worker senders have now been dropped.
        let (response_tx, response_rx) = oneshot::channel();

        if handle
            .actor_tx
            .send(MempoolCommand::Shutdown {
                response: response_tx,
            })
            .await
            .is_ok()
        {
            let _ = response_rx.await;
        }

        let _ = actor.await;
    }
}

async fn dispatch_loop(
    mut ingress_rx: mpsc::Receiver<Request>,
    workers: Vec<mpsc::Sender<WorkerMessage>>,
) {
    let mut next_worker = 0;

    while let Some(request) = ingress_rx.recv().await {
        match request {
            Request::Submit {
                transaction,
                response,
            } => {
                let worker_index = next_worker;
                next_worker = (next_worker + 1) % workers.len();

                if let Err(error) = workers[worker_index]
                    .send(WorkerMessage::Validate {
                        transaction,
                        response,
                    })
                    .await
                {
                    if let WorkerMessage::Validate { response, .. } = error.0 {
                        let _ = response.send(Err(SubmitError::ServiceClosed));
                    }
                }
            }

            Request::Shutdown => break,
        }
    }
}

async fn worker_loop(
    mut rx: mpsc::Receiver<WorkerMessage>,
    actor_tx: mpsc::Sender<MempoolCommand>,
) {
    while let Some(message) = rx.recv().await {
        match message {
            WorkerMessage::Validate {
                transaction,
                response,
            } => {
                let result = match validation::validate(&transaction) {
                    Err(error) => Err(SubmitError::Validation(error)),

                    Ok(()) => {
                        let (actor_response_tx, actor_response_rx) = oneshot::channel();

                        let command = MempoolCommand::Insert {
                            transaction,
                            response: actor_response_tx,
                        };

                        match actor_tx.send(command).await {
                            Ok(()) => match actor_response_rx.await {
                                Ok(Ok(())) => Ok(()),
                                Ok(Err(error)) => Err(SubmitError::Mempool(error)),
                                Err(_) => Err(SubmitError::ServiceClosed),
                            },
                            Err(_) => Err(SubmitError::ServiceClosed),
                        }
                    }
                };

                let _ = response.send(result);
            }
        }
    }
}
