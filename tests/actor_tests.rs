use rust_concurrent_mempool::transaction::Transaction;
use rust_concurrent_mempool::validation::ValidationError;
use rust_concurrent_mempool::worker_pool::{MempoolNode, SubmitError};

#[tokio::test]
async fn accepts_valid_transaction() {
    let node = MempoolNode::start(2, 8, 8);
    let client = node.handle();

    assert!(client
        .submit(Transaction::new(1, b"valid".to_vec()))
        .await
        .is_ok());

    assert_eq!(client.stats().await.unwrap().transaction_count, 1);

    drop(client);
    node.shutdown().await;
}

#[tokio::test]
async fn rejects_duplicate_transaction_id() {
    let node = MempoolNode::start(2, 8, 8);
    let client = node.handle();

    client
        .submit(Transaction::new(1, b"first".to_vec()))
        .await
        .unwrap();

    let result = client.submit(Transaction::new(1, b"second".to_vec())).await;

    assert_eq!(
        result,
        Err(SubmitError::Mempool(
            rust_concurrent_mempool::mempool::commands::MempoolError::DuplicateTransaction(1)
        ))
    );

    assert_eq!(client.stats().await.unwrap().transaction_count, 1);

    drop(client);
    node.shutdown().await;
}

#[tokio::test]
async fn rejects_empty_payload() {
    let node = MempoolNode::start(1, 8, 8);
    let client = node.handle();

    let result = client.submit(Transaction::new(1, Vec::new())).await;

    assert_eq!(
        result,
        Err(SubmitError::Validation(ValidationError::EmptyPayload))
    );

    assert_eq!(client.stats().await.unwrap().transaction_count, 0);

    drop(client);
    node.shutdown().await;
}

#[tokio::test]
async fn removes_transaction() {
    let node = MempoolNode::start(1, 8, 8);
    let client = node.handle();

    client
        .submit(Transaction::new(7, b"payload".to_vec()))
        .await
        .unwrap();

    assert!(client.remove(7).await.unwrap());
    assert!(!client.remove(7).await.unwrap());
    assert_eq!(client.stats().await.unwrap().transaction_count, 0);

    drop(client);
    node.shutdown().await;
}
