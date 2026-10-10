use std::sync::Arc;

use rust_concurrent_mempool::transaction::Transaction;
use rust_concurrent_mempool::worker_pool::MempoolNode;

#[tokio::test]
async fn concurrent_submissions_are_all_accounted_for() {
    let node = MempoolNode::start(4, 16, 16);
    let client = node.handle();

    let mut tasks = Vec::new();

    for id in 0..500 {
        let client = client.clone();

        tasks.push(tokio::spawn(async move {
            client
                .submit(Transaction::new(id, format!("tx-{id}")))
                .await
        }));
    }

    for task in tasks {
        assert!(task.await.unwrap().is_ok());
    }

    assert_eq!(client.stats().await.unwrap().transaction_count, 500);

    drop(client);
    node.shutdown().await;
}

#[tokio::test]
async fn concurrent_duplicate_ids_only_insert_once() {
    let node = MempoolNode::start(4, 32, 16);
    let client = Arc::new(node.handle());

    let mut tasks = Vec::new();

    for _ in 0..20 {
        let client = Arc::clone(&client);

        tasks.push(tokio::spawn(async move {
            client
                .submit(Transaction::new(42, b"payload".to_vec()))
                .await
        }));
    }

    let mut accepted = 0;

    for task in tasks {
        if task.await.unwrap().is_ok() {
            accepted += 1;
        }
    }

    assert_eq!(accepted, 1);
    assert_eq!(client.stats().await.unwrap().transaction_count, 1);

    drop(client);
    node.shutdown().await;
}
