use rust_concurrent_mempool::transaction::Transaction;
use rust_concurrent_mempool::worker_pool::MempoolNode;

#[tokio::test]
async fn bounded_queues_handle_bursts_without_losing_transactions() {
    let node = MempoolNode::start(
        2, 4, // small ingress queue
        2, // small queue per worker
    );

    let client = node.handle();
    let mut tasks = Vec::new();

    for id in 0..200 {
        let client = client.clone();

        tasks.push(tokio::spawn(async move {
            client
                .submit(Transaction::new(id, b"payload".to_vec()))
                .await
        }));
    }

    for task in tasks {
        assert!(task.await.unwrap().is_ok());
    }

    assert_eq!(client.stats().await.unwrap().transaction_count, 200);

    drop(client);
    node.shutdown().await;
}
