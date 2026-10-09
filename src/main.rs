use rust_concurrent_mempool::mempool::commands::MempoolStats;
use rust_concurrent_mempool::transaction::Transaction;
use rust_concurrent_mempool::worker_pool::MempoolNode;

#[tokio::main]
async fn main() {
    let node = MempoolNode::start(
        4,  // validation workers
        32, // ingress queue capacity
        16, // capacity per worker queue
    );

    let client = node.handle();

    for id in 1..=5 {
        let transaction = Transaction::new(id, format!("payload-{id}"));

        match client.submit(transaction).await {
            Ok(()) => println!("Transaction {id} accepted"),
            Err(error) => println!("Transaction {id} rejected: {error}"),
        }
    }

    let stats: MempoolStats = client.stats().await.unwrap();
    println!("Mempool size: {}", stats.transaction_count);

    let removed = client.remove(3).await.unwrap();
    println!("Transaction 3 removed: {removed}");

    println!(
        "Final mempool size: {}",
        client.stats().await.unwrap().transaction_count
    );

    drop(client);
    node.shutdown().await;
}
