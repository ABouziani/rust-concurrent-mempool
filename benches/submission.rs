use std::time::Instant;

use rust_concurrent_mempool::transaction::Transaction;
use rust_concurrent_mempool::worker_pool::MempoolNode;

#[tokio::main]
async fn main() {
    const TOTAL: u64 = 100_000;

    let node = MempoolNode::start(4, 256, 64);
    let client = node.handle();

    let start = Instant::now();

    for id in 0..TOTAL {
        client
            .submit(Transaction::new(id, b"benchmark-payload".to_vec()))
            .await
            .expect("submission failed");
    }

    let elapsed = start.elapsed();
    let throughput = TOTAL as f64 / elapsed.as_secs_f64();

    println!("Transactions: {TOTAL}");
    println!("Elapsed: {elapsed:?}");
    println!("Throughput: {throughput:.0} submissions/sec");
    println!(
        "Final mempool size: {}",
        client.stats().await.unwrap().transaction_count
    );

    drop(client);
    node.shutdown().await;
}
