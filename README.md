# Rust Concurrent Mempool

A concurrent transaction mempool built in Rust to explore asynchronous systems design, message passing, bounded queues, and safe state management using Tokio.

The project demonstrates how a transaction-processing component can coordinate multiple producers and validation workers while maintaining a single owner for mutable mempool state.

## Overview

A mempool temporarily stores transactions before they are selected for further processing. In a concurrent system, transactions may arrive from many clients simultaneously, making synchronization, backpressure, and correctness important design concerns.

This project implements a simplified transaction-ingestion pipeline using:

- The **Actor Model** for exclusive ownership of mutable mempool state.
- Tokio `mpsc` channels for asynchronous message passing.
- Tokio `oneshot` channels for request-response communication.
- Bounded queues to apply backpressure.
- Multiple validation workers to process independent submissions concurrently.
- A `HashMap` for transaction storage and duplicate detection.
- Integration tests for concurrent submissions, validation, and queue behavior.

The focus is on concurrency architecture and correctness rather than implementing a complete blockchain protocol.

## Architecture

```text
                    Transaction Clients
                            |
                            v
                  Bounded Ingress Queue
                            |
                            v
                       Dispatcher
                            |
               +------------+------------+
               |            |            |
               v            v            v
           Worker 1     Worker 2      Worker N
           Validation   Validation    Validation
               |            |            |
               +------------+------------+
                            |
                            v
                     Mempool Actor
                    (single owner)
                            |
                            v
                    HashMap<Transaction>
                            |
                            v
                  Results via oneshot
```

### Design principles

**Single ownership of mutable state**

The mempool actor exclusively owns the transaction `HashMap`. Workers cannot directly mutate the collection; they submit commands to the actor.

**Message passing**

Clients communicate with the system through asynchronous channels rather than directly sharing mutable mempool state.

**Bounded queues and backpressure**

Ingress and worker queues have configurable capacities. When a queue is full, producers may need to wait for capacity instead of allowing that queue to grow without a limit.

**Parallel validation**

Multiple Tokio worker tasks validate transactions independently. The actor serializes insertion and removal operations, preserving consistent state transitions.

**Explicit request-response communication**

`oneshot` channels allow clients to receive submission results and query mempool statistics without sharing the underlying collection.

## Technology Stack

- **Language:** Rust
- **Asynchronous runtime:** Tokio
- **Concurrency:** Tokio tasks and bounded `mpsc` channels
- **Request-response:** Tokio `oneshot`
- **Storage:** In-memory `HashMap`
- **Testing:** Rust unit and integration tests
- **Benchmarking:** `std::time::Instant`

## Current Features

- Transaction submission through a bounded ingress queue.
- Configurable validation worker count and queue capacities.
- Payload validation with size and emptiness checks.
- Duplicate transaction ID rejection.
- Transaction removal by ID.
- Mempool size queries.
- Concurrent transaction submission.
- Graceful shutdown of the dispatcher, workers, and actor.
- Integration tests for basic operations and concurrent workloads.
- A benchmark executable for measuring end-to-end submission throughput.

## Project Structure

```text
rust-concurrent-mempool/
├── Cargo.toml
├── Cargo.lock
├── README.md
├── src/
│   ├── main.rs
│   ├── lib.rs
│   ├── transaction.rs
│   ├── validation.rs
│   ├── worker_pool.rs
│   └── mempool/
│       ├── mod.rs
│       ├── actor.rs
│       └── commands.rs
├── tests/
│   ├── actor_tests.rs
│   ├── concurrency_tests.rs
│   └── backpressure_tests.rs
└── benches/
    └── submission.rs
```

## Getting Started

### Prerequisites

- Rust stable toolchain
- Cargo

Install Rust using [rustup](https://rustup.rs/) if it is not already installed.

### Build

```bash
cargo build
```

### Run tests

```bash
cargo test
```

### Run the example

```bash
cargo run
```

The example starts a mempool node with four validation workers, submits transactions, retrieves statistics, removes a transaction, and shuts down the node.

### Run the benchmark

```bash
cargo bench --bench submission
```

The benchmark submits transactions sequentially through the public API and reports elapsed time, submission throughput, and final mempool size.

## Benchmark Results

Initial local measurements:

| Transactions | Elapsed Time | Submission Throughput | Final Mempool Size |
|---:|---:|---:|---:|
| 10,000 | 176.32 ms | 56,714 transactions/sec | 10,000 |

These measurements were obtained using the project's current implementation.

**Benchmark methodology:** The benchmark measures the complete submission path, including queue communication, validation, actor insertion, and request-response completion. It does not measure validation throughput in isolation.

Results depend on the machine, operating system, runtime configuration, and workload. These initial measurements are a baseline, not a claim of production performance or a demonstrated improvement over another implementation.

## Testing and Correctness

The integration tests cover:

- Acceptance of valid transactions.
- Rejection of empty payloads.
- Rejection of duplicate transaction IDs.
- Removal of existing and nonexistent transactions.
- Concurrent submission of unique transactions.
- Concurrent attempts to insert the same transaction ID.
- Burst submissions through bounded queues.

Run the complete suite with:

```bash
cargo test
```

## Current Limitations

This is an educational systems project, not a production-ready blockchain mempool.

- Validation currently performs basic synchronous payload checks.
- Transactions use simplified numeric IDs rather than cryptographic transaction hashes.
- Transactions are stored only in memory; persistence and crash recovery are not implemented.
- There is no transaction signature verification.
- The system does not implement account nonces, fee prioritization, eviction policies, or transaction expiration.
- The benchmark does not yet compare worker counts or report latency percentiles.
- The current shutdown mechanism is designed for controlled application shutdown, not crash recovery.

## Potential Extensions

Future improvements may include:

- Ed25519 transaction signature verification.
- Benchmarks comparing one, two, and four validation workers.
- Submission latency measurements, including p50, p95, and p99.
- Transaction capacity limits and eviction policies.
- Transaction expiration and prioritization.
- Property-based and stress testing.
- Structured logging and operational metrics.
- More comprehensive shutdown and failure-handling tests.

These are possible extensions, not features currently implemented.

## Learning Objectives

This project explores practical Rust systems-programming concepts:

- Ownership boundaries in concurrent applications.
- Actor-based state management.
- Asynchronous message passing with Tokio.
- Bounded queues and backpressure.
- Concurrent task orchestration.
- Separation of parallel validation from serialized state mutation.
- Request-response patterns using `oneshot`.
- Integration testing of concurrent behavior.
- Basic throughput measurement.

## License

This project is released under the MIT License if the repository's `LICENSE` file specifies MIT. See `LICENSE` for the applicable terms.
