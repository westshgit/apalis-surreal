# apalis-surreal

`apalis-surreal` is a [SurrealDB](https://surrealdb.com/) storage backend for [Apalis](https://github.com/geofmureithi/apalis) (`v1.0`), enabling durable background job processing, task scheduling, and multi-step workflows in Rust backed by SurrealDB (embedded in-memory, RocksDB/SurrealKV, or remote WebSocket/HTTP engines).

---

## Features

- **Durable Job Persistence**: Stores tasks and execution metadata in `SCHEMAFULL` SurrealDB tables (`apalis_jobs` and `apalis_workers`) with secondary indexes for fast queue polling.
- **Real-Time Live Query Notifications (`LIVE SELECT`)**: Supports low-latency push notifications via SurrealDB Live Queries using `.with_pubsub()` instead of relying solely on interval polling.
- **Shared Live Query Multiplexing**: Provides [`SurrealStorageFactory`] to run multiple typed queues over a single shared SurrealDB `LIVE SELECT` stream.
- **Transactional Task Claiming & Locking**: Atomically claims batches of pending or retryable failed jobs ordered by `priority DESC, run_at ASC`.
- **Heartbeats & Orphan Recovery**: Tracks active workers in `apalis_workers` and automatically re-enqueues tasks abandoned by stalled or dead workers.
- **Workflow & Observability Support**: Compatible with `apalis-workflow` (`SteppedFlow` / DAGs) and implements Apalis introspection traits (`Metrics`, `ListQueues`, `ListTasks`, `ListWorkers`, `FetchById`, `WaitFor`).

---

## Installation

Add `apalis`, `apalis-surreal`, and `surrealdb` to your `Cargo.toml`:

```toml
[dependencies]
apalis = { version = "1.0.0", features = ["filter", "shared", "task-context"] }
apalis-surreal = "0.1.0"
surrealdb = { version = "3.3.4", features = ["kv-mem"] } # or protocol-ws, kv-surrealkv, etc.
serde = { version = "1.0", features = ["derive"] }
tokio = { version = "1.54", features = ["macros", "rt-multi-thread"] }
```

### Feature Flags

| Feature | Default | Description |
| :--- | :---: | :--- |
| `task-tracking` | Yes | Enables `apalis-core/task-context` for tracking task metadata and execution context. |

---

## How to Run / Usage

### 1. Basic Worker Setup

Before pushing or consuming jobs, initialize the SurrealDB namespace/database and run `SurrealStorage::setup(&db)` to create the required `apalis_jobs` and `apalis_workers` tables and indexes (idempotent `DEFINE ... IF NOT EXISTS`).

```rust
use std::time::Duration;
use apalis::prelude::*;
use apalis_surreal::{Config, SurrealStorage};
use serde::{Deserialize, Serialize};
use surrealdb::{Surreal, engine::local::Mem};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EmailJob {
    to: String,
    subject: String,
}

async fn send_email(job: EmailJob, ctx: WorkerContext) -> Result<(), BoxDynError> {
    println!("Sending email to {}: {}", job.to, job.subject);
    ctx.stop()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    // 1. Connect to SurrealDB (in-memory, local disk, or remote server)
    let db = Surreal::new::<Mem>(()).await?;
    db.use_ns("app").use_db("jobs").await?;

    // 2. Setup Apalis schema (idempotent)
    SurrealStorage::setup(&db).await?;

    // 3. Configure storage and enable Live Query pub/sub notifications
    let config = Config::default()
        .queue("emails")
        .batch_size(20)
        .heartbeat_interval(Duration::from_secs(30));

    let mut backend = SurrealStorage::new(db)
        .with_config(config)
        .with_pubsub();

    // 4. Enqueue a job
    backend
        .push(EmailJob {
            to: "user@example.com".to_owned(),
            subject: "Welcome to apalis-surreal!".to_owned(),
        })
        .await?;

    // 5. Build and run the worker
    let worker = WorkerBuilder::new("email-worker-1")
        .backend(backend)
        .build(send_email);

    worker.run().await?;
    Ok(())
}
```

### 2. Sharing a Single Live Query Across Multiple Queues

When running multiple workers for different job types, use `SurrealStorageFactory` so all queues share a single `LIVE SELECT` subscription on `apalis_jobs`:

```rust
use apalis_core::backend::factory::BackendFactory;
use apalis_surreal::{Config, SurrealStorageFactory};

let mut factory = SurrealStorageFactory::new(db.clone());

let email_backend = factory.create_with_config(Config::default().queue("emails"))?;
let report_backend = factory.create_with_config(Config::default().queue("reports"))?;
```

### 3. Configuration Options

`apalis_surreal::Config` allows tuning queue polling, locking, and worker liveness behavior:

| Option | Default | Description |
| :--- | :--- | :--- |
| `queue` | `"default"` (or `Args` type name) | Queue namespace (`job_type` in `apalis_jobs`). |
| `batch_size` | `10` | Maximum number of jobs claimed per poll batch. |
| `heartbeat_interval` | `30s` | Interval between worker keep-alive updates. |
| `missed_heartbeats` | `2` | Missed heartbeats before a worker's locked jobs are considered orphaned (`heartbeat_interval * missed_heartbeats`). |
| `lock_tasks` | `true` | Lock tasks while in flight to prevent duplicate execution. |
| `persist_results` | `true` | Persist task return values into `last_result` in `apalis_jobs`. |

---

## How to Test

The test suite uses SurrealDB's in-memory engine (`kv-mem`), so no external database server is required to run tests.

### Run All Tests (Unit + Integration + Doctests)

```bash
cargo test
```

### Run Only Library Unit Tests

```bash
cargo test --lib
```

### Run a Specific Test

```bash
# Test basic job push and worker consumption
cargo test basic_worker

# Test real-time wakeup via SurrealDB LIVE SELECT
cargo test notify_worker_with_live_query

# Test multi-step workflow execution (apalis-workflow)
cargo test test_workflow_complete

# Test queue metrics and statistics reporting
cargo test list_queues_reports_stats
```

### Run Documentation Tests

```bash
cargo test --doc
```

---

## Features

- **Durable Job Persistence**: Stores tasks and execution metadata in `SCHEMAFULL` SurrealDB tables (`apalis_jobs` and `apalis_workers`) with secondary indexes for fast queue polling.
- **Real-Time Live Query Notifications (`LIVE SELECT`)**: Supports low-latency push notifications via SurrealDB Live Queries using `.with_pubsub()` instead of relying solely on interval polling.
- **Shared Live Query Multiplexing**: Provides [`SurrealStorageFactory`] to run multiple typed queues over a single shared SurrealDB `LIVE SELECT` stream.
- **Transactional Task Claiming & Locking**: Atomically claims batches of pending or retryable failed jobs ordered by `priority DESC, run_at ASC`.
- **Heartbeats & Orphan Recovery**: Tracks active workers in `apalis_workers` and automatically re-enqueues tasks abandoned by stalled or dead workers.
- **Workflow & Observability Support**: Compatible with `apalis-workflow` (`SteppedFlow` / DAGs) and implements Apalis introspection traits (`Metrics`, `ListQueues`, `ListTasks`, `ListWorkers`, `FetchById`, `WaitFor`).

---

## Installation

Add `apalis`, `apalis-surreal`, and `surrealdb` to your `Cargo.toml`:

```toml
[dependencies]
apalis = { version = "1.0.0", features = ["filter", "shared", "task-context"] }
apalis-surreal = "0.1.0"
surrealdb = { version = "3.3.4", features = ["kv-mem"] } # or protocol-ws, kv-surrealkv, etc.
serde = { version = "1.0", features = ["derive"] }
tokio = { version = "1.54", features = ["macros", "rt-multi-thread"] }
```

### Feature Flags

| Feature | Default | Description |
| :--- | :---: | :--- |
| `task-tracking` | Yes | Enables `apalis-core/task-context` for tracking task metadata and execution context. |

---

## How to Run / Usage

### 1. Basic Worker Setup

Before pushing or consuming jobs, initialize the SurrealDB namespace/database and run `SurrealStorage::setup(&db)` to create the required `apalis_jobs` and `apalis_workers` tables and indexes (idempotent `DEFINE ... IF NOT EXISTS`).

```rust
use std::time::Duration;
use apalis::prelude::*;
use apalis_surreal::{Config, SurrealStorage};
use serde::{Deserialize, Serialize};
use surrealdb::{Surreal, engine::local::Mem};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EmailJob {
    to: String,
    subject: String,
}

async fn send_email(job: EmailJob, ctx: WorkerContext) -> Result<(), BoxDynError> {
    println!("Sending email to {}: {}", job.to, job.subject);
    ctx.stop()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    // 1. Connect to SurrealDB (in-memory, local disk, or remote server)
    let db = Surreal::new::<Mem>(()).await?;
    db.use_ns("app").use_db("jobs").await?;

    // 2. Setup Apalis schema (idempotent)
    SurrealStorage::setup(&db).await?;

    // 3. Configure storage and enable Live Query pub/sub notifications
    let config = Config::default()
        .queue("emails")
        .batch_size(20)
        .heartbeat_interval(Duration::from_secs(30));

    let mut backend = SurrealStorage::new(db)
        .with_config(config)
        .with_pubsub();

    // 4. Enqueue a job
    backend
        .push(EmailJob {
            to: "user@example.com".to_owned(),
            subject: "Welcome to apalis-surreal!".to_owned(),
        })
        .await?;

    // 5. Build and run the worker
    let worker = WorkerBuilder::new("email-worker-1")
        .backend(backend)
        .build(send_email);

    worker.run().await?;
    Ok(())
}
```

### 2. Sharing a Single Live Query Across Multiple Queues

When running multiple workers for different job types, use `SurrealStorageFactory` so all queues share a single `LIVE SELECT` subscription on `apalis_jobs`:

```rust
use apalis_core::backend::factory::BackendFactory;
use apalis_surreal::{Config, SurrealStorageFactory};

let mut factory = SurrealStorageFactory::new(db.clone());

let email_backend = factory.create_with_config(Config::default().queue("emails"))?;
let report_backend = factory.create_with_config(Config::default().queue("reports"))?;
```

### 3. Configuration Options

`apalis_surreal::Config` allows tuning queue polling, locking, and worker liveness behavior:

| Option | Default | Description |
| :--- | :--- | :--- |
| `queue` | `"default"` (or `Args` type name) | Queue namespace (`job_type` in `apalis_jobs`). |
| `batch_size` | `10` | Maximum number of jobs claimed per poll batch. |
| `heartbeat_interval` | `30s` | Interval between worker keep-alive updates. |
| `missed_heartbeats` | `2` | Missed heartbeats before a worker's locked jobs are considered orphaned (`heartbeat_interval * missed_heartbeats`). |
| `lock_tasks` | `true` | Lock tasks while in flight to prevent duplicate execution. |
| `persist_results` | `true` | Persist task return values into `last_result` in `apalis_jobs`. |

---

## How to Test

The test suite uses SurrealDB's in-memory engine (`kv-mem`), so no external database server is required to run tests.

### Run All Tests (Unit + Integration + Doctests)

```bash
cargo test
```

### Run Only Library Unit Tests

```bash
cargo test --lib
```

### Run a Specific Test

```bash
# Test basic job push and worker consumption
cargo test basic_worker

# Test real-time wakeup via SurrealDB LIVE SELECT
cargo test notify_worker_with_live_query

# Test multi-step workflow execution (apalis-workflow)
cargo test test_workflow_complete

# Test queue metrics and statistics reporting
cargo test list_queues_reports_stats
```

### Run Documentation Tests

```bash
cargo test --doc
```

