use apalis_core::worker::context::WorkerContext;
use surrealdb::{Connection, Surreal, types::vars};

use crate::{error::Error,
            surreal_task::{job_record_id_from_str, worker_record_id}};

pub const KEEP_ALIVE_UNTRACKED_SURQL: &str = r#"
UPDATE $worker_id SET last_seen = time::now() WHERE worker_type = $queue;
"#;

pub const KEEP_ALIVE_TRACKED_SURQL: &str = r#"
BEGIN TRANSACTION;
LET $locked = SELECT VALUE id FROM apalis_jobs WHERE lock_by = $worker_name AND id IN $tasks;
UPDATE $worker_id SET last_seen = time::now()
WHERE worker_type = $queue AND array::len($locked) = $expected_count;
COMMIT TRANSACTION;
"#;

/// Heartbeat for denoting liveliness of workers in SurrealDB.
pub async fn keep_alive<C: Connection>(db: &Surreal<C>, queue: &str, worker: &WorkerContext) -> Result<(), Error> {
    if cfg!(feature = "task-tracking") {
        keep_alive_tracked(db, queue, worker).await
    }
    else {
        keep_alive_untracked(db, queue, worker).await
    }
}

async fn keep_alive_tracked<C: Connection>(db: &Surreal<C>, queue: &str, worker: &WorkerContext) -> Result<(), Error> {
    #[cfg(feature = "task-tracking")]
    let tasks = worker
        .tasks()
        .map_err(|_| Error::WorkerOutOfSync)?
        .iter()
        .map(|task| job_record_id_from_str(task.task_id()))
        .collect::<Vec<_>>();

    #[cfg(not(feature = "task-tracking"))]
    let tasks: Vec<surrealdb::types::RecordId> = Vec::new();

    if tasks.is_empty() {
        return keep_alive_untracked(db, queue, worker).await;
    }

    let worker_name = worker.name().to_owned();
    let worker_id = worker_record_id(&worker_name);
    let expected_count = tasks.len() as i64;

    let mut response = db
        .query(KEEP_ALIVE_TRACKED_SURQL)
        .bind(vars! {
            "worker_id": worker_id,
            "worker_name": worker_name,
            "queue": queue.to_owned(),
            "tasks": tasks,
            "expected_count": expected_count,
        })
        .await?
        .check()?;

    let updated: Vec<surrealdb::types::Value> = response.take(2)?;
    if updated.len() != 1 {
        return Err(Error::WorkerOutOfSync);
    }
    Ok(())
}

async fn keep_alive_untracked<C: Connection>(db: &Surreal<C>, queue: &str, worker: &WorkerContext) -> Result<(), Error> {
    let worker_id = worker_record_id(worker.name());

    let mut response = db
        .query(KEEP_ALIVE_UNTRACKED_SURQL)
        .bind(vars! {
            "worker_id": worker_id,
            "queue": queue.to_owned(),
        })
        .await?
        .check()?;

    let updated: Vec<surrealdb::types::Value> = response.take(0)?;
    if updated.len() != 1 {
        return Err(Error::WorkerOutOfSync);
    }
    Ok(())
}
