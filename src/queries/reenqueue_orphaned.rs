use std::time::Duration;

use chrono::Utc;
use surrealdb::{Connection, Surreal, types::vars};

use crate::{error::Error,
            surreal_task::{job_record_id_from_str, worker_record_id}};

pub const REENQUEUE_ORPHANED_SURQL: &str = r#"
BEGIN TRANSACTION;
LET $dead_workers = SELECT VALUE meta::id(id) FROM apalis_workers
WHERE worker_type = $queue AND last_seen <= $cutoff;

UPDATE apalis_jobs SET
    status = 'Pending',
    done_at = NONE,
    lock_by = NONE,
    lock_at = NONE,
    attempts += 1,
    last_result = { Err: 'Re-enqueued due to worker heartbeat timeout.' }
WHERE
    (status = 'Running' OR status = 'Queued')
    AND lock_by IN $dead_workers
RETURN AFTER;
COMMIT TRANSACTION;
"#;

pub const REENQUEUE_ABANDONED_SURQL: &str = r#"
BEGIN TRANSACTION;
LET $valid_worker = SELECT VALUE id FROM $worker_id WHERE worker_type = $queue;
UPDATE $task_ids SET
    status = 'Pending',
    done_at = NONE,
    lock_by = NONE,
    lock_at = NONE,
    attempts += 1,
    last_result = { Err: 'Re-enqueued due to worker shutdown' }
WHERE
    array::len($valid_worker) > 0
    AND lock_by = $worker_name
    AND (status = 'Queued' OR status = 'Running')
RETURN AFTER;
COMMIT TRANSACTION;
"#;

/// Re-enqueue jobs orphaned by dead workers whose `last_seen` is older than `dead_for` seconds.
pub async fn reenqueue_orphaned<C: Connection>(db: &Surreal<C>, queue: &str, dead_for: u64) -> Result<u64, Error> {
    let cutoff = Utc::now() - Duration::from_secs(dead_for);

    match db
        .query(REENQUEUE_ORPHANED_SURQL)
        .bind(vars! {
            "queue": queue.to_owned(),
            "cutoff": cutoff,
        })
        .await
        .and_then(|res| res.check())
    {
    Ok(mut res) => {
        let updated: Vec<surrealdb::types::Value> = res.take(2)?;
        let count = updated.len() as u64;
        if count > 0 {
            tracing::info!("Re-enqueued {count} orphaned tasks that were being processed by dead workers");
        }
        Ok(count)
    },
    Err(e) => {
        tracing::error!("Failed to re-enqueue orphaned tasks: {e}");
        Err(e.into())
    },
    }
}

/// Rescues tasks that could not be executed after a worker shutdown.
pub async fn reenqueue_abandoned<C: Connection>(db: &Surreal<C>, queue: &str, worker: &str, task_ids: &[String]) -> Result<u64, Error> {
    if task_ids.is_empty() {
        return Ok(0);
    }

    let worker_id = worker_record_id(worker);
    let ids = task_ids.iter().map(|id| job_record_id_from_str(id)).collect::<Vec<_>>();

    let mut res = db
        .query(REENQUEUE_ABANDONED_SURQL)
        .bind(vars! {
            "worker_id": worker_id,
            "worker_name": worker.to_owned(),
            "queue": queue.to_owned(),
            "task_ids": ids,
        })
        .await?
        .check()?;

    let updated: Vec<surrealdb::types::Value> = res.take(2)?;
    Ok(updated.len() as u64)
}
