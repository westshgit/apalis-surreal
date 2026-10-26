use apalis_core::{task::Task, worker::context::WorkerContext};
use surrealdb::{Connection, Surreal, types::vars};

use crate::{config::Config, error::Error, surreal_task::SurrealTaskRow};

pub const FETCH_NEXT_SURQL: &str = r#"
BEGIN TRANSACTION;
LET $candidates = SELECT VALUE id FROM apalis_jobs
WHERE
    job_type = $job_type
    AND (
        status = 'Pending'
        OR (status = 'Failed' AND attempts < max_attempts)
    )
    AND run_at <= time::now()
    AND lock_by IS NONE
ORDER BY priority DESC, run_at ASC
LIMIT $limit;

UPDATE $candidates SET
    status = 'Queued',
    lock_by = $worker_id,
    lock_at = time::now()
RETURN AFTER;
COMMIT TRANSACTION;
"#;

/// Fetch and claim the next batch of tasks from SurrealDB.
pub async fn fetch_next<C: Connection>(db: &Surreal<C>, config: &Config, worker: &WorkerContext) -> Result<Vec<Task<Vec<u8>>>, Error> {
    let job_type = config.queue.as_ref().to_owned();
    let limit = config.batch_size as i64;
    let worker_id = worker.name().to_owned();

    let mut response = db
        .query(FETCH_NEXT_SURQL)
        .bind(vars! {
            "job_type": job_type,
            "limit": limit,
            "worker_id": worker_id,
        })
        .await?
        .check()?;

    let rows: Vec<SurrealTaskRow> = response.take(2)?;
    rows.into_iter().map(TryInto::try_into).collect()
}
