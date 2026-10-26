use surrealdb::{Connection, Surreal, types::vars};

use crate::{error::Error, surreal_task::job_record_id_from_str};

pub const LOCK_TASKS_SURQL: &str = r#"
UPDATE $ids SET
    status = 'Running',
    lock_at = time::now(),
    lock_by = $worker_id
WHERE
    (
        status = 'Pending'
        OR status = 'Queued'
        OR (status = 'Failed' AND attempts < max_attempts)
    )
    AND run_at <= time::now()
RETURN AFTER;
"#;

/// Lock multiple tasks, given a worker ID.
pub async fn lock_tasks<C: Connection>(db: &Surreal<C>, task_ids: &[String], worker_id: &str) -> Result<u64, Error> {
    if task_ids.is_empty() {
        return Ok(0);
    }

    let ids = task_ids.iter().map(|id| job_record_id_from_str(id)).collect::<Vec<_>>();
    let mut response = db
        .query(LOCK_TASKS_SURQL)
        .bind(vars! {
            "ids": ids,
            "worker_id": worker_id.to_owned(),
        })
        .await?
        .check()?;

    let updated: Vec<surrealdb::types::Value> = response.take(0)?;
    Ok(updated.len() as u64)
}
