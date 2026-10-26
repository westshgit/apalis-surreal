use apalis_core::worker::context::WorkerContext;
use chrono::{DateTime, Utc};
use surrealdb::{Connection, Surreal, types::vars};

use crate::{error::Error, surreal_task::worker_record_id};

pub const REGISTER_WORKER_SURQL: &str = r#"
UPSERT $id SET
    worker_type = $worker_type,
    storage_name = $storage_name,
    layers = $layers,
    last_seen = $last_seen,
    started_at = started_at ?? $last_seen;
"#;

/// Register a worker in SurrealDB (`apalis_workers`).
pub async fn register_worker<C: Connection>(
    db: &Surreal<C>,
    queue: &str,
    worker: &WorkerContext,
    last_seen: DateTime<Utc>,
    backend_type: &str,
) -> Result<(), Error> {
    let id = worker_record_id(worker.name());
    let layers = worker.get_service().to_owned();

    let mut response = db
        .query(REGISTER_WORKER_SURQL)
        .bind(vars! {
            "id": id,
            "worker_type": queue.to_owned(),
            "storage_name": backend_type.to_owned(),
            "layers": layers,
            "last_seen": last_seen,
        })
        .await?
        .check()?;

    let updated: Vec<surrealdb::types::Value> = response.take(0)?;
    if updated.is_empty() {
        return Err(Error::WorkerAlreadyExists(worker.name().to_owned()));
    }
    Ok(())
}
