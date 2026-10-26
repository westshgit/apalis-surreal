use apalis_core::backend::{Backend, ListWorkers, RunningWorker};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::{Connection,
                types::{RecordId, SurrealValue, vars}};

use crate::{SurrealStorage, error::Error, surreal_task::record_id_to_key_string};

#[derive(Debug, Clone, SurrealValue, Serialize, Deserialize)]
pub struct WorkerRow {
    pub id:           RecordId,
    pub worker_type:  String,
    pub storage_name: String,
    pub layers:       Option<String>,
    pub last_seen:    DateTime<Utc>,
    pub started_at:   Option<DateTime<Utc>>,
}

impl From<WorkerRow> for RunningWorker {
    fn from(w: WorkerRow) -> Self {
        Self {
            id:             record_id_to_key_string(&w.id),
            backend:        w.storage_name,
            started_at:     w.started_at.map(|dt| dt.timestamp().max(0) as u64).unwrap_or_default(),
            last_heartbeat: w.last_seen.timestamp().max(0) as u64,
            layers:         w.layers.unwrap_or_default(),
            queue:          w.worker_type,
        }
    }
}

pub const LIST_WORKERS_SURQL: &str = r#"
SELECT * FROM apalis_workers
WHERE worker_type = $queue
ORDER BY last_seen DESC
LIMIT $limit START $offset;
"#;

pub const LIST_ALL_WORKERS_SURQL: &str = r#"
SELECT * FROM apalis_workers
ORDER BY last_seen DESC
LIMIT $limit START $offset;
"#;

impl<Args: Sync, C> ListWorkers for SurrealStorage<Args, C>
where
    Self: Backend<Error = Error>,
    C: Connection + Clone + Send + Sync + 'static,
{
    fn list_workers(&self) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        let queue = self.p.config.queue.to_string();
        let db = self.p.db.clone();
        let limit: i64 = 100;
        let offset: i64 = 0;

        async move {
            let mut response = db
                .query(LIST_WORKERS_SURQL)
                .bind(vars! {
                    "queue": queue,
                    "limit": limit,
                    "offset": offset,
                })
                .await?
                .check()?;

            let rows: Vec<WorkerRow> = response.take(0)?;
            Ok(rows.into_iter().map(RunningWorker::from).collect())
        }
    }

    fn list_all_workers(&self) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        let db = self.p.db.clone();
        let limit: i64 = 100;
        let offset: i64 = 0;

        async move {
            let mut response = db
                .query(LIST_ALL_WORKERS_SURQL)
                .bind(vars! {
                    "limit": limit,
                    "offset": offset,
                })
                .await?
                .check()?;

            let rows: Vec<WorkerRow> = response.take(0)?;
            Ok(rows.into_iter().map(RunningWorker::from).collect())
        }
    }
}
