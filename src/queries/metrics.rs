use apalis_core::backend::{Backend, Metrics, Statistic};
use surrealdb::{Connection, types::vars};

use crate::{SurrealStorage,
            error::Error,
            queries::list_queues::{JobStatRow, compute_statistics_from_rows}};

pub const OVERVIEW_SURQL: &str = r#"
SELECT job_type, status, run_at, done_at, lock_by FROM apalis_jobs;
"#;

pub const OVERVIEW_BY_QUEUE_SURQL: &str = r#"
SELECT job_type, status, run_at, done_at, lock_by FROM apalis_jobs WHERE job_type = $queue;
"#;

impl<Args, C> Metrics for SurrealStorage<Args, C>
where
    Self: Backend<Error = Error>,
    C: Connection + Clone + Send + Sync + 'static,
{
    fn global(&self) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        let db = self.p.db.clone();
        async move {
            let mut response = db.query(OVERVIEW_SURQL).await?.check()?;
            let rows: Vec<JobStatRow> = response.take(0)?;
            Ok(compute_statistics_from_rows(&rows, true))
        }
    }

    fn fetch_by_queue(&self) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        let db = self.p.db.clone();
        let queue = self.p.config.queue.to_string();
        async move {
            let mut response = db
                .query(OVERVIEW_BY_QUEUE_SURQL)
                .bind(vars! { "queue": queue })
                .await?
                .check()?;
            let rows: Vec<JobStatRow> = response.take(0)?;
            Ok(compute_statistics_from_rows(&rows, true))
        }
    }
}
