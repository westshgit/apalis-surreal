use apalis_core::{backend::{Backend, Filter, ListAllTasks, ListTasks},
                  task::{Task, status::Status}};
use surrealdb::{Connection, types::vars};

use crate::{SurrealStorage, error::Error, surreal_task::SurrealTaskRow};

pub const LIST_JOBS_SURQL: &str = r#"
SELECT * FROM apalis_jobs
WHERE status = $status AND job_type = $queue
ORDER BY done_at DESC, run_at DESC
LIMIT $limit START $offset;
"#;

pub const LIST_ALL_JOBS_SURQL: &str = r#"
SELECT * FROM apalis_jobs
WHERE status = $status
ORDER BY done_at DESC, run_at DESC
LIMIT $limit START $offset;
"#;

impl<Args, C> ListTasks for SurrealStorage<Args, C>
where
    Self: Backend<Error = Error>,
    Args: 'static,
    C: Connection + Clone + Send + Sync + 'static,
{
    fn list_tasks(&self, filter: &Filter) -> impl Future<Output = Result<Vec<Task<Vec<u8>>>, Self::Error>> + Send {
        let queue = self.p.config.queue.to_string();
        let db = self.p.db.clone();
        let limit = filter.limit() as i64;
        let offset = filter.offset() as i64;
        let status = filter.status.as_ref().unwrap_or(&Status::Pending).to_string();

        async move {
            let mut response = db
                .query(LIST_JOBS_SURQL)
                .bind(vars! {
                    "status": status,
                    "queue": queue,
                    "limit": limit,
                    "offset": offset,
                })
                .await?
                .check()?;

            let rows: Vec<SurrealTaskRow> = response.take(0)?;
            rows.into_iter().map(TryInto::try_into).collect()
        }
    }
}

impl<Args, C> ListAllTasks for SurrealStorage<Args, C>
where
    Self: Backend<Error = Error>,
    C: Connection + Clone + Send + Sync + 'static,
{
    fn list_all_tasks(&self, filter: &Filter) -> impl Future<Output = Result<Vec<Task<Self::Compact>>, Self::Error>> + Send {
        let status = filter.status.as_ref().unwrap_or(&Status::Pending).to_string();
        let db = self.p.db.clone();
        let limit = filter.limit() as i64;
        let offset = filter.offset() as i64;

        async move {
            let mut response = db
                .query(LIST_ALL_JOBS_SURQL)
                .bind(vars! {
                    "status": status,
                    "limit": limit,
                    "offset": offset,
                })
                .await?
                .check()?;

            let rows: Vec<SurrealTaskRow> = response.take(0)?;
            rows.into_iter().map(TryInto::try_into).collect()
        }
    }
}
