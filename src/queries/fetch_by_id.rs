use apalis_core::{backend::{Backend, FetchById},
                  task::{Task, task_id::TaskId}};
use surrealdb::Connection;

use crate::{SurrealStorage,
            error::Error,
            surreal_task::{SurrealTaskRow, job_record_id}};

impl<Args, C> FetchById for SurrealStorage<Args, C>
where
    Self: Backend<Error = Error, Task = Task<Vec<u8>>>,
    Args: 'static,
    C: Connection + Clone + Send + Sync + 'static,
{
    fn fetch_by_id(&mut self, id: &TaskId) -> impl Future<Output = Result<Option<Self::Task>, Self::Error>> + Send {
        let db = self.p.db.clone();
        let record_id = job_record_id(id);
        async move {
            let row: Option<SurrealTaskRow> = db.select(record_id).await?;
            let task = row.map(TryInto::try_into).transpose()?;
            Ok(task)
        }
    }
}
