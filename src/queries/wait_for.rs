use std::{collections::HashSet, str::FromStr};

use apalis_core::{backend::{Backend, TaskResult, WaitForCompletion},
                  task::{status::Status, task_id::TaskId}};
use futures::{StreamExt, stream::BoxStream};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use surrealdb::{Connection,
                types::{RecordId, SurrealValue, vars}};

use crate::{SurrealStorage,
            error::Error,
            surreal_task::{job_record_id_from_str, record_id_to_key_string, record_id_to_task_id}};

#[derive(Debug, Clone, SurrealValue, Serialize, Deserialize)]
pub struct TaskResultRow {
    pub id:          Option<RecordId>,
    pub status:      Option<String>,
    pub last_result: Option<serde_json::Value>,
    pub attempts:    Option<i64>,
}

pub const FETCH_COMPLETED_TASKS_SURQL: &str = r#"
SELECT id, status, attempts, last_result FROM apalis_jobs
WHERE
    id IN $ids
    AND (
        status = 'Done'
        OR (status = 'Failed' AND attempts >= max_attempts)
        OR status = 'Killed'
    );
"#;

impl<O: 'static + Send, Args, C> WaitForCompletion<O> for SurrealStorage<Args, C>
where
    Self: Backend<Error = Error>,
    Result<O, String>: DeserializeOwned,
    C: Connection + Clone + Send + Sync + 'static,
{
    type ResultStream = BoxStream<'static, Result<TaskResult<O>, Self::Error>>;

    fn wait_for(&mut self, task_ids: impl IntoIterator<Item = TaskId>) -> Self::ResultStream {
        let ids: HashSet<String> = task_ids.into_iter().map(|id| id.to_string()).collect();
        let db = self.p.db.clone();

        let stream = futures::stream::unfold(ids, move |mut remaining_ids| {
            let db = db.clone();
            async move {
                if remaining_ids.is_empty() {
                    return None;
                }

                let record_ids = remaining_ids.iter().map(|id| job_record_id_from_str(id)).collect::<Vec<_>>();

                let mut response = db
                    .query(FETCH_COMPLETED_TASKS_SURQL)
                    .bind(vars! { "ids": record_ids })
                    .await
                    .ok()?
                    .check()
                    .ok()?;

                let rows: Vec<TaskResultRow> = response.take(0).ok()?;

                if rows.is_empty() {
                    apalis_core::timer::sleep(std::time::Duration::from_millis(500)).await;
                    return Some((futures::stream::iter(Vec::new()), remaining_ids));
                }

                let mut results = Vec::new();
                for row in rows {
                    let record_id = row.id?;
                    let key_str = record_id_to_key_string(&record_id);
                    remaining_ids.remove(&key_str);

                    let task_id = record_id_to_task_id(&record_id).ok()?;
                    let status = Status::from_str(&row.status?).ok()?;
                    let result: Result<O, String> = serde_json::from_value(row.last_result.unwrap_or(serde_json::Value::Null)).ok()?;

                    results.push(Ok(TaskResult { task_id, status, attempt: row.attempts.unwrap_or_default() as usize, result }));
                }

                Some((futures::stream::iter(results), remaining_ids))
            }
        });

        stream.flatten().boxed()
    }

    fn check_status(&mut self, task_ids: impl IntoIterator<Item = TaskId> + Send) -> impl Future<Output = Result<Vec<TaskResult<O>>, Self::Error>> + Send {
        let db = self.p.db.clone();
        let record_ids = task_ids
            .into_iter()
            .map(|id| job_record_id_from_str(&id.to_string()))
            .collect::<Vec<_>>();

        async move {
            if record_ids.is_empty() {
                return Ok(Vec::new());
            }

            let mut response = db
                .query(FETCH_COMPLETED_TASKS_SURQL)
                .bind(vars! { "ids": record_ids })
                .await?
                .check()?;

            let rows: Vec<TaskResultRow> = response.take(0)?;
            let mut results = Vec::with_capacity(rows.len());

            for row in rows {
                let record_id = row.id.ok_or(Error::ColumnNotFound("id"))?;
                let task_id = record_id_to_task_id(&record_id)?;
                let status_str = row.status.ok_or(Error::ColumnNotFound("status"))?;
                let status = Status::from_str(&status_str).map_err(Error::StatusError)?;
                let result_val = row.last_result.unwrap_or(serde_json::Value::Null);
                let result: Result<O, String> = serde_json::from_value(result_val).map_err(Error::JsonError)?;

                results.push(TaskResult { task_id, status, attempt: row.attempts.unwrap_or_default() as usize, result });
            }

            Ok(results)
        }
    }
}
