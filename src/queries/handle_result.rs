use apalis_core::backend::TaskResult;
use serde::{Deserialize, Serialize};
use surrealdb::{Connection,
                Surreal,
                types::{RecordId, SurrealValue, vars}};

use crate::{error::Error, surreal_task::job_record_id};

/// Serialized result payload including the current attempt and status.
pub type Payload = TaskResult<serde_json::Value>;

#[derive(Debug, Clone, SurrealValue, Serialize, Deserialize)]
struct ResultItem {
    id:      RecordId,
    status:  String,
    attempt: i64,
    result:  serde_json::Value,
}

pub const HANDLE_RESULTS_SURQL: &str = r#"
BEGIN TRANSACTION;
FOR $item IN $results {
    UPDATE $item.id SET
        status = $item.status,
        attempts = $item.attempt,
        last_result = $item.result,
        done_at = time::now()
    WHERE lock_by = $worker_id;
};
COMMIT TRANSACTION;
"#;

/// Acknowledge multiple tasks and persist their results for a worker.
pub async fn handle_results<C: Connection>(db: &Surreal<C>, results: &[&Payload], worker_id: &str) -> Result<u64, Error> {
    if results.is_empty() {
        return Ok(0);
    }

    let items = results
        .iter()
        .map(|payload| {
            let result_val = serde_json::to_value(payload.result()).map_err(Error::JsonError)?;
            Ok(ResultItem {
                id:      job_record_id(payload.task_id()),
                status:  payload.status().to_string(),
                attempt: payload.attempt as i64,
                result:  result_val,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;

    db.query(HANDLE_RESULTS_SURQL)
        .bind(vars! {
            "results": items,
            "worker_id": worker_id.to_owned(),
        })
        .await?
        .check()?;

    Ok(results.len() as u64)
}
