use std::{collections::HashMap,
          pin::Pin,
          task::{Context, Poll}};

use apalis_core::task::{Task, task_id::TaskId};
use chrono::{DateTime, TimeZone, Utc};
use futures::Sink;
use serde::{Deserialize, Serialize};
use surrealdb::{Connection,
                Surreal,
                types::{Bytes, RecordId, SurrealValue}};
use uuid::Uuid;

use crate::{SurrealStorage,
            error::Error,
            surreal_task::{JOBS_TABLE, job_record_id}};

#[derive(Debug, Clone, SurrealValue, Serialize, Deserialize)]
struct InsertTaskRecord {
    id:              RecordId,
    job_type:        String,
    job:             Bytes,
    status:          String,
    attempts:        i64,
    max_attempts:    i64,
    run_at:          DateTime<Utc>,
    priority:        i64,
    metadata:        HashMap<String, String>,
    idempotency_key: Option<String>,
}

/// Push a batch of tasks to SurrealDB (`apalis_jobs`).
pub async fn push_tasks<C: Connection>(db: &Surreal<C>, queue: &str, buffer: Vec<Task<Vec<u8>>>) -> Result<(), Error> {
    if buffer.is_empty() {
        return Ok(());
    }

    let now = Utc::now();
    let mut records = Vec::with_capacity(buffer.len());

    for task in buffer {
        let id = match task.task_id() {
        Some(tid) => job_record_id(tid),
        None => job_record_id(&TaskId::from_uuid(Uuid::new_v4())),
        };

        let run_at = task
            .run_at()
            .and_then(|ts| Utc.timestamp_opt(ts as i64, 0).single())
            .unwrap_or(now);

        let priority = task.priority().map(|p| p as i64).unwrap_or_default();
        let max_attempts = task.max_attempts().map(|m| m as i64).unwrap_or(25);
        let metadata = task.metadata().clone().into_inner();
        let idempotency_key = task.idempotency_key().map(ToOwned::to_owned);
        let job = Bytes::from(task.args);

        records.push(InsertTaskRecord {
            id,
            job_type: queue.to_owned(),
            job,
            status: "Pending".to_owned(),
            attempts: 0,
            max_attempts,
            run_at,
            priority,
            metadata,
            idempotency_key,
        });
    }

    let _: Vec<surrealdb::types::Value> = db.insert(JOBS_TABLE).content(records).await?;
    Ok(())
}

impl<Args, C> Sink<Task<Vec<u8>>> for SurrealStorage<Args, C>
where
    Args: Unpin + Send + Sync + 'static,
    C: Connection + Clone + Send + Sync + 'static + Unpin,
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().p.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Task<Vec<u8>>) -> Result<(), Self::Error> {
        self.project().p.start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().p.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Sink::poll_close(self.project().p, cx)
    }
}
