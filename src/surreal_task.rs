use std::{collections::HashMap, str::FromStr};

use apalis_core::task::{Task,
                        builder::TaskBuilder,
                        metadata::MetadataStore,
                        status::Status,
                        task_id::{TaskId, TaskIdError::Decode}};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use surrealdb::types::{Bytes, RecordId, SurrealValue};

use crate::Error;

/// Table name for persisted Apalis jobs in SurrealDB.
pub const JOBS_TABLE: &str = "apalis_jobs";

/// Table name for registered Apalis workers in SurrealDB.
pub const WORKERS_TABLE: &str = "apalis_workers";

/// Convert a [`RecordId`] into a string key (stripping the table prefix).
#[must_use]
pub fn record_id_to_key_string(id: &RecordId) -> String {
    match &id.key {
    surrealdb::types::RecordIdKey::String(s) => s.clone(),
    surrealdb::types::RecordIdKey::Uuid(u) => u.to_string(),
    surrealdb::types::RecordIdKey::Number(n) => n.to_string(),
    other => format!("{other:?}"),
    }
}

/// Convert a [`RecordId`] into an Apalis [`TaskId`].
pub fn record_id_to_task_id(id: &RecordId) -> Result<TaskId, Error> {
    match &id.key {
    surrealdb::types::RecordIdKey::Uuid(u) => Ok(TaskId::from_uuid(uuid::Uuid::from(*u))),
    surrealdb::types::RecordIdKey::Number(n) if *n >= 0 => Ok(TaskId::from_int(*n as u64)),
    surrealdb::types::RecordIdKey::String(s) => TaskId::from_str(s).map_err(Error::TaskIdError),
    other => Err(Error::TaskIdError(Decode(format!("{other:?}")))),
    }
}

/// Construct a [`RecordId`] in [`JOBS_TABLE`] from an Apalis [`TaskId`] or string key.
#[must_use]
pub fn job_record_id(task_id: &TaskId) -> RecordId {
    match task_id {
    TaskId::Uuid(u) => RecordId::new(JOBS_TABLE, surrealdb::types::Uuid::from(*u)),
    TaskId::Int(i) => RecordId::new(JOBS_TABLE, *i as i64),
    TaskId::String(s) => {
        if let Ok(u) = uuid::Uuid::parse_str(s) {
            RecordId::new(JOBS_TABLE, surrealdb::types::Uuid::from(u))
        }
        else {
            RecordId::new(JOBS_TABLE, s.clone())
        }
    },
    other => {
        let s = other.to_string();
        if let Ok(u) = uuid::Uuid::parse_str(&s) {
            RecordId::new(JOBS_TABLE, surrealdb::types::Uuid::from(u))
        }
        else {
            RecordId::new(JOBS_TABLE, s)
        }
    },
    }
}

/// Construct a [`RecordId`] in [`JOBS_TABLE`] from a string task ID.
#[must_use]
pub fn job_record_id_from_str(id: &str) -> RecordId {
    if let Ok(u) = uuid::Uuid::parse_str(id) {
        RecordId::new(JOBS_TABLE, surrealdb::types::Uuid::from(u))
    }
    else if let Ok(i) = id.parse::<i64>() {
        RecordId::new(JOBS_TABLE, i)
    }
    else {
        RecordId::new(JOBS_TABLE, id.to_owned())
    }
}

/// Construct a [`RecordId`] in [`WORKERS_TABLE`] from a worker name.
#[must_use]
pub fn worker_record_id(worker_id: &str) -> RecordId {
    RecordId::new(WORKERS_TABLE, worker_id.to_owned())
}

/// SurrealDB representation of a persisted Apalis job row in `apalis_jobs`.
#[derive(Debug, Clone, new, SurrealValue, Serialize, Deserialize)]
pub struct SurrealTaskRow {
    pub id:              Option<RecordId>,
    pub job:             Option<Bytes>,
    pub job_type:        Option<String>,
    pub status:          Option<String>,
    pub attempts:        Option<i64>,
    pub max_attempts:    Option<i64>,
    pub run_at:          Option<DateTime<Utc>>,
    #[allow(unused)]
    pub last_result:     Option<Value>,
    pub lock_at:         Option<DateTime<Utc>>,
    pub lock_by:         Option<String>,
    pub done_at:         Option<DateTime<Utc>>,
    pub priority:        Option<i64>,
    pub idempotency_key: Option<String>,
    pub metadata:        Option<HashMap<String, String>>,
}

impl TryInto<Task<Vec<u8>>> for SurrealTaskRow {
    type Error = Error;

    fn try_into(self) -> Result<Task<Vec<u8>>, Self::Error> {
        let job_bytes = self.job.ok_or(Error::ColumnNotFound("job"))?.into_inner().to_vec();
        let record_id = self.id.ok_or(Error::ColumnNotFound("id"))?;
        let task_id = record_id_to_task_id(&record_id)?;
        let queue = self.job_type.ok_or(Error::ColumnNotFound("job_type"))?;
        let status_str = self.status.ok_or(Error::ColumnNotFound("status"))?;
        let status = Status::from_str(&status_str).map_err(Error::StatusError)?;
        let attempts = self.attempts.ok_or(Error::ColumnNotFound("attempts"))? as usize;
        let max_attempts = self.max_attempts.map(|v| v as usize).unwrap_or(25);
        let run_at = self.run_at.ok_or(Error::ColumnNotFound("run_at"))?.timestamp().max(0) as u64;
        let lock_at = self.lock_at.map(|dt| dt.timestamp().max(0) as u64);
        let done_at = self.done_at.map(|dt| dt.timestamp().max(0) as u64);
        let priority = self.priority.map(|v| v.max(0) as usize).unwrap_or_default();
        let metadata = self.metadata.map(MetadataStore::from_map).unwrap_or_default();

        let mut builder = TaskBuilder::new(job_bytes)
            .task_id(task_id)
            .queue(queue.into())
            .status(status)
            .attempt(attempts)
            .max_attempts(max_attempts)
            .run_at_timestamp(run_at)
            .lock_at(lock_at)
            .done_at(done_at)
            .lock_by(self.lock_by)
            .priority(priority)
            .with_metadata(metadata);

        if let Some(idempotency_key) = self.idempotency_key {
            builder = builder.idempotency_key(idempotency_key);
        }

        Ok(builder.build())
    }
}
