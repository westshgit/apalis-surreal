use apalis_core::task::{status::StatusError, task_id::TaskIdError};

/// Represents a wrapper for errors encountered in `apalis-surreal`.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// SurrealDB engine error
    #[error(transparent)]
    Surreal(#[from] surrealdb::Error),

    /// Error handling JSON payloads
    #[error("JsonError: {0}")]
    JsonError(#[from] serde_json::Error),

    /// Re-enqueue mismatch error when abandoned count differs from database update count
    #[error("ReenqueueMismatch: Queued [{queued}], Abandoned [{abandoned}]")]
    ReenqueueMismatch {
        /// The count returned by the database
        queued:    usize,
        /// The worker's buffered abandoned task count
        abandoned: usize,
    },

    /// Missing required column/field when converting a SurrealDB record to an Apalis task
    #[error("ColumnNotFound: {0}")]
    ColumnNotFound(&'static str),

    /// Error decoding a `TaskId`
    #[error("TaskIdError: {0}")]
    TaskIdError(#[from] TaskIdError),

    /// Error decoding a task `Status`
    #[error("StatusError: {0}")]
    StatusError(#[from] StatusError),

    /// Worker was removed or out of sync in the database
    #[error("WorkerOutOfSync")]
    WorkerOutOfSync,

    /// Tried to register a worker that already exists
    #[error("WorkerAlreadyExists: {0}")]
    WorkerAlreadyExists(String),
}
