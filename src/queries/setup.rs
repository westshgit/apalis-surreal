use surrealdb::{Connection, Surreal};

use crate::error::Error;

/// SurrealQL schema definitions for `apalis-surreal`.
///
/// Defines `apalis_jobs` and `apalis_workers` tables with `SCHEMAFULL` field constraints,
/// default values, and secondary indexes for efficient polling, idempotency, and worker heartbeats.
pub const SETUP_SURQL: &str = r#"
DEFINE TABLE IF NOT EXISTS apalis_jobs SCHEMAFULL;

DEFINE FIELD IF NOT EXISTS job ON TABLE apalis_jobs TYPE bytes;
DEFINE FIELD IF NOT EXISTS job_type ON TABLE apalis_jobs TYPE string;
DEFINE FIELD IF NOT EXISTS status ON TABLE apalis_jobs TYPE string DEFAULT 'Pending';
DEFINE FIELD IF NOT EXISTS attempts ON TABLE apalis_jobs TYPE int DEFAULT 0;
DEFINE FIELD IF NOT EXISTS max_attempts ON TABLE apalis_jobs TYPE int DEFAULT 25;
DEFINE FIELD IF NOT EXISTS run_at ON TABLE apalis_jobs TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS last_result ON TABLE apalis_jobs TYPE any;
DEFINE FIELD IF NOT EXISTS lock_at ON TABLE apalis_jobs TYPE option<datetime>;
DEFINE FIELD IF NOT EXISTS lock_by ON TABLE apalis_jobs TYPE option<string>;
DEFINE FIELD IF NOT EXISTS done_at ON TABLE apalis_jobs TYPE option<datetime>;
DEFINE FIELD IF NOT EXISTS priority ON TABLE apalis_jobs TYPE int DEFAULT 0;
DEFINE FIELD IF NOT EXISTS idempotency_key ON TABLE apalis_jobs TYPE option<string>;
DEFINE FIELD IF NOT EXISTS metadata ON TABLE apalis_jobs TYPE option<object> FLEXIBLE;

DEFINE INDEX IF NOT EXISTS idx_apalis_jobs_poll ON TABLE apalis_jobs FIELDS job_type, status, priority, run_at;
DEFINE INDEX IF NOT EXISTS idx_apalis_jobs_lock_by ON TABLE apalis_jobs FIELDS lock_by, status;

DEFINE TABLE IF NOT EXISTS apalis_workers SCHEMAFULL;

DEFINE FIELD IF NOT EXISTS worker_type ON TABLE apalis_workers TYPE string;
DEFINE FIELD IF NOT EXISTS storage_name ON TABLE apalis_workers TYPE string;
DEFINE FIELD IF NOT EXISTS layers ON TABLE apalis_workers TYPE option<string>;
DEFINE FIELD IF NOT EXISTS last_seen ON TABLE apalis_workers TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS started_at ON TABLE apalis_workers TYPE option<datetime> DEFAULT time::now();

DEFINE INDEX IF NOT EXISTS idx_apalis_workers_type ON TABLE apalis_workers FIELDS worker_type, last_seen;
"#;

/// Executes the SurrealDB schema setup for `apalis_jobs` and `apalis_workers`.
///
/// This function is idempotent (`IF NOT EXISTS`) and safe to run on application startup.
pub async fn setup<C: Connection>(db: &Surreal<C>) -> Result<(), Error> {
    db.query(SETUP_SURQL).await?.check()?;
    Ok(())
}
