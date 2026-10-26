use std::collections::HashSet;

use apalis_core::{backend::persistence::{Persistence, TaskEvent},
                  task::Task,
                  worker::context::WorkerContext};
use chrono::Utc;
use serde_json::Value;
use surrealdb::{Connection, Surreal, engine::local::Db};

use crate::{Error,
            config::Config,
            queries::{self, fetch_next, keep_alive, reenqueue_abandoned, reenqueue_orphaned, register_worker},
            sink::push_tasks};

#[derive(Debug, new, Clone, TypedBuilder)]
pub struct SurrealPersistence<C: Connection = Db> {
    pub(crate) db:     Surreal<C>,
    #[builder(default)]
    pub(crate) config: Config,
}

impl<C> Persistence for SurrealPersistence<C>
where
    C: Connection + Clone + Send + Sync + 'static,
{
    type Compact = Vec<u8>;
    type Error = Error;
    type Response = Value;

    async fn register(&mut self, worker: &WorkerContext) -> Result<(), Self::Error> {
        let dead_for = self.config.orphaned_duration().as_secs();
        let queue = self.config.queue.as_ref();
        let count = reenqueue_orphaned(&self.db, queue, dead_for).await?;
        let now = Utc::now();
        register_worker(&self.db, queue, worker, now, "SurrealStorage").await?;
        if count > 0 {
            tracing::debug!("{count} Re-enqueued orphaned tasks by worker {}", worker.name());
        }
        tracing::debug!("Registered Worker: {}", worker.name());
        Ok(())
    }

    async fn heartbeat(&mut self, worker: &WorkerContext) -> Result<(), Self::Error> {
        let queue = self.config.queue.as_ref();
        let dead_for = self.config.orphaned_duration().as_secs();
        keep_alive(&self.db, queue, worker).await?;
        let count = reenqueue_orphaned(&self.db, queue, dead_for).await?;
        if count > 0 {
            tracing::debug!("Re-enqueued {count} orphaned tasks by worker {}", worker.name());
        }
        Ok(())
    }

    async fn fetch_next(&mut self, worker: &WorkerContext) -> Result<Vec<Task<Self::Compact>>, Self::Error> {
        fetch_next(&self.db, &self.config, worker).await
    }

    async fn reenqueue_abandoned(&mut self, tasks: Vec<Task<Self::Compact>>, worker: &WorkerContext) -> Result<u64, Self::Error> {
        let task_ids = tasks
            .iter()
            .filter_map(|t| t.task_id().map(ToString::to_string))
            .collect::<Vec<_>>();
        let queue = self.config.queue.as_ref();
        let count = reenqueue_abandoned(&self.db, queue, worker.name(), &task_ids).await?;
        if count as usize != tasks.len() {
            return Err(Error::ReenqueueMismatch { queued: tasks.len(), abandoned: count as usize });
        }
        Ok(count)
    }

    async fn handle_events(&mut self, messages: Vec<TaskEvent<Self::Response>>, worker: &WorkerContext) -> Result<(), Self::Error> {
        let mut lock_ids = messages
            .iter()
            .filter_map(|msg| {
                if let TaskEvent::Lock { task_id, .. } = msg {
                    Some(task_id.to_string())
                }
                else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let ack_payloads = messages
            .iter()
            .filter_map(|msg| {
                if let TaskEvent::Complete(payload) = msg {
                    Some(payload)
                }
                else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if lock_ids.is_empty() && ack_payloads.is_empty() {
            return Ok(());
        }

        tracing::debug!("Processing {} messages ({} locks, {} acks)", messages.len(), lock_ids.len(), ack_payloads.len());

        let ack_ids: HashSet<String> = ack_payloads.iter().map(|s| s.task_id().to_string()).collect();
        lock_ids.retain(|id| !ack_ids.contains(id));

        if !ack_payloads.is_empty() {
            queries::handle_results(&self.db, &ack_payloads, worker.name()).await?;
        }
        if !lock_ids.is_empty() {
            queries::lock_tasks(&self.db, &lock_ids, worker.name()).await?;
        }

        Ok(())
    }

    async fn push_tasks(&mut self, tasks: Vec<Task<Vec<u8>>>) -> Result<(), Self::Error> {
        let queue = self.config.queue.as_ref();
        push_tasks(&self.db, queue, tasks).await
    }
}
