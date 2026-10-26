//! SurrealDB backend factory with shared Live Query notification routing.
//!
//! [`SurrealStorageFactory`] creates independent [`SurrealStorage`] backends while sharing a single
//! SurrealDB `LIVE SELECT` subscription on `apalis_jobs`. Each backend is registered by its queue
//! name and receives task IDs for newly inserted jobs belonging to that queue.

use std::{collections::HashMap,
          pin::Pin,
          sync::Arc,
          task::{Context, Poll}};

use apalis_core::{backend::{BackendConfig,
                            ext::{BackendExt,
                                  poll_strategy::{PollWith, StreamStrategy}},
                            factory::BackendFactory},
                  task::task_id::TaskId};
use futures::{FutureExt,
              SinkExt,
              Stream,
              StreamExt,
              channel::mpsc::{self, Receiver, Sender},
              future::{BoxFuture, Shared},
              lock::Mutex};
use surrealdb::{Connection, Notification, Surreal, types::Action};

use crate::{SurrealStorage,
            config::Config,
            pubsub::InsertEvent,
            surreal_task::{JOBS_TABLE, record_id_to_task_id}};

/// A factory for creating SurrealDB-backed task queues sharing one Live Query stream.
pub struct SurrealStorageFactory<C: Connection> {
    db:       Surreal<C>,
    registry: Arc<Mutex<HashMap<String, Sender<TaskId>>>>,
    drive:    Shared<BoxFuture<'static, ()>>,
}

impl<C> SurrealStorageFactory<C>
where
    C: Connection + Clone + Send + Sync + 'static,
{
    /// Creates a new factory backed by the given SurrealDB connection handle.
    #[must_use]
    pub fn new(db: Surreal<C>) -> Self {
        let registry: Arc<Mutex<HashMap<String, Sender<TaskId>>>> = Arc::new(Mutex::new(HashMap::default()));
        let d = db.clone();
        let instances = registry.clone();

        let drive = async move {
            let Ok(mut response) = d.query(format!("LIVE SELECT id, job_type, status FROM {JOBS_TABLE}")).await
            else {
                return;
            };
            let Ok(mut stream) = response.stream::<Notification<InsertEvent>>(0)
            else {
                return;
            };

            while let Some(notification) = stream.next().await {
                let Ok(notification) = notification
                else {
                    continue;
                };
                match notification.action {
                Action::Create => {},
                Action::Update => {
                    let status = notification.data.status.as_deref().unwrap_or_default();
                    if status != "Pending" && status != "Failed" {
                        continue;
                    }
                },
                _ => continue,
                }

                let Ok(task_id) = record_id_to_task_id(&notification.data.id)
                else {
                    continue;
                };

                let mut instances = instances.lock().await;
                if let Some(sender) = instances.get_mut(&notification.data.job_type) {
                    let _ = sender.send(task_id).await;
                }
            }
        }
        .boxed()
        .shared();

        Self { db, registry, drive }
    }
}

/// Errors returned when creating a SurrealDB backend from a [`SurrealStorageFactory`].
#[derive(Debug, thiserror::Error)]
pub enum SurrealFactoryError {
    /// Namespace already registered in the factory
    #[error("namespace already exists: {0}")]
    NamespaceExists(String),

    /// Registry is currently locked
    #[error("registry locked")]
    RegistryLocked,
}

impl<Args, C> BackendFactory<Args> for SurrealStorageFactory<C>
where
    C: Connection + Clone + Send + Sync + Unpin + 'static,
{
    type Backend = PollWith<SurrealStorage<Args, C>, StreamStrategy<SharedFetcher>>;
    type Error = SurrealFactoryError;

    fn create(&mut self) -> Result<Self::Backend, Self::Error>
    where
        <Self::Backend as BackendConfig>::Config: Default,
    {
        self.create_with_config(Config::default().queue(std::any::type_name::<Args>()))
    }

    fn create_with_config(&mut self, config: Config) -> Result<Self::Backend, Self::Error> {
        let mut registry = self.registry.try_lock().ok_or(SurrealFactoryError::RegistryLocked)?;

        let channel_cap = (config.batch_size * (registry.len() + 1)).max(16);
        let (tx, rx) = mpsc::channel(channel_cap);
        if registry.insert(config.queue.to_string(), tx).is_some() {
            return Err(SurrealFactoryError::NamespaceExists(config.queue.to_string()));
        }

        Ok(SurrealStorage::new(self.db.clone())
            .with_config(config)
            .poll_with_stream(SharedFetcher { poller: self.drive.clone(), receiver: Arc::new(Mutex::new(rx)) }))
    }
}

/// A stream of task IDs received from the shared SurrealDB Live Query listener.
#[derive(Clone, Debug)]
pub struct SharedFetcher {
    poller:   Shared<BoxFuture<'static, ()>>,
    receiver: Arc<Mutex<Receiver<TaskId>>>,
}

impl Stream for SharedFetcher {
    type Item = TaskId;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let _ = this.poller.poll_unpin(cx);

        let mut receiver = this.receiver.try_lock();
        if let Some(ref mut rx) = receiver {
            rx.poll_next_unpin(cx)
        }
        else {
            Poll::Pending
        }
    }
}
