#![cfg_attr(debug_assertions, allow(unused_variables, unused_imports, dead_code))]
#![allow(non_camel_case_types)]
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]
#![warn(clippy::unwrap_used)]

use std::{
    marker::PhantomData,
    task::{Context, Poll},
};

use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::{
        Backend, BackendConfig, WireFormatBackend,
        ext::poll_strategy::{PollWith, StreamStrategy},
        finalize::Durable,
        persistence::{Persisted, TaskPersistLayer},
    },
    task::{Task, task_id::TaskId},
    worker::context::WorkerContext,
};
use serde_json::Value;
use surrealdb::{Connection, Surreal};
use uuid::Uuid;

#[macro_use]
extern crate tracing;
#[macro_use]
extern crate derive_new;
#[macro_use]
extern crate derive_more;
#[macro_use]
extern crate typed_builder;

mod config;
mod error;
pub mod factory;
mod persistence;
mod pubsub;
pub mod queries;
mod sink;
mod surreal_task;

pub use crate::{
    config::Config,
    error::Error,
    factory::{SharedFetcher, SurrealFactoryError, SurrealStorageFactory},
    persistence::SurrealPersistence,
    pubsub::{InsertEvent, Pubsub},
    surreal_task::{JOBS_TABLE, SurrealTaskRow, WORKERS_TABLE},
};

/// A backend for persisting and consuming jobs behind a SurrealDB database.
#[pin_project::pin_project]
#[derive(Debug, TypedBuilder)]
pub struct SurrealStorage<Args, C>
where
    C: Connection + Clone,
{
    #[pin]
    pub(crate) p: Persisted<SurrealPersistence<C>>,
    #[builder(default)]
    codec: JsonCodec,
    #[builder(default)]
    _marker: PhantomData<Args>,
}

impl<Args, C: Connection + Clone> Clone for SurrealStorage<Args, C> {
    fn clone(&self) -> Self {
        Self {
            p: self.p.clone(),
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl<Args, T> SurrealStorage<Args, T>
where
    T: Connection + Clone,
{
    /// Runs the SurrealDB schema setup (`DEFINE TABLE` and `DEFINE INDEX` statements)
    /// for `apalis_jobs` and `apalis_workers`.
    pub async fn setup<C: Connection>(db: &Surreal<C>) -> Result<(), Error> {
        queries::setup(db).await
    }
}

impl<Args, C> SurrealStorage<Args, C>
where
    C: Connection + Clone + Send + Sync + 'static,
{
    /// Creates a new `SurrealStorage` instance backed by a SurrealDB connection handle.
    #[must_use]
    pub fn new(db: Surreal<C>) -> Self {
        let config = Config::default().queue(std::any::type_name::<Args>());
        let p = Persisted::new(SurrealPersistence::new(db, config));
        Self {
            p,
            codec: JsonCodec::default(),
            _marker: PhantomData,
        }
    }

    /// Mount a standalone [`Pubsub`] listener powered by SurrealDB Live Queries (`LIVE SELECT`).
    #[must_use]
    pub fn with_pubsub(self) -> PollWith<Self, StreamStrategy<Pubsub<C>>>
    where
        C: Unpin,
    {
        let db = self.db().clone();
        let namespace = self.config().queue.to_string();
        PollWith::new(self, StreamStrategy::new(Pubsub::new(db, namespace)))
    }

    /// Configure a `SurrealStorage` instance with custom [`Config`].
    #[must_use]
    pub fn with_config(mut self, config: Config) -> Self {
        self.p.config = config;
        self
    }

    /// Returns a reference to the underlying SurrealDB connection handle.
    #[must_use]
    pub fn db(&self) -> &Surreal<C> {
        &self.p.db
    }

    /// Returns a reference to the [`Persisted`] wrapper.
    #[must_use]
    pub fn persistence(&self) -> &Persisted<SurrealPersistence<C>> {
        &self.p
    }

    /// Returns a reference to the [`Config`].
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.p.config
    }
}

impl<Args, C> Backend for SurrealStorage<Args, C>
where
    C: Connection + Clone + Send + Sync + 'static,
{
    type Error = Error;
    type Task = Task<Vec<u8>>;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.p
            .poll_ready(cx, worker, self.config().heartbeat_interval)
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        self.p.poll_next(cx, worker)
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.p.poll_close(cx, worker)
    }
}

impl<Args, C> BackendConfig for SurrealStorage<Args, C>
where
    C: Connection + Clone + Send + Sync + 'static,
{
    type Args = Args;
    type Config = Config;
    type Id = Uuid;
    type Kind = Durable;
    type Layer = TaskPersistLayer<JsonCodec<Value>, Value>;

    fn config(&self) -> &Self::Config {
        &self.p.config
    }

    fn middleware(&mut self, _: &mut WorkerContext) -> Self::Layer {
        self.p
            .layer(JsonCodec::<Value>::default(), self.config().batch_size)
            .persist_results(self.config().persist_results)
            .lock_tasks(self.config().lock_tasks)
    }
}

impl<Args, C> WireFormatBackend for SurrealStorage<Args, C>
where
    C: Connection + Clone + Send + Sync + 'static,
{
    type Codec = JsonCodec;
    type Compact = Vec<u8>;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use apalis::prelude::*;
    use apalis_core::backend::{ListQueues, TaskSink};
    use apalis_workflow::SteppedFlow;
    use futures::{StreamExt, stream};
    use serde::{Deserialize, Serialize};
    use surrealdb::{
        Surreal,
        engine::local::{Db, Mem},
    };

    use super::*;

    async fn setup_mem_db() -> anyhow::Result<Surreal<Db>> {
        let db = Surreal::new::<Mem>(()).await?;
        db.use_ns("test").use_db("test").await?;
        SurrealStorage::<(), Db>::setup(&db).await?;
        Ok(db)
    }

    #[tokio::test]
    async fn basic_worker() -> anyhow::Result<()> {
        let db = setup_mem_db().await?;
        let config = Config::default()
            .queue("sample")
            .batch_size(50)
            .lock_tasks(false);
        let mut backend = SurrealStorage::new(db).with_config(config);

        let mut items = stream::repeat_with(HashMap::default).take(1);
        backend.push_stream(&mut items).await?;

        async fn send_reminder(
            _: HashMap<String, String>,
            wrk: WorkerContext,
        ) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_millis(200)).await;
            wrk.stop().expect("stop worker");
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango-1")
            .backend(backend)
            .build(send_reminder);
        worker.run().await.expect("worker run");

        Ok(())
    }

    #[tokio::test]
    async fn notify_worker_with_live_query() -> anyhow::Result<()> {
        let db = setup_mem_db().await?;
        let config = Config::default()
            .queue("test_live")
            .persist_results(true)
            .lock_tasks(false)
            .batch_size(20);
        let backend = SurrealStorage::new(db).with_config(config).with_pubsub();

        let mut b = backend.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let task = TaskBuilder::new(42u32).priority(1).build();
            b.push_task(task).await?;

            Ok::<_, anyhow::Error>(())
        });

        async fn send_reminder(_: u32, wrk: WorkerContext) -> Result<(), BoxDynError> {
            wrk.stop().expect("stop worker");
            Ok(())
        }

        let ctx = WorkerContext::new("rango-tango-2");
        let worker = WorkerBuilder::new(&ctx)
            .backend(backend)
            .build(send_reminder);
        worker.run().await.expect("worker run");
        let run_for = ctx.elapsed();
        assert!(
            run_for < Duration::from_secs(3),
            "Worker did not use SurrealDB LIVE SELECT mechanism"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_workflow_complete() -> anyhow::Result<()> {
        #[derive(Debug, Serialize, Deserialize, Clone)]
        struct PipelineConfig {
            min_confidence: f32,
            enable_sentiment: bool,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct UserInput {
            text: String,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct Classified {
            text: String,
            label: String,
            confidence: f32,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct Summary {
            text: String,
            sentiment: Option<String>,
        }

        let workflow = SteppedFlow::new("text-pipeline")
            .and_then(|input: UserInput, worker: WorkerContext| async move {
                worker.emit(format!("Preprocessing input: {}", input.text));
                let processed = input.text.to_lowercase();
                Ok::<_, BoxDynError>(processed)
            })
            .and_then(|text: String| async move {
                let confidence = 0.85;
                let items = text.split_whitespace().collect::<Vec<_>>();
                let results = items
                    .into_iter()
                    .map(|x| Classified {
                        text: x.to_owned(),
                        label: if x.contains("rust") {
                            "Tech"
                        } else {
                            "General"
                        }
                        .to_owned(),
                        confidence,
                    })
                    .collect::<Vec<_>>();
                Ok::<_, BoxDynError>(results)
            })
            .filter_map(
                |c: Classified| async move { if c.confidence >= 0.6 { Some(c) } else { None } },
            )
            .filter_map(move |c: Classified, config: Data<PipelineConfig>| {
                let cfg = config.enable_sentiment;
                async move {
                    if !cfg {
                        return Some(Summary {
                            text: c.text,
                            sentiment: None,
                        });
                    }
                    let sentiment = if c.text.contains("delightful") {
                        "positive"
                    } else {
                        "neutral"
                    };
                    Some(Summary {
                        text: c.text,
                        sentiment: Some(sentiment.to_owned()),
                    })
                }
            })
            .and_then(|a: Vec<Summary>, worker: WorkerContext| async move {
                worker.emit(format!("Generated {} summaries", a.len()));
                worker.stop()
            });

        let db = setup_mem_db().await?;
        let config = Config::default().queue("test_workflow");
        let mut backend = SurrealStorage::new(db).with_config(config).with_pubsub();

        let input = UserInput {
            text: "Rust makes systems programming delightful!".to_owned(),
        };
        backend.push(input).await?;

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .data(PipelineConfig {
                min_confidence: 0.8,
                enable_sentiment: true,
            })
            .build(workflow);
        worker.run().await.expect("workflow run");

        Ok(())
    }

    #[tokio::test]
    async fn list_queues_reports_stats() -> anyhow::Result<()> {
        let db = setup_mem_db().await?;
        let queue = format!("stats-{}", Uuid::new_v4());
        let mut backend = SurrealStorage::new(db)
            .with_config(Config::default().queue(queue.clone()))
            .with_pubsub();

        backend.push_task(TaskBuilder::new(42u32).build()).await?;

        let queues = backend.list_queues().await?;
        let info = queues
            .iter()
            .find(|info| info.name == queue)
            .expect("queue is listed");

        let stat = |title: &str| {
            info.stats
                .iter()
                .find(|stat| stat.title == title)
                .unwrap_or_else(|| panic!("{title} is reported"))
                .value
                .clone()
        };

        assert_eq!(stat("TOTAL_JOBS"), "1");
        assert_eq!(stat("PENDING_JOBS"), "1");
        assert_eq!(stat("ACTIVE_JOBS"), "1");
        assert_eq!(stat("DONE_JOBS"), "0");
        assert_eq!(stat("JOBS_TODAY"), "1");
        assert!(info.activity.iter().sum::<usize>() >= 1);
        assert!(info.workers.is_empty());

        Ok(())
    }
}
