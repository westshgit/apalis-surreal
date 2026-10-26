use std::{pin::Pin,
          sync::Mutex,
          task::{Context, Poll}};

use apalis_core::{backend::future::BoxSyncFuture, task::task_id::TaskId};
use futures::{Stream, StreamExt, stream::BoxStream};
use serde::{Deserialize, Serialize};
use surrealdb::{Connection,
                Notification,
                Surreal,
                types::{Action, RecordId, SurrealValue, vars}};

use crate::{error::Error,
            surreal_task::{JOBS_TABLE, record_id_to_task_id}};

/// Event payload emitted by SurrealDB Live Queries when a job is inserted or updated in
/// `apalis_jobs`.
#[derive(Debug, Clone, SurrealValue, Serialize, Deserialize)]
pub struct InsertEvent {
    pub id:       RecordId,
    pub job_type: String,
    pub status:   Option<String>,
}

type LiveNotificationStream = BoxStream<'static, Result<Notification<InsertEvent>, surrealdb::Error>>;

/// A standalone SurrealDB Live Query listener for `apalis_jobs`.
pub struct Pubsub<C: Connection> {
    state:     State<C>,
    listener:  Option<Mutex<LiveNotificationStream>>,
    db:        Surreal<C>,
    namespace: String,
}

impl<C: Connection> Clone for Pubsub<C> {
    fn clone(&self) -> Self {
        Self {
            state:     State::Starting,
            listener:  None,
            db:        self.db.clone(),
            namespace: self.namespace.clone(),
        }
    }
}

impl<C: Connection> Pubsub<C> {
    #[must_use]
    pub fn new(db: Surreal<C>, namespace: String) -> Self {
        Self { state: State::Starting, listener: None, db, namespace }
    }
}

enum State<C: Connection> {
    Starting,
    Connecting {
        fut: BoxSyncFuture<Result<(Surreal<C>, LiveNotificationStream), Error>>,
    },
    Listening,
    Closed,
}

impl<C> Stream for Pubsub<C>
where
    C: Connection + Send + Sync + Unpin + 'static,
{
    type Item = Result<TaskId, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match &mut this.state {
            State::Starting => {
                let db = this.db.clone();
                let namespace = this.namespace.clone();
                let fut = Box::pin(async move {
                    let mut response = db
                        .query(format!("LIVE SELECT id, job_type, status FROM {JOBS_TABLE} WHERE job_type = $queue"))
                        .bind(vars! { "queue": namespace })
                        .await?
                        .check()?;
                    let stream = response.stream::<Notification<InsertEvent>>(0)?;
                    Ok((db, stream.boxed()))
                });
                this.state = State::Connecting { fut: BoxSyncFuture::new(fut) };
            },
            State::Connecting { fut } => {
                let (db, stream) = match fut.poll_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => {
                    this.state = State::Closed;
                    return Poll::Ready(Some(Err(err)));
                },
                Poll::Ready(Ok(res)) => res,
                };

                this.db = db;
                this.listener = Some(Mutex::new(stream));
                this.state = State::Listening;
            },
            State::Listening => {
                let Some(listener) = this.listener.as_mut()
                else {
                    this.state = State::Closed;
                    return Poll::Ready(None);
                };

                let Ok(stream) = listener.get_mut()
                else {
                    this.state = State::Closed;
                    return Poll::Ready(None);
                };

                match stream.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    this.state = State::Closed;
                    return Poll::Ready(None);
                },
                Poll::Ready(Some(Err(err))) => {
                    warn!(error = ?err, "SurrealDB live query notification error");
                    continue;
                },
                Poll::Ready(Some(Ok(notification))) => {
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

                    if notification.data.job_type != this.namespace {
                        continue;
                    }

                    if let Ok(task_id) = record_id_to_task_id(&notification.data.id) {
                        return Poll::Ready(Some(Ok(task_id)));
                    }
                },
                }
            },
            State::Closed => {
                return Poll::Ready(None);
            },
            }
        }
    }
}
