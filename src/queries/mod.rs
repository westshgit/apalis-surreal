//! SurrealQL queries and trait implementations for polling, updating, and exposing tasks in
//! `apalis-surreal`.

mod fetch_by_id;
mod fetch_next;
mod handle_result;
mod keep_alive;
mod list_queues;
mod list_tasks;
mod list_workers;
mod lock_task;
mod metrics;
mod reenqueue_orphaned;
mod register_worker;
pub mod setup;
mod wait_for;

pub use crate::{queries::{fetch_next::fetch_next,
                          handle_result::{Payload as ResultPayload, handle_results},
                          keep_alive::keep_alive,
                          lock_task::lock_tasks,
                          reenqueue_orphaned::{reenqueue_abandoned, reenqueue_orphaned},
                          register_worker::register_worker,
                          setup::setup},
                sink::push_tasks};
