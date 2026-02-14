//! A high-performance async cron scheduler.
//!
//! # Example
//!
//! ```
//! use cronscheduler::{SchedulerActor, WorkerActor, HttpTask, SimpleLoggingTask, ExecutionPolicy, SchedulingPolicy};
//! use std::sync::Arc;
//! use tokio::sync::mpsc;
//! use reqwest::Client;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     let (worker_tx, worker_rx) = mpsc::channel(100);
//!     let worker = WorkerActor::new(worker_rx);
//!     tokio::spawn(async move { worker.run().await; });
//!
//!     let mut scheduler = SchedulerActor::new(worker_tx);
//!     let log_task = Arc::new(SimpleLoggingTask { id: "heartbeat".to_string() });
//!     scheduler.add_task(log_task, "*/5 * * * * *", ExecutionPolicy::Parallel, SchedulingPolicy::FirstInFirstOut, 0)?;
//!
//!     Ok(())
//! }
//! ```

pub mod actor;
pub mod cron_parser;
pub mod http_tasks;
pub mod models;

pub use actor::scheduler::SchedulerActor;
pub use actor::worker::WorkerActor;
pub use cron_parser::CronParser;
pub use http_tasks::{HttpTask, SimpleLoggingTask};
pub use models::{ExecutionPolicy, ReactiveTask, TaskContext, SchedulingPolicy};

/// A high performance async cron scheduler runner.
///
/// This is a convenience function to demonstrate library usage.
pub fn run() {
    println!("Cron Task Scheduler Library initialized.");
}
