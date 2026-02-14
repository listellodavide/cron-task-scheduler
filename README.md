# Cron Task Scheduler Library 'cronscheduler'

A high-performance, async-first cron scheduler for Rust, designed for reliability and performance.

## Features

-   **Async & Blocking Task Support**: Natively handles both pure `async` tasks and CPU-heavy `blocking` tasks in separate, managed thread pools.
-   **Actor Model**: Built with the actor pattern for robust, concurrent task management.
-   **Cron Expressions**: Standard cron syntax (`* * * * * *`) for flexible scheduling.
-   **Fine-Grained Control**:
    -   **Execution Policies**: `SkipIfRunning`, `Parallel`, `Sequential`.
    -   **Scheduling Policies**: `FirstInFirstOut`, `Priority`, `Fair`, and more.
-   **Extensible**: Easily define your own tasks by implementing the `ReactiveTask` trait.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
cron-task-scheduler = "0.1.8" # Check for the latest version on crates.io
```

## Quick Start

Here's a complete example demonstrating async and blocking tasks.

```rust
use cronscheduler::{
    SchedulerActor, WorkerActor, HttpTask, SimpleLoggingTask, CommandLineTask,
    ExecutionPolicy, SchedulingPolicy
};
use reqwest::Client;
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // 1. Create a worker actor
    let (worker_tx, worker_rx) = mpsc::channel(100);
    let worker = WorkerActor::new(worker_rx);
    tokio::spawn(async move {
        worker.run().await;
    });

    // 2. Create a scheduler
    let mut scheduler = SchedulerActor::new(worker_tx);

    // 3. Add tasks

    // An async HTTP task to run every 2 seconds
    let http_task = Arc::new(HttpTask {
        id: "fetch-api".to_string(),
        url: "https://jsonplaceholder.typicode.com/todos/1".to_string(),
        client: Client::new(),
    });
    scheduler.add_task(
        http_task,
        "*/2 * * * * *",
        ExecutionPolicy::SkipIfRunning,
        SchedulingPolicy::FirstInFirstOut,
        0,
    )?;

    // A simple async logging task to run every 5 seconds
    let log_task = Arc::new(SimpleLoggingTask {
        id: "heartbeat".to_string(),
    });
    scheduler.add_task(
        log_task,
        "*/5 * * * * *",
        ExecutionPolicy::Sequential,
        SchedulingPolicy::FirstInFirstOut,
        0,
    )?;

    // A blocking command-line task to run every 10 seconds
    let ping_task = Arc::new(CommandLineTask {
        id: "ping-google".to_string(),
        command: "ping".to_string(),
        args: vec!["-c".to_string(), "1".to_string(), "8.8.8.8".to_string()],
    });
    scheduler.add_task(
        ping_task,
        "*/20 * * * * *",
        ExecutionPolicy::SkipIfRunning,
        SchedulingPolicy::FirstInFirstOut,
        0,
    )?;

    // 4. Start the scheduler
    scheduler.start_all().await;

    // Wait for Ctrl-C to shut down
    tokio::signal::ctrl_c().await?;
    Ok(())
}
```

## License

Licensed under SSPL-1.0.
