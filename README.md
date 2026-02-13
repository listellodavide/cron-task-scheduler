# My Library

A high performance async cron scheduler for Rust.

## Features

- **Async First**: Built on top of Tokio.
- **Actor Model**: Uses actors for task scheduling and execution isolation.
- **Cron Expressions**: Supports standard cron expressions via the `cron` crate.
- **Execution Policies**: Control how tasks behave when already running (Skip, Parallel, etc.).

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
cronscheduler = "0.1.5"
```

## Quick Start

```rust
use cronscheduler::{SchedulerActor, WorkerActor, SimpleLoggingTask, ExecutionPolicy};
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Create worker
    let (worker_tx, worker_rx) = mpsc::channel(100);
    let worker = WorkerActor::new(worker_rx);
    tokio::spawn(async move {
        worker.run().await;
    });

    // Create scheduler
    let mut scheduler = SchedulerActor::new(worker_tx);

    // Add a simple task
    let log_task = Arc::new(SimpleLoggingTask {
        id: "heartbeat".to_string(),
    });

    scheduler.add_task(log_task, "*/5 * * * * *", ExecutionPolicy::Parallel)?;

    // Start scheduling
    scheduler.start_all().await;

    // Wait for Ctrl-C
    tokio::signal::ctrl_c().await?;
    Ok(())
}
```

## License

Licensed under Apache License, Version 2.0.
