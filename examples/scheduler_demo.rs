use cronscheduler::{ExecutionPolicy, HttpTask, SchedulerActor, SchedulingPolicy, SimpleLoggingTask, WorkerActor};
use reqwest::Client;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    let logical = num_cpus::get();
    let physical = num_cpus::get_physical();
    println!("Logical cores: {logical}, Physical cores: {physical}");


    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Create worker actor
    let (worker_tx, worker_rx) = mpsc::channel(100);
    let worker = WorkerActor::new(worker_rx);

    // Start worker in background
    tokio::spawn(async move {
        worker.run().await;
    });

    // Create scheduler actor
    let mut scheduler = SchedulerActor::new(worker_tx);

    // Add tasks
    let http_task = Arc::new(HttpTask {
        id: "fetch-users".to_string(),
        url: "https://jsonplaceholder.typicode.com/todos/1".to_string(),
        client: Client::new(),
    });

    // Run every 2 seconds
    scheduler.add_task(http_task, "*/2 * * * * *", ExecutionPolicy::SkipIfRunning, SchedulingPolicy::FIFO, 0)?;

    let log_task = Arc::new(SimpleLoggingTask {
        id: "heartbeat".to_string(),
    });

    // Run every 5 seconds
    scheduler.add_task(log_task, "*/5 * * * * *", ExecutionPolicy::Sequential, SchedulingPolicy::FIFO, 0)?;

    // Start scheduling
    scheduler.start_all().await;

    // Keep the main task alive
    tokio::signal::ctrl_c().await?;
    Ok(())
}
