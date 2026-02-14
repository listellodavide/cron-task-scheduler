use cronscheduler::{CommandLineTask, ExecutionPolicy, HttpTask, SchedulerActor, SchedulingPolicy, SimpleLoggingTask, WorkerActor};
use reqwest::Client;
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let logical = num_cpus::get();
    let physical = num_cpus::get_physical();
    println!("Logical cores: {logical}, Physical cores: {physical}");

    // Create a worker actor channel
    let (worker_tx, worker_rx) = mpsc::channel(100);

    // Create and start a worker actor
    let worker = WorkerActor::new(worker_rx);
    tokio::spawn(async move {
        worker.run().await;
    });

    // Create a scheduler actor
    let mut scheduler = SchedulerActor::new(worker_tx);

    // 1. Add an Async HTTP Task
    let http_task = Arc::new(HttpTask {
        id: "fetch-users".to_string(),
        url: "https://jsonplaceholder.typicode.com/todos/1".to_string(),
        client: Client::new(),
    });
    // Run every 2 seconds
    scheduler.add_task(
        http_task,
        "*/2 * * * * *",
        ExecutionPolicy::SkipIfRunning,
        SchedulingPolicy::FirstInFirstOut,
        0
    )?;

    // 2. Add an Async Logging Task
    let log_task = Arc::new(SimpleLoggingTask {
        id: "heartbeat".to_string(),
    });
    // Run every 5 seconds
    scheduler.add_task(
        log_task,
        "*/5 * * * * *",
        ExecutionPolicy::Sequential,
        SchedulingPolicy::FirstInFirstOut,
        0
    )?;

    // 3. Add a Blocking Command Line Task
    let ping_task = Arc::new(CommandLineTask {
        id: "ping-google".to_string(),
        command: "ping".to_string(),
        args: vec!["-c".to_string(), "1".to_string(), "8.8.8.8".to_string()],
    });
    // Run every 10 seconds
    scheduler.add_task(
        ping_task,
        "*/30 * * * * *",
        ExecutionPolicy::SkipIfRunning,
        SchedulingPolicy::FirstInFirstOut,
        0
    )?;

    // Start scheduling
    scheduler.start_all().await;

    // Keep the main task alive
    tokio::signal::ctrl_c().await?;
    Ok(())
}
