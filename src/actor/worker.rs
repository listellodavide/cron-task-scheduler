use crate::models::{ExecutionPolicy, ReactiveTask, TaskContext};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tracing::{error, info, warn};

pub enum WorkerMessage {
    Execute {
        task: Arc<dyn ReactiveTask>,
        context: TaskContext,
        policy: ExecutionPolicy,
    },
}

pub struct WorkerActor {
    receiver: mpsc::Receiver<WorkerMessage>,
    task_locks: HashMap<String, Arc<Mutex<()>>>,
}

impl WorkerActor {
    pub fn new(receiver: mpsc::Receiver<WorkerMessage>) -> Self {
        Self {
            receiver,
            task_locks: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        info!("Worker actor started");
        while let Some(msg) = self.receiver.recv().await {
            match msg {
                WorkerMessage::Execute {
                    task,
                    context,
                    policy,
                } => {
                    self.handle_execute(task, context, policy).await;
                }
            }
        }
        info!("Worker actor stopped");
    }

    async fn handle_execute(
        &mut self,
        task: Arc<dyn ReactiveTask>,
        context: TaskContext,
        policy: ExecutionPolicy,
    ) {
        let task_id = task.id().to_string();
        // Get or create the mutex for this task ID
        let lock = self
            .task_locks
            .entry(task_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();

        match policy {
            ExecutionPolicy::SkipIfRunning => {
                // Try to acquire the lock immediately using try_lock_owned to get an OwnedMutexGuard
                if let Ok(guard) = lock.try_lock_owned() {
                    let task = task.clone();
                    tokio::spawn(async move {
                        // The guard is held as long as this variable is in scope
                        let _guard = guard;
                        info!("Executing task (skip-if-running): {}", task.id());
                        match task.execute(context).await {
                            Ok(_) => info!("Task {} completed successfully", task.id()),
                            Err(e) => error!("Task {} failed: {}", task.id(), e),
                        }
                    });
                } else {
                    warn!("Task {} already running, skipping execution", task.id());
                }
            }
            ExecutionPolicy::Parallel => {
                // Parallel: we just spawn and don't care about locking
                tokio::spawn(async move {
                    info!("Executing task (parallel): {}", task.id());
                    match task.execute(context).await {
                        Ok(_) => info!("Task {} completed successfully", task.id()),
                        Err(e) => error!("Task {} failed: {}", task.id(), e),
                    }
                });
            }
            ExecutionPolicy::Sequential => {
                // Sequential: spawn and wait for the lock
                tokio::spawn(async move {
                    // Wait for the lock using lock_owned to keep the guard across await in a spawned task
                    let _guard = lock.lock_owned().await;
                    info!("Executing task (sequential): {}", task.id());
                    match task.execute(context).await {
                        Ok(_) => info!("Task {} completed successfully", task.id()),
                        Err(e) => error!("Task {} failed: {}", task.id(), e),
                    }
                });
            }
        }
    }
}
