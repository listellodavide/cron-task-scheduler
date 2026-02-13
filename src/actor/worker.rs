use crate::models::{ExecutionPolicy, ReactiveTask, TaskContext};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;
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
    is_running: Arc<AtomicBool>,
}

impl WorkerActor {
    pub fn new(receiver: mpsc::Receiver<WorkerMessage>) -> Self {
        Self {
            receiver,
            is_running: Arc::new(AtomicBool::new(false)),
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
        &self,
        task: Arc<dyn ReactiveTask>,
        context: TaskContext,
        policy: ExecutionPolicy,
    ) {
        let is_running = self.is_running.clone();

        match policy {
            ExecutionPolicy::SkipIfRunning => {
                if is_running.load(Ordering::SeqCst) {
                    warn!("Task {} already running, skipping execution", task.id());
                    return;
                }
            }
            ExecutionPolicy::Parallel => {
                // Parallel: we just spawn and don't care about is_running state for locking
            }
            ExecutionPolicy::Sequential => {
                // For a simple worker actor with one receiver, sequential is natural if we await
                // but we want to be able to receive next messages while one is running if we spawn.
                // However, the prompt mentions simple actor behavior.
            }
        }

        tokio::spawn(async move {
            is_running.store(true, Ordering::SeqCst);
            info!("Executing task: {}", task.id());

            match task.execute(context).await {
                Ok(_) => info!("Task {} completed successfully", task.id()),
                Err(e) => error!("Task {} failed: {}", task.id(), e),
            }

            is_running.store(false, Ordering::SeqCst);
        });
    }
}
