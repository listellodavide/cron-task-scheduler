use crate::models::{ExecutionPolicy, ReactiveTask, SchedulingPolicy, TaskContext};
use chrono::{DateTime, Utc};
use priority_queue::PriorityQueue;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Semaphore};
use tracing::{error, info, warn};
use uuid::Uuid;

pub enum WorkerMessage {
    Execute {
        task: Arc<dyn ReactiveTask>,
        context: TaskContext,
        execution_policy: ExecutionPolicy,
        scheduling_policy: SchedulingPolicy,
    },
}

struct PendingTask {
    id: Uuid,
    task: Arc<dyn ReactiveTask>,
    context: TaskContext,
    execution_policy: ExecutionPolicy,
    scheduling_policy: SchedulingPolicy,
    arrival_time: DateTime<Utc>,
}

impl PartialEq for PendingTask {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for PendingTask {}
impl Hash for PendingTask {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
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
        let mut queue = PriorityQueue::new();
        // Limit concurrency to avoid spawning too many tasks if they are rate-limited or delayed
        let semaphore = Arc::new(Semaphore::new(100));

        loop {
            tokio::select! {
                msg = self.receiver.recv() => {
                    match msg {
                        Some(WorkerMessage::Execute {
                            task,
                            context,
                            execution_policy,
                            scheduling_policy,
                        }) => {
                            let pending = PendingTask {
                                id: Uuid::new_v4(),
                                task,
                                context,
                                execution_policy,
                                scheduling_policy,
                                arrival_time: Utc::now(),
                            };
                            let priority = self.calculate_priority(&pending);
                            queue.push(pending, priority);
                        }
                        None => {
                            info!("Worker actor channel closed");
                            break;
                        }
                    }
                }
                // Try to acquire a permit to execute a task
                permit = semaphore.clone().acquire_owned(), if !queue.is_empty() => {
                    if let Ok(permit) = permit {
                        if let Some((pending, _)) = queue.pop() {
                            self.execute_pending_task(pending, permit).await;
                        }
                    }
                }
            }
        }
        info!("Worker actor stopped");
    }

    // Priority is (class, value). Higher class = higher priority.
    // Class 2: Priority (weight based)
    // Class 1: FirstInFirstOut, Delayed, Fair (time based)
    // Class 0: RateLimited
    fn calculate_priority(&self, task: &PendingTask) -> (i8, i64) {
        match task.scheduling_policy {
            SchedulingPolicy::Priority => {
                // Weight: -20 (high) to 19 (low).
                // We want lower weight to have higher priority.
                // -weight: 20 (high) to -19 (low).
                (2, -(task.context.weight as i64))
            }
            SchedulingPolicy::FirstInFirstOut => {
                // Earlier arrival = higher priority.
                (1, -task.arrival_time.timestamp_nanos_opt().unwrap_or(50))
            }
            SchedulingPolicy::Delayed => {
                // Earlier scheduled time = higher priority.
                (1, -task.context.scheduled_time.timestamp_millis())
            }
            SchedulingPolicy::Fair => {
                // Treat as FIFO for now
                (1, -task.arrival_time.timestamp_nanos_opt().unwrap_or(50))
            }
            SchedulingPolicy::RateLimited => {
                // Lowest priority
                (0, 0)
            }
        }
    }

    async fn execute_pending_task(&mut self, pending: PendingTask, permit: tokio::sync::OwnedSemaphorePermit) {
        let task = pending.task;
        let context = pending.context;
        let policy = pending.execution_policy;

        let task_id = task.id().to_string();
        let lock = self
            .task_locks
            .entry(task_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();

        // We spawn the execution logic so we don't block the actor loop
        tokio::spawn(async move {
            // permit is held until this task finishes
            let _permit = permit;

            match policy {
                ExecutionPolicy::SkipIfRunning => {
                    if let Ok(guard) = lock.try_lock_owned() {
                        let _guard = guard;
                        info!("Executing task (skip-if-running): {}", task.id());
                        if let Err(e) = task.execute(context).await {
                            error!("Task {} failed: {}", task.id(), e);
                        } else {
                            info!("Task {} completed successfully", task.id());
                        }
                    } else {
                        warn!("Task {} already running, skipping execution", task.id());
                    }
                }
                ExecutionPolicy::Parallel => {
                    info!("Executing task (parallel): {}", task.id());
                    if let Err(e) = task.execute(context).await {
                        error!("Task {} failed: {}", task.id(), e);
                    } else {
                        info!("Task {} completed successfully", task.id());
                    }
                }
                ExecutionPolicy::Sequential => {
                    let _guard = lock.lock_owned().await;
                    info!("Executing task (sequential): {}", task.id());
                    if let Err(e) = task.execute(context).await {
                        error!("Task {} failed: {}", task.id(), e);
                    } else {
                        info!("Task {} completed successfully", task.id());
                    }
                }
            }
        });
    }
}
