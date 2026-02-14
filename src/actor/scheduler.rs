use crate::actor::worker::WorkerMessage;
use crate::cron_parser::CronParser;
use crate::models::{ExecutionPolicy, ReactiveTask, TaskContext};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

pub struct ScheduledTaskItem {
    pub task: Arc<dyn ReactiveTask>,
    pub cron_parser: CronParser,
    pub policy: ExecutionPolicy,
}

pub struct SchedulerActor {
    tasks: Vec<ScheduledTaskItem>,
    worker_tx: mpsc::Sender<WorkerMessage>,
}

impl SchedulerActor {
    pub fn new(worker_tx: mpsc::Sender<WorkerMessage>) -> Self {
        Self {
            tasks: Vec::new(),
            worker_tx,
        }
    }

    pub fn add_task(
        &mut self,
        task: Arc<dyn ReactiveTask>,
        cron_expr: &str,
        policy: ExecutionPolicy,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let cron_parser = CronParser::new(cron_expr)?;
        self.tasks.push(ScheduledTaskItem {
            task,
            cron_parser,
            policy,
        });
        Ok(())
    }

    pub async fn start_all(self) {
        info!("Starting all scheduled tasks (recursively)");
        for item in self.tasks {
            let worker_tx = self.worker_tx.clone();
            tokio::spawn(async move {
                schedule_loop(item, worker_tx).await;
            });
        }
    }
}

async fn schedule_loop(item: ScheduledTaskItem, worker_tx: mpsc::Sender<WorkerMessage>) {
    let mut next_run = item.cron_parser.next_after(Utc::now());

    while let Some(scheduled_time) = next_run {
        let now = Utc::now();
        let duration_to_wait = scheduled_time.signed_duration_since(now).to_std();

        match duration_to_wait {
            Ok(duration) => {
                debug!("Task {} sleeping for {:?}", item.task.id(), duration);
                tokio::time::sleep(duration).await;
            }
            Err(_) => {
                // If the duration is negative, we are already past the scheduled time
                debug!(
                    "Task {} scheduled time {:?} is in the past, running immediately",
                    item.task.id(),
                    scheduled_time
                );
            }
        }

        let context = TaskContext {
            scheduled_time,
            actual_time: Utc::now(),
            metadata: {
                let mut m = HashMap::new();
                m.insert("task_id".to_string(), item.task.id().to_string());
                m
            },
        };

        if let Err(e) = worker_tx
            .send(WorkerMessage::Execute {
                task: item.task.clone(),
                context,
                policy: item.policy,
            })
            .await
        {
            error!(
                "Failed to send execution message for task {}: {}",
                item.task.id(),
                e
            );
            break;
        }

        next_run = item.cron_parser.next_after(scheduled_time);
    }
}
