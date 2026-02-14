use crate::models::{ReactiveTask, TaskContext, TaskType};
use async_trait::async_trait;
use reqwest::Client;
use std::process::Command;
use tracing::{info, error};

#[derive(Debug)]
pub struct HttpTask {
    pub id: String,
    pub url: String,
    pub client: Client,
}

#[async_trait]
impl ReactiveTask for HttpTask {

    fn id(&self) -> &str {
        &self.id
    }

    fn task_type(&self) -> TaskType {
        TaskType::Async
    }

    async fn execute(
        &self,
        _context: TaskContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Fetching URL: {}", self.url);
        let res: reqwest::Response = self.client.get(&self.url).send().await?;
        let status = res.status();
        let body: String = res.text().await?;
        info!(
            "Response Status: {}, Body prefix: {}",
            status,
            &body[..std::cmp::min(body.len(), 50)]
        );
        Ok(())
    }
}

#[derive(Debug)]
pub struct SimpleLoggingTask {
    pub id: String,
}

#[async_trait]
impl ReactiveTask for SimpleLoggingTask {
    fn id(&self) -> &str {
        &self.id
    }

    fn task_type(&self) -> TaskType {
        TaskType::Async
    }

    async fn execute(
        &self,
        context: TaskContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "Logging task {} executed. Scheduled: {:?}, Actual: {:?}",
            self.id, context.scheduled_time, context.actual_time
        );
        Ok(())
    }
}

#[derive(Debug)]
pub struct CommandLineTask {
    pub id: String,
    pub command: String,
    pub args: Vec<String>,
}

#[async_trait]
impl ReactiveTask for CommandLineTask {
    fn id(&self) -> &str {
        &self.id
    }

    fn task_type(&self) -> TaskType {
        TaskType::Blocking
    }

    async fn execute(
        &self,
        _context: TaskContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Executing command: {} {:?}", self.command, self.args);

        let output = Command::new(&self.command)
            .args(&self.args)
            .output()?;

        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            info!("Command output: {}", stdout);
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!("Command failed: {}", stderr);
            return Err(format!("Command failed with status: {}", output.status).into());
        }

        Ok(())
    }
}
