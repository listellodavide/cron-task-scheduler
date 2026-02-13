use crate::models::{ReactiveTask, TaskContext};
use async_trait::async_trait;
use reqwest::Client;
use tracing::info;

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

    async fn execute(
        &self,
        _context: TaskContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Fetching URL: {}", self.url);
        let res = self.client.get(&self.url).send().await?;
        let status = res.status();
        let body = res.text().await?;
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
