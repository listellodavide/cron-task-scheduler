use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug, Clone)]
pub struct TaskContext {
    pub scheduled_time: DateTime<Utc>,
    pub actual_time: DateTime<Utc>,
    pub weight: i8, // -20 to +19, default 0. Lower is a higher priority (like nice).
    #[allow(dead_code)]
    pub metadata: HashMap<String, String>,
}

#[async_trait]
pub trait ReactiveTask: Send + Sync + Debug {
    fn id(&self) -> &str;
    async fn execute(
        &self,
        context: TaskContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionPolicy {
    /// Skip execution if the previous task is still running
    SkipIfRunning,
    /// Run execution in parallel even if the previous one is still running
    Parallel,
    /// Queue the execution to run sequentially after the previous one completes
    #[allow(dead_code)]
    Sequential,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SchedulingPolicy {
    FirstInFirstOut,
    Priority,
    Fair,
    Delayed,
    RateLimited,
}
