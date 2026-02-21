use async_trait::async_trait;
use gbe_jobs_domain::{TaskDefinition, TaskOutcome, TaskType};

/// Error produced by an operative during task execution.
#[derive(Debug, thiserror::Error)]
pub enum OperativeError {
    #[error("missing required param: {0}")]
    MissingParam(String),

    #[error("execution failed: {0}")]
    Execution(String),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// The Operative executes tasks. It subscribes to one or more task types,
/// claims work from the queue, decides *how* to run it, and reports outcomes.
///
/// The operative is stateless — everything it needs comes from the
/// `TaskDefinition` (via the bus event or a KV lookup). It interprets
/// `params` according to the task types it handles.
#[async_trait]
pub trait Operative: Send + Sync {
    /// Task types this operative handles.
    fn handles(&self) -> &[TaskType];

    /// Execute a single task. The operative decides how.
    async fn execute(&self, task: &TaskDefinition) -> Result<TaskOutcome, OperativeError>;
}
