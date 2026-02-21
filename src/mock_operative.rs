use async_trait::async_trait;
use gbe_jobs_domain::{TaskDefinition, TaskOutcome, TaskType};
use std::collections::HashMap;
use std::sync::Mutex;

use crate::operative::{Operative, OperativeError};

/// Mock operative for testing. Returns configurable outcomes per task name.
/// Tracks executed tasks for verification.
pub struct MockOperative {
    task_types: Vec<TaskType>,
    outcomes: Mutex<HashMap<String, TaskOutcome>>,
    default_outcome: TaskOutcome,
    executed: Mutex<HashMap<String, TaskDefinition>>,
}

impl MockOperative {
    #[must_use]
    pub fn new(task_types: Vec<TaskType>) -> Self {
        Self {
            task_types,
            outcomes: Mutex::new(HashMap::new()),
            default_outcome: TaskOutcome::Completed {
                output: vec![],
                result_ref: None,
                data: None,
            },
            executed: Mutex::new(HashMap::new()),
        }
    }

    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn set_outcome(&self, task_name: &str, outcome: TaskOutcome) {
        self.outcomes
            .lock()
            .unwrap()
            .insert(task_name.to_string(), outcome);
    }

    /// Returns the `TaskDefinition` that was passed to `execute()` for a given task name.
    /// Useful for verifying `input_from` resolution merged params correctly.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn last_executed(&self, task_name: &str) -> Option<TaskDefinition> {
        self.executed.lock().unwrap().get(task_name).cloned()
    }
}

#[async_trait]
impl Operative for MockOperative {
    fn handles(&self) -> &[TaskType] {
        &self.task_types
    }

    async fn execute(&self, task: &TaskDefinition) -> Result<TaskOutcome, OperativeError> {
        self.executed
            .lock()
            .unwrap()
            .insert(task.name.clone(), task.clone());

        let outcomes = self.outcomes.lock().unwrap();
        Ok(outcomes
            .get(&task.name)
            .cloned()
            .unwrap_or_else(|| self.default_outcome.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gbe_jobs_domain::TaskParams;
    use std::collections::HashMap;

    #[tokio::test]
    async fn returns_default_outcome() {
        let op = MockOperative::new(vec![TaskType::new("work").unwrap()]);
        let task = TaskDefinition {
            name: "test".to_string(),
            task_type: TaskType::new("work").unwrap(),
            depends_on: vec![],
            params: TaskParams::default(),
            input_from: HashMap::new(),
            timeout_secs: None,
            max_retries: None,
        };

        let outcome = op.execute(&task).await.unwrap();
        assert!(matches!(outcome, TaskOutcome::Completed { .. }));
    }

    #[tokio::test]
    async fn returns_configured_outcome() {
        let op = MockOperative::new(vec![TaskType::new("work").unwrap()]);
        op.set_outcome(
            "fail-task",
            TaskOutcome::Failed {
                exit_code: 1,
                error: "boom".to_string(),
            },
        );

        let task = TaskDefinition {
            name: "fail-task".to_string(),
            task_type: TaskType::new("work").unwrap(),
            depends_on: vec![],
            params: TaskParams::default(),
            input_from: HashMap::new(),
            timeout_secs: None,
            max_retries: None,
        };

        let outcome = op.execute(&task).await.unwrap();
        match outcome {
            TaskOutcome::Failed { exit_code, error } => {
                assert_eq!(exit_code, 1);
                assert_eq!(error, "boom");
            }
            TaskOutcome::Completed { .. } => panic!("expected Failed"),
        }
    }

    #[tokio::test]
    async fn tracks_executed_tasks() {
        let op = MockOperative::new(vec![TaskType::new("work").unwrap()]);
        let task = TaskDefinition {
            name: "tracked".to_string(),
            task_type: TaskType::new("work").unwrap(),
            depends_on: vec![],
            params: TaskParams::default(),
            input_from: HashMap::new(),
            timeout_secs: None,
            max_retries: None,
        };

        op.execute(&task).await.unwrap();
        let recorded = op.last_executed("tracked").unwrap();
        assert_eq!(recorded.name, "tracked");
    }
}
