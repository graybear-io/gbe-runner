use async_trait::async_trait;
use gbe_jobs_domain::{TaskDefinition, TaskOutcome, TaskType};
use std::collections::HashMap;
use std::sync::Arc;

use crate::operative::{Operative, OperativeError};

/// Routes task execution to child operatives based on `task_type`.
///
/// Register one operative per task type. On `execute()`, looks up the
/// child for that task's type and delegates. Returns an error if no
/// operative is registered for the type.
#[derive(Default)]
pub struct CompositeOperative {
    children: HashMap<TaskType, Arc<dyn Operative>>,
}

impl CompositeOperative {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an operative for all task types it handles.
    pub fn register(&mut self, operative: &Arc<dyn Operative>) {
        for tt in operative.handles() {
            self.children.insert(tt.clone(), operative.clone());
        }
    }

    /// Build a `CompositeOperative` from a list of operatives.
    #[must_use]
    pub fn from_operatives(operatives: &[Arc<dyn Operative>]) -> Self {
        let mut composite = Self::new();
        for op in operatives {
            composite.register(op);
        }
        composite
    }
}

#[async_trait]
impl Operative for CompositeOperative {
    fn handles(&self) -> &[TaskType] {
        // CompositeOperative handles all registered types, but we can't
        // return a slice of a HashMap's keys. Return empty — callers
        // should use the driver which doesn't check handles() on the
        // top-level operative.
        &[]
    }

    async fn execute(&self, task: &TaskDefinition) -> Result<TaskOutcome, OperativeError> {
        let child = self.children.get(&task.task_type).ok_or_else(|| {
            OperativeError::Execution(format!(
                "no operative registered for task type '{}'",
                task.task_type
            ))
        })?;

        child.execute(task).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_operative::MockOperative;
    use gbe_jobs_domain::TaskParams;

    fn task_of_type(name: &str, task_type: &str) -> TaskDefinition {
        TaskDefinition {
            name: name.to_string(),
            task_type: TaskType::new(task_type).unwrap(),
            depends_on: vec![],
            params: TaskParams::default(),
            input_from: HashMap::new(),
            timeout_secs: None,
            max_retries: None,
        }
    }

    #[tokio::test]
    async fn routes_to_correct_child() {
        let shell_op = Arc::new(MockOperative::new(vec![TaskType::new("shell").unwrap()]));
        shell_op.set_outcome(
            "task-a",
            TaskOutcome::Completed {
                output: vec!["from-shell".to_string()],
                result_ref: None,
                data: None,
            },
        );

        let http_op = Arc::new(MockOperative::new(vec![TaskType::new("http").unwrap()]));
        http_op.set_outcome(
            "task-b",
            TaskOutcome::Completed {
                output: vec!["from-http".to_string()],
                result_ref: None,
                data: None,
            },
        );

        let composite = CompositeOperative::from_operatives(&[shell_op.clone(), http_op.clone()]);

        let outcome_a = composite
            .execute(&task_of_type("task-a", "shell"))
            .await
            .unwrap();
        match outcome_a {
            TaskOutcome::Completed { output, .. } => assert_eq!(output, vec!["from-shell"]),
            TaskOutcome::Failed { .. } => panic!("expected Completed"),
        }

        let outcome_b = composite
            .execute(&task_of_type("task-b", "http"))
            .await
            .unwrap();
        match outcome_b {
            TaskOutcome::Completed { output, .. } => assert_eq!(output, vec!["from-http"]),
            TaskOutcome::Failed { .. } => panic!("expected Completed"),
        }
    }

    #[tokio::test]
    async fn unknown_type_returns_error() {
        let composite = CompositeOperative::new();
        let err = composite
            .execute(&task_of_type("task-x", "unknown"))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("no operative registered"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn tracks_execution_on_child() {
        let mock = Arc::new(MockOperative::new(vec![TaskType::new("work").unwrap()]));
        let op: Arc<dyn Operative> = mock.clone();
        let composite = CompositeOperative::from_operatives(&[op]);

        composite
            .execute(&task_of_type("tracked", "work"))
            .await
            .unwrap();

        let recorded = mock.last_executed("tracked").unwrap();
        assert_eq!(recorded.name, "tracked");
    }
}
