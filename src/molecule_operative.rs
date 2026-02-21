use async_trait::async_trait;
use gbe_jobs_domain::{JobDefinition, TaskDefinition, TaskOutcome, TaskType};
use std::sync::Arc;
use tracing::{debug, info};

use crate::driver::{run_job, DriverError};
use crate::operative::{Operative, OperativeError};

/// Molecule operative: runs an entire sub-DAG as a single task.
///
/// Reads a `JobDefinition` from the `definition` param (inline YAML/JSON)
/// or `definition_file` param (path to file). Delegates inner task execution
/// to a shared operative, then aggregates inner results into structured
/// output keyed by task name.
pub struct MoleculeOperative {
    task_types: Vec<TaskType>,
    delegate: Arc<dyn Operative>,
}

impl MoleculeOperative {
    pub fn new(task_types: Vec<TaskType>, delegate: Arc<dyn Operative>) -> Self {
        Self {
            task_types,
            delegate,
        }
    }

    /// # Errors
    ///
    /// Returns `JobsDomainError` if any type string is invalid.
    pub fn for_types(
        types: &[&str],
        delegate: Arc<dyn Operative>,
    ) -> Result<Self, gbe_jobs_domain::JobsDomainError> {
        let task_types: Result<Vec<TaskType>, _> = types.iter().map(|t| TaskType::new(t)).collect();
        Ok(Self::new(task_types?, delegate))
    }

    fn parse_definition(raw: &str) -> Result<JobDefinition, OperativeError> {
        // Try YAML first (superset of JSON)
        let def: JobDefinition = serde_yaml::from_str(raw)
            .map_err(|e| OperativeError::Execution(format!("invalid definition: {e}")))?;
        def.validate()
            .map_err(|e| OperativeError::Execution(format!("invalid definition: {e}")))?;
        Ok(def)
    }
}

#[async_trait]
impl Operative for MoleculeOperative {
    fn handles(&self) -> &[TaskType] {
        &self.task_types
    }

    async fn execute(&self, task: &TaskDefinition) -> Result<TaskOutcome, OperativeError> {
        // Resolve definition from params
        let raw_def = if let Some(inline) = task.params.entries.get("definition") {
            inline.clone()
        } else if let Some(path) = task.params.entries.get("definition_file") {
            std::fs::read_to_string(path).map_err(|e| {
                OperativeError::Execution(format!("reading definition_file '{path}': {e}"))
            })?
        } else {
            return Err(OperativeError::MissingParam(
                "definition or definition_file".to_string(),
            ));
        };

        let def = Self::parse_definition(&raw_def)?;
        debug!(task = %task.name, inner_job = %def.name, "running molecule sub-DAG");

        match run_job(&def, self.delegate.clone()).await {
            Ok(results) => {
                let mut data = serde_json::Map::new();
                let mut output_lines = Vec::new();

                for (name, outcome) in &results {
                    match outcome {
                        TaskOutcome::Completed {
                            data: task_data, ..
                        } => {
                            if let Some(d) = task_data {
                                data.insert(name.clone(), d.clone());
                            }
                        }
                        TaskOutcome::Failed { .. } => {}
                    }
                    output_lines.push(format!("{name}: completed"));
                }

                info!(
                    task = %task.name,
                    inner_tasks = results.len(),
                    "molecule sub-DAG completed"
                );

                Ok(TaskOutcome::Completed {
                    output: output_lines,
                    result_ref: None,
                    data: Some(serde_json::Value::Object(data)),
                })
            }
            Err(DriverError::TaskFailed {
                task: inner_task,
                exit_code,
                error,
            }) => {
                info!(
                    task = %task.name,
                    inner_task,
                    exit_code,
                    "molecule inner task failed"
                );
                Ok(TaskOutcome::Failed { exit_code, error })
            }
            Err(e) => Err(OperativeError::Execution(format!(
                "molecule sub-DAG error: {e}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_operative::MockOperative;
    use gbe_jobs_domain::{TaskParams, TaskType};
    use std::collections::HashMap;

    fn molecule_task(name: &str, params: TaskParams) -> TaskDefinition {
        TaskDefinition {
            name: name.to_string(),
            task_type: TaskType::new("molecule").unwrap(),
            depends_on: vec![],
            params,
            input_from: HashMap::new(),
            timeout_secs: None,
            max_retries: None,
        }
    }

    fn inner_dag_yaml() -> String {
        r#"
v: 1
name: "Inner Pipeline"
job_type: "inner"
tasks:
  - name: "step-a"
    task_type: "work"
    params:
      command: "echo a"
  - name: "step-b"
    task_type: "work"
    depends_on: ["step-a"]
    params:
      command: "echo b"
"#
        .to_string()
    }

    #[tokio::test]
    async fn missing_definition_errors() {
        let mock = Arc::new(MockOperative::new(vec![TaskType::new("work").unwrap()]));
        let op = MoleculeOperative::for_types(&["molecule"], mock).unwrap();
        let task = molecule_task("mol", TaskParams::default());

        let err = op.execute(&task).await.unwrap_err();
        assert!(matches!(err, OperativeError::MissingParam(_)));
    }

    #[tokio::test]
    async fn inline_definition_runs_sub_dag() {
        let mock = Arc::new(MockOperative::new(vec![TaskType::new("work").unwrap()]));
        mock.set_outcome(
            "step-a",
            TaskOutcome::Completed {
                output: vec!["a-out".to_string()],
                result_ref: None,
                data: Some(serde_json::json!({"val": "alpha"})),
            },
        );
        mock.set_outcome(
            "step-b",
            TaskOutcome::Completed {
                output: vec!["b-out".to_string()],
                result_ref: None,
                data: Some(serde_json::json!({"val": "beta"})),
            },
        );

        let op = MoleculeOperative::for_types(&["molecule"], mock).unwrap();
        let mut params = TaskParams::default();
        params
            .entries
            .insert("definition".to_string(), inner_dag_yaml());
        let task = molecule_task("mol", params);

        let outcome = op.execute(&task).await.unwrap();
        match outcome {
            TaskOutcome::Completed { output, data, .. } => {
                assert_eq!(output.len(), 2);
                let data = data.unwrap();
                assert_eq!(data["step-a"]["val"], "alpha");
                assert_eq!(data["step-b"]["val"], "beta");
            }
            TaskOutcome::Failed { .. } => panic!("expected success"),
        }
    }

    #[tokio::test]
    async fn definition_file_runs_sub_dag() {
        let mock = Arc::new(MockOperative::new(vec![TaskType::new("work").unwrap()]));

        let dir = std::env::temp_dir().join("gbe-molecule-test");
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("inner.yaml");
        std::fs::write(&file_path, inner_dag_yaml()).unwrap();

        let op = MoleculeOperative::for_types(&["molecule"], mock).unwrap();
        let mut params = TaskParams::default();
        params.entries.insert(
            "definition_file".to_string(),
            file_path.to_string_lossy().to_string(),
        );
        let task = molecule_task("mol-file", params);

        let outcome = op.execute(&task).await.unwrap();
        assert!(matches!(outcome, TaskOutcome::Completed { .. }));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn inner_failure_propagates() {
        let mock = Arc::new(MockOperative::new(vec![TaskType::new("work").unwrap()]));
        mock.set_outcome(
            "step-a",
            TaskOutcome::Failed {
                exit_code: 7,
                error: "inner boom".to_string(),
            },
        );

        let op = MoleculeOperative::for_types(&["molecule"], mock).unwrap();
        let mut params = TaskParams::default();
        params
            .entries
            .insert("definition".to_string(), inner_dag_yaml());
        let task = molecule_task("mol-fail", params);

        let outcome = op.execute(&task).await.unwrap();
        match outcome {
            TaskOutcome::Failed { exit_code, error } => {
                assert_eq!(exit_code, 7);
                assert_eq!(error, "inner boom");
            }
            TaskOutcome::Completed { .. } => panic!("expected failure"),
        }
    }

    #[tokio::test]
    async fn data_aggregated_by_task_name() {
        let mock = Arc::new(MockOperative::new(vec![TaskType::new("work").unwrap()]));
        mock.set_outcome(
            "step-a",
            TaskOutcome::Completed {
                output: vec![],
                result_ref: None,
                data: Some(serde_json::json!({"x": 1})),
            },
        );
        mock.set_outcome(
            "step-b",
            TaskOutcome::Completed {
                output: vec![],
                result_ref: None,
                data: Some(serde_json::json!({"y": 2})),
            },
        );

        let op = MoleculeOperative::for_types(&["molecule"], mock).unwrap();
        let mut params = TaskParams::default();
        params
            .entries
            .insert("definition".to_string(), inner_dag_yaml());
        let task = molecule_task("mol-data", params);

        let outcome = op.execute(&task).await.unwrap();
        match outcome {
            TaskOutcome::Completed { data, .. } => {
                let data = data.unwrap();
                assert!(data.is_object());
                assert_eq!(data["step-a"]["x"], 1);
                assert_eq!(data["step-b"]["y"], 2);
            }
            TaskOutcome::Failed { .. } => panic!("expected success"),
        }
    }
}
