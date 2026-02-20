use async_trait::async_trait;
use gbe_jobs_domain::{TaskDefinition, TaskOutcome, TaskType};
use tokio::process::Command;
use tracing::{debug, info};

use crate::operative::{Operative, OperativeError};

/// Shell operative: reads `command` from task params and runs it directly.
///
/// Expects `params.command` to contain the shell command string.
/// Captures stdout lines and exit code, returns a TaskOutcome.
pub struct ShellOperative {
    task_types: Vec<TaskType>,
}

impl ShellOperative {
    pub fn new(task_types: Vec<TaskType>) -> Self {
        Self { task_types }
    }

    /// Create a shell operative that handles any task type.
    /// Uses a wildcard match — `handles()` returns the registered types,
    /// but callers can also check `can_handle()` for dynamic dispatch.
    pub fn for_types(types: &[&str]) -> Result<Self, gbe_jobs_domain::JobsDomainError> {
        let task_types: Result<Vec<TaskType>, _> =
            types.iter().map(|t| TaskType::new(t)).collect();
        Ok(Self::new(task_types?))
    }
}

#[async_trait]
impl Operative for ShellOperative {
    fn handles(&self) -> &[TaskType] {
        &self.task_types
    }

    async fn execute(&self, task: &TaskDefinition) -> Result<TaskOutcome, OperativeError> {
        let command = task
            .params
            .entries
            .get("command")
            .ok_or_else(|| OperativeError::MissingParam("command".to_string()))?;

        debug!(task = %task.name, command = %command, "executing shell command");

        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg(command);
        // Expose all non-command params as env vars (enables consuming resolved inputs)
        for (k, v) in &task.params.entries {
            if k != "command" {
                cmd.env(k, v);
            }
        }
        let output = cmd.output().await?;

        let stdout_lines: Vec<String> = String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(String::from)
            .collect();

        let exit_code = output.status.code().unwrap_or(1);

        if exit_code == 0 {
            info!(task = %task.name, lines = stdout_lines.len(), "task completed");
            let full_stdout = String::from_utf8_lossy(&output.stdout);
            let data = match serde_json::from_str::<serde_json::Value>(full_stdout.trim()) {
                Ok(v) => Some(v),
                Err(_) => Some(serde_json::Value::Array(
                    stdout_lines.iter().map(|l| serde_json::Value::String(l.clone())).collect(),
                )),
            };
            Ok(TaskOutcome::Completed {
                output: stdout_lines,
                result_ref: None,
                data,
            })
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            info!(task = %task.name, exit_code, "task failed");
            Ok(TaskOutcome::Failed {
                exit_code,
                error: stderr,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gbe_jobs_domain::{TaskDefinition, TaskParams, TaskType};

    fn task_with_command(name: &str, command: &str) -> TaskDefinition {
        let mut params = TaskParams::default();
        params
            .entries
            .insert("command".to_string(), command.to_string());
        TaskDefinition {
            name: name.to_string(),
            task_type: TaskType::new("shell").unwrap(),
            depends_on: vec![],
            params,
            input_from: Default::default(),
            timeout_secs: None,
            max_retries: None,
        }
    }

    #[tokio::test]
    async fn echo_captures_output() {
        let op = ShellOperative::for_types(&["shell"]).unwrap();
        let task = task_with_command("test-echo", "echo hello world");

        let outcome = op.execute(&task).await.unwrap();
        match outcome {
            TaskOutcome::Completed { output, .. } => {
                assert_eq!(output, vec!["hello world"]);
            }
            TaskOutcome::Failed { .. } => panic!("expected success"),
        }
    }

    #[tokio::test]
    async fn failing_command_returns_failed() {
        let op = ShellOperative::for_types(&["shell"]).unwrap();
        let task = task_with_command("test-fail", "exit 42");

        let outcome = op.execute(&task).await.unwrap();
        match outcome {
            TaskOutcome::Failed { exit_code, .. } => {
                assert_eq!(exit_code, 42);
            }
            TaskOutcome::Completed { .. } => panic!("expected failure"),
        }
    }

    #[tokio::test]
    async fn missing_command_param_errors() {
        let op = ShellOperative::for_types(&["shell"]).unwrap();
        let task = TaskDefinition {
            name: "no-cmd".to_string(),
            task_type: TaskType::new("shell").unwrap(),
            depends_on: vec![],
            params: TaskParams::default(),
            input_from: Default::default(),
            timeout_secs: None,
            max_retries: None,
        };

        let err = op.execute(&task).await.unwrap_err();
        assert!(matches!(err, OperativeError::MissingParam(_)));
    }

    #[tokio::test]
    async fn multiline_output_captured() {
        let op = ShellOperative::for_types(&["shell"]).unwrap();
        let task = task_with_command("test-multi", "echo line1; echo line2; echo line3");

        let outcome = op.execute(&task).await.unwrap();
        match outcome {
            TaskOutcome::Completed { output, .. } => {
                assert_eq!(output, vec!["line1", "line2", "line3"]);
            }
            TaskOutcome::Failed { .. } => panic!("expected success"),
        }
    }

    #[tokio::test]
    async fn json_stdout_parsed_as_data() {
        let op = ShellOperative::for_types(&["shell"]).unwrap();
        let task = task_with_command("test-json", r#"echo '{"url":"https://example.com","count":42}'"#);

        let outcome = op.execute(&task).await.unwrap();
        match outcome {
            TaskOutcome::Completed { data, .. } => {
                let data = data.unwrap();
                assert_eq!(data["url"], "https://example.com");
                assert_eq!(data["count"], 42);
            }
            TaskOutcome::Failed { .. } => panic!("expected success"),
        }
    }

    #[tokio::test]
    async fn non_json_stdout_wrapped_as_array() {
        let op = ShellOperative::for_types(&["shell"]).unwrap();
        let task = task_with_command("test-plain", "echo hello; echo world");

        let outcome = op.execute(&task).await.unwrap();
        match outcome {
            TaskOutcome::Completed { data, .. } => {
                let data = data.unwrap();
                assert_eq!(data, serde_json::json!(["hello", "world"]));
            }
            TaskOutcome::Failed { .. } => panic!("expected success"),
        }
    }

    #[tokio::test]
    async fn params_exposed_as_env_vars() {
        let mut params = TaskParams::default();
        params.entries.insert("command".to_string(), "echo $MY_VAR".to_string());
        params.entries.insert("MY_VAR".to_string(), "injected_value".to_string());

        let op = ShellOperative::for_types(&["shell"]).unwrap();
        let task = TaskDefinition {
            name: "test-env".to_string(),
            task_type: TaskType::new("shell").unwrap(),
            depends_on: vec![],
            params,
            input_from: Default::default(),
            timeout_secs: None,
            max_retries: None,
        };

        let outcome = op.execute(&task).await.unwrap();
        match outcome {
            TaskOutcome::Completed { output, .. } => {
                assert_eq!(output, vec!["injected_value"]);
            }
            TaskOutcome::Failed { .. } => panic!("expected success"),
        }
    }
}
