use gbe_jobs_domain::{
    payloads, subjects, JobDefinition, JobId, OrgId, TaskDefinition, TaskId, TaskOutcome,
    TaskParams,
};
use gbe_nexus::{dedup_id, EventEmitter};
use gbe_oracle::SimpleOracle;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::{error, info, warn};

use crate::operative::{Operative, OperativeError};

/// Error from the driver loop.
#[derive(Debug, thiserror::Error)]
pub enum DriverError {
    #[error("oracle: {0}")]
    Oracle(#[from] gbe_oracle::OracleError),

    #[error("task {task} failed: exit {exit_code}")]
    TaskFailed {
        task: String,
        exit_code: i32,
        error: String,
    },

    #[error("task {task} execution error: {source}")]
    Execution {
        task: String,
        source: OperativeError,
    },

    #[error("task join error: {0}")]
    Join(String),

    #[error("input resolution for task '{task}': ref '{reference}' {reason}")]
    InputResolution {
        task: String,
        reference: String,
        reason: String,
    },
}

/// Result of one task within the `JoinSet`.
struct TaskCompletion {
    name: String,
    result: Result<TaskOutcome, OperativeError>,
}

/// Runs jobs with an operative, optionally emitting lifecycle events.
pub struct Driver {
    operative: Arc<dyn Operative>,
    emitter: Option<Arc<EventEmitter>>,
}

impl Driver {
    pub fn new(operative: Arc<dyn Operative>, emitter: Option<Arc<EventEmitter>>) -> Self {
        Self { operative, emitter }
    }

    /// Run a job to completion using a `SimpleOracle` and the operative.
    ///
    /// Dispatches ready tasks concurrently via `JoinSet`. Fail-fast on
    /// first task failure. Returns all successful outcomes on completion.
    /// Resolves `input_from` references before dispatching downstream tasks.
    ///
    /// When an `EventEmitter` is present, emits `TaskQueued`, `TaskCompleted`,
    /// and `TaskFailed` events. Emission failures are logged and swallowed.
    pub async fn run_job(
        &self,
        def: &JobDefinition,
        job_id: &JobId,
        org_id: &OrgId,
    ) -> Result<Vec<(String, TaskOutcome)>, DriverError> {
        let mut oracle = SimpleOracle::new(def.clone())?;
        let mut join_set: JoinSet<TaskCompletion> = JoinSet::new();
        let mut results: Vec<(String, TaskOutcome)> = Vec::new();
        let mut outcome_data: HashMap<String, serde_json::Value> = HashMap::new();

        // Seed with root tasks (no input_from resolution needed for roots)
        let initial: Vec<_> = oracle.ready_tasks().into_iter().cloned().collect();
        for td in initial {
            self.emit_task_queued(&td, job_id, org_id).await;
            let op = self.operative.clone();
            oracle.mark_dispatched(&td.name);
            let name = td.name.clone();
            info!(task = %name, "dispatching task");
            join_set.spawn(async move {
                let result = op.execute(&td).await;
                TaskCompletion { name, result }
            });
        }

        // Process completions
        while let Some(join_result) = join_set.join_next().await {
            let completion = join_result.map_err(|e| DriverError::Join(e.to_string()))?;

            match completion.result {
                Ok(ref outcome) => match outcome {
                    TaskOutcome::Completed { result_ref, .. } => {
                        info!(task = %completion.name, "task completed");
                        self.emit_task_completed(
                            &completion.name,
                            def,
                            job_id,
                            result_ref.as_deref(),
                        )
                        .await;

                        // Store structured data for downstream input_from resolution
                        if let Some(data) = outcome_to_data(outcome) {
                            outcome_data.insert(completion.name.clone(), data);
                        }

                        let newly_ready: Vec<_> = oracle
                            .task_completed(&completion.name)
                            .into_iter()
                            .cloned()
                            .collect();
                        results.push((completion.name, outcome.clone()));

                        for td in newly_ready {
                            let resolved = prepare_task(&td, &outcome_data)?;
                            self.emit_task_queued(&td, job_id, org_id).await;
                            let op = self.operative.clone();
                            oracle.mark_dispatched(&td.name);
                            let name = td.name.clone();
                            info!(task = %name, "dispatching task");
                            join_set.spawn(async move {
                                let result = op.execute(&resolved).await;
                                TaskCompletion { name, result }
                            });
                        }
                    }
                    TaskOutcome::Failed {
                        exit_code, error, ..
                    } => {
                        error!(task = %completion.name, exit_code, "task failed");
                        self.emit_task_failed(
                            &completion.name,
                            def,
                            job_id,
                            error,
                        )
                        .await;
                        oracle.task_failed(&completion.name);
                        join_set.abort_all();
                        return Err(DriverError::TaskFailed {
                            task: completion.name,
                            exit_code: *exit_code,
                            error: error.clone(),
                        });
                    }
                },
                Err(e) => {
                    error!(task = %completion.name, error = %e, "operative error");
                    self.emit_task_failed(
                        &completion.name,
                        def,
                        job_id,
                        &e.to_string(),
                    )
                    .await;
                    oracle.task_failed(&completion.name);
                    join_set.abort_all();
                    return Err(DriverError::Execution {
                        task: completion.name,
                        source: e,
                    });
                }
            }
        }

        Ok(results)
    }

    async fn emit_task_queued(&self, td: &TaskDefinition, job_id: &JobId, org_id: &OrgId) {
        let Some(ref emitter) = self.emitter else {
            return;
        };
        let Some(task_id) = make_task_id(&td.name) else {
            return;
        };
        let subject = subjects::tasks::queue(td.task_type.as_str());
        let dedup = dedup_id(emitter.component(), emitter.instance_id(), "task-queued");
        let payload = payloads::TaskQueued {
            task_id,
            job_id: job_id.clone(),
            org_id: org_id.clone(),
            task_type: td.task_type.clone(),
            params: td.params.clone(),
            retry_count: 0,
        };
        if let Err(e) = emitter.emit(&subject, 1, dedup, payload).await {
            warn!(error = %e, subject, "failed to emit TaskQueued");
        }
    }

    async fn emit_task_completed(
        &self,
        task_name: &str,
        def: &JobDefinition,
        job_id: &JobId,
        result_ref: Option<&str>,
    ) {
        let Some(ref emitter) = self.emitter else {
            return;
        };
        let Some(task_id) = make_task_id(task_name) else {
            return;
        };
        let task_type = def
            .tasks
            .iter()
            .find(|t| t.name == task_name)
            .map(|t| t.task_type.clone());
        let Some(task_type) = task_type else { return };
        let subject = subjects::tasks::terminal(task_type.as_str());
        let dedup = dedup_id(emitter.component(), emitter.instance_id(), "task-completed");
        let payload = payloads::TaskCompleted {
            task_id,
            job_id: job_id.clone(),
            task_type,
            completed_at: now_millis(),
            result_ref: result_ref.map(String::from),
        };
        if let Err(e) = emitter.emit(&subject, 1, dedup, payload).await {
            warn!(error = %e, subject, "failed to emit TaskCompleted");
        }
    }

    async fn emit_task_failed(
        &self,
        task_name: &str,
        def: &JobDefinition,
        job_id: &JobId,
        error_msg: &str,
    ) {
        let Some(ref emitter) = self.emitter else {
            return;
        };
        let Some(task_id) = make_task_id(task_name) else {
            return;
        };
        let task_def = def.tasks.iter().find(|t| t.name == task_name);
        let Some(td) = task_def else { return };
        let subject = subjects::tasks::terminal(td.task_type.as_str());
        let dedup = dedup_id(emitter.component(), emitter.instance_id(), "task-failed");
        let payload = payloads::TaskFailed {
            task_id,
            job_id: job_id.clone(),
            task_type: td.task_type.clone(),
            failed_at: now_millis(),
            error: error_msg.to_string(),
            retry_count: 0,
            max_retries: td.max_retries.unwrap_or(0),
        };
        if let Err(e) = emitter.emit(&subject, 1, dedup, payload).await {
            warn!(error = %e, subject, "failed to emit TaskFailed");
        }
    }
}

/// Convenience wrapper: runs a job without event emission.
///
/// Creates a `Driver` with no emitter and synthesized IDs.
/// All existing callers and tests use this unchanged.
pub async fn run_job(
    def: &JobDefinition,
    operative: Arc<dyn Operative>,
) -> Result<Vec<(String, TaskOutcome)>, DriverError> {
    let driver = Driver::new(operative, None);
    let job_id = JobId::new("job_untracked").expect("hardcoded valid id");
    let org_id = OrgId::new("org_untracked").expect("hardcoded valid id");
    driver.run_job(def, &job_id, &org_id).await
}

fn make_task_id(name: &str) -> Option<TaskId> {
    TaskId::new(&format!("task_{name}")).ok()
}

#[allow(clippy::cast_possible_truncation)]
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_millis() as u64
}

/// Build structured data from a `TaskOutcome` for downstream consumption.
fn outcome_to_data(outcome: &TaskOutcome) -> Option<serde_json::Value> {
    match outcome {
        TaskOutcome::Completed { output, data, .. } => {
            if let Some(d) = data {
                Some(d.clone())
            } else {
                Some(serde_json::Value::Array(
                    output
                        .iter()
                        .map(|l| serde_json::Value::String(l.clone()))
                        .collect(),
                ))
            }
        }
        TaskOutcome::Failed { .. } => None,
    }
}

/// Resolve `input_from` references against completed task outputs.
/// Returns a new `TaskParams` with resolved values merged in.
fn resolve_inputs(
    task: &TaskDefinition,
    outcome_data: &HashMap<String, serde_json::Value>,
) -> Result<TaskParams, DriverError> {
    let mut params = task.params.clone();

    for (param_name, ref_str) in &task.input_from {
        let parts: Vec<&str> = ref_str.splitn(2, '.').collect();
        let source_task = parts[0];

        let data = outcome_data
            .get(source_task)
            .ok_or_else(|| DriverError::InputResolution {
                task: task.name.clone(),
                reference: ref_str.clone(),
                reason: "source task has no data".to_string(),
            })?;

        let value = if parts.len() > 1 {
            let pointer = format!("/{}", parts[1].replace('.', "/"));
            data.pointer(&pointer)
                .ok_or_else(|| DriverError::InputResolution {
                    task: task.name.clone(),
                    reference: ref_str.clone(),
                    reason: format!("path '{pointer}' not found"),
                })?
        } else {
            data
        };

        // Flatten to string for params
        let str_val = match value {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        params.entries.insert(param_name.clone(), str_val);
    }

    Ok(params)
}

/// Prepare a `TaskDefinition` for dispatch: resolve `input_from` if present.
fn prepare_task(
    td: &TaskDefinition,
    outcome_data: &HashMap<String, serde_json::Value>,
) -> Result<TaskDefinition, DriverError> {
    if td.input_from.is_empty() {
        return Ok(td.clone());
    }
    let resolved_params = resolve_inputs(td, outcome_data)?;
    let mut resolved = td.clone();
    resolved.params = resolved_params;
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_operative::MockOperative;
    use gbe_jobs_domain::{JobDefinition, TaskDefinition, TaskParams, TaskType};

    fn linear_dag() -> JobDefinition {
        JobDefinition {
            v: 1,
            name: "Linear".to_string(),
            job_type: "linear".to_string(),
            tasks: vec![
                TaskDefinition {
                    name: "a".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec![],
                    params: TaskParams::default(),
                    input_from: HashMap::new(),
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "b".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["a".to_string()],
                    params: TaskParams::default(),
                    input_from: HashMap::new(),
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "c".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["b".to_string()],
                    params: TaskParams::default(),
                    input_from: HashMap::new(),
                    timeout_secs: None,
                    max_retries: None,
                },
            ],
        }
    }

    fn diamond_dag() -> JobDefinition {
        JobDefinition {
            v: 1,
            name: "Diamond".to_string(),
            job_type: "diamond".to_string(),
            tasks: vec![
                TaskDefinition {
                    name: "root".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec![],
                    params: TaskParams::default(),
                    input_from: HashMap::new(),
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "left".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["root".to_string()],
                    params: TaskParams::default(),
                    input_from: HashMap::new(),
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "right".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["root".to_string()],
                    params: TaskParams::default(),
                    input_from: HashMap::new(),
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "join".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["left".to_string(), "right".to_string()],
                    params: TaskParams::default(),
                    input_from: HashMap::new(),
                    timeout_secs: None,
                    max_retries: None,
                },
            ],
        }
    }

    fn shell_dag() -> JobDefinition {
        let mut params = TaskParams::default();
        params
            .entries
            .insert("command".to_string(), "echo hello".to_string());
        JobDefinition {
            v: 1,
            name: "Shell".to_string(),
            job_type: "shell".to_string(),
            tasks: vec![TaskDefinition {
                name: "greet".to_string(),
                task_type: TaskType::new("shell").unwrap(),
                depends_on: vec![],
                params,
                input_from: HashMap::new(),
                timeout_secs: None,
                max_retries: None,
            }],
        }
    }

    #[tokio::test]
    async fn linear_dag_runs_in_order() {
        let mock = Arc::new(MockOperative::new(vec![TaskType::new("work").unwrap()]));
        let results = run_job(&linear_dag(), mock).await.unwrap();
        let names: Vec<&str> = results.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn diamond_dag_completes_all() {
        let mock = Arc::new(MockOperative::new(vec![TaskType::new("work").unwrap()]));
        let results = run_job(&diamond_dag(), mock).await.unwrap();
        let mut names: Vec<String> = results.into_iter().map(|(n, _)| n).collect();
        names.sort();
        assert_eq!(names, vec!["join", "left", "right", "root"]);
    }

    #[tokio::test]
    async fn fail_fast_on_task_failure() {
        let mock = Arc::new(MockOperative::new(vec![TaskType::new("work").unwrap()]));
        mock.set_outcome(
            "a",
            TaskOutcome::Failed {
                exit_code: 1,
                error: "boom".to_string(),
            },
        );

        let err = run_job(&linear_dag(), mock).await.unwrap_err();
        match err {
            DriverError::TaskFailed {
                task, exit_code, ..
            } => {
                assert_eq!(task, "a");
                assert_eq!(exit_code, 1);
            }
            other => panic!("expected TaskFailed, got: {other}"),
        }
    }

    #[tokio::test]
    async fn shell_operative_integration() {
        use crate::shell_operative::ShellOperative;
        let op = Arc::new(ShellOperative::for_types(&["shell"]).unwrap());
        let results = run_job(&shell_dag(), op).await.unwrap();
        assert_eq!(results.len(), 1);
        match &results[0].1 {
            TaskOutcome::Completed { output, .. } => {
                assert_eq!(output, &["hello"]);
            }
            TaskOutcome::Failed { .. } => panic!("expected success"),
        }
    }

    #[tokio::test]
    async fn input_from_resolves_json_field() {
        let mock = Arc::new(MockOperative::new(vec![TaskType::new("work").unwrap()]));
        mock.set_outcome(
            "producer",
            TaskOutcome::Completed {
                output: vec![],
                result_ref: None,
                data: Some(serde_json::json!({"url": "https://example.com", "count": 42})),
            },
        );

        let mut input_from = HashMap::new();
        input_from.insert("resolved_url".to_string(), "producer.url".to_string());

        let def = JobDefinition {
            v: 1,
            name: "Wiring".to_string(),
            job_type: "wiring".to_string(),
            tasks: vec![
                TaskDefinition {
                    name: "producer".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec![],
                    params: TaskParams::default(),
                    input_from: HashMap::new(),
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "consumer".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["producer".to_string()],
                    params: TaskParams::default(),
                    input_from,
                    timeout_secs: None,
                    max_retries: None,
                },
            ],
        };

        let results = run_job(&def, mock.clone()).await.unwrap();
        assert_eq!(results.len(), 2);

        // Verify consumer received the resolved param
        let consumer_task = mock.last_executed("consumer").unwrap();
        assert_eq!(
            consumer_task.params.entries.get("resolved_url").unwrap(),
            "https://example.com"
        );
    }

    #[tokio::test]
    async fn input_from_missing_field_errors() {
        let mock = Arc::new(MockOperative::new(vec![TaskType::new("work").unwrap()]));
        mock.set_outcome(
            "producer",
            TaskOutcome::Completed {
                output: vec![],
                result_ref: None,
                data: Some(serde_json::json!({"name": "test"})),
            },
        );

        let mut input_from = HashMap::new();
        input_from.insert(
            "missing".to_string(),
            "producer.nonexistent.field".to_string(),
        );

        let def = JobDefinition {
            v: 1,
            name: "Bad Ref".to_string(),
            job_type: "bad-ref".to_string(),
            tasks: vec![
                TaskDefinition {
                    name: "producer".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec![],
                    params: TaskParams::default(),
                    input_from: HashMap::new(),
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "consumer".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["producer".to_string()],
                    params: TaskParams::default(),
                    input_from,
                    timeout_secs: None,
                    max_retries: None,
                },
            ],
        };

        let err = run_job(&def, mock).await.unwrap_err();
        assert!(matches!(err, DriverError::InputResolution { .. }));
    }

    #[tokio::test]
    async fn input_from_uses_output_lines_when_no_data() {
        let mock = Arc::new(MockOperative::new(vec![TaskType::new("work").unwrap()]));
        mock.set_outcome(
            "producer",
            TaskOutcome::Completed {
                output: vec!["line1".to_string(), "line2".to_string()],
                result_ref: None,
                data: None,
            },
        );

        let mut input_from = HashMap::new();
        input_from.insert("first_line".to_string(), "producer.0".to_string());

        let def = JobDefinition {
            v: 1,
            name: "Lines".to_string(),
            job_type: "lines".to_string(),
            tasks: vec![
                TaskDefinition {
                    name: "producer".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec![],
                    params: TaskParams::default(),
                    input_from: HashMap::new(),
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "consumer".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["producer".to_string()],
                    params: TaskParams::default(),
                    input_from,
                    timeout_secs: None,
                    max_retries: None,
                },
            ],
        };

        let results = run_job(&def, mock.clone()).await.unwrap();
        assert_eq!(results.len(), 2);

        let consumer_task = mock.last_executed("consumer").unwrap();
        assert_eq!(
            consumer_task.params.entries.get("first_line").unwrap(),
            "line1"
        );
    }
}
