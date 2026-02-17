use crate::error::RunnerError;
use crate::executor::{TaskContext, TaskExecutor, TaskResult};
use gbe_jobs_domain::JobDefinition;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::{error, info};

/// Result of a completed task within the JoinSet.
struct TaskCompletion {
    name: String,
    result: Result<TaskResult, RunnerError>,
}

/// Orchestrates DAG execution: dispatches ready tasks concurrently via
/// JoinSet, tracks completions, unblocks dependents, and fails fast
/// on any task failure.
pub struct Orchestrator {
    executor: Arc<dyn TaskExecutor>,
}

impl Orchestrator {
    pub fn new(executor: Arc<dyn TaskExecutor>) -> Self {
        Self { executor }
    }

    /// Run all tasks in the job definition according to the DAG.
    /// Returns Ok with all task results on success, or the first error.
    pub async fn run(
        &self,
        def: &JobDefinition,
        ctx: &TaskContext,
    ) -> Result<Vec<(String, TaskResult)>, RunnerError> {
        let mut completed: HashSet<String> = HashSet::new();
        let mut dispatched: HashSet<String> = HashSet::new();
        let mut results: Vec<(String, TaskResult)> = Vec::new();
        let mut join_set: JoinSet<TaskCompletion> = JoinSet::new();

        // Seed with root tasks
        for root_name in def.roots() {
            let task_def = find_task(def, root_name)?;
            dispatch_task(
                &mut join_set,
                &mut dispatched,
                task_def.clone(),
                ctx.clone(),
                self.executor.clone(),
            );
        }

        // Process completions
        while let Some(join_result) = join_set.join_next().await {
            let completion = join_result
                .map_err(|e| RunnerError::Other(format!("task join error: {e}")))?;

            match completion.result {
                Ok(ref task_result) if task_result.exit_code == 0 => {
                    info!(task = %completion.name, "task completed successfully");
                    completed.insert(completion.name.clone());
                    results.push((completion.name, task_result.clone()));

                    // Unblock and dispatch newly ready tasks
                    for task_def in &def.tasks {
                        if dispatched.contains(&task_def.name) {
                            continue;
                        }
                        let all_deps_met = task_def
                            .depends_on
                            .iter()
                            .all(|dep| completed.contains(dep));
                        if all_deps_met {
                            dispatch_task(
                                &mut join_set,
                                &mut dispatched,
                                task_def.clone(),
                                ctx.clone(),
                                self.executor.clone(),
                            );
                        }
                    }
                }
                Ok(ref task_result) => {
                    // Non-zero exit code: fail fast
                    error!(
                        task = %completion.name,
                        exit_code = task_result.exit_code,
                        "task failed"
                    );
                    join_set.abort_all();
                    return Err(RunnerError::Adapter {
                        task: completion.name,
                        exit_code: task_result.exit_code,
                    });
                }
                Err(e) => {
                    error!(task = %completion.name, error = %e, "task execution error");
                    join_set.abort_all();
                    return Err(e);
                }
            }
        }

        Ok(results)
    }
}

fn find_task<'a>(
    def: &'a JobDefinition,
    name: &str,
) -> Result<&'a gbe_jobs_domain::TaskDefinition, RunnerError> {
    def.tasks
        .iter()
        .find(|t| t.name == name)
        .ok_or_else(|| RunnerError::Other(format!("task not found in definition: {name}")))
}

fn dispatch_task(
    join_set: &mut JoinSet<TaskCompletion>,
    dispatched: &mut HashSet<String>,
    task_def: gbe_jobs_domain::TaskDefinition,
    ctx: TaskContext,
    executor: Arc<dyn TaskExecutor>,
) {
    let name = task_def.name.clone();
    dispatched.insert(name.clone());
    info!(task = %name, "dispatching task");
    join_set.spawn(async move {
        let result = executor.execute(&task_def, &ctx).await;
        TaskCompletion { name, result }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::mock::MockExecutor;
    use gbe_jobs_domain::{JobDefinition, OrgId, TaskDefinition, TaskParams, TaskType};

    fn test_ctx() -> TaskContext {
        TaskContext {
            org_id: OrgId::new("org_test").unwrap(),
            date: "2026-02-16".to_string(),
            router_socket: "/tmp/test.sock".to_string(),
        }
    }

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
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "b".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["a".to_string()],
                    params: TaskParams::default(),
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "c".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["b".to_string()],
                    params: TaskParams::default(),
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
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "left".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["root".to_string()],
                    params: TaskParams::default(),
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "right".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["root".to_string()],
                    params: TaskParams::default(),
                    timeout_secs: None,
                    max_retries: None,
                },
                TaskDefinition {
                    name: "join".to_string(),
                    task_type: TaskType::new("work").unwrap(),
                    depends_on: vec!["left".to_string(), "right".to_string()],
                    params: TaskParams::default(),
                    timeout_secs: None,
                    max_retries: None,
                },
            ],
        }
    }

    #[tokio::test]
    async fn linear_dag_runs_in_order() {
        let mock = Arc::new(MockExecutor::new());
        let orch = Orchestrator::new(mock);
        let ctx = test_ctx();

        let results = orch.run(&linear_dag(), &ctx).await.unwrap();
        let names: Vec<&str> = results.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn diamond_dag_completes_all_four() {
        let mock = Arc::new(MockExecutor::new());
        let orch = Orchestrator::new(mock);
        let ctx = test_ctx();

        let results = orch.run(&diamond_dag(), &ctx).await.unwrap();
        let mut names: Vec<String> = results.into_iter().map(|(n, _)| n).collect();
        names.sort();
        assert_eq!(names, vec!["join", "left", "right", "root"]);
    }

    #[tokio::test]
    async fn diamond_dag_join_runs_last() {
        let mock = Arc::new(MockExecutor::new());
        let orch = Orchestrator::new(mock);
        let ctx = test_ctx();

        let results = orch.run(&diamond_dag(), &ctx).await.unwrap();
        let last = results.last().unwrap();
        assert_eq!(last.0, "join");
    }

    #[tokio::test]
    async fn fail_fast_on_task_failure() {
        let mock = Arc::new(MockExecutor::new());
        mock.set_result(
            "a",
            TaskResult {
                exit_code: 1,
                output: vec!["error".to_string()],
            },
        );
        let orch = Orchestrator::new(mock);
        let ctx = test_ctx();

        let err = orch.run(&linear_dag(), &ctx).await.unwrap_err();
        match err {
            RunnerError::Adapter { task, exit_code } => {
                assert_eq!(task, "a");
                assert_eq!(exit_code, 1);
            }
            other => panic!("expected Adapter error, got: {other}"),
        }
    }

    #[tokio::test]
    async fn fail_fast_skips_downstream() {
        let mock = Arc::new(MockExecutor::new());
        mock.set_result(
            "b",
            TaskResult {
                exit_code: 42,
                output: vec![],
            },
        );
        let orch = Orchestrator::new(mock);
        let ctx = test_ctx();

        let err = orch.run(&linear_dag(), &ctx).await.unwrap_err();
        match err {
            RunnerError::Adapter { task, exit_code } => {
                assert_eq!(task, "b");
                assert_eq!(exit_code, 42);
            }
            other => panic!("expected Adapter error, got: {other}"),
        }
    }
}
