mod config;
mod error;
mod executor;
mod memory_store;
mod orchestrator;
mod router_client;
mod state;

use crate::config::{Backend, RunnerConfig};
use crate::error::RunnerError;
use crate::executor::{EnvoyExecutor, TaskContext};
use crate::memory_store::MemoryStateStore;
use crate::orchestrator::Orchestrator;
use crate::state::{NexusStateManager, StateManager};
use clap::Parser;
use gbe_jobs_domain::{JobDefinition, JobId, OrgId, TaskId};
use gbe_nexus_memory::{MemoryTransport, MemoryTransportConfig};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use tokio::process::Command;
use tracing::{error, info};

#[derive(Parser)]
#[command(name = "gbe-runner", about = "DAG-based job orchestrator")]
struct Cli {
    /// Organization ID (e.g. org_acme)
    #[arg(long)]
    org: String,

    /// Target date (e.g. 2026-02-16)
    #[arg(long)]
    date: String,

    /// Path to YAML job definition
    #[arg(long)]
    job: PathBuf,

    /// Transport/state backend
    #[arg(long, default_value = "memory")]
    backend: String,

    /// Path to gbe-router binary
    #[arg(long, default_value = "gbe-router")]
    router_bin: PathBuf,

    /// Path to gbe-adapter binary
    #[arg(long, default_value = "gbe-adapter")]
    adapter_bin: PathBuf,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt::init();

    match run().await {
        Ok(()) => ExitCode::from(0),
        Err(RunnerError::Adapter { task, exit_code }) => {
            error!(task, exit_code, "task failed");
            ExitCode::from(1)
        }
        Err(e) => {
            error!(error = %e, "runner error");
            ExitCode::from(2)
        }
    }
}

async fn run() -> Result<(), RunnerError> {
    let cli = Cli::parse();

    // Validate org ID
    let org_id = OrgId::new(&cli.org)?;

    // Load and validate job definition
    let yaml_bytes = std::fs::read(&cli.job)
        .map_err(|e| RunnerError::Other(format!("read job file: {e}")))?;
    let def: JobDefinition = serde_yaml::from_slice(&yaml_bytes)?;
    def.validate()?;
    info!(job = %def.name, tasks = def.tasks.len(), "loaded job definition");

    // Build config
    let config = RunnerConfig {
        router_bin: cli.router_bin,
        adapter_bin: cli.adapter_bin,
        backend: match cli.backend.as_str() {
            "memory" => Backend::Memory,
            other if other.starts_with("redis://") => Backend::Redis(other.to_string()),
            other => return Err(RunnerError::Other(format!("unknown backend: {other}"))),
        },
        ..RunnerConfig::default()
    };

    // Start router
    let socket_path = format!("/tmp/gbe-runner-{}.sock", std::process::id());
    let mut router = Command::new(&config.router_bin)
        .arg("--socket")
        .arg(&socket_path)
        .spawn()
        .map_err(|e| RunnerError::Router(format!("spawn router: {e}")))?;

    // Brief pause for router to bind
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let result = run_job(&org_id, &cli.date, &def, &config, &socket_path).await;

    // Stop router
    let _ = router.kill().await;
    let _ = std::fs::remove_file(&socket_path);

    result
}

async fn run_job(
    org_id: &OrgId,
    date: &str,
    def: &JobDefinition,
    config: &RunnerConfig,
    socket_path: &str,
) -> Result<(), RunnerError> {
    // Construct backends
    let transport: Arc<dyn gbe_nexus::Transport> =
        Arc::new(MemoryTransport::new(MemoryTransportConfig::default()));
    let store: Arc<dyn gbe_state_store::StateStore> = Arc::new(MemoryStateStore::new());
    let state_mgr = Arc::new(NexusStateManager::new(store, transport));

    // Generate IDs
    let job_id = JobId::new(&format!("job_{}", ulid::Ulid::new().to_string().to_lowercase()))?;
    let task_ids: Vec<(String, TaskId)> = def
        .tasks
        .iter()
        .map(|t| {
            let id = TaskId::new(&format!(
                "task_{}",
                ulid::Ulid::new().to_string().to_lowercase()
            ))
            .unwrap();
            (t.name.clone(), id)
        })
        .collect();

    // Create job + task records
    state_mgr.create_job(&job_id, org_id, def, &task_ids).await?;
    info!(job_id = %job_id, "created job");

    // Build executor and context
    let executor: Arc<dyn crate::executor::TaskExecutor> =
        Arc::new(EnvoyExecutor::new(config.clone()));
    let ctx = TaskContext {
        org_id: org_id.clone(),
        date: date.to_string(),
        router_socket: socket_path.to_string(),
    };

    // Run orchestrator
    let orchestrator = Orchestrator::new(executor);
    let results = orchestrator.run(def, &ctx).await?;

    info!(completed = results.len(), "all tasks completed");
    state_mgr.complete_job(&job_id, org_id, &def.job_type).await?;

    Ok(())
}
