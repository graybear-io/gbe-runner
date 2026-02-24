use clap::Parser;
use gbe_jobs_domain::{subjects, ComponentStarted, Heartbeat, JobDefinition, JobId, OrgId};
use gbe_nexus::{dedup_id, EventEmitter};
use gbe_operative::{
    CompositeOperative, Driver, DriverError, HttpOperative, LlmOperative, MoleculeOperative,
    ShellOperative,
};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use tracing::{error, info, warn};

#[derive(Parser)]
#[command(name = "gbe-operative", about = "DAG-based job runner")]
struct Cli {
    /// Path to YAML job definition
    #[arg(long)]
    job: PathBuf,

    /// Task types the shell operative handles (comma-separated)
    #[arg(long, default_value = "shell")]
    task_types: String,

    /// Organization ID for event emission
    #[arg(long, default_value = "org_default")]
    org_id: String,
}

#[allow(clippy::cast_possible_truncation)]
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_millis() as u64
}

fn build_emitter() -> Option<Arc<EventEmitter>> {
    let url = std::env::var("GBE_TRANSPORT_URL").ok()?;
    // No transport implementation bundled yet.
    // When a concrete Transport (NATS/Redis) is available, construct it from `url`.
    info!(url = %url, "GBE_TRANSPORT_URL set but no transport implementation available");
    let _ = url;
    None::<Arc<EventEmitter>>
}

fn spawn_heartbeat(
    emitter: Arc<EventEmitter>,
    start: std::time::Instant,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            interval.tick().await;
            let payload = Heartbeat {
                component: emitter.component().to_string(),
                instance_id: emitter.instance_id().to_string(),
                timestamp: now_millis(),
                uptime_secs: start.elapsed().as_secs(),
            };
            let subject = subjects::lifecycle::heartbeat(emitter.component());
            let dedup = dedup_id(emitter.component(), emitter.instance_id(), "heartbeat");
            if let Err(e) = emitter.emit(&subject, 1, dedup, payload).await {
                warn!(error = %e, "failed to emit heartbeat");
            }
        }
    })
}

async fn emit_started(emitter: &EventEmitter) {
    let payload = ComponentStarted {
        component: emitter.component().to_string(),
        instance_id: emitter.instance_id().to_string(),
        started_at: now_millis(),
        version: option_env!("CARGO_PKG_VERSION").map(String::from),
    };
    let subject = subjects::lifecycle::started(emitter.component());
    let dedup = dedup_id(emitter.component(), emitter.instance_id(), "started");
    if let Err(e) = emitter.emit(&subject, 1, dedup, payload).await {
        warn!(error = %e, "failed to emit ComponentStarted");
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    let start = std::time::Instant::now();

    let yaml_bytes = match std::fs::read(&cli.job) {
        Ok(bytes) => bytes,
        Err(e) => {
            error!(path = %cli.job.display(), error = %e, "failed to read job file");
            return ExitCode::from(2);
        }
    };

    let def: JobDefinition = match serde_yaml::from_slice(&yaml_bytes) {
        Ok(def) => def,
        Err(e) => {
            error!(error = %e, "invalid job YAML");
            return ExitCode::from(2);
        }
    };

    if let Err(e) = def.validate() {
        error!(error = %e, "invalid job definition");
        return ExitCode::from(2);
    }

    info!(job = %def.name, tasks = def.tasks.len(), "loaded job definition");

    let types: Vec<&str> = cli.task_types.split(',').collect();
    let shell = match ShellOperative::for_types(&types) {
        Ok(op) => Arc::new(op),
        Err(e) => {
            error!(error = %e, "invalid task types");
            return ExitCode::from(2);
        }
    };
    let http = Arc::new(HttpOperative::for_types(&["http"]).unwrap());

    let llm_base_url =
        std::env::var("LLM_API_URL").unwrap_or_else(|_| "https://api.openai.com".to_string());
    let llm = Arc::new(LlmOperative::with_defaults(
        vec![gbe_jobs_domain::TaskType::new("llm").unwrap()],
        llm_base_url,
    ));

    // Two-phase construction: build shell+http+llm as the molecule delegate,
    // then build the final composite with all four including molecule.
    let delegate: Arc<dyn gbe_operative::Operative> =
        Arc::new(CompositeOperative::from_operatives(&[
            shell.clone(),
            http.clone(),
            llm.clone(),
        ]));
    let molecule = Arc::new(MoleculeOperative::for_types(&["molecule"], delegate).unwrap());
    let operative = Arc::new(CompositeOperative::from_operatives(&[
        shell, http, llm, molecule,
    ]));

    // Build optional event emitter from transport
    let emitter = build_emitter();

    // Emit ComponentStarted and spawn heartbeat if transport is available
    let heartbeat_handle = if let Some(ref em) = emitter {
        emit_started(em).await;
        Some(spawn_heartbeat(Arc::clone(em), start))
    } else {
        None
    };

    let job_id = JobId::new(&format!("job_{}", def.job_type))
        .unwrap_or_else(|_| JobId::new("job_unknown").expect("hardcoded valid id"));
    let org_id = OrgId::new(&cli.org_id)
        .unwrap_or_else(|_| OrgId::new("org_default").expect("hardcoded valid id"));

    let driver = Driver::new(operative, emitter);
    let result = driver.run_job(&def, &job_id, &org_id).await;

    // Cancel heartbeat
    if let Some(handle) = heartbeat_handle {
        handle.abort();
    }

    match result {
        Ok(results) => {
            info!(completed = results.len(), "all tasks completed");
            ExitCode::from(0)
        }
        Err(DriverError::TaskFailed {
            task, exit_code, ..
        }) => {
            error!(task, exit_code, "task failed");
            ExitCode::from(1)
        }
        Err(e) => {
            error!(error = %e, "driver error");
            ExitCode::from(2)
        }
    }
}
