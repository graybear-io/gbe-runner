use clap::Parser;
use gbe_jobs_domain::JobDefinition;
use gbe_operative::{
    run_job, CompositeOperative, DriverError, HttpOperative, MoleculeOperative, ShellOperative,
};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use tracing::{error, info};

#[derive(Parser)]
#[command(name = "gbe-operative", about = "DAG-based job runner")]
struct Cli {
    /// Path to YAML job definition
    #[arg(long)]
    job: PathBuf,

    /// Task types the shell operative handles (comma-separated)
    #[arg(long, default_value = "shell")]
    task_types: String,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

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

    // Two-phase construction: build shell+http as the molecule delegate,
    // then build the final composite with all three including molecule.
    let delegate: Arc<dyn gbe_operative::Operative> =
        Arc::new(CompositeOperative::from_operatives(&[
            shell.clone(),
            http.clone(),
        ]));
    let molecule = Arc::new(MoleculeOperative::for_types(&["molecule"], delegate).unwrap());
    let operative = Arc::new(CompositeOperative::from_operatives(&[
        shell, http, molecule,
    ]));

    match run_job(&def, operative).await {
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
