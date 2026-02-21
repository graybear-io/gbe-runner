use gbe_jobs_domain::JobDefinition;
use gbe_operative::{run_job, CompositeOperative, DriverError, MoleculeOperative, ShellOperative};
use std::sync::Arc;

fn load_fixture(name: &str) -> JobDefinition {
    let path = format!("{}/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    let yaml = std::fs::read(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let def: JobDefinition = serde_yaml::from_slice(&yaml).unwrap();
    def.validate().unwrap();
    def
}

fn shell_operative() -> Arc<ShellOperative> {
    Arc::new(ShellOperative::for_types(&["shell"]).unwrap())
}

#[tokio::test]
async fn linear_pipeline_runs_in_order() {
    let def = load_fixture("shell-pipeline.yaml");
    let results = run_job(&def, shell_operative()).await.unwrap();

    let names: Vec<&str> = results.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, vec!["greet", "count", "summarize"]);
}

#[tokio::test]
async fn linear_pipeline_captures_output() {
    let def = load_fixture("shell-pipeline.yaml");
    let results = run_job(&def, shell_operative()).await.unwrap();

    let greet_output = &results[0].1;
    match greet_output {
        gbe_jobs_domain::TaskOutcome::Completed { output, .. } => {
            assert_eq!(output, &["hello from greet"]);
        }
        gbe_jobs_domain::TaskOutcome::Failed { .. } => panic!("expected Completed"),
    }

    let count_output = &results[1].1;
    match count_output {
        gbe_jobs_domain::TaskOutcome::Completed { output, .. } => {
            assert_eq!(output, &["one", "two", "three"]);
        }
        gbe_jobs_domain::TaskOutcome::Failed { .. } => panic!("expected Completed"),
    }
}

#[tokio::test]
async fn diamond_dag_completes_all_tasks() {
    let def = load_fixture("diamond.yaml");
    let results = run_job(&def, shell_operative()).await.unwrap();

    let mut names: Vec<String> = results.into_iter().map(|(n, _)| n).collect();
    names.sort();
    assert_eq!(names, vec!["join", "left", "right", "root"]);
}

#[tokio::test]
async fn diamond_dag_join_runs_after_branches() {
    let def = load_fixture("diamond.yaml");
    let results = run_job(&def, shell_operative()).await.unwrap();

    let names: Vec<&str> = results.iter().map(|(n, _)| n.as_str()).collect();
    let join_pos = names.iter().position(|n| *n == "join").unwrap();
    let left_pos = names.iter().position(|n| *n == "left").unwrap();
    let right_pos = names.iter().position(|n| *n == "right").unwrap();
    assert!(join_pos > left_pos, "join must run after left");
    assert!(join_pos > right_pos, "join must run after right");
}

#[tokio::test]
async fn fail_fast_stops_on_bad_step() {
    let def = load_fixture("fail-fast.yaml");
    let err = run_job(&def, shell_operative()).await.unwrap_err();

    match err {
        DriverError::TaskFailed {
            task, exit_code, ..
        } => {
            assert_eq!(task, "bad-step");
            assert_eq!(exit_code, 42);
        }
        other => panic!("expected TaskFailed, got: {other}"),
    }
}

#[tokio::test]
async fn cli_runs_fixture() {
    let fixture = format!(
        "{}/fixtures/shell-pipeline.yaml",
        env!("CARGO_MANIFEST_DIR")
    );
    let bin = env!("CARGO_BIN_EXE_gbe-operative");

    let output = std::process::Command::new(bin)
        .args(["--job", &fixture, "--task-types", "shell"])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "CLI should exit 0, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn input_wiring_resolves_across_tasks() {
    let def = load_fixture("input-wiring.yaml");
    let results = run_job(&def, shell_operative()).await.unwrap();

    let mut names: Vec<String> = results.iter().map(|(n, _)| n.clone()).collect();
    names.sort();
    assert_eq!(names, vec!["consume", "produce"]);

    // Verify consume received the resolved URL
    let consume_outcome = results.iter().find(|(n, _)| n == "consume").unwrap();
    match &consume_outcome.1 {
        gbe_jobs_domain::TaskOutcome::Completed { output, .. } => {
            assert_eq!(output, &["fetching https://example.com"]);
        }
        gbe_jobs_domain::TaskOutcome::Failed { .. } => panic!("expected Completed"),
    }
}

#[tokio::test]
async fn mixed_types_routed_through_composite() {
    let def = load_fixture("mixed-types.yaml");
    let shell = Arc::new(ShellOperative::for_types(&["shell"]).unwrap());
    let cmd = Arc::new(ShellOperative::for_types(&["cmd"]).unwrap());
    let composite = Arc::new(CompositeOperative::from_operatives(&[shell, cmd]));

    let results = run_job(&def, composite).await.unwrap();
    let mut names: Vec<String> = results.iter().map(|(n, _)| n.clone()).collect();
    names.sort();
    assert_eq!(names, vec!["count", "greet"]);
}

#[tokio::test]
async fn cli_exits_nonzero_on_failure() {
    let fixture = format!("{}/fixtures/fail-fast.yaml", env!("CARGO_MANIFEST_DIR"));
    let bin = env!("CARGO_BIN_EXE_gbe-operative");

    let output = std::process::Command::new(bin)
        .args(["--job", &fixture, "--task-types", "shell"])
        .output()
        .unwrap();

    assert_eq!(
        output.status.code(),
        Some(1),
        "CLI should exit 1 on task failure"
    );
}

#[tokio::test]
async fn molecule_runs_inner_shell_pipeline() {
    let def = load_fixture("molecule.yaml");
    let shell = Arc::new(ShellOperative::for_types(&["shell"]).unwrap());
    let delegate: Arc<dyn gbe_operative::Operative> =
        Arc::new(CompositeOperative::from_operatives(&[
            shell.clone() as Arc<dyn gbe_operative::Operative>
        ]));
    let molecule = Arc::new(MoleculeOperative::for_types(&["molecule"], delegate).unwrap());
    let composite = Arc::new(CompositeOperative::from_operatives(&[shell, molecule]));

    let results = run_job(&def, composite).await.unwrap();

    let mut names: Vec<String> = results.iter().map(|(n, _)| n.clone()).collect();
    names.sort();
    assert_eq!(names, vec!["finalize", "inner-pipeline", "setup"]);

    // Verify molecule task produced aggregated data
    let mol_outcome = results.iter().find(|(n, _)| n == "inner-pipeline").unwrap();
    match &mol_outcome.1 {
        gbe_jobs_domain::TaskOutcome::Completed { data, output, .. } => {
            assert_eq!(output.len(), 2); // greet + count
            let data = data.as_ref().unwrap();
            assert!(data.get("greet").is_some());
            assert!(data.get("count").is_some());
        }
        gbe_jobs_domain::TaskOutcome::Failed { .. } => panic!("expected Completed"),
    }
}
