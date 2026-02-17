use std::path::PathBuf;
use std::time::Duration;

/// Runner configuration: paths to binaries, timeouts, backend selection.
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    pub router_bin: PathBuf,
    pub adapter_bin: PathBuf,
    pub backend: Backend,
    pub task_timeout: Duration,
    pub discovery_timeout: Duration,
    pub discovery_interval: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Backend {
    Memory,
    Redis(String),
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            router_bin: PathBuf::from("gbe-router"),
            adapter_bin: PathBuf::from("gbe-adapter"),
            backend: Backend::Memory,
            task_timeout: Duration::from_secs(300),
            discovery_timeout: Duration::from_secs(5),
            discovery_interval: Duration::from_millis(100),
        }
    }
}
