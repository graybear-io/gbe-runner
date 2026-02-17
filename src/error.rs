use std::io;

/// Errors produced by the gbe-runner orchestrator.
#[derive(Debug, thiserror::Error)]
pub enum RunnerError {
    #[error("yaml: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("invalid job definition: {0}")]
    Definition(#[from] gbe_jobs_domain::JobsDomainError),

    #[error("adapter failed: task {task} exited {exit_code}")]
    Adapter { task: String, exit_code: i32 },

    #[error("router: {0}")]
    Router(String),

    #[error("timeout: task {0} exceeded deadline")]
    Timeout(String),

    #[error("state store: {0}")]
    StateStore(#[from] gbe_state_store::StateStoreError),

    #[error("transport: {0}")]
    Transport(#[from] gbe_nexus::TransportError),

    #[error("protocol: {0}")]
    Protocol(#[from] gbe_protocol::ProtocolError),

    #[error("io: {0}")]
    Io(#[from] io::Error),

    #[error("{0}")]
    Other(String),
}
