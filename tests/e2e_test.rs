//! E2E tests with real gbe-router and gbe-adapter.
//!
//! Requires pre-built binaries in ../gbe-envoy/target/debug/.
//! Run with: cargo test --test e2e_test -- --ignored

use gbe_protocol::{ControlMessage, DataFrame};
use gbe_runner::router_client::RouterClient;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;

static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

// -- Test infrastructure --

/// Manages a router process and its spawned subprocesses (proxy, etc.)
/// with automatic cleanup on drop.
struct TestRouter {
    child: Child,
    socket: String,
    pid: u32,
}

impl TestRouter {
    async fn start() -> Self {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let socket = format!(
            "/tmp/gbe-runner-e2e-{}-{}.sock",
            std::process::id(),
            id
        );
        let _ = std::fs::remove_file(&socket);

        let child = Command::new(envoy_bin("gbe-router"))
            .args(["--socket", &socket])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to start gbe-router");

        let pid = child.id();

        for _ in 0..50 {
            if std::path::Path::new(&socket).exists() {
                return Self { child, socket, pid };
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("router socket not created after 500ms");
    }

    fn start_adapter(&self, cmd: &[&str]) -> ManagedChild {
        let mut args: Vec<&str> = vec!["--router", &self.socket, "--"];
        args.extend_from_slice(cmd);
        let child = Command::new(envoy_bin("gbe-adapter"))
            .args(&args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to start gbe-adapter");
        ManagedChild { child }
    }
}

impl Drop for TestRouter {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.socket);

        // Clean up data and proxy sockets created by this router (keyed by PID)
        let data_pattern = format!("/tmp/gbe-{}-", self.pid);
        let proxy_pattern = format!("/tmp/gbe-proxy-{}-", self.pid);
        if let Ok(entries) = std::fs::read_dir("/tmp") {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.starts_with(&data_pattern[5..])
                    || name.starts_with(&proxy_pattern[5..])
                {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }

        // Kill any proxy processes spawned by this router
        let _ = Command::new("pkill")
            .args(["-f", &format!("gbe-proxy.*gbe-proxy-{}-", self.pid)])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// Child process with automatic kill on drop.
struct ManagedChild {
    child: Child,
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn envoy_bin(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../gbe-envoy/target/debug")
        .join(name);
    assert!(
        path.exists(),
        "{} not found — run `cargo build` in gbe-envoy first",
        path.display()
    );
    path.to_string_lossy().into_owned()
}

/// Register as a client with the router (required before Subscribe).
async fn register_client(client: &mut RouterClient) {
    client
        .send(&ControlMessage::Connect {
            capabilities: vec![],
        })
        .await
        .unwrap();

    match client.recv().await.unwrap() {
        ControlMessage::ConnectAck { .. } => {}
        other => panic!("expected ConnectAck, got: {:?}", other),
    }
}

/// Subscribe to a tool's data stream and read all output lines.
async fn subscribe_and_read(
    client: &mut RouterClient,
    tool_id: String,
) -> Vec<String> {
    client
        .send(&ControlMessage::Subscribe { target: tool_id })
        .await
        .unwrap();

    let data_address = match client.recv().await.unwrap() {
        ControlMessage::SubscribeAck {
            data_connect_address,
            ..
        } => data_connect_address,
        other => panic!("expected SubscribeAck, got: {:?}", other),
    };

    let path = data_address
        .strip_prefix("unix://")
        .unwrap_or(&data_address);

    // Retry connection briefly — proxy socket may take a moment to accept
    let mut stream = None;
    for _ in 0..10 {
        match UnixStream::connect(path).await {
            Ok(s) => {
                stream = Some(s);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    let mut stream = stream.unwrap_or_else(|| {
        panic!("failed to connect to data socket at {}", path);
    });

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();

    let mut lines = Vec::new();
    let mut cursor = std::io::Cursor::new(&buf);
    while let Ok(frame) = DataFrame::read_from(&mut cursor) {
        if let Ok(line) = String::from_utf8(frame.payload) {
            let trimmed = line.trim().to_string();
            if !trimmed.is_empty() {
                lines.push(trimmed);
            }
        }
    }
    lines
}

// -- Tests --

#[tokio::test]
#[ignore] // Requires pre-built gbe-envoy binaries
async fn e2e_echo_output_capture() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let router = TestRouter::start().await;
        let mut adapter = router.start_adapter(&["echo", "hello world"]);

        // Wait for adapter to register with router
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut client = RouterClient::connect(&router.socket).await.unwrap();
        register_client(&mut client).await;

        let tools = client.query_tools().await.unwrap();
        assert!(!tools.is_empty(), "adapter should have registered");

        let tool_id = tools[0].tool_id.clone();
        let lines = subscribe_and_read(&mut client, tool_id).await;

        assert!(!lines.is_empty(), "should capture output");
        assert!(
            lines.iter().any(|l| l.contains("hello world")),
            "output should contain 'hello world', got: {:?}",
            lines
        );

        let status = adapter.child.wait().expect("adapter wait failed");
        assert!(status.success(), "echo should exit 0");
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
#[ignore] // Requires pre-built gbe-envoy binaries
async fn e2e_failing_command_exit_code() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let router = TestRouter::start().await;
        let mut adapter = router.start_adapter(&["false"]);

        // Wait for adapter to register with router
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut client = RouterClient::connect(&router.socket).await.unwrap();
        register_client(&mut client).await;

        let tools = client.query_tools().await.unwrap();
        assert!(!tools.is_empty(), "adapter should have registered");

        let tool_id = tools[0].tool_id.clone();
        // Subscribe to unblock adapter's data accept() thread
        let _ = subscribe_and_read(&mut client, tool_id).await;

        let status = adapter.child.wait().expect("adapter wait failed");
        assert!(!status.success(), "`false` should exit non-zero");
        assert_eq!(status.code(), Some(1));
    })
    .await
    .expect("test timed out");
}
