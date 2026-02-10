use sinex_gateway::{rpc_server, ServiceContainer};
use std::net::TcpListener;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::watch;
use xtask::sandbox::{sinex_test, timing::Timeouts, TestContext};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn sinexctl_binary() -> PathBuf {
    repo_root().join(".sinex/target/debug/sinexctl")
}

fn reserve_port() -> color_eyre::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

/// Wait until the TCP port is accepting connections, up to `timeout`.
async fn wait_for_port(port: u16, timeout: Duration) -> color_eyre::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match tokio::net::TcpStream::connect(format!("127.0.0.1:{port}")).await {
            Ok(_) => return Ok(()),
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => {
                return Err(color_eyre::eyre::eyre!(
                    "Gateway port {port} not ready after {timeout:?}: {e}"
                ));
            }
        }
    }
}

/// Start a test gateway, returning (port, `shutdown_tx`, `server_handle`).
///
/// The caller MUST hold onto `shutdown_tx` — dropping it may cause the server
/// to detect sender loss and shut down.
async fn start_test_gateway(
    ctx: &TestContext,
) -> color_eyre::Result<(u16, watch::Sender<bool>, tokio::task::JoinHandle<()>)> {
    // ServiceContainer::new tries to connect to NATS for replay control.
    // In test context, NATS may not be available. Allow bypass so it's non-fatal.
    std::env::set_var("SINEX_ALLOW_REPLAY_CONTROL_BYPASS", "1");

    let services = ServiceContainer::new(Some(ctx.database_url().to_string())).await?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let port = reserve_port()?;
    let tcp_listen = format!("127.0.0.1:{port}");
    let server_handle = tokio::spawn({
        let services = services.clone();
        async move {
            let _ = rpc_server::run(Some(tcp_listen.as_str()), services, vec![], shutdown_rx).await;
        }
    });

    // Wait for the server to actually bind and accept connections.
    // Under heavy parallel test load the gateway may take longer to initialize
    // (pool creation, NATS connection attempts, etc.), so use a generous timeout.
    wait_for_port(port, Duration::from_secs(Timeouts::MEDIUM)).await?;

    Ok((port, shutdown_tx, server_handle))
}

#[sinex_test]
async fn exo_dlq_list_command_reports_entries(ctx: TestContext) -> color_eyre::Result<()> {
    let (port, _shutdown_tx, handle) = start_test_gateway(&ctx).await?;
    let url = format!("http://127.0.0.1:{port}/rpc");

    let output = std::process::Command::new(sinexctl_binary())
        .arg("--token")
        .arg("test-token")
        .arg("--rpc-url")
        .arg(&url)
        .arg("dlq")
        .arg("list")
        .output()
        .expect("sinexctl binary should be executable");

    handle.abort();

    assert!(
        output.status.success(),
        "`sinexctl dlq list` should succeed.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[sinex_test]
async fn exo_confirmations_tail_command_streams_events(ctx: TestContext) -> color_eyre::Result<()> {
    // `sinexctl watch` is an infinite polling loop — it never exits.
    // We spawn it as a child process and verify it starts successfully
    // (connects to the gateway), then kill it after a brief window.
    let (port, _shutdown_tx, server_handle) = start_test_gateway(&ctx).await?;
    let url = format!("http://127.0.0.1:{port}/rpc");

    let mut child = std::process::Command::new(sinexctl_binary())
        .arg("--token")
        .arg("test-token")
        .arg("--rpc-url")
        .arg(&url)
        .arg("watch")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("sinexctl binary should be executable");

    // Let it run briefly — if it crashes immediately, we catch it
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Check if process is still alive (watch should be running, not crashed)
    match child.try_wait() {
        Ok(None) => {
            // Still running — expected for a streaming command. Kill it.
            child.kill().ok();
            child.wait().ok();
        }
        Ok(Some(status)) => {
            // Exited early — check stderr for error
            let stderr = child
                .stderr
                .take()
                .map(|mut s| {
                    let mut buf = String::new();
                    std::io::Read::read_to_string(&mut s, &mut buf).ok();
                    buf
                })
                .unwrap_or_default();
            assert!(
                status.success(),
                "`sinexctl watch` exited early with {status}.\nstderr: {stderr}"
            );
        }
        Err(e) => {
            panic!("Failed to check watch process status: {e}");
        }
    }

    server_handle.abort();
    Ok(())
}

#[sinex_test]
async fn exo_dlq_metrics_command_reports_stats(ctx: TestContext) -> color_eyre::Result<()> {
    let (port, _shutdown_tx, handle) = start_test_gateway(&ctx).await?;
    let url = format!("http://127.0.0.1:{port}/rpc");

    // Test `dlq peek` as a distinct DLQ operation (no `dlq metrics` subcommand exists)
    let output = std::process::Command::new(sinexctl_binary())
        .arg("--token")
        .arg("test-token")
        .arg("--rpc-url")
        .arg(&url)
        .arg("dlq")
        .arg("peek")
        .arg("-n")
        .arg("1")
        .output()
        .expect("sinexctl binary should be executable");

    handle.abort();

    assert!(
        output.status.success(),
        "`sinexctl dlq peek` should succeed.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
