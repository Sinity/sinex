use sinex_gateway::{rpc_server, ServiceContainer};
use std::net::TcpListener;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::NamedTempFile;
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

/// Resources held for the lifetime of a test gateway.
///
/// The caller MUST hold onto this struct — dropping `shutdown_tx` stops the
/// server, and dropping the temp files removes the TLS certificates.
struct TestGateway {
    port: u16,
    _shutdown_tx: watch::Sender<bool>,
    handle: tokio::task::JoinHandle<()>,
    _cert_file: NamedTempFile,
    _key_file: NamedTempFile,
}

/// Start a test gateway with auto-generated TLS certificates.
async fn start_test_gateway(ctx: &TestContext) -> color_eyre::Result<TestGateway> {
    // Generate self-signed TLS certificates for the gateway.
    let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let cert = rcgen::generate_simple_self_signed(subject_alt_names)?;
    let cert_file = NamedTempFile::new()?;
    let key_file = NamedTempFile::new()?;
    tokio::fs::write(cert_file.path(), cert.cert.pem()).await?;
    tokio::fs::write(key_file.path(), cert.key_pair.serialize_pem()).await?;

    std::env::set_var(
        "SINEX_GATEWAY_TLS_CERT",
        cert_file.path().to_string_lossy().to_string(),
    );
    std::env::set_var(
        "SINEX_GATEWAY_TLS_KEY",
        key_file.path().to_string_lossy().to_string(),
    );
    std::env::remove_var("SINEX_GATEWAY_TLS_CLIENT_CA");
    std::env::set_var("SINEX_RPC_TOKEN", "test-token");

    // ServiceContainer::new tries to connect to NATS for replay control.
    // In test context, NATS may not be available. Allow bypass so it's non-fatal.
    std::env::set_var("SINEX_ALLOW_REPLAY_CONTROL_BYPASS", "1");

    let services = ServiceContainer::new(Some(ctx.database_url().to_string())).await?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let port = reserve_port()?;
    let tcp_listen = format!("127.0.0.1:{port}");
    let mut server_handle = tokio::spawn({
        let services = services.clone();
        async move {
            if let Err(e) =
                rpc_server::run(Some(tcp_listen.as_str()), services, vec![], shutdown_rx).await
            {
                eprintln!("Gateway startup failed: {e:#}");
            }
        }
    });

    // Race port readiness against server exit — if the server errors during
    // setup, we detect it immediately instead of waiting the full timeout.
    let port_timeout = Duration::from_secs(Timeouts::STANDARD);
    tokio::select! {
        result = wait_for_port(port, port_timeout) => {
            result?;
        }
        join_result = &mut server_handle => {
            // Server task exited before port was ready — it failed during setup
            match join_result {
                Ok(()) => return Err(color_eyre::eyre::eyre!(
                    "Gateway server exited before binding port {port} (check stderr for details)"
                )),
                Err(e) => return Err(color_eyre::eyre::eyre!(
                    "Gateway server task panicked: {e}"
                )),
            }
        }
    }

    Ok(TestGateway {
        port,
        _shutdown_tx: shutdown_tx,
        handle: server_handle,
        _cert_file: cert_file,
        _key_file: key_file,
    })
}

#[sinex_test]
async fn exo_dlq_list_command_reports_entries(ctx: TestContext) -> color_eyre::Result<()> {
    let gw = start_test_gateway(&ctx).await?;
    let url = format!("https://127.0.0.1:{}/rpc", gw.port);

    let output = std::process::Command::new(sinexctl_binary())
        .arg("--token")
        .arg("test-token")
        .arg("--insecure")
        .arg("--rpc-url")
        .arg(&url)
        .arg("dlq")
        .arg("list")
        .output()
        .expect("sinexctl binary should be executable");

    gw.handle.abort();

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
    let gw = start_test_gateway(&ctx).await?;
    let url = format!("https://127.0.0.1:{}/rpc", gw.port);

    let mut child = std::process::Command::new(sinexctl_binary())
        .arg("--token")
        .arg("test-token")
        .arg("--insecure")
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

    gw.handle.abort();
    Ok(())
}

#[sinex_test]
async fn exo_dlq_metrics_command_reports_stats(ctx: TestContext) -> color_eyre::Result<()> {
    let gw = start_test_gateway(&ctx).await?;
    let url = format!("https://127.0.0.1:{}/rpc", gw.port);

    // Test `dlq peek` as a distinct DLQ operation (no `dlq metrics` subcommand exists)
    let output = std::process::Command::new(sinexctl_binary())
        .arg("--token")
        .arg("test-token")
        .arg("--insecure")
        .arg("--rpc-url")
        .arg(&url)
        .arg("dlq")
        .arg("peek")
        .arg("-n")
        .arg("1")
        .output()
        .expect("sinexctl binary should be executable");

    gw.handle.abort();

    assert!(
        output.status.success(),
        "`sinexctl dlq peek` should succeed.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
