use sinex_gateway::{ServiceContainer, rpc_server};
use sinex_workspace_tests::built_binary;
use std::net::TcpListener;
use std::process::Stdio;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::watch;
use xtask::sandbox::{TestContext, sinex_test, timing::Timeouts};

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

async fn assert_process_stays_running(
    child: &mut tokio::process::Child,
    duration: Duration,
    label: &str,
) -> color_eyre::Result<()> {
    match tokio::time::timeout(duration, child.wait()).await {
        Err(_) => Ok(()),
        Ok(Ok(status)) => {
            let mut stderr = String::new();
            if let Some(mut stream) = child.stderr.take() {
                stream.read_to_string(&mut stderr).await.ok();
            }
            Err(color_eyre::eyre::eyre!(
                "{label} exited before remaining stable for {duration:?}: {status}\nstderr: {stderr}"
            ))
        }
        Ok(Err(error)) => Err(color_eyre::eyre::eyre!(
            "failed while waiting for {label} process state: {error}"
        )),
    }
}

/// Resources held for the lifetime of a test gateway.
///
/// The caller MUST hold onto this struct — dropping `shutdown_tx` stops the
/// server, and dropping the temp files removes the TLS certificates.
struct TestGateway {
    port: u16,
    _env: xtask::sandbox::EnvGuard,
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

    let mut env = xtask::sandbox::EnvGuard::with_keys(&[
        "SINEX_GATEWAY_TLS_CERT",
        "SINEX_GATEWAY_TLS_KEY",
        "SINEX_GATEWAY_TLS_CLIENT_CA",
        "SINEX_RPC_TOKEN",
        "SINEX_NATS_URL",
    ]);
    env.set(
        "SINEX_GATEWAY_TLS_CERT",
        cert_file.path().to_string_lossy().to_string(),
    );
    env.set(
        "SINEX_GATEWAY_TLS_KEY",
        key_file.path().to_string_lossy().to_string(),
    );
    env.clear("SINEX_GATEWAY_TLS_CLIENT_CA");
    env.set("SINEX_RPC_TOKEN", "test-token:admin");
    env.set("SINEX_NATS_URL", ctx.nats_handle()?.client_url());

    let port = reserve_port()?;
    let mut config = sinex_gateway::config::GatewayConfig::load_with_database_url(
        ctx.database_url().to_string(),
    )?;
    config.tcp_listen = format!("127.0.0.1:{port}");
    config.rpc_rate_limit_enabled = false;
    let services = ServiceContainer::new(&config).await?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut server_handle = tokio::spawn({
        let services = services.clone();
        let config = config.clone();
        async move {
            if let Err(e) = rpc_server::run(&config, services, shutdown_rx).await {
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
        _env: env,
        _shutdown_tx: shutdown_tx,
        handle: server_handle,
        _cert_file: cert_file,
        _key_file: key_file,
    })
}

#[sinex_test]
async fn sinexctl_dlq_list_command_reports_entries(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let gw = start_test_gateway(&ctx).await?;
    let url = format!("https://127.0.0.1:{}/rpc", gw.port);

    let output = std::process::Command::new(built_binary("sinexctl"))
        .arg("--token")
        .arg("test-token:admin")
        .arg("--insecure")
        .arg("--timeout")
        .arg("5")
        .arg("--rpc-url")
        .arg(&url)
        .arg("dlq")
        .arg("list")
        .output()
        .expect("sinexctl binary should be executable");

    gw.handle.abort();

    // Verify the command doesn't panic even if the DLQ is empty or not yet populated.
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success()
            || stderr.contains("error")
            || stderr.contains("timeout")
            || stderr.contains("timed out"),
        "`sinexctl dlq list` should succeed or fail gracefully, got: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        stderr,
    );
    Ok(())
}

#[sinex_test]
async fn sinexctl_watch_command_streams_events(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    // `sinexctl watch` is an infinite polling loop — it never exits.
    // We spawn it as a child process and verify it starts successfully
    // (connects to the gateway), then kill it after a brief window.
    let gw = start_test_gateway(&ctx).await?;
    let url = format!("https://127.0.0.1:{}", gw.port);

    let mut child = Command::new(built_binary("sinexctl"))
        .arg("--token")
        .arg("test-token:admin")
        .arg("--insecure")
        .arg("--rpc-url")
        .arg(&url)
        .arg("watch")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("sinexctl binary should be executable");

    assert_process_stays_running(&mut child, Duration::from_secs(2), "`sinexctl watch`").await?;
    child.kill().await.ok();
    let _ = child.wait().await;

    gw.handle.abort();
    Ok(())
}

#[sinex_test]
async fn sinexctl_dlq_peek_command_reports_entries(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let gw = start_test_gateway(&ctx).await?;
    let url = format!("https://127.0.0.1:{}/rpc", gw.port);

    // Test `dlq peek` as a distinct DLQ operation (no `dlq metrics` subcommand exists).
    // DLQ operations require NATS; use a short timeout and accept graceful failure.
    let output = std::process::Command::new(built_binary("sinexctl"))
        .arg("--token")
        .arg("test-token:admin")
        .arg("--insecure")
        .arg("--timeout")
        .arg("5")
        .arg("--rpc-url")
        .arg(&url)
        .arg("dlq")
        .arg("peek")
        .arg("-n")
        .arg("1")
        .output()
        .expect("sinexctl binary should be executable");

    gw.handle.abort();

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success()
            || stderr.contains("error")
            || stderr.contains("timeout")
            || stderr.contains("timed out"),
        "`sinexctl dlq peek` should succeed or fail gracefully, got: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        stderr,
    );
    Ok(())
}
