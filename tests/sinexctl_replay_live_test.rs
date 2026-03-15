//! CLI live tests for `sinexctl replay *` commands against a real gateway.
//!
//! Follows the same pattern as `sinexctl_integration_test.rs`: starts an
//! in-process gateway with self-signed TLS, then invokes the sinexctl binary
//! as a subprocess for each replay command.

use sinex_gateway::{ServiceContainer, config::GatewayConfig, rpc_server};
use std::net::TcpListener;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::sync::watch;
use xtask::sandbox::{TestContext, sinex_test, timing::Timeouts};

const TEST_TOKEN: &str = "test-token:admin";

fn sinexctl_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".sinex/target/debug/sinexctl")
}

fn reserve_port() -> color_eyre::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

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

struct TestGateway {
    port: u16,
    _shutdown_tx: watch::Sender<bool>,
    handle: tokio::task::JoinHandle<()>,
    _cert_file: NamedTempFile,
    _key_file: NamedTempFile,
}

async fn start_test_gateway(ctx: &TestContext) -> color_eyre::Result<TestGateway> {
    let nats_url = ctx.nats_handle()?.client_url().to_string();

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])?;
    let cert_file = NamedTempFile::new()?;
    let key_file = NamedTempFile::new()?;
    tokio::fs::write(cert_file.path(), cert.cert.pem()).await?;
    tokio::fs::write(key_file.path(), cert.key_pair.serialize_pem()).await?;

    unsafe {
        std::env::set_var(
            "SINEX_GATEWAY_TLS_CERT",
            cert_file.path().to_string_lossy().to_string(),
        );
        std::env::set_var(
            "SINEX_GATEWAY_TLS_KEY",
            key_file.path().to_string_lossy().to_string(),
        );
        std::env::remove_var("SINEX_GATEWAY_TLS_CLIENT_CA");
        std::env::set_var("SINEX_RPC_TOKEN", TEST_TOKEN);
        std::env::set_var("SINEX_NATS_URL", &nats_url);
        std::env::remove_var("SINEX_REPLAY_CONTROL_OPTIONAL");
    }

    let port = reserve_port()?;
    let mut config = GatewayConfig::load();
    config.database_url = ctx.database_url().to_string();
    config.tcp_listen = format!("127.0.0.1:{port}");
    config.rpc_rate_limit_enabled = false;
    let services = ServiceContainer::new(&config).await?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut server_handle = tokio::spawn({
        let services = services.clone();
        async move {
            if let Err(e) = rpc_server::run(&config, services, shutdown_rx).await {
                eprintln!("Gateway startup failed: {e:#}");
            }
        }
    });

    let port_timeout = Duration::from_secs(Timeouts::STANDARD);
    tokio::select! {
        result = wait_for_port(port, port_timeout) => { result?; }
        join_result = &mut server_handle => {
            match join_result {
                Ok(()) => return Err(color_eyre::eyre::eyre!(
                    "Gateway exited before binding port {port}"
                )),
                Err(e) => return Err(color_eyre::eyre::eyre!(
                    "Gateway panicked: {e}"
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

/// Helper: run sinexctl with common flags + replay subcommand args.
///
/// Uses `tokio::process::Command` to avoid blocking a tokio worker thread.
/// The in-process gateway serves requests on the same runtime, so blocking
/// a worker with `std::process::Command::output()` can starve the TLS
/// acceptor and cause sinexctl's HTTPS requests to time out.
async fn sinexctl_replay(url: &str, args: &[&str]) -> std::process::Output {
    let mut cmd = tokio::process::Command::new(sinexctl_binary());
    cmd.arg("--token")
        .arg(TEST_TOKEN)
        .arg("--insecure")
        .arg("--timeout")
        .arg("10")
        .arg("--rpc-url")
        .arg(url)
        .arg("replay");
    for arg in args {
        cmd.arg(arg);
    }
    cmd.output()
        .await
        .expect("sinexctl binary should be executable")
}

/// Extract `operation_id` from sinexctl JSON output.
fn extract_op_id(output: &std::process::Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!(
            "failed to parse sinexctl JSON output: {e}\nstdout: {stdout}\nstderr: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    // sinexctl outputs the operation_id at the top level or under "operation"
    json["operation_id"]
        .as_str()
        .or_else(|| json["operation"]["operation_id"].as_str())
        .unwrap_or_else(|| panic!("no operation_id in output: {json}"))
        .to_string()
}

#[sinex_test(timeout = 60)]
async fn sinexctl_replay_plan_creates_operation(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let gw = start_test_gateway(&ctx).await?;
    let url = format!("https://127.0.0.1:{}/rpc", gw.port);

    let output = sinexctl_replay(
        &url,
        &["plan", "--node", "test-node", "--since", "1h", "-f", "json"],
    )
    .await;
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout_str = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "sinexctl replay plan should succeed.\nstderr: {stderr}\nstdout: {stdout_str}",
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("operation_id") || stdout.contains("operation"),
        "JSON output should contain operation data: {stdout}"
    );

    gw.handle.abort();
    Ok(())
}

#[sinex_test(timeout = 60)]
async fn sinexctl_replay_preview_after_plan(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let gw = start_test_gateway(&ctx).await?;
    let url = format!("https://127.0.0.1:{}/rpc", gw.port);

    // Plan
    let plan_output = sinexctl_replay(
        &url,
        &["plan", "--node", "test-node", "--since", "1h", "-f", "json"],
    )
    .await;
    assert!(plan_output.status.success(), "plan should succeed");
    let op_id = extract_op_id(&plan_output);

    // Preview
    let preview_output = sinexctl_replay(&url, &["preview", &op_id, "-f", "json"]).await;
    assert!(
        preview_output.status.success(),
        "sinexctl replay preview should succeed.\nstderr: {}",
        String::from_utf8_lossy(&preview_output.stderr)
    );

    let stdout = String::from_utf8_lossy(&preview_output.stdout);
    assert!(
        stdout.contains("preview") || stdout.contains("total_events"),
        "preview output should contain preview data: {stdout}"
    );

    gw.handle.abort();
    Ok(())
}

#[sinex_test(timeout = 60)]
async fn sinexctl_replay_approve_after_preview(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let gw = start_test_gateway(&ctx).await?;
    let url = format!("https://127.0.0.1:{}/rpc", gw.port);

    let plan_output = sinexctl_replay(
        &url,
        &["plan", "--node", "test-node", "--since", "1h", "-f", "json"],
    )
    .await;
    assert!(plan_output.status.success());
    let op_id = extract_op_id(&plan_output);

    sinexctl_replay(&url, &["preview", &op_id, "-f", "json"]).await;

    let approve_output = sinexctl_replay(&url, &["approve", &op_id, "-f", "json"]).await;
    assert!(
        approve_output.status.success(),
        "sinexctl replay approve should succeed.\nstderr: {}",
        String::from_utf8_lossy(&approve_output.stderr)
    );

    gw.handle.abort();
    Ok(())
}

#[sinex_test(timeout = 60)]
async fn sinexctl_replay_cancel_with_reason(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let gw = start_test_gateway(&ctx).await?;
    let url = format!("https://127.0.0.1:{}/rpc", gw.port);

    let plan_output = sinexctl_replay(
        &url,
        &["plan", "--node", "test-node", "--since", "1h", "-f", "json"],
    )
    .await;
    assert!(plan_output.status.success());
    let op_id = extract_op_id(&plan_output);

    sinexctl_replay(&url, &["preview", &op_id, "-f", "json"]).await;

    let cancel_output = sinexctl_replay(
        &url,
        &[
            "cancel",
            &op_id,
            "--reason",
            "test cancel from CLI",
            "-f",
            "json",
        ],
    )
    .await;
    assert!(
        cancel_output.status.success(),
        "sinexctl replay cancel should succeed.\nstderr: {}",
        String::from_utf8_lossy(&cancel_output.stderr)
    );

    let stdout = String::from_utf8_lossy(&cancel_output.stdout);
    assert!(
        stdout.contains("cancel") || stdout.contains("Cancelled"),
        "cancel output should confirm cancellation: {stdout}"
    );

    gw.handle.abort();
    Ok(())
}

#[sinex_test(timeout = 60)]
async fn sinexctl_replay_status_shows_state(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let gw = start_test_gateway(&ctx).await?;
    let url = format!("https://127.0.0.1:{}/rpc", gw.port);

    let plan_output = sinexctl_replay(
        &url,
        &["plan", "--node", "test-node", "--since", "1h", "-f", "json"],
    )
    .await;
    assert!(plan_output.status.success());
    let op_id = extract_op_id(&plan_output);

    let status_output = sinexctl_replay(&url, &["status", &op_id, "-f", "json"]).await;
    assert!(
        status_output.status.success(),
        "sinexctl replay status should succeed.\nstderr: {}",
        String::from_utf8_lossy(&status_output.stderr)
    );

    let stdout = String::from_utf8_lossy(&status_output.stdout);
    assert!(
        stdout.contains("state") || stdout.contains("Planning"),
        "status output should contain state: {stdout}"
    );

    gw.handle.abort();
    Ok(())
}

#[sinex_test(timeout = 60)]
async fn sinexctl_replay_list_returns_operations(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let gw = start_test_gateway(&ctx).await?;
    let url = format!("https://127.0.0.1:{}/rpc", gw.port);

    // Create two plans
    let plan1 = sinexctl_replay(
        &url,
        &["plan", "--node", "node-a", "--since", "1h", "-f", "json"],
    )
    .await;
    assert!(plan1.status.success());
    let plan2 = sinexctl_replay(
        &url,
        &["plan", "--node", "node-b", "--since", "1h", "-f", "json"],
    )
    .await;
    assert!(plan2.status.success());

    let list_output = sinexctl_replay(&url, &["list", "-f", "json"]).await;
    assert!(
        list_output.status.success(),
        "sinexctl replay list should succeed.\nstderr: {}",
        String::from_utf8_lossy(&list_output.stderr)
    );

    let stdout = String::from_utf8_lossy(&list_output.stdout);
    // JSON list output should contain both operations
    assert!(
        stdout.contains("node-a") && stdout.contains("node-b"),
        "list should contain both operations: {stdout}"
    );

    gw.handle.abort();
    Ok(())
}

#[sinex_test(timeout = 60)]
async fn sinexctl_replay_list_filters_by_state(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let gw = start_test_gateway(&ctx).await?;
    let url = format!("https://127.0.0.1:{}/rpc", gw.port);

    // Create a plan, preview, then cancel it
    let plan_output = sinexctl_replay(
        &url,
        &["plan", "--node", "test-node", "--since", "1h", "-f", "json"],
    )
    .await;
    assert!(plan_output.status.success());
    let op_id = extract_op_id(&plan_output);
    sinexctl_replay(&url, &["preview", &op_id, "-f", "json"]).await;
    let cancel = sinexctl_replay(
        &url,
        &["cancel", &op_id, "--reason", "filter test", "-f", "json"],
    )
    .await;
    assert!(cancel.status.success());

    // Create another plan (stays in Planning)
    let plan2 = sinexctl_replay(
        &url,
        &[
            "plan",
            "--node",
            "test-node-2",
            "--since",
            "1h",
            "-f",
            "json",
        ],
    )
    .await;
    assert!(plan2.status.success());

    // List with --state cancelled filter
    let list_cancelled =
        sinexctl_replay(&url, &["list", "--state", "cancelled", "-f", "json"]).await;
    assert!(list_cancelled.status.success());
    let stdout_cancelled = String::from_utf8_lossy(&list_cancelled.stdout);

    // List with --state planning filter
    let list_planning = sinexctl_replay(&url, &["list", "--state", "planning", "-f", "json"]).await;
    assert!(list_planning.status.success());
    let stdout_planning = String::from_utf8_lossy(&list_planning.stdout);

    // Cancelled list should not contain the planning-state operation's node
    // Planning list should not contain the cancelled operation
    assert!(
        !stdout_cancelled.contains("test-node-2"),
        "cancelled filter should not include planning operations: {stdout_cancelled}"
    );
    assert!(
        stdout_planning.contains("test-node-2"),
        "planning filter should include the planning operation: {stdout_planning}"
    );

    gw.handle.abort();
    Ok(())
}
