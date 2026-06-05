//! CLI live tests for `sinexctl replay *` commands against a real gateway.
//!
//! Follows the same pattern as `sinexctl_integration_test.rs`: starts an
//! in-process gateway with self-signed TLS, then invokes the sinexctl binary
//! as a subprocess for each replay command.

use sinex_workspace_tests::built_binary;
use sinexd::api::{ServiceContainer, config::GatewayConfig, rpc_server};
use std::net::TcpListener;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::sync::watch;
use xtask::sandbox::{TestContext, sinex_test, timing::Timeouts};

const TEST_TOKEN: &str = "test-token:admin";

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
            "SINEX_API_TLS_CERT",
            cert_file.path().to_string_lossy().to_string(),
        );
        std::env::set_var(
            "SINEX_API_TLS_KEY",
            key_file.path().to_string_lossy().to_string(),
        );
        std::env::remove_var("SINEX_API_TLS_CLIENT_CA");
        std::env::set_var("SINEX_API_TOKEN", TEST_TOKEN);
        std::env::set_var("SINEX_NATS_URL", &nats_url);
    }

    let port = reserve_port()?;
    let mut config = GatewayConfig::load_with_database_url(ctx.database_url().to_string())?;
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
    let mut cmd = tokio::process::Command::new(built_binary("sinexctl"));
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
    let json = parse_json_stdout(output, "replay operation");
    // sinexctl outputs the operation_id at the top level or under "operation"
    json["operation_id"]
        .as_str()
        .or_else(|| json["operation"]["operation_id"].as_str())
        .unwrap_or_else(|| panic!("no operation_id in output: {json}"))
        .to_string()
}

fn parse_json_stdout(output: &std::process::Output, label: &str) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!(
            "failed to parse {label} JSON output: {e}\nstdout: {stdout}\nstderr: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

fn parse_json_lines_stdout(output: &std::process::Output, label: &str) -> Vec<serde_json::Value> {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let trimmed = stdout.trim();
    if trimmed.is_empty() || trimmed == "[]" {
        return Vec::new();
    }

    trimmed
        .lines()
        .map(|line| {
            serde_json::from_str(line).unwrap_or_else(|e| {
                panic!(
                    "failed to parse {label} JSON line: {e}\nline: {line}\nstdout: {stdout}\nstderr: {}",
                    String::from_utf8_lossy(&output.stderr)
                )
            })
        })
        .collect()
}

#[sinex_test(timeout = 60)]
async fn sinexctl_replay_plan_creates_operation(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let gw = start_test_gateway(&ctx).await?;
    let url = format!("https://127.0.0.1:{}/rpc", gw.port);

    let output = sinexctl_replay(
        &url,
        &["plan", "--source", "test-source", "--since", "1h", "-f", "json"],
    )
    .await;
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout_str = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "sinexctl replay plan should succeed.\nstderr: {stderr}\nstdout: {stdout_str}",
    );

    let operation = parse_json_stdout(&output, "replay plan");
    assert_eq!(operation["state"].as_str(), Some("Planning"));
    assert_eq!(
        operation["scope"]["source_name"].as_str(),
        Some("test-source")
    );
    assert!(
        operation["operation_id"].as_str().is_some(),
        "plan output should contain operation_id: {operation}"
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
        &["plan", "--source", "test-source", "--since", "1h", "-f", "json"],
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

    let preview = parse_json_stdout(&preview_output, "replay preview");
    assert_eq!(
        preview["operation"]["operation_id"].as_str(),
        Some(op_id.as_str())
    );
    assert_eq!(preview["operation"]["state"].as_str(), Some("Previewed"));
    assert!(
        preview["preview"]["total_events"].as_u64().is_some(),
        "preview output should contain numeric total_events: {preview}"
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
        &["plan", "--source", "test-source", "--since", "1h", "-f", "json"],
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
    let approved = parse_json_stdout(&approve_output, "replay approve");
    assert_eq!(approved["operation_id"].as_str(), Some(op_id.as_str()));
    assert_eq!(approved["state"].as_str(), Some("Approved"));

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
        &["plan", "--source", "test-source", "--since", "1h", "-f", "json"],
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

    let cancelled = parse_json_stdout(&cancel_output, "replay cancel");
    assert_eq!(cancelled["operation_id"].as_str(), Some(op_id.as_str()));
    assert_eq!(cancelled["state"].as_str(), Some("Cancelled"));
    assert_eq!(cancelled["cancelled"].as_bool(), Some(true));

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
        &["plan", "--source", "test-source", "--since", "1h", "-f", "json"],
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

    let status = parse_json_stdout(&status_output, "replay status");
    assert_eq!(status["operation_id"].as_str(), Some(op_id.as_str()));
    assert_eq!(status["state"].as_str(), Some("Planning"));

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
        &["plan", "--source", "source-a", "--since", "1h", "-f", "json"],
    )
    .await;
    assert!(plan1.status.success());
    let plan2 = sinexctl_replay(
        &url,
        &["plan", "--source", "source-b", "--since", "1h", "-f", "json"],
    )
    .await;
    assert!(plan2.status.success());

    let list_output = sinexctl_replay(&url, &["list", "-f", "json"]).await;
    assert!(
        list_output.status.success(),
        "sinexctl replay list should succeed.\nstderr: {}",
        String::from_utf8_lossy(&list_output.stderr)
    );

    let operations = parse_json_lines_stdout(&list_output, "replay list");
    let source_ids: Vec<_> = operations
        .iter()
        .filter_map(|operation| operation["scope"]["source_name"].as_str())
        .collect();
    assert!(
        source_ids.contains(&"source-a") && source_ids.contains(&"source-b"),
        "list should contain both operations, got sources {source_ids:?}"
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
        &["plan", "--source", "test-source", "--since", "1h", "-f", "json"],
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
            "--source",
            "test-source-2",
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

    // List with --state planning filter
    let list_planning = sinexctl_replay(&url, &["list", "--state", "planning", "-f", "json"]).await;
    assert!(list_planning.status.success());

    let cancelled_ops = parse_json_lines_stdout(&list_cancelled, "replay cancelled list");
    let planning_ops = parse_json_lines_stdout(&list_planning, "replay planning list");
    let cancelled_sources: Vec<_> = cancelled_ops
        .iter()
        .filter_map(|operation| operation["scope"]["source_name"].as_str())
        .collect();
    let planning_sources: Vec<_> = planning_ops
        .iter()
        .filter_map(|operation| operation["scope"]["source_name"].as_str())
        .collect();

    assert!(
        cancelled_ops
            .iter()
            .all(|operation| operation["state"].as_str() == Some("Cancelled")),
        "cancelled filter should only return cancelled operations: {cancelled_ops:?}"
    );
    assert!(
        planning_ops
            .iter()
            .all(|operation| operation["state"].as_str() == Some("Planning")),
        "planning filter should only return planning operations: {planning_ops:?}"
    );
    assert!(
        !cancelled_sources.contains(&"test-source-2"),
        "cancelled filter should not include planning operations: {cancelled_sources:?}"
    );
    assert!(
        planning_sources.contains(&"test-source-2"),
        "planning filter should include the planning operation: {planning_sources:?}"
    );

    gw.handle.abort();
    Ok(())
}
