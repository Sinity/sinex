//! Tests replay idempotency guard: rejects duplicate operations for the same node.
//!
//! Verifies the guard added in `create_operation()` that prevents concurrent
//! replay operations targeting the same node_id.

use color_eyre::eyre::bail;
use serde_json::json;
use sinex_gateway::{ServiceContainer, config::GatewayConfig, rpc_server};
use sinex_primitives::rpc::methods;
use sinex_primitives::temporal::Timestamp;
use std::net::TcpListener;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::sync::watch;
use xtask::sandbox::{EnvGuard, prelude::*};

const RPC_TOKEN: &str = "idempotency-test-token:admin";

struct LiveGateway {
    port: u16,
    _shutdown_tx: watch::Sender<bool>,
    _handle: tokio::task::JoinHandle<()>,
    _cert_file: NamedTempFile,
    _key_file: NamedTempFile,
    client: reqwest::Client,
}

impl LiveGateway {
    async fn start(database_url: &str, env_guard: &mut EnvGuard) -> TestResult<Self> {
        let cert =
            rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])?;
        let cert_file = NamedTempFile::new()?;
        let key_file = NamedTempFile::new()?;
        tokio::fs::write(cert_file.path(), cert.cert.pem()).await?;
        tokio::fs::write(key_file.path(), cert.key_pair.serialize_pem()).await?;

        env_guard.set("SINEX_GATEWAY_TLS_CERT", cert_file.path().to_string_lossy().to_string());
        env_guard.set("SINEX_GATEWAY_TLS_KEY", key_file.path().to_string_lossy().to_string());
        env_guard.clear("SINEX_GATEWAY_TLS_CLIENT_CA");
        env_guard.set("SINEX_RPC_TOKEN", RPC_TOKEN);
        env_guard.set("DATABASE_URL", database_url);
        env_guard.set("SINEX_ALLOW_TEST_ACTORS", "1");

        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        drop(listener);

        let mut config = GatewayConfig::load()?;
        config.database_url = database_url.to_string();
        config.tcp_listen = format!("127.0.0.1:{port}");
        config.rpc_rate_limit_enabled = false;
        let services = ServiceContainer::new(&config).await?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            if let Err(e) = rpc_server::run(&config, services, shutdown_rx).await {
                eprintln!("Gateway RPC server failed: {e:#}");
            }
        });

        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            match tokio::net::TcpStream::connect(format!("127.0.0.1:{port}")).await {
                Ok(_) => break,
                Err(_) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => bail!("Gateway port {port} not ready: {e}"),
            }
        }

        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()?;

        Ok(Self {
            port,
            _shutdown_tx: shutdown_tx,
            _handle: handle,
            _cert_file: cert_file,
            _key_file: key_file,
            client,
        })
    }

    async fn rpc(&self, method: &str, params: serde_json::Value) -> TestResult<serde_json::Value> {
        let body = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1,
        });

        let resp = self
            .client
            .post(format!("https://127.0.0.1:{}/rpc", self.port))
            .header("Authorization", format!("Bearer {RPC_TOKEN}"))
            .json(&body)
            .send()
            .await?;

        Ok(resp.json().await?)
    }
}

fn scope_for_node(node_id: &str) -> serde_json::Value {
    let ts = Timestamp::now();
    let scope_start = ts - time::Duration::seconds(10);
    let scope_end = ts + time::Duration::seconds(10);
    json!({
        "scope": {
            "node_id": node_id,
            "time_window": [scope_start.format_rfc3339(), scope_end.format_rfc3339()],
        },
        "actor": "test:idempotency-tester"
    })
}

/// Creating two operations for the same node should fail on the second.
#[sinex_test(timeout = 60)]
async fn duplicate_plan_for_same_node_rejected(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", ctx.nats_handle()?.client_url());

    let gw = LiveGateway::start(ctx.database_url(), &mut env_guard).await?;

    // First creation: succeeds
    let first = gw
        .rpc(methods::REPLAY_CREATE_OPERATION, scope_for_node("idem-node"))
        .await?;
    assert!(
        first.get("result").is_some(),
        "First create should succeed: {first}"
    );

    // Second creation for same node: should fail
    let second = gw
        .rpc(methods::REPLAY_CREATE_OPERATION, scope_for_node("idem-node"))
        .await?;
    let error_msg = second
        .get("error")
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
        .unwrap_or("");
    assert!(
        error_msg.contains("already active"),
        "Second create should be rejected with 'already active' error, got: {second}"
    );

    Ok(())
}

/// Concurrent creates for the same node should still admit only one active operation.
#[sinex_test(timeout = 60)]
async fn concurrent_duplicate_plan_for_same_node_rejected(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", ctx.nats_handle()?.client_url());

    let gw = LiveGateway::start(ctx.database_url(), &mut env_guard).await?;

    let first = gw.rpc(methods::REPLAY_CREATE_OPERATION, scope_for_node("idem-race-node"));
    let second = gw.rpc(methods::REPLAY_CREATE_OPERATION, scope_for_node("idem-race-node"));
    let (first, second) = tokio::join!(first, second);
    let first = first?;
    let second = second?;

    let successes = [&first, &second]
        .into_iter()
        .filter(|response| response.get("result").is_some())
        .count();
    let errors: Vec<&str> = [&first, &second]
        .into_iter()
        .filter_map(|response| {
            response
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(|message| message.as_str())
        })
        .collect();

    assert_eq!(successes, 1, "exactly one concurrent create should succeed: first={first}, second={second}");
    assert!(
        errors.iter().any(|message| message.contains("already active")),
        "one concurrent create should be rejected as already active: first={first}, second={second}"
    );

    Ok(())
}

/// Different nodes can have concurrent operations.
#[sinex_test(timeout = 60)]
async fn different_nodes_allowed_concurrent(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", ctx.nats_handle()?.client_url());

    let gw = LiveGateway::start(ctx.database_url(), &mut env_guard).await?;

    let first = gw
        .rpc(methods::REPLAY_CREATE_OPERATION, scope_for_node("node-a"))
        .await?;
    assert!(first.get("result").is_some(), "First node create should succeed: {first}");

    let second = gw
        .rpc(methods::REPLAY_CREATE_OPERATION, scope_for_node("node-b"))
        .await?;
    assert!(
        second.get("result").is_some(),
        "Different node create should succeed: {second}"
    );

    Ok(())
}

/// After cancelling, a new operation for the same node should succeed.
#[sinex_test(timeout = 60)]
async fn cancelled_allows_new_operation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", ctx.nats_handle()?.client_url());

    let gw = LiveGateway::start(ctx.database_url(), &mut env_guard).await?;

    // Create and cancel
    let create_resp = gw
        .rpc(methods::REPLAY_CREATE_OPERATION, scope_for_node("cancel-node"))
        .await?;
    let op_id = create_resp["result"]["operation"]["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();

    let cancel_resp = gw
        .rpc(
            methods::REPLAY_CANCEL_OPERATION,
            json!({ "operation_id": op_id, "reason": "testing idempotency" }),
        )
        .await?;
    assert!(cancel_resp.get("result").is_some(), "Cancel should succeed: {cancel_resp}");

    // New operation for same node after cancel: should succeed
    let new_resp = gw
        .rpc(methods::REPLAY_CREATE_OPERATION, scope_for_node("cancel-node"))
        .await?;
    assert!(
        new_resp.get("result").is_some(),
        "New operation after cancel should succeed: {new_resp}"
    );

    Ok(())
}
