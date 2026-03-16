//! Tests role-based access control (RBAC) on replay RPC endpoints.
//!
//! Verifies that the RPC registry's role assignments are enforced:
//! - `ReadOnly` tokens can list/status but not create/approve/execute
//! - Write tokens can create/preview but not approve/execute
//! - Admin tokens have full access
//!
//! Each test starts a `LiveGateway` with the specific role token under test.

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

/// In-process gateway for role-scoped auth testing.
struct RoleGateway {
    port: u16,
    token: String,
    _shutdown_tx: watch::Sender<bool>,
    _handle: tokio::task::JoinHandle<()>,
    _cert_file: NamedTempFile,
    _key_file: NamedTempFile,
    client: reqwest::Client,
}

impl RoleGateway {
    async fn start(
        database_url: &str,
        nats_url: &str,
        role_token: &str,
        env_guard: &mut EnvGuard,
    ) -> TestResult<Self> {
        let cert =
            rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])?;
        let cert_file = NamedTempFile::new()?;
        let key_file = NamedTempFile::new()?;
        tokio::fs::write(cert_file.path(), cert.cert.pem()).await?;
        tokio::fs::write(key_file.path(), cert.key_pair.serialize_pem()).await?;

        env_guard.set(
            "SINEX_GATEWAY_TLS_CERT",
            cert_file.path().to_string_lossy().to_string(),
        );
        env_guard.set(
            "SINEX_GATEWAY_TLS_KEY",
            key_file.path().to_string_lossy().to_string(),
        );
        env_guard.clear("SINEX_GATEWAY_TLS_CLIENT_CA");
        env_guard.set("SINEX_RPC_TOKEN", role_token);
        env_guard.clear("SINEX_REPLAY_CONTROL_OPTIONAL");
        env_guard.set("DATABASE_URL", database_url);
        env_guard.set("SINEX_NATS_URL", nats_url);

        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        drop(listener);

        let mut config = GatewayConfig::load();
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
            token: role_token.to_string(),
            _shutdown_tx: shutdown_tx,
            _handle: handle,
            _cert_file: cert_file,
            _key_file: key_file,
            client,
        })
    }

    /// Make an authenticated JSON-RPC call, returning the full response body.
    /// Does NOT bail on JSON-RPC errors — caller inspects the response.
    async fn rpc_raw(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> TestResult<serde_json::Value> {
        let body = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1,
        });

        let resp = self
            .client
            .post(format!("https://127.0.0.1:{}/rpc", self.port))
            .header("Authorization", format!("Bearer {}", self.token))
            .json(&body)
            .send()
            .await?;

        Ok(resp.json().await?)
    }

    /// Check if a JSON-RPC response is a permission-denied error.
    fn is_permission_denied(response: &serde_json::Value) -> bool {
        response
            .get("error")
            .and_then(|e| e.get("message"))
            .and_then(|m| m.as_str())
            .is_some_and(|msg| msg.contains("requires") && msg.contains("role"))
    }

    /// Check if a JSON-RPC response has a result (success).
    fn has_result(response: &serde_json::Value) -> bool {
        response.get("result").is_some()
    }
}

fn test_scope_params() -> serde_json::Value {
    let ts = Timestamp::now();
    let scope_start = ts - time::Duration::seconds(10);
    let scope_end = ts + time::Duration::seconds(10);
    json!({
        "scope": {
            "node_id": "auth-test-node",
            "time_window": [scope_start.format_rfc3339(), scope_end.format_rfc3339()],
        },
        "actor": "test:auth-tester"
    })
}

// ── ReadOnly role tests ─────────────────────────────────────────────

#[sinex_test(timeout = 60)]
async fn readonly_can_list_operations(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut env_guard = EnvGuard::new();
    let gw = RoleGateway::start(
        ctx.database_url(),
        ctx.nats_handle()?.client_url(),
        "auth-test-token:readonly",
        &mut env_guard,
    )
    .await?;

    let resp = gw
        .rpc_raw(methods::REPLAY_LIST_OPERATIONS, json!({}))
        .await?;
    assert!(
        RoleGateway::has_result(&resp),
        "ReadOnly should be able to list operations: {resp}"
    );
    Ok(())
}

#[sinex_test(timeout = 60)]
async fn readonly_cannot_create_operation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut env_guard = EnvGuard::new();
    let gw = RoleGateway::start(
        ctx.database_url(),
        ctx.nats_handle()?.client_url(),
        "auth-test-token:readonly",
        &mut env_guard,
    )
    .await?;

    let resp = gw
        .rpc_raw(methods::REPLAY_CREATE_OPERATION, test_scope_params())
        .await?;
    assert!(
        RoleGateway::is_permission_denied(&resp),
        "ReadOnly should not be able to create operations: {resp}"
    );
    Ok(())
}

// ── Write role tests ────────────────────────────────────────────────

#[sinex_test(timeout = 60)]
async fn write_can_create_operation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut env_guard = EnvGuard::new();
    let gw = RoleGateway::start(
        ctx.database_url(),
        ctx.nats_handle()?.client_url(),
        "auth-test-token:write",
        &mut env_guard,
    )
    .await?;

    let resp = gw
        .rpc_raw(methods::REPLAY_CREATE_OPERATION, test_scope_params())
        .await?;
    assert!(
        RoleGateway::has_result(&resp),
        "Write should be able to create operations: {resp}"
    );
    Ok(())
}

#[sinex_test(timeout = 60)]
async fn write_cannot_approve_operation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut env_guard = EnvGuard::new();
    let gw = RoleGateway::start(
        ctx.database_url(),
        ctx.nats_handle()?.client_url(),
        "auth-test-token:write",
        &mut env_guard,
    )
    .await?;

    // Approve requires a real operation_id — just pass a dummy UUID.
    // The role check happens before the operation lookup, so we get
    // permission denied before "operation not found".
    let resp = gw
        .rpc_raw(
            methods::REPLAY_APPROVE_OPERATION,
            json!({
                "operation_id": "00000000-0000-0000-0000-000000000001",
                "approver": "admin:superuser"
            }),
        )
        .await?;
    assert!(
        RoleGateway::is_permission_denied(&resp),
        "Write should not be able to approve operations: {resp}"
    );
    Ok(())
}

// ── Admin role tests ────────────────────────────────────────────────

#[sinex_test(timeout = 120)]
async fn admin_full_lifecycle(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut env_guard = EnvGuard::new();
    let gw = RoleGateway::start(
        ctx.database_url(),
        ctx.nats_handle()?.client_url(),
        "auth-test-token:admin",
        &mut env_guard,
    )
    .await?;

    // Admin should be able to: list, create, preview, approve, cancel
    let list_resp = gw
        .rpc_raw(methods::REPLAY_LIST_OPERATIONS, json!({}))
        .await?;
    assert!(
        RoleGateway::has_result(&list_resp),
        "Admin list failed: {list_resp}"
    );

    let create_resp = gw
        .rpc_raw(methods::REPLAY_CREATE_OPERATION, test_scope_params())
        .await?;
    assert!(
        RoleGateway::has_result(&create_resp),
        "Admin create failed: {create_resp}"
    );

    let op_id = create_resp["result"]["operation"]["operation_id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("operation_id missing from create response"))?
        .to_string();

    let preview_resp = gw
        .rpc_raw(
            methods::REPLAY_PREVIEW_OPERATION,
            json!({ "operation_id": op_id }),
        )
        .await?;
    assert!(
        RoleGateway::has_result(&preview_resp),
        "Admin preview failed: {preview_resp}"
    );

    let approve_resp = gw
        .rpc_raw(
            methods::REPLAY_APPROVE_OPERATION,
            json!({ "operation_id": op_id, "approver": "admin:superuser" }),
        )
        .await?;
    assert!(
        RoleGateway::has_result(&approve_resp),
        "Admin approve failed: {approve_resp}"
    );

    // Cancel instead of execute (no fake node to handle scan)
    let cancel_resp = gw
        .rpc_raw(
            methods::REPLAY_CANCEL_OPERATION,
            json!({ "operation_id": op_id, "reason": "auth test cleanup" }),
        )
        .await?;
    assert!(
        RoleGateway::has_result(&cancel_resp),
        "Admin cancel failed: {cancel_resp}"
    );

    Ok(())
}
