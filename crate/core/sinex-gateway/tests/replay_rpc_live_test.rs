//! Tests the replay lifecycle over the HTTP JSON-RPC endpoint.
//!
//! Complements `replay_lifecycle_test.rs` (NATS control subjects) and
//! `replay_failure_test.rs` (failure edge cases). These verify the same
//! operations work through the actual HTTP API that sinexctl and other
//! clients use.

use color_eyre::eyre::bail;
use futures::StreamExt;
use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_gateway::{ServiceContainer, config::GatewayConfig, rpc_server};
use sinex_node_sdk::{Checkpoint, NodeScanAck, NodeScanCommand, NodeScanProgress, ScanReport};
use sinex_primitives::rpc::methods;
use sinex_primitives::{DynamicPayload, temporal::Timestamp};
use std::collections::HashMap;
use std::net::TcpListener;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::sync::watch;
use xtask::sandbox::{EnvGuard, prelude::*, sinex_test};

const RPC_TOKEN: &str = "live-rpc-test-token:admin";

/// An in-process gateway with HTTP RPC for testing.
///
/// Starts the full gateway stack (RPC server with TLS) and provides a
/// `reqwest`-based client for making JSON-RPC 2.0 calls. This exercises the
/// same HTTP path that sinexctl uses, rather than raw NATS control subjects.
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

        env_guard.set(
            "SINEX_GATEWAY_TLS_CERT",
            cert_file.path().to_string_lossy().to_string(),
        );
        env_guard.set(
            "SINEX_GATEWAY_TLS_KEY",
            key_file.path().to_string_lossy().to_string(),
        );
        env_guard.clear("SINEX_GATEWAY_TLS_CLIENT_CA");
        env_guard.set("SINEX_RPC_TOKEN", RPC_TOKEN);
        env_guard.set("DATABASE_URL", database_url);

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

        // Wait for port readiness
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            match tokio::net::TcpStream::connect(format!("127.0.0.1:{port}")).await {
                Ok(_) => break,
                Err(_) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => {
                    bail!("Gateway port {port} not ready after 30s: {e}");
                }
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

    fn rpc_url(&self) -> String {
        format!("https://127.0.0.1:{}/rpc", self.port)
    }

    /// Make an authenticated JSON-RPC 2.0 call, returning the `result` field.
    async fn rpc(&self, method: &str, params: serde_json::Value) -> TestResult<serde_json::Value> {
        let body = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1,
        });

        let resp = self
            .client
            .post(self.rpc_url())
            .header("Authorization", format!("Bearer {RPC_TOKEN}"))
            .json(&body)
            .send()
            .await?;

        let status = resp.status();
        let response: serde_json::Value = resp.json().await?;

        if let Some(error) = response.get("error") {
            bail!("JSON-RPC error on {method} (HTTP {status}): {error}");
        }

        response
            .get("result")
            .cloned()
            .ok_or_else(|| color_eyre::eyre::eyre!("No 'result' field in JSON-RPC response"))
    }

    /// Make a JSON-RPC call without authentication.
    async fn rpc_unauthed(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> TestResult<reqwest::Response> {
        let body = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1,
        });

        Ok(self.client.post(self.rpc_url()).json(&body).send().await?)
    }
}

/// Spawn a fake scan node on NATS that accepts the scan command and reports success.
async fn spawn_fake_scan_node(
    nats: async_nats::Client,
    env: sinex_primitives::environment::SinexEnvironment,
    node_name: &str,
    events_processed: u64,
) -> TestResult<tokio::task::JoinHandle<()>> {
    let node_name = node_name.to_string();
    let subject = env.nats_subject(&format!("sinex.control.nodes.{node_name}.scan"));
    let mut sub = nats.subscribe(subject).await?;

    let handle = tokio::spawn(async move {
        let Some(msg) = sub.next().await else { return };

        let Ok(command) = serde_json::from_slice::<NodeScanCommand>(&msg.payload) else {
            return;
        };
        let operation_id = command.operation_id;
        let progress_subject =
            env.nats_subject(&format!("sinex.control.replay.progress.{operation_id}"));

        if let Some(reply) = msg.reply {
            let ack = NodeScanAck {
                operation_id,
                node_name: node_name.clone(),
                accepted: true,
                error: None,
            };
            if let Ok(bytes) = serde_json::to_vec(&ack) {
                let _ = nats.publish(reply, bytes.into()).await;
            }
        }

        let progress = NodeScanProgress {
            operation_id,
            node_name: node_name.clone(),
            events_processed,
            events_emitted: events_processed,
            final_report: Some(ScanReport {
                events_processed,
                duration: Duration::from_millis(5),
                final_checkpoint: Checkpoint::None,
                time_range: None,
                node_stats: HashMap::from([("events_emitted".into(), events_processed)]),
                successful_targets: vec![node_name.clone()],
                failed_targets: Vec::new(),
                warnings: Vec::new(),
            }),
            error: None,
        };
        if let Ok(bytes) = serde_json::to_vec(&progress) {
            let _ = nats.publish(progress_subject, bytes.into()).await;
        }
    });

    Ok(handle)
}

/// Full replay lifecycle over HTTP JSON-RPC: plan → preview → approve → execute → status → list.
///
/// Verifies that the same operations available via NATS control subjects also
/// work correctly through the HTTP RPC endpoint that sinexctl uses.
#[sinex_test(timeout = 120)]
async fn replay_full_lifecycle_over_http_rpc(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", ctx.nats_handle()?.client_url());

    let gw = LiveGateway::start(ctx.database_url(), &mut env_guard).await?;

    // ── Seed a target event with material provenance ────────────────
    let material_id = ctx.create_source_material(Some("rpc-live-match")).await?;
    let event = DynamicPayload::new(
        "test-node",
        "file.created",
        json!({ "path": "/tmp/rpc-live.txt" }),
    )
    .from_material(material_id)
    .build()?;
    let inserted = ctx.pool.events().insert(event).await?;
    let target_id = inserted.id.expect("seeded event must have an id").to_uuid();
    let ts = inserted.ts_orig.expect("seeded event must have ts_orig");

    // ── Spawn fake scan node ────────────────────────────────────────
    // Use the environment directly — creating a second ServiceContainer would
    // spawn a duplicate ReplayControlServer on the same NATS subject, causing
    // message races where the second server's error reply may beat the first.
    let nats = ctx.nats_client();
    let env = sinex_primitives::environment::environment();
    let scan_handle = spawn_fake_scan_node(nats.clone(), env, "test-node", 1).await?;

    let scope_start = ts - time::Duration::seconds(1);
    let scope_end = ts + time::Duration::seconds(1);

    // ── Step 1: Plan via HTTP RPC ───────────────────────────────────
    let plan_result = gw
        .rpc(
            methods::REPLAY_CREATE_OPERATION,
            json!({
                "scope": {
                    "node_id": "test-node",
                    "time_window": [scope_start.format_rfc3339(), scope_end.format_rfc3339()],
                    "material_filter": [material_id.as_uuid().to_string()],
                    "filters": { "event_types": ["file.created"] }
                },
                "actor": "admin:test-user"
            }),
        )
        .await?;

    let op_id = plan_result["operation"]["operation_id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("operation_id missing from plan response"))?
        .to_string();
    assert_eq!(
        plan_result["operation"]["state"].as_str(),
        Some("Planning"),
        "newly created operation should be in Planning state"
    );

    // ── Step 2: Preview via HTTP RPC ────────────────────────────────
    let preview_result = gw
        .rpc(
            methods::REPLAY_PREVIEW_OPERATION,
            json!({ "operation_id": op_id }),
        )
        .await?;

    assert_eq!(
        preview_result["operation"]["state"].as_str(),
        Some("Previewed")
    );
    assert_eq!(
        preview_result["preview"]["total_events"].as_i64(),
        Some(1),
        "preview should find exactly 1 event in scope"
    );
    assert_eq!(
        preview_result["preview"]["replay_semantics"].as_str(),
        Some("reexecute_material_roots_via_node_scan"),
        "preview should declare replay semantics"
    );

    // ── Step 3: Approve via HTTP RPC ────────────────────────────────
    let approve_result = gw
        .rpc(
            methods::REPLAY_APPROVE_OPERATION,
            json!({ "operation_id": op_id, "approver": "admin:superuser" }),
        )
        .await?;
    assert_eq!(
        approve_result["operation"]["state"].as_str(),
        Some("Approved")
    );

    // ── Step 4: Execute via HTTP RPC ────────────────────────────────
    let _execute_result = gw
        .rpc(
            methods::REPLAY_EXECUTE_OPERATION,
            json!({ "operation_id": op_id, "executor": "service:worker-1" }),
        )
        .await?;

    // ── Step 5: Poll status until completion ────────────────────────
    let mut status = json!(null);
    for _ in 0..60 {
        status = gw
            .rpc(
                methods::REPLAY_OPERATION_STATUS,
                json!({ "operation_id": op_id }),
            )
            .await?;
        if status["operation"]["state"].as_str() == Some("Completed") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        status["operation"]["state"].as_str(),
        Some("Completed"),
        "operation should reach Completed state"
    );
    assert_eq!(
        status["operation"]["checkpoint"]["total_events"].as_u64(),
        Some(1)
    );

    // ── Step 6: List operations ─────────────────────────────────────
    let list_result = gw.rpc(methods::REPLAY_LIST_OPERATIONS, json!({})).await?;
    let ops = list_result["operations"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("operations array missing from list response"))?;
    assert!(
        ops.iter()
            .any(|op| op["operation_id"].as_str() == Some(&op_id)),
        "our operation should appear in the list"
    );

    // ── Step 7: Verify archive-and-replace ──────────────────────────
    let live: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(target_id)
            .fetch_one(&ctx.pool)
            .await?;
    let archived: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(target_id)
    .fetch_one(&ctx.pool)
    .await?;

    assert_eq!(live, 0, "target event should be removed from core.events");
    assert_eq!(
        archived, 1,
        "target event should be in audit.archived_events"
    );

    scan_handle.await?;
    Ok(())
}

/// Replay cancel lifecycle over HTTP JSON-RPC: plan → preview → cancel → verify.
#[sinex_test(timeout = 60)]
async fn replay_cancel_lifecycle_over_http_rpc(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", ctx.nats_handle()?.client_url());

    let gw = LiveGateway::start(ctx.database_url(), &mut env_guard).await?;

    let ts = Timestamp::now();
    let scope_start = ts - time::Duration::seconds(1);
    let scope_end = ts + time::Duration::seconds(1);

    // Plan
    let plan_result = gw
        .rpc(
            methods::REPLAY_CREATE_OPERATION,
            json!({
                "scope": {
                    "node_id": "test-node",
                    "time_window": [scope_start.format_rfc3339(), scope_end.format_rfc3339()],
                },
                "actor": "admin:test-user"
            }),
        )
        .await?;
    let op_id = plan_result["operation"]["operation_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Preview
    gw.rpc(
        methods::REPLAY_PREVIEW_OPERATION,
        json!({ "operation_id": op_id }),
    )
    .await?;

    // Cancel with reason
    let cancel_result = gw
        .rpc(
            methods::REPLAY_CANCEL_OPERATION,
            json!({
                "operation_id": op_id,
                "canceller": "admin:test-user",
                "reason": "Testing cancel over HTTP RPC"
            }),
        )
        .await?;
    assert_eq!(
        cancel_result["cancelled"].as_bool(),
        Some(true),
        "cancel response should confirm cancellation"
    );

    // Verify via status
    let status = gw
        .rpc(
            methods::REPLAY_OPERATION_STATUS,
            json!({ "operation_id": op_id }),
        )
        .await?;
    assert_eq!(
        status["operation"]["state"].as_str(),
        Some("Cancelled"),
        "operation should be in Cancelled state"
    );

    Ok(())
}

/// RPC calls without a valid bearer token should be rejected.
#[sinex_test(timeout = 60)]
async fn replay_rpc_requires_authentication(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", ctx.nats_handle()?.client_url());

    let gw = LiveGateway::start(ctx.database_url(), &mut env_guard).await?;

    let resp = gw
        .rpc_unauthed(methods::REPLAY_LIST_OPERATIONS, json!({}))
        .await?;

    assert_eq!(
        resp.status().as_u16(),
        401,
        "unauthenticated request should receive 401 Unauthorized"
    );

    Ok(())
}
