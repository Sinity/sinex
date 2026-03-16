//! Deterministic replay regression tests.
//!
//! Proves that replaying the same inputs produces consistent results:
//! - Archived events preserve original content (payload, ts_orig)
//! - Double-replaying the same scope is idempotent (event count stable)

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
use xtask::sandbox::{EnvGuard, prelude::*};

const RPC_TOKEN: &str = "determinism-test-token:admin";

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
        env_guard.clear("SINEX_REPLAY_CONTROL_OPTIONAL");
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
}

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

/// Run a full replay lifecycle and wait for completion.
async fn run_replay(
    gw: &LiveGateway,
    node_id: &str,
    scope_start: Timestamp,
    scope_end: Timestamp,
    material_ids: &[uuid::Uuid],
) -> TestResult<String> {
    let plan_result = gw
        .rpc(
            methods::REPLAY_CREATE_OPERATION,
            json!({
                "scope": {
                    "node_id": node_id,
                    "time_window": [scope_start.format_rfc3339(), scope_end.format_rfc3339()],
                    "material_filter": material_ids.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
                },
                "actor": "test:determinism-tester"
            }),
        )
        .await?;

    let op_id = plan_result["operation"]["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();

    gw.rpc(methods::REPLAY_PREVIEW_OPERATION, json!({ "operation_id": op_id }))
        .await?;
    gw.rpc(
        methods::REPLAY_APPROVE_OPERATION,
        json!({ "operation_id": op_id, "approver": "admin:superuser" }),
    )
    .await?;
    gw.rpc(
        methods::REPLAY_EXECUTE_OPERATION,
        json!({ "operation_id": op_id, "executor": "service:worker-1" }),
    )
    .await?;

    // Poll for completion
    for _ in 0..120 {
        let status = gw
            .rpc(methods::REPLAY_OPERATION_STATUS, json!({ "operation_id": op_id }))
            .await?;
        match status["operation"]["state"].as_str() {
            Some("Completed") => return Ok(op_id),
            Some("Failed") => bail!("Replay operation {op_id} failed: {status}"),
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    bail!("Replay operation {op_id} did not complete in time")
}

/// Verify that archived events preserve the original content after replay.
#[sinex_test(timeout = 120)]
async fn material_replay_archives_preserve_content(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", ctx.nats_handle()?.client_url());

    let gw = LiveGateway::start(ctx.database_url(), &mut env_guard).await?;

    // Seed 3 material events with known payloads
    let material_id = ctx.create_source_material(Some("determinism-test")).await?;
    let mut original_payloads = Vec::new();
    let mut seeded_ids = Vec::new();

    for i in 0..3 {
        let event = DynamicPayload::new(
            "det-node",
            "file.created",
            json!({ "path": format!("/tmp/det-{i}.txt"), "size": i * 100 }),
        )
        .from_material(material_id)
        .build()?;
        let inserted = ctx.pool.events().insert(event).await?;
        let id = inserted.id.expect("seeded event must have an id").to_uuid();
        original_payloads.push(inserted.payload.clone());
        seeded_ids.push(id);
    }

    // Spawn fake scan node
    let nats = ctx.nats_client();
    let env = sinex_primitives::environment::environment();
    let scan_handle = spawn_fake_scan_node(nats.clone(), env, "det-node", 3).await?;

    let ts = Timestamp::now();
    let scope_start = ts - time::Duration::seconds(60);
    let scope_end = ts + time::Duration::seconds(60);

    run_replay(&gw, "det-node", scope_start, scope_end, &[*material_id.as_uuid()]).await?;
    scan_handle.await?;

    // Verify archived events preserve payload content
    for (i, original_id) in seeded_ids.iter().enumerate() {
        let archived: Option<serde_json::Value> = sqlx::query_scalar(
            "SELECT payload FROM audit.archived_events WHERE id = $1::uuid",
        )
        .bind(original_id)
        .fetch_optional(&ctx.pool)
        .await?;

        let archived_payload = archived.unwrap_or_else(|| {
            panic!("Event {original_id} should be in audit.archived_events")
        });

        assert_eq!(
            archived_payload, original_payloads[i],
            "Archived payload for event {i} should match original"
        );
    }

    Ok(())
}

/// Verify that replaying the same scope twice yields stable event counts.
#[sinex_test(timeout = 180)]
async fn double_replay_idempotent(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", ctx.nats_handle()?.client_url());

    let gw = LiveGateway::start(ctx.database_url(), &mut env_guard).await?;

    let material_id = ctx.create_source_material(Some("double-replay")).await?;

    // Seed 2 events
    for i in 0..2 {
        let event = DynamicPayload::new(
            "dbl-node",
            "file.created",
            json!({ "path": format!("/tmp/dbl-{i}.txt") }),
        )
        .from_material(material_id)
        .build()?;
        ctx.pool.events().insert(event).await?;
    }

    let ts = Timestamp::now();
    let scope_start = ts - time::Duration::seconds(60);
    let scope_end = ts + time::Duration::seconds(60);

    // First replay
    let nats = ctx.nats_client();
    let env = sinex_primitives::environment::environment();
    let scan1 = spawn_fake_scan_node(nats.clone(), env.clone(), "dbl-node", 2).await?;
    run_replay(&gw, "dbl-node", scope_start, scope_end, &[*material_id.as_uuid()]).await?;
    scan1.await?;

    // Count events + archives after first replay
    let live_after_1: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM core.events WHERE source = 'dbl-node'",
    )
    .fetch_one(&ctx.pool)
    .await?;
    let archived_after_1: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE source = 'dbl-node'",
    )
    .fetch_one(&ctx.pool)
    .await?;

    // Second replay of same scope — need a new fake node since the first was consumed
    let scan2 = spawn_fake_scan_node(nats.clone(), env, "dbl-node", live_after_1 as u64).await?;
    run_replay(&gw, "dbl-node", scope_start, scope_end, &[*material_id.as_uuid()]).await?;
    scan2.await?;

    let live_after_2: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM core.events WHERE source = 'dbl-node'",
    )
    .fetch_one(&ctx.pool)
    .await?;

    // Live count should be stable: second replay archives the first replay's
    // outputs and the fake node re-emits the same count.
    assert_eq!(
        live_after_1, live_after_2,
        "Live event count should be stable across replays (was {live_after_1}, now {live_after_2})"
    );

    // Archives should accumulate (first set + second set)
    let archived_after_2: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE source = 'dbl-node'",
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert!(
        archived_after_2 > archived_after_1,
        "Archive count should grow after second replay ({archived_after_1} → {archived_after_2})"
    );

    Ok(())
}
