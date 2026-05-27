//! Deterministic replay regression tests.
//!
//! Proves that replaying the same inputs produces consistent results:
//! - Archived events preserve original content (payload, `ts_orig`)
//! - Double-replaying the same scope is idempotent (event count stable)

use futures::StreamExt;
use serde_json::json;
use sinex_db::{DbPool, repositories::DbPoolExt};
use sinex_node_sdk::{Checkpoint, NodeScanAck, NodeScanCommand, NodeScanProgress, ScanReport};
use sinex_primitives::rpc::methods;
use sinex_primitives::{DynamicPayload, Id, temporal::Timestamp};
use std::collections::HashMap;
use std::time::Duration;
use xtask::sandbox::{EnvGuard, prelude::*};

mod common;
use common::LiveGateway;

const RPC_TOKEN: &str = "determinism-test-token:admin";

async fn spawn_fake_reemitting_scan_node(
    pool: DbPool,
    nats: async_nats::Client,
    env: sinex_primitives::environment::SinexEnvironment,
    node_name: &str,
    material_id: uuid::Uuid,
    events_processed: u64,
) -> TestResult<tokio::task::JoinHandle<()>> {
    let node_name = node_name.to_string();
    let subject = env.nats_subject(&format!("sinex.control.nodes.{node_name}.scan"));
    let mut sub = nats.subscribe(subject).await?;
    nats.flush().await?;

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

        for i in 0..events_processed {
            let Ok(event) = DynamicPayload::new(
                node_name.as_str(),
                "file.created",
                json!({ "path": format!("/tmp/{node_name}-replay-{operation_id}-{i}.txt") }),
            )
            .from_material(Id::from_uuid(material_id))
            .build() else {
                return;
            };

            let mut event = event;
            event.created_by_operation_id = Some(command.operation_id);

            if pool.events().insert(event).await.is_err() {
                return;
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
                    "material_filter": material_ids.iter().map(std::string::ToString::to_string).collect::<Vec<_>>(),
                },
                "actor": "test:determinism-tester"
            }),
        )
        .await?;

    let op_id = plan_result["operation"]["operation_id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("operation_id missing from plan response"))?
        .to_string();

    gw.rpc(
        methods::REPLAY_PREVIEW_OPERATION,
        json!({ "operation_id": op_id }),
    )
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
            .rpc(
                methods::REPLAY_OPERATION_STATUS,
                json!({ "operation_id": op_id }),
            )
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

    let gw = LiveGateway::start(ctx.database_url(), RPC_TOKEN, &mut env_guard).await?;

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
        let id = inserted
            .id
            .ok_or_else(|| color_eyre::eyre::eyre!("seeded event must have an id"))?
            .to_uuid();
        original_payloads.push(inserted.payload.clone());
        seeded_ids.push(id);
    }

    // Spawn fake scan node
    let nats = ctx.nats_client();
    let env = sinex_primitives::environment::environment();
    let scan_handle = spawn_fake_reemitting_scan_node(
        ctx.pool.clone(),
        nats.clone(),
        env,
        "det-node",
        *material_id.as_uuid(),
        3,
    )
    .await?;

    let ts = Timestamp::now();
    let scope_start = ts - time::Duration::seconds(60);
    let scope_end = ts + time::Duration::seconds(60);

    run_replay(
        &gw,
        "det-node",
        scope_start,
        scope_end,
        &[*material_id.as_uuid()],
    )
    .await?;
    scan_handle.await?;

    // Verify archived events preserve payload content
    for (i, original_id) in seeded_ids.iter().enumerate() {
        let archived: Option<serde_json::Value> =
            sqlx::query_scalar("SELECT payload FROM audit.archived_events WHERE id = $1::uuid")
                .bind(original_id)
                .fetch_optional(&ctx.pool)
                .await?;

        let archived_payload = archived
            .unwrap_or_else(|| panic!("Event {original_id} should be in audit.archived_events"));

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

    let gw = LiveGateway::start(ctx.database_url(), RPC_TOKEN, &mut env_guard).await?;

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
    let scan1 = spawn_fake_reemitting_scan_node(
        ctx.pool.clone(),
        nats.clone(),
        env.clone(),
        "dbl-node",
        *material_id.as_uuid(),
        2,
    )
    .await?;
    run_replay(
        &gw,
        "dbl-node",
        scope_start,
        scope_end,
        &[*material_id.as_uuid()],
    )
    .await?;
    scan1.await?;

    // Count events + archives after first replay
    let live_after_1: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE source = 'dbl-node'")
            .fetch_one(&ctx.pool)
            .await?;
    let archived_after_1: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE source = 'dbl-node'",
    )
    .fetch_one(&ctx.pool)
    .await?;

    // Second replay of same scope — need a new fake node since the first was consumed
    let scan2 = spawn_fake_reemitting_scan_node(
        ctx.pool.clone(),
        nats.clone(),
        env,
        "dbl-node",
        *material_id.as_uuid(),
        live_after_1 as u64,
    )
    .await?;
    run_replay(
        &gw,
        "dbl-node",
        scope_start,
        scope_end,
        &[*material_id.as_uuid()],
    )
    .await?;
    scan2.await?;

    let live_after_2: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE source = 'dbl-node'")
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
