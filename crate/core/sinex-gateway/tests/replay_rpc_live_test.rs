//! Tests the replay lifecycle over the HTTP JSON-RPC endpoint.
//!
//! Complements `replay_lifecycle_test.rs` (NATS control subjects) and
//! `replay_failure_test.rs` (failure edge cases). These verify the same
//! operations work through the actual HTTP API that sinexctl and other
//! clients use.

use color_eyre::eyre::bail;
use futures::StreamExt;
use serde_json::json;
use sinex_db::{DbPool, repositories::DbPoolExt};
use sinex_node_sdk::{Checkpoint, NodeScanAck, NodeScanCommand, NodeScanProgress, ScanReport};
use sinex_primitives::rpc::methods;
use sinex_primitives::{DynamicPayload, Id, temporal::Timestamp};
use std::collections::HashMap;
use std::time::Duration;
use xtask::sandbox::{EnvGuard, prelude::*, sinex_test};

mod common;
use common::LiveGateway;

const RPC_TOKEN: &str = "live-rpc-test-token:admin";

/// Spawn a fake scan node on NATS that accepts the scan command and reports success.
async fn spawn_fake_scan_node(
    pool: DbPool,
    nats: async_nats::Client,
    env: sinex_primitives::environment::SinexEnvironment,
    node_name: &str,
    source: &'static str,
    event_type: &'static str,
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

        let material_id = command
            .args
            .replay
            .as_ref()
            .and_then(|replay| replay.materials.first())
            .map(|material| material.source_material_id);

        let Some(material_id) = material_id else {
            return;
        };

        for i in 0..events_processed {
            let Ok(event) = DynamicPayload::new(
                source,
                event_type,
                json!({ "path": format!("/tmp/{node_name}-replay-{operation_id}-{i}.txt") }),
            )
            .from_material(Id::from_uuid(material_id))
            .build() else {
                return;
            };

            let mut event = event;
            event.created_by_operation_id = Some(operation_id);

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

/// Full replay lifecycle over HTTP JSON-RPC: plan → preview → approve → execute → status → list.
///
/// Verifies that the same operations available via NATS control subjects also
/// work correctly through the HTTP RPC endpoint that sinexctl uses.
#[sinex_test(timeout = 120)]
async fn replay_full_lifecycle_over_http_rpc(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", ctx.nats_handle()?.client_url());

    let gw = LiveGateway::start(ctx.database_url(), RPC_TOKEN, &mut env_guard).await?;

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
    let scan_handle = spawn_fake_scan_node(
        ctx.pool.clone(),
        nats.clone(),
        env,
        "test-node",
        "test-node",
        "file.created",
        1,
    )
    .await?;

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
    assert_eq!(
        plan_result["operation"]["actor"].as_str(),
        Some("admin:token:live-rpc"),
        "gateway RPC must ignore caller-supplied replay actor params"
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

    // ── Step 3: Submit via HTTP RPC (atomic approve+execute) ────────
    let submit_result = gw
        .rpc(
            methods::REPLAY_SUBMIT_OPERATION,
            json!({ "operation_id": op_id }),
        )
        .await?;
    assert_eq!(
        submit_result["operation"]["state"].as_str(),
        Some("Completed")
    );
    assert_eq!(
        submit_result["operation"]["approved_by"].as_str(),
        Some("admin:token:live-rpc"),
        "gateway RPC must persist the authenticated submitter identity"
    );
    assert_eq!(
        submit_result["operation"]["executor_node"].as_str(),
        Some("admin:token:live-rpc"),
        "gateway RPC submit must record the authenticated executor identity"
    );

    // ── Step 4: Poll status until completion ────────────────────────
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

    // ── Step 5: List operations ─────────────────────────────────────
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

    let gw = LiveGateway::start(ctx.database_url(), RPC_TOKEN, &mut env_guard).await?;

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

    let gw = LiveGateway::start(ctx.database_url(), RPC_TOKEN, &mut env_guard).await?;

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
