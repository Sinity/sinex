//! Tests the replay lifecycle over the HTTP JSON-RPC endpoint.
//!
//! Complements `replay_lifecycle_test.rs` (NATS control subjects) and
//! `replay_failure_test.rs` (failure edge cases). These verify the same
//! operations work through the actual HTTP API that sinexctl and other
//! clients use.

use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::rpc::methods;
use sinex_primitives::{DynamicPayload, temporal::Timestamp};
use std::time::Duration;
use xtask::sandbox::{EnvGuard, sinex_test};

mod common;
use common::{FakeReplayScanSource, LiveGateway, spawn_fake_replay_scan_source};

const RPC_TOKEN: &str = "live-rpc-test-token:admin";

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
        "test-source",
        "file.created",
        json!({ "path": "/tmp/rpc-live.txt" }),
    )
    .from_material(material_id)
    .build()?;
    let inserted = ctx.pool.events().insert(event).await?;
    let target_id = inserted.id.expect("seeded event must have an id").to_uuid();
    let ts = inserted.ts_orig.expect("seeded event must have ts_orig");

    // ── Spawn fake scan source runtime ────────────────────────────────────────
    // Use the environment directly — creating a second ServiceContainer would
    // spawn a duplicate ReplayControlServer on the same NATS subject, causing
    // message races where the second server's error reply may beat the first.
    let nats = ctx.nats_client();
    let env = sinex_primitives::environment::environment();
    let scan_handle = spawn_fake_replay_scan_source(
        ctx.pool.clone(),
        nats.clone(),
        env,
        FakeReplayScanSource::from_replay_command(
            "test-source",
            "test-source",
            "file.created",
            1,
        ),
    )
    .await?;

    let scope_start = ts - time::Duration::seconds(1);
    let scope_end = ts + time::Duration::seconds(1);

    // ── Step 1: Plan via HTTP RPC ───────────────────────────────────
    let plan_result = gw
        .create_replay_operation(
            json!({
                "scope": {
                    "module_name": "test-source",
                    "time_window": [scope_start.format_rfc3339(), scope_end.format_rfc3339()],
                    "material_filter": [material_id.as_uuid().to_string()],
                    "filters": { "event_types": ["file.created"] }
                },
                "actor": "admin:test-user"
            }),
        )
        .await?;

    let op_id = LiveGateway::replay_operation_id(&plan_result)?;
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
    let preview_result = gw.preview_replay_operation(&op_id).await?;

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
        Some("reexecute_material_roots_via_source_scan"),
        "preview should declare replay semantics"
    );

    // ── Step 3: Submit via HTTP RPC (atomic approve+execute) ────────
    let submit_result = gw.submit_replay_operation(&op_id).await?;
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
        submit_result["operation"]["executor_module"].as_str(),
        Some("admin:token:live-rpc"),
        "gateway RPC submit must record the authenticated executor identity"
    );

    // ── Step 4: Poll status until completion ────────────────────────
    let status = gw
        .wait_for_replay_completed(&op_id, 60, Duration::from_millis(100))
        .await?;
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
    let list_result = gw.list_replay_operations().await?;
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
        .create_replay_operation(
            json!({
                "scope": {
                    "module_name": "test-source",
                    "time_window": [scope_start.format_rfc3339(), scope_end.format_rfc3339()],
                },
                "actor": "admin:test-user"
            }),
        )
        .await?;
    let op_id = LiveGateway::replay_operation_id(&plan_result)?;

    // Preview
    gw.preview_replay_operation(&op_id).await?;

    // Cancel with reason
    let cancel_result = gw
        .cancel_replay_operation(&op_id, "Testing cancel over HTTP RPC")
        .await?;
    assert_eq!(
        cancel_result["cancelled"].as_bool(),
        Some(true),
        "cancel response should confirm cancellation"
    );

    // Verify via status
    let status = gw.replay_operation_status(&op_id).await?;
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
