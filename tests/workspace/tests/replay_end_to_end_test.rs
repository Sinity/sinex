//! End-to-end replay test exercising the full sinex service stack.
//!
//! Unlike `replay_lifecycle_test.rs` (which uses an in-process `ServiceContainer`),
//! this test starts NATS + ingestd + gateway as real subprocesses via `TestCoreStack`,
//! seeds events through the full ingest path (NATS → ingestd → `PostgreSQL`), then
//! orchestrates a replay through the gateway's HTTP JSON-RPC API.
//!
//! What this test proves:
//! - Events seeded via NATS are persisted by ingestd and visible to the gateway
//! - The replay plan/preview/approve/execute RPC sequence works over HTTPS
//! - A fake scan node receives the scan command via NATS and can report progress
//! - All scoped events are archived to `audit.archived_events` and removed from `core.events`

use futures::StreamExt;
use serde_json::json;
use sinex_node_sdk::{Checkpoint, NodeScanAck, NodeScanCommand, NodeScanProgress, ScanReport};
use sinex_primitives::rpc::methods;
use sinex_primitives::temporal::Duration as TemporalDuration;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{DynamicPayload, Id, Uuid};
use std::collections::HashMap;
use std::time::Duration;
use xtask::sandbox::prelude::*;

// ─────────────────────────────────────────────────────────────────────────────
// Fake scan node helper
// ─────────────────────────────────────────────────────────────────────────────

async fn spawn_fake_scan_node(
    pool: DbPool,
    nats: async_nats::Client,
    env: sinex_primitives::environment::SinexEnvironment,
    node_name: &str,
    events_processed: u64,
) -> TestResult<(
    tokio::sync::oneshot::Receiver<NodeScanCommand>,
    tokio::task::JoinHandle<()>,
)> {
    let node_name = node_name.to_string();
    let subject = env.nats_subject(&format!("sinex.control.nodes.{node_name}.scan"));
    let mut sub = nats.subscribe(subject).await?;
    let (command_tx, command_rx) = tokio::sync::oneshot::channel();

    let handle = tokio::spawn(async move {
        let Some(msg) = sub.next().await else {
            return;
        };

        let Ok(command) = serde_json::from_slice::<NodeScanCommand>(&msg.payload) else {
            eprintln!("fake scan node: invalid scan command payload");
            return;
        };
        let operation_id = command.operation_id;
        let progress_subject =
            env.nats_subject(&format!("sinex.control.replay.progress.{operation_id}"));

        let _ = command_tx.send(command.clone());

        let replay_context = command.args.replay.clone();

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

        if let Some(replay_context) = replay_context.as_ref() {
            let logical_source_identifier = replay_context
                .materials
                .first()
                .and_then(|material| {
                    material
                        .material_metadata
                        .get("logical_source_identifier")
                        .and_then(serde_json::Value::as_str)
                        .or_else(|| material.source_identifier.split("#material=").next())
                })
                .unwrap_or("/tmp/replay-end-to-end.txt");
            let event_type = replay_context
                .replay_scope
                .event_types
                .as_ref()
                .and_then(|types| types.first())
                .map_or("file.created", String::as_str);
            let material_id = Uuid::now_v7();
            let source_identifier = format!("{logical_source_identifier}#material={material_id}");
            if let Err(error) = sqlx::query(
                r"
                INSERT INTO raw.source_material_registry (
                    id,
                    material_kind,
                    source_identifier,
                    status,
                    timing_info_type,
                    metadata
                )
                VALUES ($1::uuid, 'annex', $2, 'completed', 'realtime', $3::jsonb)
                ",
            )
            .bind(material_id)
            .bind(&source_identifier)
            .bind(json!({ "logical_source_identifier": logical_source_identifier }))
            .execute(&pool)
            .await
            {
                eprintln!("fake scan node: failed to create replay source material: {error}");
                return;
            }
            for index in 0..events_processed {
                let mut event = match DynamicPayload::new(
                    node_name.as_str(),
                    event_type,
                    json!({ "path": logical_source_identifier, "replay_index": index }),
                )
                .from_material(Id::from_uuid(material_id))
                .build()
                {
                    Ok(event) => event,
                    Err(error) => {
                        eprintln!("fake scan node: failed to build replay output event: {error}");
                        return;
                    }
                };
                event.created_by_operation_id = Some(operation_id);
                if let Err(error) = pool.events().insert(event).await {
                    eprintln!("fake scan node: failed to insert replay output event: {error}");
                    return;
                }
            }
        }

        let report = ScanReport {
            events_processed,
            duration: Duration::from_millis(5),
            final_checkpoint: Checkpoint::None,
            time_range: None,
            node_stats: HashMap::from([("events_emitted".to_string(), events_processed)]),
            successful_targets: vec![node_name.clone()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        };
        let progress = NodeScanProgress {
            operation_id,
            node_name,
            events_processed,
            events_emitted: events_processed,
            final_report: Some(report),
            error: None,
        };
        if let Ok(bytes) = serde_json::to_vec(&progress) {
            let _ = nats.publish(progress_subject, bytes.into()).await;
        }
    });

    Ok((command_rx, handle))
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP JSON-RPC helper
// ─────────────────────────────────────────────────────────────────────────────

async fn json_rpc(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    method: &str,
    params: serde_json::Value,
) -> TestResult<serde_json::Value> {
    let body = json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": 1,
    });
    let resp = client
        .post(url)
        .header("Authorization", format!("Bearer {token}"))
        .json(&body)
        .send()
        .await?;
    let response: serde_json::Value = resp.json().await?;
    if let Some(error) = response.get("error") {
        return Err(color_eyre::eyre::eyre!(
            "JSON-RPC error on {method}: {error}"
        ));
    }
    response
        .get("result")
        .cloned()
        .ok_or_else(|| color_eyre::eyre::eyre!("No result in JSON-RPC response for {method}"))
}

// ─────────────────────────────────────────────────────────────────────────────
// Test
// ─────────────────────────────────────────────────────────────────────────────

/// Full end-to-end replay: seed → plan → preview → approve → execute → verify archived.
///
/// Exercises the full request path:
/// - Events published to NATS, persisted by the ingestd subprocess
/// - Fake scan node listening on NATS for scan commands
/// - Replay orchestration through the gateway subprocess via HTTPS JSON-RPC
/// - Post-execution verification: archived rows in `audit.archived_events`, none in `core.events`
#[sinex_test(timeout = 180)]
async fn replay_end_to_end_seeds_executes_archives(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let stack = TestCoreStack::new(&ctx).await?;

    // ── Step 1: Seed events through the full ingest path ─────────────────
    let (material_id, event_ids) = stack
        .seed_material_with_events("test-node", "file.created", 3)
        .await?;

    assert_eq!(event_ids.len(), 3, "seeded 3 events");

    // ── Step 2: Determine time bounds for the replay scope ────────────────
    // Use a window that comfortably brackets the just-seeded events.
    let scope_start = Timestamp::now() - TemporalDuration::seconds(30);
    let scope_end = Timestamp::now() + TemporalDuration::seconds(30);

    // ── Step 3: Spawn fake scan node on the stack's NATS ─────────────────
    let nats = stack.nats_client();
    // SinexEnvironment::default() picks up the same environment as the stack
    let env = sinex_primitives::environment::SinexEnvironment::default();
    let (scan_command_rx, scan_handle) =
        spawn_fake_scan_node(stack.pool().clone(), nats, env, "test-node", 3).await?;

    // ── Step 4: Build HTTPS client (accepts self-signed test cert) ────────
    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(15))
        .build()?;
    let url = stack.rpc_url();
    let token = stack.rpc_token().to_string();

    // ── Step 5: Create replay operation ──────────────────────────────────
    let create_result = json_rpc(
        &http_client,
        &url,
        &token,
        methods::REPLAY_CREATE_OPERATION,
        json!({
            "scope": {
                "node_id": "test-node",
                "time_window": [
                    scope_start.format_rfc3339(),
                    scope_end.format_rfc3339()
                ],
                "material_filter": [material_id.as_uuid().to_string()],
                "filters": { "event_types": ["file.created"] }
            },
            "actor": "admin:test-user"
        }),
    )
    .await?;

    let op_id = create_result["operation"]["operation_id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("create_operation: missing operation_id"))?
        .to_string();

    assert_eq!(
        create_result["operation"]["state"].as_str(),
        Some("Planning"),
        "operation should start in Planning state"
    );

    // ── Step 6: Preview ───────────────────────────────────────────────────
    let preview_result = json_rpc(
        &http_client,
        &url,
        &token,
        methods::REPLAY_PREVIEW_OPERATION,
        json!({ "operation_id": op_id }),
    )
    .await?;

    assert_eq!(
        preview_result["operation"]["state"].as_str(),
        Some("Previewed"),
        "operation should be in Previewed state after preview"
    );

    let total_events = preview_result["preview"]["total_events"]
        .as_u64()
        .unwrap_or(0);
    assert!(
        total_events > 0,
        "preview should find events to replay (got {total_events})"
    );

    // ── Step 7: Approve ───────────────────────────────────────────────────
    let approve_result = json_rpc(
        &http_client,
        &url,
        &token,
        methods::REPLAY_APPROVE_OPERATION,
        json!({
            "operation_id": op_id,
            "approver": "admin:superuser"
        }),
    )
    .await?;

    assert_eq!(
        approve_result["operation"]["state"].as_str(),
        Some("Approved"),
        "operation should be in Approved state after approval"
    );

    // ── Step 8: Execute ───────────────────────────────────────────────────
    let execute_result = json_rpc(
        &http_client,
        &url,
        &token,
        methods::REPLAY_EXECUTE_OPERATION,
        json!({
            "operation_id": op_id,
            "executor": "service:worker-1"
        }),
    )
    .await?;

    // The operation transitions to executing (or immediately to completed for fast paths)
    let executing_state = execute_result["operation"]["state"].as_str().unwrap_or("");
    assert!(
        matches!(executing_state, "Executing" | "Completed"),
        "operation should be Executing or Completed after execute call, got: {executing_state}"
    );

    // ── Step 9: Poll status until Completed ──────────────────────────────
    let mut final_status = serde_json::Value::Null;
    for _ in 0..60 {
        let status_result = json_rpc(
            &http_client,
            &url,
            &token,
            methods::REPLAY_OPERATION_STATUS,
            json!({ "operation_id": op_id }),
        )
        .await?;

        let state = status_result["operation"]["state"].as_str().unwrap_or("");
        if state == "Completed" || state == "Failed" || state == "Cancelled" {
            final_status = status_result;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert_eq!(
        final_status["operation"]["state"].as_str(),
        Some("Completed"),
        "replay operation should complete; final status: {final_status}"
    );

    // ── Step 10: Verify archived rows in the database ─────────────────────
    let pool = stack.pool();

    // All 3 original events should be archived (not in core.events)
    for event_id in &event_ids {
        let live_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
                .bind(event_id.as_uuid())
                .fetch_one(pool)
                .await?;

        let archived_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
        )
        .bind(event_id.as_uuid())
        .fetch_one(pool)
        .await?;

        assert_eq!(
            live_count, 0,
            "event {event_id} should no longer be in core.events after replay"
        );
        assert_eq!(
            archived_count, 1,
            "event {event_id} should be in audit.archived_events after replay"
        );
    }

    // Belt-and-suspenders: aggregate check
    let total_archived: i64 = sqlx::query_scalar(
        r"
        SELECT COUNT(*)::bigint
        FROM audit.archived_events ae
        WHERE ae.id = ANY(
            SELECT id::uuid FROM unnest($1::text[]) AS t(id)
        )
        ",
    )
    .bind(
        event_ids
            .iter()
            .map(|id| id.as_uuid().to_string())
            .collect::<Vec<_>>(),
    )
    .fetch_one(pool)
    .await?;

    assert_eq!(
        total_archived, 3,
        "all 3 seeded events should be in audit.archived_events"
    );

    // ── Step 11: Verify scan command was dispatched to the fake node ──────
    let dispatched_command = scan_command_rx.await.map_err(|_| {
        color_eyre::eyre::eyre!("fake test-node did not receive scan command within timeout")
    })?;

    let replay_context = dispatched_command
        .args
        .replay
        .expect("gateway must populate typed replay context in scan command");

    assert_eq!(
        replay_context.replay_scope.material_ids,
        Some(vec![*material_id.as_uuid()]),
        "replay context should carry the seeded material_id"
    );
    assert_eq!(
        replay_context.replay_scope.event_types,
        Some(vec!["file.created".to_string()]),
        "replay context should carry the event type filter"
    );

    scan_handle
        .await
        .map_err(|e| color_eyre::eyre::eyre!("fake scan node task panicked: {e}"))?;

    // ── Cleanup ───────────────────────────────────────────────────────────
    stack.shutdown().await?;
    Ok(())
}
