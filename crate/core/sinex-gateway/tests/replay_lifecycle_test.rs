use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy, pull::Config as ConsumerConfig};
use futures::StreamExt;
use sinex_db::repositories::DbPoolExt;
use sinex_gateway::ServiceContainer;
use sinex_node_sdk::{Checkpoint, NodeScanAck, NodeScanCommand, NodeScanProgress, ScanReport};
use sinex_primitives::{DynamicPayload, Uuid, temporal::Timestamp};
use std::time::Duration;
use tokio::time::sleep;
use xtask::sandbox::prelude::*;

async fn spawn_fake_scan_node(
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
        let Some(msg) = sub.next().await else { return };

        let Ok(command) = serde_json::from_slice::<NodeScanCommand>(&msg.payload) else {
            eprintln!("fake scan node: invalid scan command payload");
            return;
        };
        let operation_id = command.operation_id;
        let progress_subject =
            env.nats_subject(&format!("sinex.control.replay.progress.{operation_id}"));

        let _ = command_tx.send(command.clone());

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

        let report = ScanReport {
            events_processed,
            duration: Duration::from_millis(5),
            final_checkpoint: Checkpoint::None,
            time_range: None,
            node_stats: std::collections::HashMap::from([(
                "events_emitted".to_string(),
                events_processed,
            )]),
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

#[sinex_test]
async fn replay_lifecycle_enforces_reexecution_invariants(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let nats_url = ctx.nats_handle()?.client_url().to_string();
    let mut env_guard = EnvGuard::new();
    env_guard.set("SINEX_NATS_URL", &nats_url);
    env_guard.clear("SINEX_REPLAY_CONTROL_OPTIONAL");

    let services = ServiceContainer::from_database_url(ctx.database_url().to_string()).await?;
    let nats = services
        .nats_client()
        .expect("NATS required for replay test")
        .clone();
    let control_subject = services.environment().nats_subject("sinex.control.replay");
    let env = services.environment().clone();

    let js = async_nats::jetstream::new(nats.clone());
    let stream_name = format!("replay-lifecycle-{}", Uuid::now_v7().simple());
    js.get_or_create_stream(async_nats::jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![env.nats_subject("events.raw.>")],
        ..Default::default()
    })
    .await?;
    let (scan_command_rx, scan_handle) =
        spawn_fake_scan_node(nats.clone(), env.clone(), "test-node", 1).await?;

    let replay_material = ctx
        .create_source_material(Some("replay-lifecycle-match"))
        .await?;
    let replay_event = DynamicPayload::new(
        "test-node",
        "file.created",
        serde_json::json!({ "path": "/tmp/replay-lifecycle-match.txt" }),
    )
    .from_material(replay_material)
    .build()?;
    let inserted_replay = ctx.pool.events().insert(replay_event).await?;
    let replay_target_event_id = inserted_replay
        .id
        .expect("seeded replay target event must have an id");
    let replay_target_id = replay_target_event_id.to_uuid();
    let replay_ts = inserted_replay
        .ts_orig
        .expect("seeded replay target should carry ts_orig");

    let cascade_event = DynamicPayload::new(
        "derived-node",
        "file.derived",
        serde_json::json!({ "path": "/tmp/replay-lifecycle-derived.txt" }),
    )
    .from_parents([replay_target_event_id])?
    .build()?;
    let inserted_cascade = ctx.pool.events().insert(cascade_event).await?;
    let cascade_id = inserted_cascade
        .id
        .expect("seeded derived event must have an id")
        .to_uuid();

    let nonmatch_material = ctx
        .create_source_material(Some("replay-lifecycle-nonmatch"))
        .await?;
    let nonmatch_event = DynamicPayload::new(
        "test-node",
        "file.created",
        serde_json::json!({ "path": "/tmp/replay-lifecycle-nonmatch.txt" }),
    )
    .from_material(nonmatch_material)
    .build()?;
    let inserted_nonmatch = ctx.pool.events().insert(nonmatch_event).await?;
    let nonmatch_id = inserted_nonmatch
        .id
        .expect("seeded non-matching event must have an id")
        .to_uuid();

    let scope_start = replay_ts - time::Duration::seconds(1);
    let scope_end = replay_ts + time::Duration::seconds(1);

    let plan_req = serde_json::json!({
        "command": "plan",
        "actor": "admin:test-user",
        "scope": {
            "node_id": "test-node",
            "time_window": [scope_start.format_rfc3339(), scope_end.format_rfc3339()],
            "material_filter": [replay_material.as_uuid().to_string()],
            "filters": { "event_types": ["file.created"] }
        }
    });
    let plan_msg = nats
        .request(
            control_subject.clone(),
            serde_json::to_vec(&plan_req)?.into(),
        )
        .await?;
    let plan_resp: serde_json::Value = serde_json::from_slice(&plan_msg.payload)?;
    if plan_resp["status"].as_str() == Some("error") {
        bail!("plan failed: {plan_resp:?}");
    }
    let op_id = plan_resp["operation"]["operation_id"]
        .as_str()
        .expect("operation id should be present")
        .to_string();

    let preview_req = serde_json::json!({
        "command": "preview",
        "operation_id": op_id,
    });
    let preview_msg = nats
        .request(
            control_subject.clone(),
            serde_json::to_vec(&preview_req)?.into(),
        )
        .await?;
    let preview_resp: serde_json::Value = serde_json::from_slice(&preview_msg.payload)?;
    if preview_resp["status"].as_str() == Some("error") {
        bail!("preview failed: {preview_resp:?}");
    }
    assert_eq!(
        preview_resp["operation"]["state"].as_str(),
        Some("Previewed")
    );
    assert_eq!(preview_resp["preview"]["total_events"].as_i64(), Some(1));
    assert_eq!(
        preview_resp["preview"]["replay_semantics"].as_str(),
        Some("reexecute_material_roots_via_node_scan")
    );

    let approve_req = serde_json::json!({
        "command": "approve",
        "operation_id": op_id,
        "approver": "admin:superuser",
    });
    let approve_msg = nats
        .request(
            control_subject.clone(),
            serde_json::to_vec(&approve_req)?.into(),
        )
        .await?;
    let approve_resp: serde_json::Value = serde_json::from_slice(&approve_msg.payload)?;
    if approve_resp["status"].as_str() == Some("error") {
        bail!("approve failed: {approve_resp:?}");
    }
    assert_eq!(
        approve_resp["operation"]["state"].as_str(),
        Some("Approved")
    );

    let execute_req = serde_json::json!({
        "command": "execute",
        "operation_id": op_id,
        "executor": "service:worker-1",
    });
    let execute_msg = nats
        .request(
            control_subject.clone(),
            serde_json::to_vec(&execute_req)?.into(),
        )
        .await?;
    let execute_resp: serde_json::Value = serde_json::from_slice(&execute_msg.payload)?;
    if execute_resp["status"].as_str() == Some("error") {
        bail!("execute failed: {execute_resp:?}");
    }

    let status_req = serde_json::json!({
        "command": "status",
        "operation_id": op_id,
    });
    let mut status_resp = serde_json::Value::Null;
    for _ in 0..40 {
        let status_msg = nats
            .request(
                control_subject.clone(),
                serde_json::to_vec(&status_req)?.into(),
            )
            .await?;
        status_resp = serde_json::from_slice(&status_msg.payload)?;
        if status_resp["operation"]["state"].as_str() == Some("Completed") {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(status_resp["status"].as_str(), Some("ok"));
    assert_eq!(
        status_resp["operation"]["state"].as_str(),
        Some("Completed")
    );
    assert_eq!(
        status_resp["operation"]["checkpoint"]["total_events"].as_u64(),
        Some(1)
    );
    assert_eq!(
        status_resp["operation"]["checkpoint"]["processed_events"].as_u64(),
        Some(1)
    );

    let stream = js.get_stream(&stream_name).await?;
    let consumer_name = format!("replay-lifecycle-consumer-{}", Uuid::now_v7().simple());
    let consumer = stream
        .get_or_create_consumer(
            &consumer_name,
            ConsumerConfig {
                durable_name: Some(consumer_name.clone()),
                name: Some(consumer_name.clone()),
                deliver_policy: DeliverPolicy::All,
                ack_policy: AckPolicy::Explicit,
                filter_subject: env.nats_subject("events.raw.test-node.file_created"),
                ..Default::default()
            },
        )
        .await?;

    let mut batch = consumer
        .fetch()
        .max_messages(8)
        .expires(Duration::from_secs(2))
        .messages()
        .await?;
    let mut replay_payloads = Vec::new();
    while let Some(message) = batch.next().await {
        let message = message.map_err(|e| color_eyre::eyre::eyre!(e.to_string()))?;
        replay_payloads.push(serde_json::from_slice::<serde_json::Value>(
            &message.payload,
        )?);
        message
            .ack()
            .await
            .map_err(|e| color_eyre::eyre::eyre!(e.to_string()))?;
    }
    assert_eq!(replay_payloads.len(), 0);

    let replay_target_live: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(replay_target_id)
            .fetch_one(&ctx.pool)
            .await?;
    let replay_target_archived: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(replay_target_id)
    .fetch_one(&ctx.pool)
    .await?;
    let cascade_live: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(cascade_id)
            .fetch_one(&ctx.pool)
            .await?;
    let cascade_archived: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(cascade_id)
    .fetch_one(&ctx.pool)
    .await?;
    let nonmatch_live: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(nonmatch_id)
            .fetch_one(&ctx.pool)
            .await?;
    let nonmatch_archived: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(nonmatch_id)
    .fetch_one(&ctx.pool)
    .await?;

    assert_eq!(replay_target_live, 0);
    assert_eq!(replay_target_archived, 1);
    assert_eq!(cascade_live, 0);
    assert_eq!(cascade_archived, 1);
    assert_eq!(nonmatch_live, 1);
    assert_eq!(nonmatch_archived, 0);

    let created_at = Timestamp::parse_rfc3339(
        status_resp["operation"]["created_at"]
            .as_str()
            .expect("created_at should be present"),
    )?;
    let approved_at = Timestamp::parse_rfc3339(
        status_resp["operation"]["approved_at"]
            .as_str()
            .expect("approved_at should be present"),
    )?;
    let finished_at = Timestamp::parse_rfc3339(
        status_resp["operation"]["finished_at"]
            .as_str()
            .expect("finished_at should be present"),
    )?;
    assert!(created_at <= approved_at);
    assert!(approved_at <= finished_at);

    let dispatched_command = scan_command_rx
        .await
        .map_err(|_| color_eyre::eyre::eyre!("fake test-node did not receive scan command"))?;
    let replay_context = dispatched_command
        .args
        .replay
        .expect("gateway must populate typed replay context");
    assert_eq!(replay_context.materials.len(), 1);
    assert_eq!(
        replay_context.materials[0].source_material_id,
        *replay_material.as_uuid()
    );
    assert_eq!(
        replay_context.replay_scope.material_ids,
        Some(vec![*replay_material.as_uuid()])
    );
    assert_eq!(
        replay_context.replay_scope.event_types,
        Some(vec!["file.created".to_string()])
    );

    scan_handle
        .await
        .map_err(|e| color_eyre::eyre::eyre!("fake scan node task failed: {e}"))?;

    Ok(())
}
