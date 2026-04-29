use super::*;
use futures::StreamExt;
use serde_json::json;
use sinex_db::DbPool;
use sinex_db::repositories::DbPoolExt;
use sinex_db::repositories::state::Operation;
use sinex_node_sdk::runtime::stream::ScanReport;
use sinex_primitives::events::{EventPayload, payloads::filesystem::FileCreatedPayload};
use sinex_primitives::{DynamicPayload, Id, Uuid};
use tokio::time::sleep;
use xtask::sandbox::{EnvGuard, sinex_test};

/// Subscribe to scope-invalidation messages in tests.
///
/// `js.publish` requires the target subject to be covered by an existing
/// JetStream stream, and the production stream bootstrap (in
/// `sinex_ingestd::jetstream_consumer::bootstrap_streams`) does not run
/// in test contexts that use ephemeral NATS. This helper:
///
/// 1. `get_or_create_stream`s the canonical
///    `SINEX_RAW_EVENTS_DERIVED_INVALIDATIONS` stream so publishes succeed.
/// 2. Creates an ephemeral push consumer and forwards each delivered
///    payload onto an `mpsc::UnboundedReceiver<Vec<u8>>` so call sites
///    can `.recv()` the bytes directly without juggling the
///    `Result<jetstream::Message, _>` shape.
async fn spawn_invalidation_listener_for_test(
    nats_client: &async_nats::Client,
) -> Result<tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>> {
    use async_nats::jetstream::{consumer::push, stream as js_stream};
    let env = sinex_primitives::environment::environment();
    let stream_name = env.nats_stream_name("SINEX_RAW_EVENTS_DERIVED_INVALIDATIONS");
    let invalidation_subject = env.nats_subject(INVALIDATION_SUBJECT);
    let js = async_nats::jetstream::new(nats_client.clone());
    let stream = js
        .get_or_create_stream(js_stream::Config {
            name: stream_name,
            subjects: vec![invalidation_subject],
            ..Default::default()
        })
        .await
        .map_err(|e| eyre!("failed to bootstrap invalidation stream: {e}"))?;
    let deliver_subject = nats_client.new_inbox();
    let consumer = stream
        .create_consumer(push::Config {
            deliver_subject,
            ..Default::default()
        })
        .await
        .map_err(|e| eyre!("failed to create invalidation consumer: {e}"))?;
    let mut messages = consumer
        .messages()
        .await
        .map_err(|e| eyre!("failed to start invalidation message stream: {e}"))?;

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(item) = messages.next().await {
            let Ok(msg) = item else { break };
            let _ = msg.ack().await;
            if tx.send(msg.payload.to_vec()).is_err() {
                break;
            }
        }
    });
    Ok(rx)
}

fn sample_scope() -> ReplayScope {
    ReplayScope {
        node_id: "fs-test".to_string(),
        time_window: None,
        material_filter: None,
        filters: HashMap::new(),
    }
}

async fn wait_for_operation(pool: &DbPool, operation_id: Uuid) -> Result<()> {
    let op_id = Id::<Operation>::from_uuid(operation_id);
    for attempt in 0..20 {
        if pool.state().operation_exists(&op_id).await? {
            return Ok(());
        }
        sleep(Duration::from_millis(10 * (attempt + 1) as u64)).await;
    }
    Err(eyre!(
        "operation record {operation_id} not found after waiting for repository persistence"
    ))
}

async fn drive_to_state(
    replay: &Arc<ReplayStateMachine>,
    pool: &DbPool,
    operation_id: Uuid,
    targets: &[ReplayState],
) -> Result<()> {
    wait_for_operation(pool, operation_id).await?;
    for state in targets {
        replay.transition(operation_id, *state).await?;
    }
    Ok(())
}

async fn wait_for_operation_state(
    replay: &Arc<ReplayStateMachine>,
    operation_id: Uuid,
    target: ReplayState,
) -> Result<()> {
    for _ in 0..40 {
        let operation = replay.load_operation(operation_id).await?;
        if operation.state == target {
            return Ok(());
        }
        sleep(Duration::from_millis(25)).await;
    }
    Err(eyre!(
        "operation {operation_id} did not reach state {:?} before timeout",
        target
    ))
}

async fn corrupt_operation_preview_summary(pool: &DbPool, operation_id: Uuid) -> Result<()> {
    sqlx::query!(
        r#"
        UPDATE core.operations_log
        SET preview_summary = '"broken"'::jsonb
        WHERE id = $1::uuid
        "#,
        operation_id,
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn spawn_fake_scan_node(
    nats: Client,
    env: SinexEnvironment,
    node_name: &str,
    events_processed: u64,
) -> Result<(
    tokio::sync::oneshot::Receiver<NodeScanCommand>,
    tokio::task::JoinHandle<()>,
)> {
    let node_name = node_name.to_string();
    let subject = env.nats_subject(&format!("sinex.control.nodes.{node_name}.scan"));
    let mut sub = nats
        .subscribe(subject)
        .await
        .map_err(|e| eyre!("failed to subscribe fake node dispatcher: {e}"))?;
    let (command_tx, command_rx) = tokio::sync::oneshot::channel();

    let handle = tokio::spawn(async move {
        if let Some(msg) = sub.next().await {
            let command: NodeScanCommand = serde_json::from_slice(&msg.payload)
                .expect("fake node must receive a valid scan command");
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
                nats.publish(reply, serde_json::to_vec(&ack).unwrap().into())
                    .await
                    .expect("fake node ack publish should succeed");
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
                node_name: node_name.clone(),
                events_processed,
                events_emitted: events_processed,
                final_report: Some(report),
                error: None,
            };
            nats.publish(
                progress_subject,
                serde_json::to_vec(&progress).unwrap().into(),
            )
            .await
            .expect("fake node progress publish should succeed");
        }
    });

    Ok((command_rx, handle))
}

async fn spawn_fake_scan_node_with_progress(
    nats: Client,
    env: SinexEnvironment,
    node_name: &str,
    events_processed: u64,
    events_emitted: u64,
) -> Result<(
    tokio::sync::oneshot::Receiver<NodeScanCommand>,
    tokio::task::JoinHandle<()>,
)> {
    let node_name = node_name.to_string();
    let subject = env.nats_subject(&format!("sinex.control.nodes.{node_name}.scan"));
    let mut sub = nats
        .subscribe(subject)
        .await
        .map_err(|e| eyre!("failed to subscribe fake node dispatcher: {e}"))?;
    let (command_tx, command_rx) = tokio::sync::oneshot::channel();

    let handle = tokio::spawn(async move {
        if let Some(msg) = sub.next().await {
            let command: NodeScanCommand = serde_json::from_slice(&msg.payload)
                .expect("fake node must receive a valid scan command");
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
                nats.publish(reply, serde_json::to_vec(&ack).unwrap().into())
                    .await
                    .expect("fake node ack publish should succeed");
            }

            let report = ScanReport {
                events_processed,
                duration: Duration::from_millis(5),
                final_checkpoint: Checkpoint::None,
                time_range: None,
                node_stats: HashMap::from([("events_emitted".to_string(), events_emitted)]),
                successful_targets: vec![node_name.clone()],
                failed_targets: Vec::new(),
                warnings: Vec::new(),
            };
            let progress = NodeScanProgress {
                operation_id,
                node_name: node_name.clone(),
                events_processed,
                events_emitted,
                final_report: Some(report),
                error: None,
            };
            nats.publish(
                progress_subject,
                serde_json::to_vec(&progress).unwrap().into(),
            )
            .await
            .expect("fake node progress publish should succeed");
        }
    });

    Ok((command_rx, handle))
}

async fn spawn_fake_scan_node_ack_only(
    nats: Client,
    env: SinexEnvironment,
    node_name: &str,
) -> Result<(
    tokio::sync::oneshot::Receiver<NodeScanCommand>,
    tokio::task::JoinHandle<()>,
)> {
    let node_name = node_name.to_string();
    let subject = env.nats_subject(&format!("sinex.control.nodes.{node_name}.scan"));
    let mut sub = nats
        .subscribe(subject)
        .await
        .map_err(|e| eyre!("failed to subscribe fake node dispatcher: {e}"))?;
    let (command_tx, command_rx) = tokio::sync::oneshot::channel();

    let handle = tokio::spawn(async move {
        if let Some(msg) = sub.next().await {
            let command: NodeScanCommand = serde_json::from_slice(&msg.payload)
                .expect("fake node must receive a valid scan command");
            let _ = command_tx.send(command.clone());

            if let Some(reply) = msg.reply {
                let ack = NodeScanAck {
                    operation_id: command.operation_id,
                    node_name: node_name.clone(),
                    accepted: true,
                    error: None,
                };
                nats.publish(reply, serde_json::to_vec(&ack).unwrap().into())
                    .await
                    .expect("fake node ack publish should succeed");
            }
        }
    });

    Ok((command_rx, handle))
}

fn spawn_replay_output_inserter(
    pool: DbPool,
    command_rx: tokio::sync::oneshot::Receiver<NodeScanCommand>,
    source: &'static str,
    event_type: &'static str,
    path: &'static str,
    equivalence_key: Option<&'static str>,
) -> tokio::task::JoinHandle<Result<NodeScanCommand>> {
    tokio::spawn(async move {
        let command = command_rx
            .await
            .map_err(|_| eyre!("fake replay output inserter did not receive scan command"))?;
        let logical_source_identifier = command
            .args
            .replay
            .as_ref()
            .and_then(|replay| replay.materials.first())
            .map_or(path, ReplayExecutionEngine::logical_source_identifier)
            .to_string();
        let material_id = Uuid::now_v7();
        let source_identifier = format!("{logical_source_identifier}#material={material_id}");
        sqlx::query!(
            r#"
            INSERT INTO raw.source_material_registry (
                id,
                material_kind,
                source_identifier,
                status,
                timing_info_type,
                metadata
            )
            VALUES ($1::uuid, 'annex', $2, 'completed', 'realtime', $3::jsonb)
            "#,
            material_id,
            source_identifier,
            json!({ "logical_source_identifier": logical_source_identifier }),
        )
        .execute(&pool)
        .await?;
        let mut event = DynamicPayload::new(source, event_type, json!({ "path": path }))
            .from_material(Id::from_uuid(material_id))
            .build()?;
        event.created_by_operation_id = Some(command.operation_id);
        if let Some(equivalence_key) = equivalence_key {
            event.equivalence_key = Some(equivalence_key.to_string());
        }
        pool.events().insert(event).await?;
        Ok(command)
    })
}

#[test]
fn replay_output_expectations_deduplicate_logical_sources() {
    let logical_source = "/tmp/replay-dedup.txt";
    let expected = ExpectedReplayOutputs {
        minimum_visible_count: 0,
        sources: vec!["fs-test".to_string()],
        event_types: vec![FileCreatedPayload::EVENT_TYPE.as_static_str().to_string()],
        logical_source_identifiers: Vec::new(),
    };
    let replay_materials = vec![
        ResolvedReplayMaterial {
            source_material_id: Uuid::now_v7(),
            material_kind: "annex".to_string(),
            source_identifier: format!("{logical_source}#material={}", Uuid::now_v7()),
            material_metadata: json!({ "logical_source_identifier": logical_source }),
            material_start_time: None,
            material_end_time: None,
        },
        ResolvedReplayMaterial {
            source_material_id: Uuid::now_v7(),
            material_kind: "annex".to_string(),
            source_identifier: format!("{logical_source}#material={}", Uuid::now_v7()),
            material_metadata: json!({ "logical_source_identifier": logical_source }),
            material_start_time: None,
            material_end_time: None,
        },
    ];

    let expected =
        ReplayExecutionEngine::with_logical_source_identifiers(expected, &replay_materials)
            .expect("logical source expectation should succeed");

    assert_eq!(expected.minimum_visible_count, 1);
    assert_eq!(
        expected.logical_source_identifiers,
        vec![logical_source.to_string()]
    );
}

#[sinex_test]
async fn telemetry_reports_state_counts(ctx: TestContext) -> Result<()> {
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let telemetry = ReplayTelemetry::with_interval(replay.clone(), Duration::from_millis(5));
    let planning_scope = sample_scope();
    let mut executing_scope = sample_scope();
    executing_scope.node_id = "fs-test-executing".to_string();
    let mut failed_scope = sample_scope();
    failed_scope.node_id = "fs-test-failed".to_string();

    let _planning = replay
        .create_operation(planning_scope, "planner".into())
        .await?;

    let executing = replay
        .create_operation(executing_scope, "executor".into())
        .await?;
    drive_to_state(
        &replay,
        &ctx.pool,
        executing.operation_id,
        &[
            ReplayState::Previewed,
            ReplayState::Approved,
            ReplayState::Executing,
        ],
    )
    .await?;

    let failed = replay
        .create_operation(failed_scope, "runner".into())
        .await?;
    drive_to_state(
        &replay,
        &ctx.pool,
        failed.operation_id,
        &[
            ReplayState::Previewed,
            ReplayState::Approved,
            ReplayState::Executing,
            ReplayState::Failed,
        ],
    )
    .await?;

    telemetry.sample().await?;
    let snapshot = telemetry.latest_snapshot();

    assert_eq!(snapshot.total_operations, 3);
    assert_eq!(snapshot.active_operations, 2);
    assert_eq!(snapshot.counts.get(&ReplayState::Planning), Some(&1));
    assert_eq!(snapshot.counts.get(&ReplayState::Executing), Some(&1));
    assert_eq!(snapshot.counts.get(&ReplayState::Failed), Some(&1));

    Ok(())
}

#[sinex_test]
async fn telemetry_handles_empty_operation_set(ctx: TestContext) -> Result<()> {
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let telemetry = ReplayTelemetry::with_interval(replay, Duration::from_millis(5));

    telemetry.sample().await?;
    let snapshot = telemetry.latest_snapshot();

    assert_eq!(snapshot.total_operations, 0);
    assert_eq!(snapshot.active_operations, 0);
    assert!(snapshot.counts.is_empty());

    Ok(())
}

#[sinex_test]
async fn replay_client_errors_when_broker_disappears(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let nats = ctx.nats_handle()?;

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let client = spawn_replay_control(replay, nats_client, Duration::from_secs(30)).await?;

    // Shut down the broker to simulate a partition mid-flight.
    nats.shutdown().await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let scope = sample_scope();
    let err = client
        .plan_with_timeout("test:user".into(), scope, Duration::from_secs(1))
        .await
        .expect_err("plan should fail after broker drop");
    assert!(
        !err.to_string().is_empty(),
        "error message should be populated"
    );
    let health = client.health_snapshot();
    let last_error = health
        .last_error
        .expect("health snapshot should retain the last replay control error");
    assert!(
        !last_error.message.is_empty(),
        "last replay control error message should be populated"
    );
    Ok(())
}

#[sinex_test]
async fn replay_control_reconnects_when_subscription_closes_after_startup(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let nats = ctx.nats_handle()?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let env = sinex_primitives::environment::environment();
    let executor = ReplayExecutionEngine::new(replay.clone(), nats_client.clone());
    let health = Arc::new(Mutex::new(ReplayControlHealthState::default()));

    let server_task = ReplayControlServer::new(
        &env,
        nats_client.clone(),
        replay,
        executor,
        Arc::clone(&health),
    )
    .spawn()
    .await?;
    let client = ReplayControlClient::new(
        &env,
        nats_client,
        Duration::from_secs(30),
        Arc::clone(&health),
    );

    nats.shutdown().await?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert!(
        !server_task.is_finished(),
        "closing the live replay-control subscription should keep the server retrying instead of exiting"
    );
    let snapshot = client.health_snapshot();
    assert!(
        !snapshot.connected,
        "replay-control health must reflect that the live subscription was lost"
    );
    assert!(
        snapshot.last_error.is_some(),
        "replay-control health must retain a clue after the live subscription is lost"
    );

    server_task.abort();
    Ok(())
}

#[sinex_test]
async fn replay_control_health_reports_inactive_subscription(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let health = Arc::new(Mutex::new(ReplayControlHealthState::default()));
    let client = ReplayControlClient::new(
        &sinex_primitives::environment::environment(),
        ctx.nats_client(),
        Duration::from_secs(30),
        Arc::clone(&health),
    );

    let disconnected = client.health_snapshot();
    assert!(!disconnected.connected);
    assert_eq!(
        disconnected
            .last_error
            .as_ref()
            .map(|error| error.message.as_str()),
        Some("Replay control server subscription is not active")
    );

    {
        let mut guard = health.lock();
        guard.server_subscribed = true;
    }

    let connected = client.health_snapshot();
    assert!(connected.connected);
    assert!(connected.last_error.is_none());
    Ok(())
}

#[sinex_test]
async fn replay_preview_surfaces_safety_analysis_failure(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let pool = ctx.pool.clone();
    pool.close().await;

    let analysis = run_safety_analysis(&pool, &[Uuid::now_v7()]).await;

    assert_eq!(
        analysis.get("status").and_then(serde_json::Value::as_str),
        Some("failed")
    );
    assert!(
        analysis
            .get("error")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|message| !message.is_empty()),
        "expected concrete analyzer failure message, got: {analysis:?}"
    );
    assert_eq!(
        analysis.get("warning").and_then(serde_json::Value::as_str),
        Some("Cascade impact could not be determined. Approve with caution.")
    );
    Ok(())
}

#[sinex_test]
async fn replay_execution_records_outcome(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let (material_id, inserted) = loop {
        let material_id = ctx.create_source_material(Some("replay-outcome")).await?;
        let event = DynamicPayload::new(
            "fs-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": "/tmp/replay.txt" }),
        )
        .from_material(material_id)
        .build()?;
        let inserted = ctx.pool.events().insert(event).await?;
        if let Some(ts_orig) = inserted.ts_orig
            && ts_orig.inner().nanosecond() > 0
        {
            break (material_id, inserted);
        }
    };

    let replay_target_event_id = inserted.id.expect("inserted replay target must have id");
    let replay_target_id = replay_target_event_id.to_uuid();
    let target_window_end = replay_target_event_id.timestamp();
    let target_window_start = target_window_end - time::Duration::milliseconds(1);

    let cascaded = DynamicPayload::new(
        "analytics-test",
        "analytics.summary",
        json!({ "path": "/tmp/replay-summary.txt" }),
    )
    .from_parents([replay_target_event_id])?
    .build()?;
    let cascaded_inserted = ctx.pool.events().insert(cascaded).await?;
    let cascaded_id = cascaded_inserted
        .id
        .expect("inserted cascaded event must have id")
        .to_uuid();

    let nonmatch_material = ctx
        .create_source_material(Some("replay-outcome-nonmatch"))
        .await?;
    let nonmatch_event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-nonmatch.txt" }),
    )
    .from_material(nonmatch_material)
    .build()?;
    let inserted_nonmatch = ctx.pool.events().insert(nonmatch_event).await?;
    let nonmatch_id = inserted_nonmatch
        .id
        .expect("inserted non-matching event must have id")
        .to_uuid();

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();

    // The replay engine should no longer publish raw replay rows itself.
    // Keep a stream around so the test can assert that this count stays zero.
    let env = sinex_primitives::environment::environment();
    let js = async_nats::jetstream::new(nats_client.clone());
    let stream_name = format!("replay-test-{}", Uuid::now_v7().simple());
    js.get_or_create_stream(async_nats::jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![env.nats_subject("events.raw.>")],
        ..Default::default()
    })
    .await?;
    let (scan_command_rx, scan_handle) =
        spawn_fake_scan_node(nats_client.clone(), env.clone(), "fs-test", 1).await?;
    let replay_output_handle = spawn_replay_output_inserter(
        ctx.pool.clone(),
        scan_command_rx,
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        "/tmp/replay-output.txt",
        None,
    );

    let client = spawn_replay_control(replay, nats_client, Duration::from_secs(30)).await?;

    let mut scope = sample_scope();
    scope.time_window = Some((target_window_start, target_window_end));
    scope.material_filter = Some(vec![*material_id.as_uuid()]);
    scope.filters.insert(
        "event_types".to_string(),
        json!([FileCreatedPayload::EVENT_TYPE.as_static_str()]),
    );

    let planned = client
        .plan("test:replay-user".into(), scope.clone())
        .await?;
    assert_eq!(planned.state, ReplayState::Planning);

    let (previewed, preview) = client.preview(planned.operation_id).await?;
    assert_eq!(previewed.state, ReplayState::Previewed);
    assert_eq!(
        preview
            .get("total_events")
            .and_then(serde_json::Value::as_i64),
        Some(1),
        "preview should match only the filtered replay target"
    );
    assert_eq!(
        preview
            .get("replay_semantics")
            .and_then(serde_json::Value::as_str),
        Some("reexecute_material_roots_via_node_scan")
    );

    let approved = client
        .approve(planned.operation_id, "admin:approver".into())
        .await?;
    assert_eq!(approved.state, ReplayState::Approved);

    let executed = client
        .execute(planned.operation_id, "service:executor-node".into(), false)
        .await?;
    assert_eq!(executed.state, ReplayState::Completed);
    assert_eq!(executed.checkpoint.processed_events, 1);
    assert_eq!(executed.checkpoint.total_events, 1);
    assert_eq!(
        preview
            .get("total_events")
            .and_then(serde_json::Value::as_u64),
        Some(executed.checkpoint.total_events),
        "execute checkpoint totals must match preview totals"
    );

    assert!(
        executed.outcome.is_some(),
        "Replay execution should record a concrete outcome for automation consumers"
    );

    let dispatched_command = replay_output_handle
        .await
        .map_err(|e| eyre!("fake replay output task failed: {e}"))??;
    let replay_context = dispatched_command
        .args
        .replay
        .expect("gateway must populate typed replay context");
    assert_eq!(replay_context.materials.len(), 1);
    assert_eq!(
        replay_context.materials[0].source_material_id,
        *material_id.as_uuid(),
        "replay context must carry resolved source material identity"
    );
    assert_eq!(
        replay_context.replay_scope.material_ids,
        Some(vec![*material_id.as_uuid()]),
        "gateway must preserve normalized material filter in replay scope"
    );
    assert_eq!(
        replay_context.replay_scope.event_types,
        Some(vec![
            FileCreatedPayload::EVENT_TYPE.as_static_str().to_string()
        ]),
        "gateway must preserve normalized event type filter in replay scope"
    );

    use async_nats::jetstream::consumer::{
        AckPolicy, DeliverPolicy, pull::Config as ConsumerConfig,
    };
    let stream = js.get_stream(&stream_name).await?;
    let consumer_name = format!("replay-test-consumer-{}", Uuid::now_v7().simple());
    let consumer = stream
        .get_or_create_consumer(
            &consumer_name,
            ConsumerConfig {
                durable_name: Some(consumer_name.clone()),
                name: Some(consumer_name.clone()),
                deliver_policy: DeliverPolicy::All,
                ack_policy: AckPolicy::Explicit,
                filter_subject: env.nats_subject("events.raw.fs-test.file_created"),
                ..Default::default()
            },
        )
        .await?;

    let mut replay_batch = consumer
        .fetch()
        .max_messages(8)
        .expires(Duration::from_secs(2))
        .messages()
        .await?;
    let mut replay_payloads = Vec::new();
    while let Some(message) = replay_batch.next().await {
        let message = message.map_err(|e| eyre!(e.to_string()))?;
        replay_payloads.push(serde_json::from_slice::<serde_json::Value>(
            &message.payload,
        )?);
        message.ack().await.map_err(|e| eyre!(e.to_string()))?;
    }
    assert_eq!(
        replay_payloads.len(),
        0,
        "gateway replay must not republish stored raw rows"
    );

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
    let cascaded_live: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(cascaded_id)
            .fetch_one(&ctx.pool)
            .await?;
    let cascaded_archived: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(cascaded_id)
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
    assert_eq!(cascaded_live, 0);
    assert_eq!(cascaded_archived, 1);
    assert_eq!(nonmatch_live, 1);
    assert_eq!(nonmatch_archived, 0);

    let material_root_id = ctx
        .create_source_material(Some("replay-node-scan-parity"))
        .await?;
    let root = DynamicPayload::new(
        "reexecution-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/reexecution-root.txt" }),
    )
    .from_material(material_root_id)
    .build()?;
    let root_inserted = ctx.pool.events().insert(root).await?;
    let root_event_id = root_inserted.id.expect("reexecution root must have id");
    let root_id = root_event_id.to_uuid();
    let reexecution_derived = DynamicPayload::new(
        "reexecution-test",
        "file.derived",
        json!({ "path": "/tmp/reexecution-derived.txt" }),
    )
    .from_parents([root_event_id])?
    .build()?;
    let derived_inserted = ctx.pool.events().insert(reexecution_derived).await?;
    let derived_id = derived_inserted
        .id
        .expect("reexecution derived must have id")
        .to_uuid();
    let reexecution_root_ts = root_event_id.timestamp();
    let reexecution_scope = ReplayScope {
        node_id: "reexecution-test".to_string(),
        time_window: Some((
            reexecution_root_ts - time::Duration::seconds(1),
            reexecution_root_ts + time::Duration::seconds(1),
        )),
        material_filter: None,
        filters: HashMap::new(),
    };
    let planned_reexecution = client
        .plan("test:replay-user".into(), reexecution_scope)
        .await?;
    let (_, reexecution_preview) = client.preview(planned_reexecution.operation_id).await?;
    assert_eq!(
        reexecution_preview
            .get("total_events")
            .and_then(serde_json::Value::as_i64),
        Some(1),
        "preview must count only material roots for node-scan replay semantics"
    );
    client
        .approve(planned_reexecution.operation_id, "admin:approver".into())
        .await?;
    let (reexecution_command_rx, reexecution_handle) =
        spawn_fake_scan_node(ctx.nats_client(), env.clone(), "reexecution-test", 1).await?;
    let reexecution_output_handle = spawn_replay_output_inserter(
        ctx.pool.clone(),
        reexecution_command_rx,
        "reexecution-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        "/tmp/reexecution-root.txt",
        None,
    );
    let reexecution_executed = client
        .execute(
            planned_reexecution.operation_id,
            "service:executor-node".into(),
            false,
        )
        .await?;
    assert_eq!(reexecution_executed.state, ReplayState::Completed);
    assert_eq!(reexecution_executed.checkpoint.total_events, 1);
    assert_eq!(reexecution_executed.checkpoint.processed_events, 1);
    let reexecution_command = reexecution_output_handle
        .await
        .map_err(|e| eyre!("fake reexecution replay output task failed: {e}"))??;
    let reexecution_context = reexecution_command
        .args
        .replay
        .expect("reexecution must still carry replay context");
    assert_eq!(reexecution_context.materials.len(), 1);
    assert_eq!(
        reexecution_context.materials[0].source_material_id,
        *material_root_id.as_uuid(),
    );
    assert_eq!(
        reexecution_context.replay_scope.material_ids, None,
        "implicit replay scopes should not invent material filters"
    );
    let root_archived_after_reexecution: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(root_id)
    .fetch_one(&ctx.pool)
    .await?;
    let derived_archived_after_reexecution: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(derived_id)
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(root_archived_after_reexecution, 1);
    assert_eq!(derived_archived_after_reexecution, 1);

    scan_handle
        .await
        .map_err(|e| eyre!("fake fs-test node task failed: {e}"))?;
    reexecution_handle
        .await
        .map_err(|e| eyre!("fake reexecution-test node task failed: {e}"))?;

    Ok(())
}

#[sinex_test]
async fn replay_replacement_recording_follows_operation_outputs(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let engine = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client());

    let source_material = ctx
        .create_source_material(Some("replay-replacement-old"))
        .await?;
    let mut old_event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-replacement-old.txt" }),
    )
    .from_material(source_material)
    .build()?;
    old_event.equivalence_key = Some("replacement-eq".to_string());
    let old_inserted = ctx.pool.events().insert(old_event).await?;
    let old_id = old_inserted.id.expect("old replay event must have an id");
    let execution_window = (
        old_id.timestamp() - time::Duration::milliseconds(1),
        old_id.timestamp() + time::Duration::milliseconds(1),
    );

    let mut scope = sample_scope();
    scope.time_window = Some(execution_window);

    let operation = replay
        .create_operation(scope.clone(), "test:replacement-recorder".into())
        .await?;
    let operation_id = operation.operation_id;

    ctx.pool
        .events()
        .execute_cascade_archive(
            &[old_id.to_uuid()],
            "archive old replay target",
            &operation_id.to_string(),
            "test:replacement-recorder",
        )
        .await?;

    let replacement_material = ctx
        .create_source_material(Some("replay-replacement-new"))
        .await?;
    let mut replacement_event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-replacement-new.txt" }),
    )
    .from_material(replacement_material)
    .build()?;
    replacement_event.equivalence_key = Some("replacement-eq".to_string());
    replacement_event.created_by_operation_id = Some(operation_id);
    let replacement_inserted = ctx.pool.events().insert(replacement_event).await?;
    let replacement_id = replacement_inserted
        .id
        .expect("replacement replay event must have an id")
        .to_uuid();

    engine
        .record_event_replacements(&ctx.pool, operation_id, &[old_id.to_uuid()])
        .await?;

    let replacements = ctx
        .pool
        .events()
        .get_replacements_by_operation(operation_id)
        .await?;
    assert_eq!(replacements.len(), 1);
    assert_eq!(replacements[0].0, old_id.to_uuid());
    assert_eq!(replacements[0].1, replacement_id);
    assert_eq!(replacements[0].2, "superseded");

    Ok(())
}

#[sinex_test]
async fn replay_replacement_recording_skips_unmatched_old_events(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let engine = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client());

    let source_material = ctx
        .create_source_material(Some("replay-replacement-unmatched-old"))
        .await?;
    let mut old_event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-replacement-unmatched-old.txt" }),
    )
    .from_material(source_material)
    .build()?;
    old_event.equivalence_key = Some("old-eq".to_string());
    let old_inserted = ctx.pool.events().insert(old_event).await?;
    let old_id = old_inserted.id.expect("old replay event must have an id");
    let execution_window = (
        old_id.timestamp() - time::Duration::milliseconds(1),
        old_id.timestamp() + time::Duration::milliseconds(1),
    );

    let mut scope = sample_scope();
    scope.time_window = Some(execution_window);

    let operation = replay
        .create_operation(scope.clone(), "test:replacement-recorder".into())
        .await?;
    let operation_id = operation.operation_id;

    ctx.pool
        .events()
        .execute_cascade_archive(
            &[old_id.to_uuid()],
            "archive old replay target",
            &operation_id.to_string(),
            "test:replacement-recorder",
        )
        .await?;

    let replacement_material = ctx
        .create_source_material(Some("replay-replacement-unmatched-new"))
        .await?;
    let mut replacement_event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-replacement-unmatched-new.txt" }),
    )
    .from_material(replacement_material)
    .build()?;
    replacement_event.equivalence_key = Some("new-eq".to_string());
    replacement_event.created_by_operation_id = Some(operation_id);
    ctx.pool.events().insert(replacement_event).await?;

    engine
        .record_event_replacements(&ctx.pool, operation_id, &[old_id.to_uuid()])
        .await?;

    let replacements = ctx
        .pool
        .events()
        .get_replacements_by_operation(operation_id)
        .await?;
    assert!(
        replacements.is_empty(),
        "unmatched replay rows must not fabricate replacement lineage"
    );

    Ok(())
}

#[sinex_test]
async fn replay_execution_fails_when_outputs_never_become_query_visible(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let material_id = ctx
        .create_source_material(Some("replay-output-visibility-timeout"))
        .await?;
    let event = DynamicPayload::new(
        "visibility-timeout-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-output-visibility-timeout.txt" }),
    )
    .from_material(material_id)
    .build()?;
    let inserted = ctx.pool.events().insert(event).await?;
    let target_id = inserted
        .id
        .expect("inserted replay target must have id")
        .to_uuid();
    let target_ts = inserted
        .id
        .expect("inserted replay target must have id")
        .timestamp();

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let env = environment();
    let (scan_command_rx, scan_handle) = spawn_fake_scan_node_with_progress(
        nats_client.clone(),
        env,
        "visibility-timeout-test",
        1,
        1,
    )
    .await?;

    let mut scope = sample_scope();
    scope.node_id = "visibility-timeout-test".to_string();
    scope.time_window = Some((
        target_ts - time::Duration::milliseconds(1),
        target_ts + time::Duration::milliseconds(1),
    ));

    let planned = replay
        .create_operation(scope.clone(), "test:output-visibility-timeout".into())
        .await?;
    let preview = replay.generate_preview_summary(&scope).await?;
    replay.update_preview(planned.operation_id, preview).await?;
    replay
        .approve(planned.operation_id, "admin:approver".into())
        .await?;

    let executor = ReplayExecutionEngine::new(replay.clone(), nats_client)
        .with_scan_completion_timeout(Duration::from_millis(100));
    let err = executor
        .execute(planned.operation_id, "service:executor-node".into())
        .await
        .expect_err("missing replay outputs must fail before completion");
    assert!(
        err.to_string()
            .contains("Replay outputs were not query-visible after successful scan"),
        "unexpected error: {err}"
    );

    let failed = replay.load_operation(planned.operation_id).await?;
    assert_eq!(failed.state, ReplayState::Failed);
    assert_eq!(
        failed.outcome,
        Some(sinex_primitives::domain::ReplayOutcome::Failed)
    );

    let live_target_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(target_id)
            .fetch_one(&ctx.pool)
            .await?;
    let archived_target_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(target_id)
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(live_target_count, 0);
    assert_eq!(archived_target_count, 1);

    let dispatched_command = scan_command_rx.await.map_err(|_| {
        eyre!("fake visibility-timeout-test node did not receive a scan command")
    })?;
    assert_eq!(dispatched_command.operation_id, planned.operation_id);

    scan_handle
        .await
        .map_err(|e| eyre!("fake visibility-timeout-test node task failed: {e}"))?;

    Ok(())
}

#[sinex_test]
async fn replay_execution_fails_when_node_never_reports_completion(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let material_id = ctx.create_source_material(Some("replay-timeout")).await?;
    let event = DynamicPayload::new(
        "timeout-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-timeout.txt" }),
    )
    .from_material(material_id)
    .build()?;
    let inserted = ctx.pool.events().insert(event).await?;
    let target_id = inserted
        .id
        .expect("inserted replay target must have id")
        .to_uuid();
    let target_ts = inserted
        .id
        .expect("inserted replay target must have id")
        .timestamp();

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let env = sinex_primitives::environment::environment();
    let (scan_command_rx, scan_handle) =
        spawn_fake_scan_node_ack_only(nats_client.clone(), env.clone(), "timeout-test").await?;

    let executor = ReplayExecutionEngine::new(replay.clone(), nats_client.clone())
        .with_scan_completion_timeout(Duration::from_millis(100));
    ReplayTelemetry::new(replay.clone()).spawn();
    let health = Arc::new(Mutex::new(ReplayControlHealthState::default()));
    ReplayControlServer::new(
        &env,
        nats_client.clone(),
        replay.clone(),
        executor,
        Arc::clone(&health),
    )
    .spawn()
    .await?;
    let client = ReplayControlClient::new(&env, nats_client, Duration::from_secs(30), health);

    let mut scope = sample_scope();
    scope.node_id = "timeout-test".to_string();
    scope.time_window = Some((
        target_ts - time::Duration::milliseconds(1),
        target_ts + time::Duration::milliseconds(1),
    ));

    let planned = client.plan("test:replay-user".into(), scope).await?;
    let (previewed, _) = client.preview(planned.operation_id).await?;
    let approved = client
        .approve(previewed.operation_id, "admin:approver".into())
        .await?;
    let err = client
        .execute(approved.operation_id, "service:executor-node".into(), false)
        .await
        .expect_err("execute should fail when the node never reports completion");
    assert!(
        err.to_string().contains("archived cascade left untouched"),
        "timeout failure should explain why replay execution failed: {err}"
    );

    let operation = replay.load_operation(approved.operation_id).await?;
    assert_eq!(operation.state, ReplayState::Failed);
    assert_eq!(operation.checkpoint.processed_events, 0);

    let live_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(target_id)
            .fetch_one(&ctx.pool)
            .await?;
    let archived_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(target_id)
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        live_count, 0,
        "timed-out replay should not resurrect archived rows"
    );
    assert_eq!(
        archived_count, 1,
        "timed-out replay should leave the archived cascade untouched"
    );

    let dispatched_command = scan_command_rx
        .await
        .map_err(|_| eyre!("fake timeout-test node did not receive a scan command"))?;
    assert_eq!(dispatched_command.operation_id, approved.operation_id);

    scan_handle
        .await
        .map_err(|e| eyre!("fake timeout-test node task failed: {e}"))?;

    Ok(())
}

#[sinex_test]
async fn replay_execution_fails_fast_when_progress_checkpoint_persist_fails(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let material_id = ctx
        .create_source_material(Some("replay-checkpoint-persist-fail"))
        .await?;
    let event = DynamicPayload::new(
        "checkpoint-fail-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-checkpoint-persist-fail.txt" }),
    )
    .from_material(material_id)
    .build()?;
    let inserted = ctx.pool.events().insert(event).await?;
    let target_id = inserted
        .id
        .expect("inserted replay target must have id")
        .to_uuid();
    let target_ts = inserted
        .id
        .expect("inserted replay target must have id")
        .timestamp();

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let env = environment();
    let (_scan_command_rx, scan_handle) = spawn_fake_scan_node_with_progress(
        nats_client.clone(),
        env,
        "checkpoint-fail-test",
        1,
        0,
    )
    .await?;

    let mut scope = sample_scope();
    scope.node_id = "checkpoint-fail-test".to_string();
    scope.time_window = Some((
        target_ts - time::Duration::milliseconds(1),
        target_ts + time::Duration::milliseconds(1),
    ));

    let planned = replay
        .create_operation(scope.clone(), "test:checkpoint-fail".into())
        .await?;
    let preview = replay.generate_preview_summary(&scope).await?;
    replay.update_preview(planned.operation_id, preview).await?;
    replay
        .approve(planned.operation_id, "admin:approver".into())
        .await?;

    let executor = ReplayExecutionEngine::new(replay.clone(), nats_client)
        .with_checkpoint_failures(Arc::new(AtomicUsize::new(1)))
        .with_scan_completion_timeout(Duration::from_secs(5));
    let err = executor
        .execute(planned.operation_id, "service:executor-node".into())
        .await
        .expect_err("checkpoint persistence failure should abort replay execution");
    assert!(
        err.chain().any(|cause| {
            cause
                .to_string()
                .contains("Failed to persist replay progress checkpoint")
        }),
        "unexpected error: {err}"
    );

    let failed = replay.load_operation(planned.operation_id).await?;
    assert_eq!(failed.state, ReplayState::Failed);
    assert_eq!(
        failed.outcome,
        Some(sinex_primitives::domain::ReplayOutcome::Failed)
    );
    assert!(
        failed.error_details.as_deref().is_some_and(
            |details| details.contains("Failed to persist replay progress checkpoint")
        ),
        "failure details should include checkpoint persistence context: {:?}",
        failed.error_details
    );

    let live_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(target_id)
            .fetch_one(&ctx.pool)
            .await?;
    let archived_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(target_id)
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        live_count, 1,
        "checkpoint persistence failure before replacements should restore live rows"
    );
    assert_eq!(
        archived_count, 0,
        "checkpoint persistence failure before replacements should not leave archived rows behind"
    );

    scan_handle
        .await
        .map_err(|e| eyre!("fake checkpoint-fail-test node task failed: {e}"))?;

    Ok(())
}

#[sinex_test]
async fn replay_execution_fails_when_replacement_recording_fails(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let material_id = ctx
        .create_source_material(Some("replay-replacement-record-fail"))
        .await?;
    let mut event = DynamicPayload::new(
        "replacement-record-fail-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-replacement-record-fail.txt" }),
    )
    .from_material(material_id)
    .build()?;
    event.equivalence_key = Some("replacement-record-eq".to_string());
    let inserted = ctx.pool.events().insert(event).await?;
    let target_id = inserted
        .id
        .expect("inserted replay target must have id")
        .to_uuid();
    let target_ts = inserted
        .id
        .expect("inserted replay target must have id")
        .timestamp();

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let env = environment();
    let (scan_command_rx, scan_handle) = spawn_fake_scan_node_with_progress(
        nats_client.clone(),
        env,
        "replacement-record-fail-test",
        1,
        1,
    )
    .await?;

    let mut scope = sample_scope();
    scope.node_id = "replacement-record-fail-test".to_string();
    scope.time_window = Some((
        target_ts - time::Duration::milliseconds(1),
        target_ts + time::Duration::milliseconds(1),
    ));

    let planned = replay
        .create_operation(scope.clone(), "test:replacement-record-fail".into())
        .await?;
    let preview = replay.generate_preview_summary(&scope).await?;
    replay.update_preview(planned.operation_id, preview).await?;
    replay
        .approve(planned.operation_id, "admin:approver".into())
        .await?;

    let replay_output_handle = spawn_replay_output_inserter(
        ctx.pool.clone(),
        scan_command_rx,
        "replacement-record-fail-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        "/tmp/replay-replacement-record-fail-output.txt",
        Some("replacement-record-eq"),
    );

    let executor = ReplayExecutionEngine::new(replay.clone(), nats_client)
        .with_replacement_record_failures(Arc::new(AtomicUsize::new(1)))
        .with_scan_completion_timeout(Duration::from_secs(5));
    let err = executor
        .execute(planned.operation_id, "service:executor-node".into())
        .await
        .expect_err("replacement-record failure should abort replay execution");
    assert!(
        err.chain().any(|cause| {
            cause
                .to_string()
                .contains("Failed to record replay replacement relations")
        }),
        "unexpected error: {err}"
    );

    let failed = replay.load_operation(planned.operation_id).await?;
    assert_eq!(failed.state, ReplayState::Failed);
    assert_eq!(
        failed.outcome,
        Some(sinex_primitives::domain::ReplayOutcome::Failed)
    );
    assert!(
        failed.error_details.as_deref().is_some_and(|details| {
            details.contains("Failed to record replay replacement relations")
        }),
        "failure details should include replacement recording context: {:?}",
        failed.error_details
    );

    let replay_command = replay_output_handle
        .await
        .map_err(|e| eyre!("fake replacement-record replay output task failed: {e}"))??;

    let live_target_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(target_id)
            .fetch_one(&ctx.pool)
            .await?;
    let archived_target_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(target_id)
    .fetch_one(&ctx.pool)
    .await?;
    let live_replacement_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM core.events WHERE created_by_operation_id = $1::uuid",
    )
    .bind(replay_command.operation_id)
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        live_target_count, 0,
        "replacement-record failure occurs after the original event has already been archived"
    );
    assert_eq!(
        archived_target_count, 1,
        "replacement-record failure must leave the archived target in audit storage"
    );
    assert_eq!(
        live_replacement_count, 1,
        "replacement-record failure must not delete already-emitted replay outputs"
    );

    let replacements = ctx
        .pool
        .events()
        .get_replacements_by_operation(planned.operation_id)
        .await?;
    assert!(
        replacements.is_empty(),
        "failed replacement recording must not partially insert lineage rows"
    );

    scan_handle
        .await
        .map_err(|e| eyre!("fake replacement-record-fail-test node task failed: {e}"))?;

    Ok(())
}

#[sinex_test]
async fn replay_execution_restores_archived_cascade_when_dispatch_fails_before_ack(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let material_id = ctx
        .create_source_material(Some("replay-pre-ack-failure"))
        .await?;
    let event = DynamicPayload::new(
        "pre-ack-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-pre-ack-failure.txt" }),
    )
    .from_material(material_id)
    .build()?;
    let inserted = ctx.pool.events().insert(event).await?;
    let target_id = inserted
        .id
        .expect("inserted replay target must have id")
        .to_uuid();
    let target_ts = inserted
        .id
        .expect("inserted replay target must have id")
        .timestamp();

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let env = sinex_primitives::environment::environment();

    let executor = ReplayExecutionEngine::new(replay.clone(), nats_client.clone())
        .with_scan_ack_timeout(Duration::from_millis(100));
    ReplayTelemetry::new(replay.clone()).spawn();
    let health = Arc::new(Mutex::new(ReplayControlHealthState::default()));
    ReplayControlServer::new(
        &env,
        nats_client.clone(),
        replay.clone(),
        executor,
        Arc::clone(&health),
    )
    .spawn()
    .await?;
    let client = ReplayControlClient::new(&env, nats_client, Duration::from_secs(30), health);

    let mut scope = sample_scope();
    scope.node_id = "pre-ack-test".to_string();
    scope.time_window = Some((
        target_ts - time::Duration::milliseconds(1),
        target_ts + time::Duration::milliseconds(1),
    ));

    let planned = client.plan("test:replay-user".into(), scope).await?;
    let (previewed, _) = client.preview(planned.operation_id).await?;
    let approved = client
        .approve(previewed.operation_id, "admin:approver".into())
        .await?;
    let err = client
        .execute(approved.operation_id, "service:executor-node".into(), false)
        .await
        .expect_err("execute should fail before scan ack when no node responder exists");
    assert!(
        err.to_string().contains("restored archived cascade"),
        "pre-ack dispatch failures must explain that the archived cascade was restored: {err}"
    );

    let operation = replay.load_operation(approved.operation_id).await?;
    assert_eq!(operation.state, ReplayState::Failed);
    assert_eq!(operation.checkpoint.processed_events, 0);

    let live_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(target_id)
            .fetch_one(&ctx.pool)
            .await?;
    let archived_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(target_id)
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        live_count, 1,
        "pre-ack dispatch failures must restore the live row"
    );
    assert_eq!(
        archived_count, 0,
        "pre-ack dispatch failures must not leave the archived cascade behind"
    );

    Ok(())
}

#[sinex_test]
async fn replay_execution_fails_before_archive_when_scope_metadata_collection_fails(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let material_id = ctx
        .create_source_material(Some("replay-scope-metadata-failure"))
        .await?;
    let event = DynamicPayload::new(
        "scope-metadata-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-scope-metadata-failure.txt" }),
    )
    .from_material(material_id)
    .build()?;
    let inserted = ctx.pool.events().insert(event).await?;
    let target_id = inserted
        .id
        .expect("inserted replay target must have id")
        .to_uuid();
    let target_ts = inserted
        .id
        .expect("inserted replay target must have id")
        .timestamp();

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let mut scope = sample_scope();
    scope.node_id = "scope-metadata-test".to_string();
    scope.time_window = Some((
        target_ts - time::Duration::milliseconds(1),
        target_ts + time::Duration::milliseconds(1),
    ));

    let planned = replay
        .create_operation(scope.clone(), "test:scope-metadata-fail".into())
        .await?;
    let preview = replay.generate_preview_summary(&scope).await?;
    replay.update_preview(planned.operation_id, preview).await?;
    replay
        .approve(planned.operation_id, "admin:approver".into())
        .await?;

    let executor = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client())
        .with_scope_metadata_failures(Arc::new(AtomicUsize::new(1)));
    let err = executor
        .execute(planned.operation_id, "service:executor-node".into())
        .await
        .expect_err("scope metadata collection failure should abort replay execution");
    assert!(
        err.chain().any(|cause| {
            cause
                .to_string()
                .contains("Failed to collect replay cascade scope metadata")
        }),
        "unexpected error: {err}"
    );

    let failed = replay.load_operation(planned.operation_id).await?;
    assert_eq!(failed.state, ReplayState::Failed);
    assert_eq!(
        failed.outcome,
        Some(sinex_primitives::domain::ReplayOutcome::Failed)
    );
    assert!(
        failed.error_details.as_deref().is_some_and(
            |details| details.contains("Failed to collect replay cascade scope metadata")
        ),
        "failure details should include scope metadata context: {:?}",
        failed.error_details
    );

    let live_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(target_id)
            .fetch_one(&ctx.pool)
            .await?;
    let archived_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(target_id)
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        live_count, 1,
        "scope metadata failure must leave the live row untouched"
    );
    assert_eq!(
        archived_count, 0,
        "scope metadata failure must abort before archiving the cascade"
    );

    Ok(())
}

#[sinex_test]
async fn replay_execution_restores_cascade_when_initial_scope_invalidation_publish_fails(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let material_id = ctx
        .create_source_material(Some("replay-scope-invalidation-publish-failure"))
        .await?;
    let mut event = DynamicPayload::new(
        "scope-invalidation-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-scope-invalidation-publish-failure.txt" }),
    )
    .from_material(material_id)
    .build()?;
    event.scope_key = Some("scope://scope-invalidation-test/replay".to_string());
    let inserted = ctx.pool.events().insert(event).await?;
    let target_id = inserted
        .id
        .expect("inserted replay target must have id")
        .to_uuid();
    let target_ts = inserted
        .id
        .expect("inserted replay target must have id")
        .timestamp();

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let mut scope = sample_scope();
    scope.node_id = "scope-invalidation-test".to_string();
    scope.time_window = Some((
        target_ts - time::Duration::milliseconds(1),
        target_ts + time::Duration::milliseconds(1),
    ));

    let planned = replay
        .create_operation(scope.clone(), "test:scope-invalidation-fail".into())
        .await?;
    let preview = replay.generate_preview_summary(&scope).await?;
    replay.update_preview(planned.operation_id, preview).await?;
    replay
        .approve(planned.operation_id, "admin:approver".into())
        .await?;

    let mut invalidation_rx =
        spawn_invalidation_listener_for_test(&ctx.nats_client()).await?;

    let executor = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client())
        .with_scope_invalidation_publish_failures(Arc::new(AtomicUsize::new(1)));
    let err = executor
        .execute(planned.operation_id, "service:executor-node".into())
        .await
        .expect_err("scope invalidation publish failure should abort replay execution");
    assert!(
        err.chain().any(|cause| {
            cause
                .to_string()
                .contains("Failed to publish replay scope invalidations before dispatch")
        }),
        "unexpected error: {err}"
    );

    let failed = replay.load_operation(planned.operation_id).await?;
    assert_eq!(failed.state, ReplayState::Failed);
    assert_eq!(
        failed.outcome,
        Some(sinex_primitives::domain::ReplayOutcome::Failed)
    );
    assert!(
        failed.error_details.as_deref().is_some_and(|details| {
            details.contains("Failed to publish replay scope invalidations before dispatch")
        }),
        "failure details should include invalidation publish context: {:?}",
        failed.error_details
    );

    let live_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(target_id)
            .fetch_one(&ctx.pool)
            .await?;
    let archived_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(target_id)
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        live_count, 1,
        "scope invalidation publish failure must restore the live row"
    );
    assert_eq!(
        archived_count, 0,
        "scope invalidation publish failure must not leave archived rows behind"
    );

    let payload_bytes = tokio::time::timeout(Duration::from_secs(1), invalidation_rx.recv())
        .await?
        .expect("compensating invalidation should still publish after restore");
    let payload = String::from_utf8(payload_bytes)?;
    assert!(payload.contains("scope://scope-invalidation-test/replay"));
    assert!(payload.contains(&target_id.to_string()));

    Ok(())
}

#[sinex_test]
async fn replay_execute_rejects_zero_event_preview_before_execution(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let client =
        spawn_replay_control(replay.clone(), ctx.nats_client(), Duration::from_secs(30))
            .await?;

    let operation = replay
        .create_operation(sample_scope(), "test:planner".to_string())
        .await?;
    let now = Timestamp::now();
    replay
        .update_preview(
            operation.operation_id,
            json!({
                "total_events": 0,
                "time_window": {
                    "start": now.format_rfc3339(),
                    "end": (now + time::Duration::seconds(1)).format_rfc3339(),
                }
            }),
        )
        .await?;
    replay
        .approve(operation.operation_id, "admin:approver".to_string())
        .await?;

    let err = client
        .execute(
            operation.operation_id,
            "service:executor-node".into(),
            false,
        )
        .await
        .expect_err("zero-event previews must not enter execution");
    assert!(
        err.to_string().contains("preview matches zero events"),
        "unexpected error: {err}"
    );

    let stored = replay.load_operation(operation.operation_id).await?;
    assert_eq!(stored.state, ReplayState::Failed);
    assert_eq!(
        stored.outcome,
        Some(sinex_primitives::domain::ReplayOutcome::Failed)
    );
    assert_eq!(
        stored.error_details.as_deref(),
        Some(err.to_string().as_str())
    );
    assert!(stored.executor_node.is_none());

    Ok(())
}

#[sinex_test]
async fn replay_preview_rejects_refresh_after_approval(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let client =
        spawn_replay_control(replay.clone(), ctx.nats_client(), Duration::from_secs(30))
            .await?;

    let planned = client.plan("test:planner".into(), sample_scope()).await?;
    let (previewed, _) = client.preview(planned.operation_id).await?;
    let approved = client
        .approve(previewed.operation_id, "admin:approver".into())
        .await?;

    let err = client
        .preview(approved.operation_id)
        .await
        .expect_err("approved operations must not accept preview refreshes");
    assert!(
        err.to_string().contains("already approved"),
        "unexpected error: {err}"
    );

    let stored = replay.load_operation(approved.operation_id).await?;
    assert_eq!(stored.state, ReplayState::Approved);
    Ok(())
}

#[sinex_test]
async fn replay_execute_dry_run_is_rejected_without_state_changes(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let client =
        spawn_replay_control(replay.clone(), ctx.nats_client(), Duration::from_secs(30))
            .await?;

    let planned = client.plan("test:planner".into(), sample_scope()).await?;
    let (previewed, _) = client.preview(planned.operation_id).await?;
    let approved = client
        .approve(previewed.operation_id, "admin:approver".into())
        .await?;

    let err = client
        .execute(approved.operation_id, "service:executor-node".into(), true)
        .await
        .expect_err("dry-run execute should redirect callers back to preview");
    assert!(
        err.to_string()
            .contains("does not support dry-run semantics"),
        "unexpected error: {err}"
    );

    let stored = replay.load_operation(approved.operation_id).await?;
    assert_eq!(stored.state, ReplayState::Approved);
    assert!(stored.finished_at.is_none());
    Ok(())
}

#[sinex_test]
async fn replay_execute_fails_when_live_scope_disappears_after_approval(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let material_id = ctx
        .create_source_material(Some("replay-scope-disappeared"))
        .await?;
    let event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-scope-disappeared.txt" }),
    )
    .from_material(material_id)
    .build()?;
    let inserted = ctx.pool.events().insert(event).await?;
    let target_event_id = inserted.id.expect("inserted replay target must have id");
    let target_id = target_event_id.to_uuid();
    let target_ts = target_event_id.timestamp();

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let client =
        spawn_replay_control(replay.clone(), ctx.nats_client(), Duration::from_secs(30))
            .await?;

    let mut scope = sample_scope();
    scope.time_window = Some((
        target_ts - time::Duration::milliseconds(1),
        target_ts + time::Duration::milliseconds(1),
    ));
    scope.material_filter = Some(vec![*material_id.as_uuid()]);
    scope.filters.insert(
        "event_types".to_string(),
        json!([FileCreatedPayload::EVENT_TYPE.as_static_str()]),
    );

    let planned = client.plan("test:replay-user".into(), scope).await?;
    let (previewed, preview) = client.preview(planned.operation_id).await?;
    assert_eq!(
        preview
            .get("total_events")
            .and_then(serde_json::Value::as_i64),
        Some(1)
    );
    let approved = client
        .approve(previewed.operation_id, "admin:approver".into())
        .await?;

    ctx.pool
        .events()
        .execute_cascade_archive(
            &[target_id],
            "archive replay target before execution",
            &Uuid::now_v7().to_string(),
            "test:archive-before-replay",
        )
        .await?;

    let err = client
        .execute(approved.operation_id, "service:executor-node".into(), false)
        .await
        .expect_err("execution should fail once the approved live scope has vanished");
    assert!(
        err.to_string().contains("matched zero live events"),
        "unexpected error: {err}"
    );

    let failed = replay.load_operation(approved.operation_id).await?;
    assert_eq!(failed.state, ReplayState::Failed);
    assert_eq!(
        failed.outcome,
        Some(sinex_primitives::domain::ReplayOutcome::Failed)
    );

    Ok(())
}

#[sinex_test]
async fn replay_execute_fails_when_live_scope_drifts_after_approval(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;

    let first_material = ctx
        .create_source_material(Some("replay-scope-drift-first"))
        .await?;
    let second_material = ctx
        .create_source_material(Some("replay-scope-drift-second"))
        .await?;

    let first = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-scope-drift-first.txt" }),
    )
    .from_material(first_material)
    .build()?;
    let second = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-scope-drift-second.txt" }),
    )
    .from_material(second_material)
    .build()?;

    let inserted_first = ctx.pool.events().insert(first).await?;
    let inserted_second = ctx.pool.events().insert(second).await?;
    let first_event_id = inserted_first.id.expect("first replay target must have id");
    let second_event_id = inserted_second
        .id
        .expect("second replay target must have id");
    let first_ts = first_event_id.timestamp();
    let second_ts = second_event_id.timestamp();

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let client =
        spawn_replay_control(replay.clone(), ctx.nats_client(), Duration::from_secs(30))
            .await?;

    let mut scope = sample_scope();
    scope.time_window = Some((
        std::cmp::min(first_ts, second_ts) - time::Duration::milliseconds(1),
        std::cmp::max(first_ts, second_ts) + time::Duration::milliseconds(1),
    ));
    scope.filters.insert(
        "event_types".to_string(),
        json!([FileCreatedPayload::EVENT_TYPE.as_static_str()]),
    );

    let planned = client.plan("test:replay-user".into(), scope).await?;
    let (previewed, preview) = client.preview(planned.operation_id).await?;
    assert_eq!(
        preview
            .get("total_events")
            .and_then(serde_json::Value::as_i64),
        Some(2)
    );
    let approved = client
        .approve(previewed.operation_id, "admin:approver".into())
        .await?;

    ctx.pool
        .events()
        .execute_cascade_archive(
            &[first_event_id.to_uuid()],
            "archive one replay target before execution",
            &Uuid::now_v7().to_string(),
            "test:archive-before-replay",
        )
        .await?;

    let err = client
        .execute(approved.operation_id, "service:executor-node".into(), false)
        .await
        .expect_err("execution should fail once the approved live scope drifts");
    assert!(
        err.to_string().contains("preview is stale"),
        "unexpected error: {err}"
    );
    assert!(
        err.to_string()
            .contains(&second_event_id.to_uuid().to_string())
            || err
                .to_string()
                .contains(&first_event_id.to_uuid().to_string()),
        "drift error should expose the changed root set: {err}"
    );

    let failed = replay.load_operation(approved.operation_id).await?;
    assert_eq!(failed.state, ReplayState::Failed);
    assert_eq!(
        failed.outcome,
        Some(sinex_primitives::domain::ReplayOutcome::Failed)
    );

    Ok(())
}

#[sinex_test]
async fn replay_abort_before_scan_ack_restores_cascade_and_emits_compensating_invalidation(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let engine = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client());

    let material_id = ctx
        .create_source_material(Some("replay-compensating-invalidation"))
        .await?;
    let mut event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-compensating-invalidation.txt" }),
    )
    .from_material(material_id)
    .build()?;
    event.scope_key = Some("scope://fs-test/replay-compensating-invalidation".to_string());
    let inserted = ctx.pool.events().insert(event).await?;
    let event_id = inserted.id.expect("inserted replay target must have id");
    let operation_id = Uuid::now_v7();

    let scope_metadata = engine
        .collect_cascade_scope_metadata(&ctx.pool, &[event_id.to_uuid()])
        .await?;
    assert_eq!(scope_metadata.len(), 1);
    assert_eq!(scope_metadata[0].event_source, "fs-test");
    assert_eq!(
        scope_metadata[0].event_type,
        FileCreatedPayload::EVENT_TYPE.as_static_str()
    );
    assert!(!scope_metadata[0].has_lineage);
    assert_eq!(scope_metadata[0].event_ids, vec![event_id.to_uuid()]);

    ctx.pool
        .events()
        .execute_cascade_archive(
            &[event_id.to_uuid()],
            "archive before compensating restore test",
            &operation_id.to_string(),
            "test:replay-compensating",
        )
        .await?;

    let mut invalidation_rx =
        spawn_invalidation_listener_for_test(&ctx.nats_client()).await?;

    let err = engine
        .abort_before_scan_ack(
            &ctx.pool,
            &[event_id.to_uuid()],
            &scope_metadata,
            operation_id,
            eyre!("boom"),
        )
        .await
        .expect_err("abort helper should surface the caller failure");
    assert!(
        err.to_string()
            .contains("published compensating scope invalidations"),
        "unexpected error: {err}"
    );

    let live_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(event_id.to_uuid())
            .fetch_one(&ctx.pool)
            .await?;
    assert_eq!(
        live_count, 1,
        "aborted replay should restore the archived event"
    );

    let payload_bytes = tokio::time::timeout(Duration::from_secs(1), invalidation_rx.recv())
        .await?
        .expect("compensating invalidation should be published");
    let payload = String::from_utf8(payload_bytes)?;
    assert!(payload.contains("scope://fs-test/replay-compensating-invalidation"));
    assert!(payload.contains(&event_id.to_string()));

    Ok(())
}

#[sinex_test]
async fn replay_abort_before_scan_ack_surfaces_compensating_invalidation_failure(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let engine = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client())
        .with_scope_invalidation_publish_failures(Arc::new(AtomicUsize::new(1)));

    let material_id = ctx
        .create_source_material(Some("replay-compensating-invalidation-failure"))
        .await?;
    let mut event = DynamicPayload::new(
        "fs-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-compensating-invalidation-failure.txt" }),
    )
    .from_material(material_id)
    .build()?;
    event.scope_key =
        Some("scope://fs-test/replay-compensating-invalidation-failure".to_string());
    let inserted = ctx.pool.events().insert(event).await?;
    let event_id = inserted.id.expect("inserted replay target must have id");
    let operation_id = Uuid::now_v7();

    let scope_metadata = engine
        .collect_cascade_scope_metadata(&ctx.pool, &[event_id.to_uuid()])
        .await?;
    assert_eq!(scope_metadata.len(), 1);

    ctx.pool
        .events()
        .execute_cascade_archive(
            &[event_id.to_uuid()],
            "archive before compensating restore failure test",
            &operation_id.to_string(),
            "test:replay-compensating-failure",
        )
        .await?;

    let err = engine
        .abort_before_scan_ack(
            &ctx.pool,
            &[event_id.to_uuid()],
            &scope_metadata,
            operation_id,
            eyre!("boom"),
        )
        .await
        .expect_err("compensating invalidation publish failure should surface");
    assert!(
        err.to_string()
            .contains("failed to publish compensating scope invalidations"),
        "unexpected error: {err}"
    );

    let live_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(event_id.to_uuid())
            .fetch_one(&ctx.pool)
            .await?;
    let archived_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(event_id.to_uuid())
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        live_count, 1,
        "aborted replay should still restore the archived event"
    );
    assert_eq!(
        archived_count, 0,
        "aborted replay should not leave the archived event behind"
    );

    Ok(())
}

#[sinex_test]
async fn replay_execution_returns_cancelled_operation_when_cancelled_midflight(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let nats_url = ctx.nats_handle()?.client_url().to_string();

    let material_id = ctx
        .create_source_material(Some("replay-cancel-midflight"))
        .await?;
    let event = DynamicPayload::new(
        "cancel-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-cancel.txt" }),
    )
    .from_material(material_id)
    .build()?;
    let inserted = ctx.pool.events().insert(event).await?;
    let target_id = inserted
        .id
        .expect("inserted replay target must have id")
        .to_uuid();
    let target_ts = inserted
        .id
        .expect("inserted replay target must have id")
        .timestamp();

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let env = sinex_primitives::environment::environment();
    let (_scan_command_rx, scan_handle) =
        spawn_fake_scan_node_ack_only(nats_client.clone(), env.clone(), "cancel-test").await?;

    let executor = ReplayExecutionEngine::new(replay.clone(), nats_client.clone())
        .with_scan_completion_timeout(Duration::from_secs(5));
    let health = Arc::new(Mutex::new(ReplayControlHealthState::default()));
    ReplayControlServer::new(
        &env,
        nats_client.clone(),
        replay.clone(),
        executor,
        Arc::clone(&health),
    )
    .spawn()
    .await?;

    let execute_client = ReplayControlClient::new(
        &env,
        async_nats::connect(&nats_url).await?,
        Duration::from_secs(30),
        Arc::clone(&health),
    );
    let control_client = ReplayControlClient::new(
        &env,
        async_nats::connect(&nats_url).await?,
        Duration::from_secs(30),
        health,
    );

    let mut scope = sample_scope();
    scope.node_id = "cancel-test".to_string();
    scope.time_window = Some((
        target_ts - time::Duration::milliseconds(1),
        target_ts + time::Duration::milliseconds(1),
    ));

    let planned = control_client
        .plan("test:replay-user".into(), scope)
        .await?;
    let (previewed, _) = control_client.preview(planned.operation_id).await?;
    let approved = control_client
        .approve(previewed.operation_id, "admin:approver".into())
        .await?;

    let operation_id = approved.operation_id;
    let execute_task = tokio::spawn(async move {
        execute_client
            .execute(operation_id, "service:executor-node".into(), false)
            .await
    });

    let mut saw_executing = false;
    for _ in 0..40 {
        let operation = replay.load_operation(operation_id).await?;
        if operation.state == ReplayState::Executing {
            saw_executing = true;
            break;
        }
        sleep(Duration::from_millis(25)).await;
    }
    assert!(
        saw_executing,
        "replay operation should enter Executing before cancellation"
    );

    let cancellation_requested = control_client
        .cancel(
            operation_id,
            "admin:approver".into(),
            Some("operator requested stop".to_string()),
        )
        .await?;
    assert_eq!(cancellation_requested.state, ReplayState::Cancelling);
    assert!(cancellation_requested.outcome.is_none());
    assert_eq!(
        cancellation_requested.error_details.as_deref(),
        Some("operator requested stop")
    );
    assert!(cancellation_requested.finished_at.is_none());

    let executed = execute_task
        .await
        .map_err(|e| eyre!("execute task failed: {e}"))??;
    assert_eq!(executed.state, ReplayState::Cancelled);
    assert_eq!(
        executed.outcome,
        Some(sinex_primitives::domain::ReplayOutcome::Cancelled)
    );
    assert_eq!(
        executed.error_details.as_deref(),
        Some("operator requested stop")
    );

    let live_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(target_id)
            .fetch_one(&ctx.pool)
            .await?;
    let archived_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(target_id)
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        live_count, 1,
        "cancelled replay should restore live rows when no replacement events were emitted"
    );
    assert_eq!(
        archived_count, 0,
        "cancelled replay should not leave archived rows behind when execution never emitted replacements"
    );

    scan_handle
        .await
        .map_err(|e| eyre!("fake cancel-test node task failed: {e}"))?;

    Ok(())
}

#[sinex_test]
async fn replay_execution_surfaces_operation_state_corruption_after_failure(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let nats_url = ctx.nats_handle()?.client_url().to_string();

    let material_id = ctx
        .create_source_material(Some("replay-corrupt-failure"))
        .await?;
    let event = DynamicPayload::new(
        "corrupt-failure-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-corrupt-failure.txt" }),
    )
    .from_material(material_id)
    .build()?;
    ctx.pool.events().insert(event).await?;

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let env = sinex_primitives::environment::environment();
    let (_scan_command_rx, scan_handle) =
        spawn_fake_scan_node_ack_only(nats_client.clone(), env.clone(), "corrupt-failure-test")
            .await?;

    let executor = ReplayExecutionEngine::new(replay.clone(), nats_client.clone())
        .with_scan_completion_timeout(Duration::from_millis(200));
    let health = Arc::new(Mutex::new(ReplayControlHealthState::default()));
    ReplayControlServer::new(
        &env,
        nats_client.clone(),
        replay.clone(),
        executor,
        Arc::clone(&health),
    )
    .spawn()
    .await?;

    let control_client = ReplayControlClient::new(
        &env,
        async_nats::connect(&nats_url).await?,
        Duration::from_secs(30),
        Arc::clone(&health),
    );
    let execute_client = ReplayControlClient::new(
        &env,
        async_nats::connect(&nats_url).await?,
        Duration::from_secs(30),
        health,
    );

    let mut scope = sample_scope();
    scope.node_id = "corrupt-failure-test".to_string();

    let planned = control_client
        .plan("test:replay-user".into(), scope)
        .await?;
    let (previewed, _) = control_client.preview(planned.operation_id).await?;
    let approved = control_client
        .approve(previewed.operation_id, "admin:approver".into())
        .await?;

    let operation_id = approved.operation_id;
    let execute_task = tokio::spawn(async move {
        execute_client
            .execute(operation_id, "service:executor-node".into(), false)
            .await
    });

    wait_for_operation_state(&replay, operation_id, ReplayState::Executing).await?;
    corrupt_operation_preview_summary(&ctx.pool, operation_id).await?;

    let err = execute_task
        .await
        .map_err(|e| eyre!("execute task failed: {e}"))?
        .expect_err("corrupt replay metadata should surface as execution failure");
    assert!(
        err.to_string()
            .contains("failed to finalize replay execution bookkeeping"),
        "unexpected error: {err:#}"
    );
    assert!(
        err.to_string()
            .contains("failed to inspect replay operation state after execution"),
        "unexpected error: {err:#}"
    );

    scan_handle
        .await
        .map_err(|e| eyre!("fake corrupt-failure-test node task failed: {e}"))?;

    Ok(())
}

#[sinex_test]
async fn replay_execution_surfaces_cancellation_bookkeeping_corruption(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let nats_url = ctx.nats_handle()?.client_url().to_string();

    let material_id = ctx
        .create_source_material(Some("replay-corrupt-cancel"))
        .await?;
    let event = DynamicPayload::new(
        "corrupt-cancel-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/replay-corrupt-cancel.txt" }),
    )
    .from_material(material_id)
    .build()?;
    let inserted = ctx.pool.events().insert(event).await?;
    let target_ts = inserted
        .id
        .expect("inserted replay target must have id")
        .timestamp();

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let env = sinex_primitives::environment::environment();
    let (_scan_command_rx, scan_handle) =
        spawn_fake_scan_node_ack_only(nats_client.clone(), env.clone(), "corrupt-cancel-test")
            .await?;

    let executor = ReplayExecutionEngine::new(replay.clone(), nats_client.clone())
        .with_scan_completion_timeout(Duration::from_secs(5));
    let health = Arc::new(Mutex::new(ReplayControlHealthState::default()));
    ReplayControlServer::new(
        &env,
        nats_client.clone(),
        replay.clone(),
        executor,
        Arc::clone(&health),
    )
    .spawn()
    .await?;

    let execute_client = ReplayControlClient::new(
        &env,
        async_nats::connect(&nats_url).await?,
        Duration::from_secs(30),
        Arc::clone(&health),
    );
    let control_client = ReplayControlClient::new(
        &env,
        async_nats::connect(&nats_url).await?,
        Duration::from_secs(30),
        health,
    );

    let mut scope = sample_scope();
    scope.node_id = "corrupt-cancel-test".to_string();
    scope.time_window = Some((
        target_ts - time::Duration::milliseconds(1),
        target_ts + time::Duration::milliseconds(1),
    ));

    let planned = control_client
        .plan("test:replay-user".into(), scope)
        .await?;
    let (previewed, _) = control_client.preview(planned.operation_id).await?;
    let approved = control_client
        .approve(previewed.operation_id, "admin:approver".into())
        .await?;

    let operation_id = approved.operation_id;
    let execute_task = tokio::spawn(async move {
        execute_client
            .execute(operation_id, "service:executor-node".into(), false)
            .await
    });

    wait_for_operation_state(&replay, operation_id, ReplayState::Executing).await?;

    let cancellation_requested = control_client
        .cancel(
            operation_id,
            "admin:approver".into(),
            Some("operator requested stop".to_string()),
        )
        .await?;
    assert_eq!(cancellation_requested.state, ReplayState::Cancelling);

    corrupt_operation_preview_summary(&ctx.pool, operation_id).await?;

    let err = execute_task
        .await
        .map_err(|e| eyre!("execute task failed: {e}"))?
        .expect_err(
            "corrupt replay metadata should surface as cancellation bookkeeping failure",
        );
    assert!(
        err.to_string()
            .contains("failed to finalize replay execution bookkeeping"),
        "unexpected error: {err:#}"
    );
    assert!(
        err.to_string()
            .contains("failed to inspect replay operation state after execution"),
        "unexpected error: {err:#}"
    );

    scan_handle
        .await
        .map_err(|e| eyre!("fake corrupt-cancel-test node task failed: {e}"))?;

    Ok(())
}


#[sinex_test]
async fn replay_list_rejects_missing_operations_payload(_ctx: TestContext) -> Result<()> {
    let err = ReplayControlClient::require_operations(ReplayControlResponse::success(
        None, None, None,
    ))
    .expect_err("list responses without operations must be rejected");
    assert!(
        err.to_string()
            .contains("Replay control response missing operations")
    );
    Ok(())
}

#[sinex_test]
async fn plan_rejects_invalid_actor(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let client = spawn_replay_control(replay, nats_client, Duration::from_secs(30)).await?;

    let scope = sample_scope();
    let result = client.plan("invalid-actor".into(), scope).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Invalid actor"));
    Ok(())
}

#[sinex_test]
async fn plan_rejects_inverted_time_window(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let client = spawn_replay_control(replay, nats_client, Duration::from_secs(30)).await?;

    let end = Timestamp::now();
    let start = end + time::Duration::hours(1);
    let mut scope = sample_scope();
    scope.time_window = Some((start, end));

    let result = client.plan("test:replay-user".into(), scope).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("invalid replay time_window")
    );
    Ok(())
}
