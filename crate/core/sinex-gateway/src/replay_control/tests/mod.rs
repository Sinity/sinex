use super::*;
use super::execution::{ExpectedReplayOutputs, ReplayExecutionEngine};
use super::server::ReplayControlServer;
use super::validation::run_safety_analysis;
use async_nats::Client;
use color_eyre::eyre::eyre;
use futures::StreamExt;
use sinex_db::replay::state_machine::ReplayState;
use sinex_node_sdk::derived_node::invalidation::INVALIDATION_SUBJECT;
use sinex_node_sdk::runtime::stream::{
    Checkpoint, NodeScanAck, NodeScanCommand, NodeScanProgress, ResolvedReplayMaterial,
};
use sinex_primitives::environment::{SinexEnvironment, environment};
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
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


mod execution_outcome;
mod execution_failures;
mod abort;
mod bookkeeping;
