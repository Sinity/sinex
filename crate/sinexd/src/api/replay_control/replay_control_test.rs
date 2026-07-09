use super::execution::{ExpectedReplayOutputs, ReplayExecutionEngine};
use super::server::ReplayControlServer;
use super::validation::run_safety_analysis;
use super::*;
use crate::runtime::automaton::invalidation::INVALIDATION_SUBJECT;
use crate::runtime::stream::ScanReport;
use crate::runtime::stream::{
    Checkpoint, ResolvedReplayMaterial, SourceScanAck, SourceScanCommand, SourceScanProgress,
};
use async_nats::Client;
use futures::StreamExt;
use serde_json::json;
use sinex_db::DbPool;
use sinex_db::replay::state_machine::ReplayState;
use sinex_db::repositories::DbPoolExt;
use sinex_db::repositories::state::Operation;
use sinex_primitives::environment::{SinexEnvironment, environment};
use sinex_primitives::events::{EventPayload, payloads::filesystem::FileCreatedPayload};
use sinex_primitives::{DynamicPayload, Id, SinexError, Uuid};
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use tokio::time::sleep;
use xtask::sandbox::sinex_test;

fn test_error(message: impl std::fmt::Display) -> SinexError {
    SinexError::service(message)
}

fn error_contains(error: &SinexError, needle: &str) -> bool {
    error.to_string().contains(needle)
}

/// Subscribe to scope-invalidation messages in tests.
///
/// `js.publish` requires the target subject to be covered by an existing
/// `JetStream` stream, and the production stream bootstrap (in
/// `sinexd::event_engine::jetstream_consumer::bootstrap_streams`) does not run
/// in test contexts that use ephemeral NATS. This helper:
///
/// 1. `get_or_create_stream`s the canonical
///    `SINEX_RAW_EVENTS_DERIVED_INVALIDATIONS` stream so publishes succeed.
/// 2. Creates an ephemeral push consumer and forwards each delivered
///    payload onto an `mpsc::UnboundedReceiver<Result<Vec<u8>>>` so stream
///    delivery and ack failures propagate instead of collapsing into a closed
///    channel or timeout.
async fn spawn_invalidation_listener_for_test(
    nats_client: &async_nats::Client,
) -> Result<tokio::sync::mpsc::UnboundedReceiver<Result<Vec<u8>>>> {
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
        .map_err(|e| test_error(format!("failed to bootstrap invalidation stream: {e}")))?;
    let deliver_subject = nats_client.new_inbox();
    let consumer = stream
        .create_consumer(push::Config {
            deliver_subject,
            ..Default::default()
        })
        .await
        .map_err(|e| test_error(format!("failed to create invalidation consumer: {e}")))?;
    let mut messages = consumer
        .messages()
        .await
        .map_err(|e| test_error(format!("failed to start invalidation message stream: {e}")))?;

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(item) = messages.next().await {
            let msg = match item {
                Ok(msg) => msg,
                Err(error) => {
                    let _ = tx.send(Err(test_error(format!(
                        "invalidation listener message stream failed: {error}"
                    ))));
                    break;
                }
            };
            let payload = msg.payload.to_vec();
            if let Err(error) = msg.ack().await {
                let _ = tx.send(Err(test_error(format!(
                    "invalidation listener failed to ack message: {error}"
                ))));
                break;
            }
            if tx.send(Ok(payload)).is_err() {
                break;
            }
        }
    });
    Ok(rx)
}

fn sample_scope() -> ReplayScope {
    ReplayScope {
        source_name: "fs-test".to_string(),
        time_window: None,
        material_filter: None,
        filters: HashMap::new(),
        ..Default::default()
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
    Err(test_error(format!(
        "operation record {operation_id} not found after waiting for repository persistence"
    )))
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
    Err(test_error(format!(
        "operation {operation_id} did not reach state {target:?} before timeout"
    )))
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

async fn spawn_fake_scan_source_runtime(
    nats: Client,
    env: SinexEnvironment,
    module_name: &str,
    events_processed: u64,
) -> Result<(
    tokio::sync::oneshot::Receiver<SourceScanCommand>,
    tokio::task::JoinHandle<Result<()>>,
)> {
    let module_name = module_name.to_string();
    let subject = env.nats_subject(&format!("sinex.control.sources.{module_name}.scan"));
    let mut sub = nats.subscribe(subject).await.map_err(|e| {
        test_error(format!(
            "failed to subscribe fake source runtime dispatcher: {e}"
        ))
    })?;
    let (command_tx, command_rx) = tokio::sync::oneshot::channel();

    let handle = tokio::spawn(async move {
        let Some(msg) = sub.next().await else {
            return Err(test_error(format!(
                "fake {module_name} source runtime dispatcher ended before receiving a scan command"
            )));
        };

        let command: SourceScanCommand = serde_json::from_slice(&msg.payload).map_err(|error| {
            test_error(format!(
                "fake {module_name} source runtime received an invalid scan command: {error}"
            ))
        })?;
        let operation_id = command.operation_id;
        let progress_subject =
            env.nats_subject(&format!("sinex.control.replay.progress.{operation_id}"));

        command_tx.send(command.clone()).map_err(|_| {
            test_error(format!(
                "fake {module_name} source runtime could not hand scan command to test harness"
            ))
        })?;

        if let Some(reply) = msg.reply {
            let ack = SourceScanAck {
                operation_id,
                module_name: module_name.clone(),
                accepted: true,
                error: None,
            };
            let payload = serde_json::to_vec(&ack).map_err(|error| {
                test_error(format!(
                    "fake {module_name} source runtime could not encode ack: {error}"
                ))
            })?;
            nats.publish(reply, payload.into()).await.map_err(|error| {
                test_error(format!(
                    "fake {module_name} source runtime could not publish ack: {error}"
                ))
            })?;
        }

        let report = ScanReport {
            events_processed,
            duration: Duration::from_millis(5),
            final_checkpoint: Checkpoint::None,
            time_range: None,
            runtime_stats: HashMap::from([("events_emitted".to_string(), events_processed)]),
            successful_targets: vec![module_name.clone()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        };
        let progress = SourceScanProgress {
            operation_id,
            module_name: module_name.clone(),
            events_processed,
            events_emitted: events_processed,
            final_report: Some(report),
            error: None,
        };
        let payload = serde_json::to_vec(&progress).map_err(|error| {
            test_error(format!(
                "fake {module_name} source runtime could not encode progress: {error}"
            ))
        })?;
        nats.publish(progress_subject, payload.into())
            .await
            .map_err(|error| {
                test_error(format!(
                    "fake {module_name} source runtime could not publish progress: {error}"
                ))
            })?;

        Ok(())
    });

    Ok((command_rx, handle))
}

async fn spawn_fake_scan_source_runtime_with_progress(
    nats: Client,
    env: SinexEnvironment,
    module_name: &str,
    events_processed: u64,
    events_emitted: u64,
) -> Result<(
    tokio::sync::oneshot::Receiver<SourceScanCommand>,
    tokio::task::JoinHandle<Result<()>>,
)> {
    let module_name = module_name.to_string();
    let subject = env.nats_subject(&format!("sinex.control.sources.{module_name}.scan"));
    let mut sub = nats.subscribe(subject).await.map_err(|e| {
        test_error(format!(
            "failed to subscribe fake source runtime dispatcher: {e}"
        ))
    })?;
    let (command_tx, command_rx) = tokio::sync::oneshot::channel();

    let handle = tokio::spawn(async move {
        let Some(msg) = sub.next().await else {
            return Err(test_error(format!(
                "fake {module_name} source runtime dispatcher ended before receiving a scan command"
            )));
        };

        let command: SourceScanCommand = serde_json::from_slice(&msg.payload).map_err(|error| {
            test_error(format!(
                "fake {module_name} source runtime received an invalid scan command: {error}"
            ))
        })?;
        let operation_id = command.operation_id;
        let progress_subject =
            env.nats_subject(&format!("sinex.control.replay.progress.{operation_id}"));

        command_tx.send(command.clone()).map_err(|_| {
            test_error(format!(
                "fake {module_name} source runtime could not hand scan command to test harness"
            ))
        })?;

        if let Some(reply) = msg.reply {
            let ack = SourceScanAck {
                operation_id,
                module_name: module_name.clone(),
                accepted: true,
                error: None,
            };
            let payload = serde_json::to_vec(&ack).map_err(|error| {
                test_error(format!(
                    "fake {module_name} source runtime could not encode ack: {error}"
                ))
            })?;
            nats.publish(reply, payload.into()).await.map_err(|error| {
                test_error(format!(
                    "fake {module_name} source runtime could not publish ack: {error}"
                ))
            })?;
        }

        let report = ScanReport {
            events_processed,
            duration: Duration::from_millis(5),
            final_checkpoint: Checkpoint::None,
            time_range: None,
            runtime_stats: HashMap::from([("events_emitted".to_string(), events_emitted)]),
            successful_targets: vec![module_name.clone()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        };
        let progress = SourceScanProgress {
            operation_id,
            module_name: module_name.clone(),
            events_processed,
            events_emitted,
            final_report: Some(report),
            error: None,
        };
        let payload = serde_json::to_vec(&progress).map_err(|error| {
            test_error(format!(
                "fake {module_name} source runtime could not encode progress: {error}"
            ))
        })?;
        nats.publish(progress_subject, payload.into())
            .await
            .map_err(|error| {
                test_error(format!(
                    "fake {module_name} source runtime could not publish progress: {error}"
                ))
            })?;

        Ok(())
    });

    Ok((command_rx, handle))
}

async fn spawn_fake_scan_source_runtime_ack_only(
    nats: Client,
    env: SinexEnvironment,
    module_name: &str,
) -> Result<(
    tokio::sync::oneshot::Receiver<SourceScanCommand>,
    tokio::task::JoinHandle<Result<()>>,
)> {
    let module_name = module_name.to_string();
    let subject = env.nats_subject(&format!("sinex.control.sources.{module_name}.scan"));
    let mut sub = nats.subscribe(subject).await.map_err(|e| {
        test_error(format!(
            "failed to subscribe fake source runtime dispatcher: {e}"
        ))
    })?;
    let (command_tx, command_rx) = tokio::sync::oneshot::channel();

    let handle = tokio::spawn(async move {
        let Some(msg) = sub.next().await else {
            return Err(test_error(format!(
                "fake {module_name} source runtime dispatcher ended before receiving a scan command"
            )));
        };

        let command: SourceScanCommand = serde_json::from_slice(&msg.payload).map_err(|error| {
            test_error(format!(
                "fake {module_name} source runtime received an invalid scan command: {error}"
            ))
        })?;
        command_tx.send(command.clone()).map_err(|_| {
            test_error(format!(
                "fake {module_name} source runtime could not hand scan command to test harness"
            ))
        })?;

        if let Some(reply) = msg.reply {
            let ack = SourceScanAck {
                operation_id: command.operation_id,
                module_name: module_name.clone(),
                accepted: true,
                error: None,
            };
            let payload = serde_json::to_vec(&ack).map_err(|error| {
                test_error(format!(
                    "fake {module_name} source runtime could not encode ack: {error}"
                ))
            })?;
            nats.publish(reply, payload.into()).await.map_err(|error| {
                test_error(format!(
                    "fake {module_name} source runtime could not publish ack: {error}"
                ))
            })?;
        }

        Ok(())
    });

    Ok((command_rx, handle))
}

async fn await_fake_scan_source_runtime(
    handle: tokio::task::JoinHandle<Result<()>>,
    module_name: &str,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .map_err(|_| {
            test_error(format!(
                "fake {module_name} source runtime task did not finish after receiving scan command"
            ))
        })?
        .map_err(|error| {
            test_error(format!(
                "fake {module_name} source runtime task panicked before reporting its result: {error}"
            ))
        })??;
    Ok(())
}

fn spawn_replay_output_inserter(
    pool: DbPool,
    command_rx: tokio::sync::oneshot::Receiver<SourceScanCommand>,
    source: &'static str,
    event_type: &'static str,
    path: &'static str,
) -> tokio::task::JoinHandle<Result<SourceScanCommand>> {
    tokio::spawn(async move {
        let command = command_rx
            .await
            .map_err(|_| test_error("fake replay output inserter did not receive scan command"))?;
        let material_id = command
            .args
            .replay
            .as_ref()
            .and_then(|replay| replay.materials.first())
            .map(|material| material.source_material_id)
            .ok_or_else(|| {
                test_error("fake replay output inserter requires a replay source material")
            })?;
        let mut event = DynamicPayload::new(source, event_type, json!({ "path": path }))
            .from_material(Id::from_uuid(material_id))
            .build()?;
        event.created_by_operation_id = Some(command.operation_id);
        pool.events().insert(event).await?;
        Ok(command)
    })
}

#[sinex_test]
async fn replay_output_expectations_deduplicate_logical_sources() -> Result<()> {
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
    Ok(())
}

#[sinex_test]
async fn replay_scan_control_source_prefers_material_runtime_identity() -> Result<()> {
    let scope = ReplayScope {
        source_name: "activitywatch".to_string(),
        ..ReplayScope::default()
    };
    let material_id = Uuid::now_v7();
    let replay_materials = vec![ResolvedReplayMaterial {
        source_material_id: material_id,
        material_kind: "sqlite".to_string(),
        source_identifier: format!("desktop.activitywatch#material={material_id}"),
        material_metadata: json!({ "logical_source_identifier": "desktop.activitywatch" }),
        material_start_time: None,
        material_end_time: None,
    }];

    let source = ReplayExecutionEngine::scan_control_source_name(&scope, &replay_materials)?;

    assert_eq!(source, "desktop.activitywatch");
    Ok(())
}

#[sinex_test]
async fn replay_scan_control_source_keeps_scope_without_runtime_identity() -> Result<()> {
    let scope = sample_scope();
    let material_id = Uuid::now_v7();
    let replay_materials = vec![ResolvedReplayMaterial {
        source_material_id: material_id,
        material_kind: "annex".to_string(),
        source_identifier: format!("synthetic-material#material={material_id}"),
        material_metadata: json!({}),
        material_start_time: None,
        material_end_time: None,
    }];

    let source = ReplayExecutionEngine::scan_control_source_name(&scope, &replay_materials)?;

    assert_eq!(source, "fs-test");
    Ok(())
}

#[sinex_test]
async fn telemetry_reports_state_counts(ctx: TestContext) -> Result<()> {
    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let telemetry = ReplayTelemetry::with_interval(replay.clone(), Duration::from_millis(5));
    let planning_scope = sample_scope();
    let mut executing_scope = sample_scope();
    executing_scope.source_name = "fs-test-executing".to_string();
    let mut failed_scope = sample_scope();
    failed_scope.source_name = "fs-test-failed".to_string();

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

    let scope = sample_scope();
    let err = client
        .plan_with_timeout("test:user".into(), scope, Duration::from_millis(25))
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
    for _ in 0..40 {
        let snapshot = client.health_snapshot();
        if !snapshot.connected && snapshot.last_error.is_some() {
            break;
        }
        sleep(Duration::from_millis(25)).await;
    }

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
async fn replay_client_honors_a_configured_timeout_longer_than_ten_seconds(
    ctx: TestContext,
) -> Result<()> {
    // Regression test for the bug this session found and fixed: `send()` used
    // to wrap `self.client.request(...)` (async-nats's plain, no-explicit-
    // timeout request form) in an OUTER `tokio::time::timeout`. Async-nats's
    // own internal default request timeout (10s) fired first regardless of
    // how long the outer wrapper's duration was, so any configured
    // `SINEX_REPLAY_CONTROL_TIMEOUT_SECS` value above 10s was silently
    // capped at ~10s. The fix uses `async_nats::Request::new().timeout(Some(_))`
    // to set the deadline on the request itself.
    //
    // With the broker gone, a pending request has no other signal except its
    // own configured deadline (matches `replay_client_errors_when_broker_disappears`'s
    // shutdown approach), so timing the failure precisely proves whether the
    // EFFECTIVE timeout matches the CONFIGURED one rather than being capped.
    let ctx = ctx.with_nats().dedicated().await?;
    let nats = ctx.nats_handle()?;

    let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
    let nats_client = ctx.nats_client();
    let client = spawn_replay_control(replay, nats_client, Duration::from_secs(30)).await?;

    nats.shutdown().await?;

    let configured_timeout = Duration::from_millis(10_500);
    let scope = sample_scope();
    let started = std::time::Instant::now();
    let err = client
        .plan_with_timeout("test:user".into(), scope, configured_timeout)
        .await
        .expect_err("plan should fail once the broker is gone");
    let elapsed = started.elapsed();

    assert!(
        error_contains(&err, "timed out"),
        "expected a timeout error, got: {err}"
    );
    assert!(
        elapsed >= Duration::from_millis(10_200),
        "request failed after {elapsed:?}, well under the configured {configured_timeout:?} \
         deadline -- the pre-fix bug capped every request at async-nats's internal 10s default \
         regardless of the configured value"
    );
    Ok(())
}

#[path = "tests/abort.rs"]
mod abort;
#[path = "tests/bookkeeping.rs"]
mod bookkeeping;
#[path = "tests/execution_failures.rs"]
mod execution_failures;
#[path = "tests/execution_outcome.rs"]
mod execution_outcome;
