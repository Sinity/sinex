//! Tests for `NodeRunner<T>` private control-plane and runtime helpers.
//! Inline because they cover items that are not exposed beyond the runner module.

// Inline because these cover private control-plane encoding helpers.
use super::*;
use crate::checkpoint::CheckpointManager;
use crate::{IngestorNode, IngestorNodeAdapter, NatsPublisher};
use async_nats::jetstream;
use serde::ser::Error as _;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::builder::EventId;
use tempfile::tempdir;
use tokio::sync::Notify;
use xtask::sandbox::prelude::*;

#[derive(Default)]
struct RuntimeTestNode;

#[derive(Default)]
struct FailingShutdownNode;

#[derive(Default)]
struct FailingBatchNode;

#[derive(Debug, Clone, PartialEq)]
struct RecordedScan {
    from: Checkpoint,
    until: &'static str,
}

struct StartupSequenceTestNode {
    checkpoint: std::sync::Arc<tokio::sync::Mutex<Checkpoint>>,
    scans: std::sync::Arc<tokio::sync::Mutex<Vec<RecordedScan>>>,
    snapshot_checkpoint: Checkpoint,
    capabilities: NodeCapabilities,
}

#[cfg(feature = "messaging")]
struct DrainTestIngestor {
    started: Arc<Notify>,
    drain_observed: Arc<Notify>,
    release_exit: Arc<Notify>,
    final_checkpoint: Checkpoint,
}

#[cfg(feature = "messaging")]
impl Default for DrainTestIngestor {
    fn default() -> Self {
        Self {
            started: Arc::new(Notify::new()),
            drain_observed: Arc::new(Notify::new()),
            release_exit: Arc::new(Notify::new()),
            final_checkpoint: Checkpoint::timestamp(Timestamp::now(), None),
        }
    }
}

#[cfg(feature = "messaging")]
#[derive(Default)]
struct DrainBridgeTestNode {
    processing_started: Arc<Notify>,
    release_processing: Arc<Notify>,
    processed_event_ids: Arc<tokio::sync::Mutex<Vec<Uuid>>>,
}

impl StartupSequenceTestNode {
    fn new(initial_checkpoint: Checkpoint, snapshot_checkpoint: Checkpoint) -> Self {
        Self {
            checkpoint: std::sync::Arc::new(tokio::sync::Mutex::new(initial_checkpoint)),
            scans: std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new())),
            snapshot_checkpoint,
            capabilities: NodeCapabilities {
                supports_continuous: false,
                supports_historical: true,
                supports_snapshot: true,
                ..NodeCapabilities::default()
            },
        }
    }
}

#[cfg(feature = "messaging")]
impl IngestorNode for DrainTestIngestor {
    type Config = ();
    type State = ();

    fn name(&self) -> &str {
        "drain-test-ingestor"
    }

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            supports_continuous: true,
            supports_historical: false,
            supports_snapshot: false,
            manages_own_continuous_loop: true,
            manages_own_checkpoints: true,
            ..NodeCapabilities::default()
        }
    }

    async fn initialize(
        &mut self,
        _config: Self::Config,
        _runtime: &NodeRuntimeState,
        _state: &mut Self::State,
    ) -> NodeResult<()> {
        Ok(())
    }

    async fn scan_snapshot(
        &mut self,
        _state: &mut Self::State,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::ZERO,
            final_checkpoint: Checkpoint::None,
            time_range: None,
            node_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    async fn scan_historical(
        &mut self,
        _state: &mut Self::State,
        _from: Checkpoint,
        _until: TimeHorizon,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::ZERO,
            final_checkpoint: Checkpoint::None,
            time_range: None,
            node_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    async fn run_continuous(
        &mut self,
        _state: &mut Self::State,
        _start: ContinuousStart,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> NodeResult<ScanReport> {
        self.started.notify_one();
        shutdown_rx.changed().await.map_err(|error| {
            SinexError::lifecycle(format!(
                "drain-test-ingestor shutdown channel dropped before drain: {error}"
            ))
        })?;
        self.drain_observed.notify_one();
        self.release_exit.notified().await;
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::ZERO,
            final_checkpoint: self.final_checkpoint.clone(),
            time_range: None,
            node_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }
}

impl Node for RuntimeTestNode {
    type Config = ();

    async fn initialize(&mut self, _init: NodeInitContext<Self::Config>) -> NodeResult<()> {
        Ok(())
    }

    async fn scan(
        &mut self,
        _from: Checkpoint,
        _until: TimeHorizon,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::ZERO,
            final_checkpoint: Checkpoint::None,
            time_range: None,
            node_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn node_name(&self) -> &'static str {
        "runtime-test-node"
    }

    fn node_type(&self) -> NodeType {
        NodeType::Automaton
    }

    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
        Ok(Checkpoint::None)
    }
}

impl Node for FailingShutdownNode {
    type Config = ();

    async fn initialize(&mut self, _init: NodeInitContext<Self::Config>) -> NodeResult<()> {
        Ok(())
    }

    async fn scan(
        &mut self,
        _from: Checkpoint,
        _until: TimeHorizon,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::ZERO,
            final_checkpoint: Checkpoint::None,
            time_range: None,
            node_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn node_name(&self) -> &'static str {
        "failing-shutdown-node"
    }

    fn node_type(&self) -> NodeType {
        NodeType::Automaton
    }

    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
        Ok(Checkpoint::None)
    }

    async fn shutdown(&mut self) -> NodeResult<()> {
        Err(SinexError::processing("node shutdown failed"))
    }
}

impl Node for FailingBatchNode {
    type Config = ();

    async fn initialize(&mut self, _init: NodeInitContext<Self::Config>) -> NodeResult<()> {
        Ok(())
    }

    async fn scan(
        &mut self,
        _from: Checkpoint,
        _until: TimeHorizon,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::ZERO,
            final_checkpoint: Checkpoint::None,
            time_range: None,
            node_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn node_name(&self) -> &'static str {
        "runtime-failing-batch-node"
    }

    fn node_type(&self) -> NodeType {
        NodeType::Automaton
    }

    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
        Ok(Checkpoint::None)
    }

    async fn process_event_batch(
        &mut self,
        _events: Vec<Event<JsonValue>>,
    ) -> NodeResult<ProcessingStats> {
        Err(SinexError::processing("batch processing boom"))
    }
}

impl Node for StartupSequenceTestNode {
    type Config = ();

    async fn initialize(&mut self, _init: NodeInitContext<Self::Config>) -> NodeResult<()> {
        Ok(())
    }

    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        let phase = match until {
            TimeHorizon::Snapshot => {
                *self.checkpoint.lock().await = self.snapshot_checkpoint.clone();
                "snapshot"
            }
            TimeHorizon::Historical { .. } => "historical",
            TimeHorizon::Continuous => "continuous",
        };
        self.scans
            .lock()
            .await
            .push(RecordedScan { from, until: phase });

        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::ZERO,
            final_checkpoint: Checkpoint::None,
            time_range: None,
            node_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn node_name(&self) -> &'static str {
        "startup-sequence-test-node"
    }

    fn node_type(&self) -> NodeType {
        NodeType::Ingestor
    }

    fn capabilities(&self) -> NodeCapabilities {
        self.capabilities.clone()
    }

    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
        Ok(self.checkpoint.lock().await.clone())
    }
}

#[cfg(feature = "messaging")]
impl Node for DrainBridgeTestNode {
    type Config = ();

    async fn initialize(&mut self, _init: NodeInitContext<Self::Config>) -> NodeResult<()> {
        Ok(())
    }

    async fn scan(
        &mut self,
        _from: Checkpoint,
        _until: TimeHorizon,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::ZERO,
            final_checkpoint: Checkpoint::None,
            time_range: None,
            node_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn node_name(&self) -> &'static str {
        "drain-bridge-test-node"
    }

    fn node_type(&self) -> NodeType {
        NodeType::Automaton
    }

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            supports_historical: false,
            ..NodeCapabilities::default()
        }
    }

    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
        Ok(Checkpoint::None)
    }

    async fn process_event_batch(
        &mut self,
        events: Vec<Event<JsonValue>>,
    ) -> NodeResult<ProcessingStats> {
        self.processing_started.notify_one();
        self.release_processing.notified().await;
        let mut processed = self.processed_event_ids.lock().await;
        processed.extend(
            events
                .iter()
                .filter_map(|event| event.id.map(|id| *id.as_uuid())),
        );
        Ok(ProcessingStats {
            processed: events.len(),
            skipped: 0,
            failed: 0,
            duration: std::time::Duration::ZERO,
            errors: Vec::new(),
        })
    }
}

struct FailingSerialize;

impl Serialize for FailingSerialize {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(S::Error::custom("boom"))
    }
}

#[cfg(feature = "messaging")]
async fn ensure_default_bridge_streams(client: &async_nats::Client) -> TestResult<()> {
    let js = jetstream::new(client.clone());
    let env = sinex_primitives::environment();
    let topology = sinex_primitives::nats::JetStreamTopology::new(
        &env,
        env.nats_stream_name("SINEX_RAW_EVENTS"),
        "runtime-drain-test-consumer".to_string(),
        None,
    );
    js.get_or_create_stream(jetstream::stream::Config {
        name: topology.events_stream.clone(),
        subjects: vec![topology.events_subject.clone()],
        storage: jetstream::stream::StorageType::Memory,
        ..Default::default()
    })
    .await?;
    js.get_or_create_stream(jetstream::stream::Config {
        name: topology.confirmations_stream,
        subjects: vec![topology.confirmations_subject],
        storage: jetstream::stream::StorageType::Memory,
        ..Default::default()
    })
    .await?;
    Ok(())
}

#[cfg(feature = "messaging")]
async fn request_drain_until_applied(
    client: &async_nats::Client,
    control_identity: &str,
    drain_controller: &RuntimeDrainController,
    reason: Option<&str>,
) -> TestResult<()> {
    let env = sinex_primitives::environment();
    let subject = env.nats_subject(&format!("sinex.control.nodes.{control_identity}.drain"));
    let payload = serde_json::to_vec(&sinex_primitives::rpc::nodes::NodeDrainRequest {
        node_id: control_identity.to_string().into(),
        reason: reason.map(ToOwned::to_owned),
    })?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);

    while tokio::time::Instant::now() < deadline {
        client
            .publish(subject.clone(), payload.clone().into())
            .await?;
        client.flush().await?;
        if drain_controller.is_requested() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    Err(color_eyre::eyre::eyre!(
        "drain command was not applied for control identity {control_identity}"
    ))
}

#[cfg(feature = "messaging")]
fn runtime_test_material_event(
    event_id: Uuid,
    source: &str,
    event_type: &str,
    payload: JsonValue,
) -> TestResult<Event<JsonValue>> {
    Ok(Event {
        id: Some(EventId::from_uuid(event_id)),
        source: EventSource::new(source)?,
        event_type: EventType::new(event_type)?,
        payload,
        ts_orig: Some(Timestamp::now()),
        host: HostName::from_static("runtime-test-host"),
        node_run_id: None,
        payload_schema_id: None,
        provenance: Provenance::Material {
            id: Id::<SourceMaterial>::from_uuid(Uuid::now_v7()),
            anchor_byte: 0,
            offset_start: None,
            offset_end: None,
            offset_kind: OffsetKind::Byte,
        },
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        node_model: None,
    })
}

#[cfg(feature = "messaging")]
async fn publish_confirmed_raw_event(
    client: &async_nats::Client,
    event: &Event<JsonValue>,
) -> TestResult<()> {
    let env = sinex_primitives::environment();
    let raw_subject = env.nats_raw_event_subject_with_namespace(
        None,
        event.source.as_str(),
        event.event_type.as_str(),
    );
    client
        .publish(raw_subject, serde_json::to_vec(event)?.into())
        .await?;

    let event_id = event
        .id
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("test event is missing an id"))?;
    let confirmation_subject =
        env.nats_subject(&format!("events.confirmations.{}", event_id.as_uuid()));
    let confirmation = serde_json::json!({
        "event_id": event_id.to_string(),
        "persisted": true,
        "ts_ingest": Timestamp::now().format_rfc3339(),
    });
    client
        .publish(
            confirmation_subject,
            serde_json::to_vec(&confirmation)?.into(),
        )
        .await?;
    client.flush().await?;
    Ok(())
}

#[cfg(feature = "messaging")]
async fn node_run_status(pool: &sinex_db::DbPool, node_run_id: Uuid) -> TestResult<String> {
    let status = sqlx::query_scalar::<_, String>(
        "SELECT status::text FROM core.node_runs WHERE id = $1",
    )
    .bind(node_run_id)
    .fetch_one(pool)
    .await?;
    Ok(status)
}

#[sinex_test]
async fn encode_control_message_serializes_scan_ack() -> TestResult<()> {
    let operation_id = Uuid::now_v7();
    let ack = NodeScanAck {
        operation_id,
        node_name: "test-node".to_string(),
        accepted: true,
        error: None,
    };

    let encoded =
        encode_control_message("scan acknowledgement", operation_id, &ack.node_name, &ack)?;
    let decoded: NodeScanAck = serde_json::from_slice(&encoded)?;

    assert_eq!(decoded.operation_id, operation_id);
    assert!(decoded.accepted);
    Ok(())
}

#[sinex_test]
async fn encode_control_message_reports_serialization_failure() -> TestResult<()> {
    let operation_id = Uuid::now_v7();
    let err = encode_control_message(
        "scan acknowledgement",
        operation_id,
        "test-node",
        &FailingSerialize,
    )
    .expect_err("failing serializers must surface explicit control-plane errors");

    let text = err.to_string();
    assert!(text.contains("Failed to serialize scan acknowledgement"));
    assert!(text.contains("test-node"));
    assert!(text.contains(&operation_id.to_string()));
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn publish_scan_ack_reports_nats_failures(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let client = ctx.nats_client();

    let operation_id = Uuid::now_v7();
    let ack = NodeScanAck {
        operation_id,
        node_name: "test-node".to_string(),
        accepted: true,
        error: Some("x".repeat(2_000_000)),
    };

    let error = NodeRunner::<RuntimeTestNode>::publish_scan_ack(
        &client,
        Some("sinex.test.reply".into()),
        &ack,
    )
    .await
    .expect_err("oversized control payloads must fail scan acknowledgements honestly");

    let message = error.to_string();
    assert!(message.contains("Failed to publish scan acknowledgement"));
    assert!(message.contains("sinex.test.reply"));
    assert!(message.contains(&operation_id.to_string()));
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn run_service_drain_persists_ingestor_checkpoint_and_updates_status(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let client = ctx.nats_client();
    ensure_default_bridge_streams(&client).await?;
    let transport = EventTransport::Nats(Arc::new(NatsPublisher::new(client.clone())));
    let work_dir = tempdir()?;

    let ingestor = DrainTestIngestor::default();
    let started = ingestor.started.clone();
    let drain_observed = ingestor.drain_observed.clone();
    let release_exit = ingestor.release_exit.clone();
    let expected_checkpoint = ingestor.final_checkpoint.clone();

    let mut runner = NodeRunner::new(IngestorNodeAdapter::new(ingestor));
    runner
        .initialize_with_transport(
            "runtime-drain-ingestor-service".to_string(),
            HashMap::new(),
            Some(ctx.pool().clone()),
            transport,
            work_dir.path().to_path_buf(),
            false,
        )
        .await?;

    let runtime = runner
        .runtime_state()
        .ok_or_else(|| color_eyre::eyre::eyre!("runtime state missing after init"))?;
    let control_identity = runtime.control_identity().to_string();
    let drain_controller = runtime.runtime_drain();
    let checkpoint_manager = runtime.checkpoint_manager();
    let node_run_id = runtime
        .node_run_id()
        .ok_or_else(|| color_eyre::eyre::eyre!("node run id missing after db-backed init"))?;
    let drain_complete_subject = sinex_primitives::environment().nats_subject(&format!(
        "sinex.control.nodes.{control_identity}.drain_complete"
    ));
    let mut drain_complete_sub = client.subscribe(drain_complete_subject).await?;

    let run_handle = tokio::spawn(async move { runner.run_service().await });
    tokio::time::timeout(Duration::from_secs(3), started.notified())
        .await
        .map_err(|_| color_eyre::eyre::eyre!("ingestor continuous loop did not start"))?;

    request_drain_until_applied(
        &client,
        &control_identity,
        &drain_controller,
        Some("test drain"),
    )
    .await?;
    tokio::time::timeout(Duration::from_secs(3), drain_observed.notified())
        .await
        .map_err(|_| color_eyre::eyre::eyre!("ingestor did not observe runtime drain"))?;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if node_run_status(ctx.pool(), node_run_id).await? == "draining" {
                return Ok::<(), color_eyre::Report>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| color_eyre::eyre::eyre!("node run status never reached draining"))??;

    release_exit.notify_one();

    let drain_complete =
        tokio::time::timeout(Duration::from_secs(3), drain_complete_sub.next())
            .await
            .map_err(|_| color_eyre::eyre::eyre!("drain_complete was not published"))?
            .ok_or_else(|| color_eyre::eyre::eyre!("drain_complete subscription closed"))?;
    let payload: NodeDrainComplete = serde_json::from_slice(&drain_complete.payload)?;
    assert_eq!(payload.node_name, control_identity);
    assert_eq!(
        payload.checkpoint.as_deref(),
        Some(expected_checkpoint.description().as_str())
    );

    let run_result = tokio::time::timeout(Duration::from_secs(3), run_handle)
        .await
        .map_err(|_| color_eyre::eyre::eyre!("drained ingestor service did not exit"))?;
    run_result??;

    let saved = checkpoint_manager.load_checkpoint().await?;
    assert_eq!(saved.checkpoint, expected_checkpoint);
    assert_eq!(node_run_status(ctx.pool(), node_run_id).await?, "stopped");
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn publish_scan_progress_reports_nats_failures(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let client = ctx.nats_client();

    let operation_id = Uuid::now_v7();
    let progress = NodeScanProgress {
        operation_id,
        node_name: "test-node".to_string(),
        events_processed: 1,
        events_emitted: 2,
        final_report: None,
        error: Some("x".repeat(2_000_000)),
    };

    let error = NodeRunner::<RuntimeTestNode>::publish_scan_progress(
        &client,
        "sinex.test.progress".to_string(),
        &progress,
    )
    .await
    .expect_err("oversized control payloads must fail scan progress honestly");

    let message = error.to_string();
    assert!(message.contains("Failed to publish scan progress update"));
    assert!(message.contains("sinex.test.progress"));
    assert!(message.contains(&operation_id.to_string()));
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn finish_replay_forwarder_surfaces_forwarder_error() -> TestResult<()> {
    let emitted_counter = Arc::new(AtomicU64::new(7));
    let handle = tokio::spawn(async {
        Err(SinexError::processing(
            "replay forwarder target channel closed".to_string(),
        ))
    });

    let outcome =
        NodeRunner::<RuntimeTestNode>::finish_replay_forwarder(handle, emitted_counter)
            .await
            .expect_err("forwarder failures must fail the dispatched scan honestly");

    assert_eq!(outcome.events_emitted, 7);
    assert!(
        outcome
            .error
            .to_string()
            .contains("replay forwarder target channel closed")
    );
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn finish_replay_forwarder_surfaces_join_error() -> TestResult<()> {
    let emitted_counter = Arc::new(AtomicU64::new(3));
    let handle: tokio::task::JoinHandle<NodeResult<()>> = tokio::spawn(async move {
        panic!("forwarder panic");
    });

    let outcome =
        NodeRunner::<RuntimeTestNode>::finish_replay_forwarder(handle, emitted_counter)
            .await
            .expect_err("forwarder panics must fail the dispatched scan honestly");

    assert_eq!(outcome.events_emitted, 3);
    assert!(
        outcome
            .error
            .to_string()
            .contains("Replay forwarder join failed")
    );
    Ok(())
}

#[sinex_test]
async fn resolve_provisionals_to_events_surfaces_missing_confirmed_event(
    ctx: TestContext,
) -> TestResult<()> {
    let provisional = ProvisionalEvent {
        event_id: EventId::from(Uuid::now_v7()),
        source: EventSource::new("runtime-test-source")?,
        event_type: EventType::new("runtime.test")?,
        payload: serde_json::json!({"ok": true}),
        ts_orig: Timestamp::now(),
        received_at: Timestamp::now(),
    };

    let Err(error) = NodeRunner::<RuntimeTestNode>::resolve_provisionals_to_events(
        &[provisional],
        &Some(ctx.pool().clone()),
    )
    .await
    else {
        return Err(color_eyre::eyre::eyre!(
            "missing confirmed events must fail honestly"
        ));
    };

    let message = format!("{error:#}");
    assert!(message.contains("Confirmed event missing from database"));
    Ok(())
}

#[sinex_test]
async fn build_event_from_provisional_rejects_invalid_node_run_id() -> TestResult<()> {
    let provisional = ProvisionalEvent {
        event_id: EventId::from(Uuid::now_v7()),
        source: EventSource::new("runtime-test-source")?,
        event_type: EventType::new("runtime.test")?,
        payload: serde_json::json!({
            "source": "runtime-test-source",
            "event_type": "runtime.test",
            "host": "runtime-test-host",
            "payload": {"ok": true},
            "source_event_ids": [Uuid::now_v7().to_string()],
            "node_run_id": "not-a-uuid"
        }),
        ts_orig: Timestamp::now(),
        received_at: Timestamp::now(),
    };

    let error = NodeRunner::<RuntimeTestNode>::build_event_from_provisional(&provisional)
        .expect_err("invalid persisted node_run_id must fail honestly");
    assert!(error.to_string().contains("Invalid UUID for node_run_id"));
    Ok(())
}

#[sinex_test]
async fn ingestor_startup_skips_gap_fill_when_only_snapshot_created_checkpoint(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let snapshot_checkpoint = Checkpoint::timestamp(Timestamp::now(), None);
    let node = StartupSequenceTestNode::new(Checkpoint::None, snapshot_checkpoint);
    let scans = node.scans.clone();
    let mut runner = NodeRunner::new(node);
    let work_dir = tempdir()?;
    runner
        .initialize_with_transport(
            "startup-sequence-snapshot-only".to_string(),
            HashMap::new(),
            None,
            EventTransport::Nats(Arc::new(crate::NatsPublisher::new(ctx.nats_client()))),
            work_dir.path().to_path_buf(),
            false,
        )
        .await?;

    runner.run_ingestor_startup_sequence().await?;

    let recorded = scans.lock().await.clone();
    assert_eq!(
        recorded,
        vec![RecordedScan {
            from: Checkpoint::None,
            until: "snapshot",
        }]
    );
    Ok(())
}

#[sinex_test]
async fn ingestor_startup_gap_fill_uses_preexisting_checkpoint(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let preexisting_checkpoint =
        Checkpoint::timestamp(Timestamp::now() - time::Duration::minutes(15), None);
    let snapshot_checkpoint = Checkpoint::timestamp(Timestamp::now(), None);
    let node =
        StartupSequenceTestNode::new(preexisting_checkpoint.clone(), snapshot_checkpoint);
    let scans = node.scans.clone();
    let mut runner = NodeRunner::new(node);
    let work_dir = tempdir()?;
    runner
        .initialize_with_transport(
            "startup-sequence-gap-fill".to_string(),
            HashMap::new(),
            None,
            EventTransport::Nats(Arc::new(crate::NatsPublisher::new(ctx.nats_client()))),
            work_dir.path().to_path_buf(),
            false,
        )
        .await?;

    runner.run_ingestor_startup_sequence().await?;

    let recorded = scans.lock().await.clone();
    assert_eq!(
        recorded,
        vec![
            RecordedScan {
                from: Checkpoint::None,
                until: "snapshot",
            },
            RecordedScan {
                from: preexisting_checkpoint,
                until: "historical",
            },
        ]
    );
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn resolve_provisionals_to_events_surfaces_invalid_payload_without_db() -> TestResult<()>
{
    let provisional = ProvisionalEvent {
        event_id: EventId::from(Uuid::now_v7()),
        source: EventSource::new("runtime-test-source")?,
        event_type: EventType::new("runtime.test")?,
        payload: serde_json::json!({
            "source": "runtime-test-source",
            "event_type": "runtime.test",
            "host": "runtime-test-host",
            "payload": {"ok": true},
            "source_event_ids": [Uuid::now_v7().to_string()],
            "node_run_id": "not-a-uuid"
        }),
        ts_orig: Timestamp::now(),
        received_at: Timestamp::now(),
    };

    let Err(error) =
        NodeRunner::<RuntimeTestNode>::resolve_provisionals_to_events(&[provisional], &None)
            .await
    else {
        return Err(color_eyre::eyre::eyre!(
            "invalid provisional payloads must fail honestly when no db pool is available"
        ));
    };

    let message = format!("{error:#}");
    assert!(
        message.contains("Confirmed event could not be reconstructed from provisional payload")
    );
    assert!(message.contains("Invalid UUID for node_run_id"));
    Ok(())
}

/// Node that returns a checkpoint error from `process_event_batch`. The
/// real adapter does this after 3 consecutive checkpoint CAS failures
/// (see `derived_node::adapter::process_batch`); we shortcut that
/// behaviour to drive the runtime fallback path directly.
struct CheckpointErrorBatchNode;

impl Node for CheckpointErrorBatchNode {
    type Config = ();

    async fn initialize(&mut self, _init: NodeInitContext<Self::Config>) -> NodeResult<()> {
        Ok(())
    }

    async fn scan(
        &mut self,
        _from: Checkpoint,
        _until: TimeHorizon,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::ZERO,
            final_checkpoint: Checkpoint::None,
            time_range: None,
            node_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn node_name(&self) -> &'static str {
        "runtime-checkpoint-error-node"
    }

    fn node_type(&self) -> NodeType {
        NodeType::Automaton
    }

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            supports_historical: false,
            ..NodeCapabilities::default()
        }
    }

    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
        Ok(Checkpoint::None)
    }

    async fn process_event_batch(
        &mut self,
        _events: Vec<Event<JsonValue>>,
    ) -> NodeResult<ProcessingStats> {
        Err(SinexError::checkpoint(
            "Checkpoint save failed 3 consecutive times; halting to prevent silent progress loss on crash+restart",
        ))
    }
}

/// Regression for #581. The pre-fix code caught `Err` from the batch path
/// and tried per-event DLQ routing; with a checkpoint error every event
/// hit the same KV-revision conflict and the function returned Ok,
/// looping forever and saturating I/O. The fix matches
/// `SinexError::Checkpoint` and propagates immediately.
#[cfg(feature = "messaging")]
#[sinex_test]
async fn process_batch_with_dlq_fallback_propagates_checkpoint_errors(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let transport =
        EventTransport::Nats(Arc::new(crate::NatsPublisher::new(ctx.nats_client())));
    let mut node = CheckpointErrorBatchNode;
    let event = Event {
        id: Some(EventId::from(Uuid::now_v7())),
        source: EventSource::new("runtime-test-source")?,
        event_type: EventType::new("runtime.test")?,
        payload: serde_json::json!({"ok": true}),
        ts_orig: Some(Timestamp::now()),
        host: HostName::from_static("runtime-test-host"),
        node_run_id: None,
        payload_schema_id: None,
        provenance: Provenance::Material {
            id: Id::<SourceMaterial>::from_uuid(Uuid::now_v7()),
            anchor_byte: 0,
            offset_start: None,
            offset_end: None,
            offset_kind: OffsetKind::Byte,
        },
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        node_model: None,
    };

    let error = NodeRunner::<CheckpointErrorBatchNode>::process_batch_with_dlq_fallback(
        &mut node,
        &transport,
        vec![event],
    )
    .await
    .expect_err("checkpoint error must propagate, not be DLQ-fallback'd");

    // The error must remain a Checkpoint variant — the runtime supervisor
    // matches on this to halt the consumer instead of advancing.
    assert!(
        matches!(error, SinexError::Checkpoint(_)),
        "expected SinexError::Checkpoint, got: {error:?}"
    );
    let message = format!("{error:#}");
    assert!(
        message.contains("Checkpoint save failed"),
        "checkpoint error message lost in propagation: {message}"
    );
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn process_batch_with_dlq_fallback_fails_when_dlq_route_fails(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let transport =
        EventTransport::Nats(Arc::new(crate::NatsPublisher::new(ctx.nats_client())));
    let mut node = FailingBatchNode;
    let event = Event {
        id: Some(EventId::from(Uuid::now_v7())),
        source: EventSource::new("runtime-test-source")?,
        event_type: EventType::new("runtime.test")?,
        payload: serde_json::json!({"ok": true}),
        ts_orig: Some(Timestamp::now()),
        host: HostName::from_static("runtime-test-host"),
        node_run_id: None,
        payload_schema_id: None,
        provenance: Provenance::Material {
            id: Id::<SourceMaterial>::from_uuid(Uuid::now_v7()),
            anchor_byte: 0,
            offset_start: None,
            offset_end: None,
            offset_kind: OffsetKind::Byte,
        },
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        node_model: None,
    };

    let error = NodeRunner::<FailingBatchNode>::process_batch_with_dlq_fallback(
        &mut node,
        &transport,
        vec![event],
    )
    .await
    .expect_err("failed DLQ routing must stop checkpoint advancement");

    let message = format!("{error:#}");
    assert!(message.contains("failed to route failed automaton event to processing-failure stream"));
    assert!(message.contains("batch processing boom"));
    assert!(message.contains("runtime-failing-batch-node"));
    Ok(())
}

#[sinex_test]
async fn load_bridge_checkpoint_state_surfaces_corrupt_kv(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let manager = CheckpointManager::new(
        kv.clone(),
        "runtime-test-node".to_string(),
        "runtime-group".to_string(),
        "runtime-consumer".to_string(),
    );
    manager
        .save_checkpoint(&crate::checkpoint::CheckpointState::default())
        .await?;

    let mut keys = kv.keys().await?;
    let key = futures::TryStreamExt::try_next(&mut keys)
        .await?
        .expect("checkpoint key should exist");
    kv.put(&key, b"{ definitely not valid json".as_slice().into())
        .await?;

    let error = NodeRunner::<RuntimeTestNode>::load_bridge_checkpoint_state(&manager)
        .await
        .expect_err("corrupt bridge checkpoint state must surface");
    let message = format!("{error:#}");
    assert!(message.contains("Failed to load checkpoint state for automaton bridge"));
    assert!(message.contains("Failed to decode checkpoint from KV"));
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn run_service_drain_finishes_inflight_automaton_batch_and_emits_completion(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let client = ctx.nats_client();
    ensure_default_bridge_streams(&client).await?;

    let transport = EventTransport::Nats(Arc::new(NatsPublisher::new(client.clone())));
    let work_dir = tempdir()?;

    let node = DrainBridgeTestNode::default();
    let processing_started = node.processing_started.clone();
    let release_processing = node.release_processing.clone();
    let processed_event_ids = node.processed_event_ids.clone();

    let mut runner = NodeRunner::new(node);
    runner
        .initialize_with_transport(
            "runtime-drain-automaton-service".to_string(),
            HashMap::new(),
            None,
            transport,
            work_dir.path().to_path_buf(),
            false,
        )
        .await?;

    let runtime = runner
        .runtime_state()
        .ok_or_else(|| color_eyre::eyre::eyre!("runtime state missing after init"))?;
    let control_identity = runtime.control_identity().to_string();
    let drain_controller = runtime.runtime_drain();
    let checkpoint_manager = runtime.checkpoint_manager();
    let drain_complete_subject = sinex_primitives::environment().nats_subject(&format!(
        "sinex.control.nodes.{control_identity}.drain_complete"
    ));
    let mut drain_complete_sub = client.subscribe(drain_complete_subject).await?;

    let run_handle = tokio::spawn(async move { runner.run_service().await });

    let event_id = Uuid::now_v7();
    let event = runtime_test_material_event(
        event_id,
        "runtime-test-source",
        "runtime.test.input",
        serde_json::json!({"value": "drain"}),
    )?;
    publish_confirmed_raw_event(&client, &event).await?;

    tokio::time::timeout(Duration::from_secs(3), processing_started.notified())
        .await
        .map_err(|_| color_eyre::eyre::eyre!("automaton batch did not start"))?;

    request_drain_until_applied(
        &client,
        &control_identity,
        &drain_controller,
        Some("test drain"),
    )
    .await?;

    release_processing.notify_one();

    let drain_complete =
        tokio::time::timeout(Duration::from_secs(3), drain_complete_sub.next())
            .await
            .map_err(|_| color_eyre::eyre::eyre!("automaton drain_complete was not published"))?
            .ok_or_else(|| color_eyre::eyre::eyre!("drain_complete subscription closed"))?;
    let payload: NodeDrainComplete = serde_json::from_slice(&drain_complete.payload)?;

    let run_result = tokio::time::timeout(Duration::from_secs(3), run_handle)
        .await
        .map_err(|_| color_eyre::eyre::eyre!("drained automaton service did not exit"))?;
    run_result??;

    assert_eq!(processed_event_ids.lock().await.as_slice(), &[event_id]);

    let saved = checkpoint_manager.load_checkpoint().await?;
    let expected_checkpoint = Checkpoint::internal(event_id, 1);
    assert_eq!(saved.checkpoint, expected_checkpoint);
    assert_eq!(payload.node_name, control_identity);
    assert_eq!(
        payload.checkpoint.as_deref(),
        Some(expected_checkpoint.description().as_str())
    );
    Ok(())
}

#[sinex_test]
async fn signal_shutdown_channel_reports_dropped_receiver() -> TestResult<()> {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    drop(rx);

    assert!(!NodeRunner::<RuntimeTestNode>::signal_shutdown_channel(
        tx,
        "heartbeat"
    ));
    Ok(())
}

#[sinex_test]
async fn signal_shutdown_channel_delivers_to_receiver() -> TestResult<()> {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    assert!(NodeRunner::<RuntimeTestNode>::signal_shutdown_channel(
        tx,
        "heartbeat"
    ));
    rx.await?;
    Ok(())
}

#[sinex_test]
async fn signal_watch_shutdown_reports_dropped_receiver() -> TestResult<()> {
    let (tx, rx) = tokio::sync::watch::channel(false);
    drop(rx);

    assert!(!NodeRunner::<RuntimeTestNode>::signal_watch_shutdown(
        tx, "listener"
    ));
    Ok(())
}

#[sinex_test]
async fn signal_watch_shutdown_delivers_to_receiver() -> TestResult<()> {
    let (tx, mut rx) = tokio::sync::watch::channel(false);

    assert!(NodeRunner::<RuntimeTestNode>::signal_watch_shutdown(
        tx, "listener"
    ));
    rx.changed().await?;
    assert!(*rx.borrow());
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn acquire_leader_standby_waits_for_existing_leader_release(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let transport =
        EventTransport::Nats(Arc::new(crate::NatsPublisher::new(ctx.nats_client())));
    let mut runner = NodeRunner::new(RuntimeTestNode);
    runner
        .initialize_with_transport(
            "runtime-standby-test".to_string(),
            HashMap::new(),
            Some(ctx.pool().clone()),
            transport,
            std::env::temp_dir(),
            false,
        )
        .await?;

    let runtime = runner
        .runtime_state()
        .ok_or_else(|| color_eyre::eyre::eyre!("runtime state missing after init"))?;
    let nats_client = runtime
        .nats_client()
        .ok_or_else(|| color_eyre::eyre::eyre!("nats client missing after init"))?;
    let js = async_nats::jetstream::new(nats_client.clone());
    let kv_client = sinex_primitives::coordination::CoordinationKvClient::new(
        js,
        runtime.service_info().service_name().to_string(),
    );

    kv_client.acquire_leadership("existing-leader").await?;

    let runner = Arc::new(tokio::sync::Mutex::new(runner));
    let acquired = Arc::new(AtomicBool::new(false));
    let runner_task = runner.clone();
    let acquired_task = acquired.clone();

    let wait_handle = tokio::spawn(async move {
        let mut guard = runner_task.lock().await;
        guard.acquire_leader_standby().await?;
        acquired_task.store(true, Ordering::SeqCst);
        Ok::<(), SinexError>(())
    });

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !acquired.load(Ordering::SeqCst),
        "standby runner should wait while another instance holds leadership"
    );

    kv_client.release_leadership("existing-leader").await?;
    let _ = tokio::time::timeout(Duration::from_secs(6), wait_handle).await??;
    assert!(
        acquired.load(Ordering::SeqCst),
        "runner should acquire leadership after the prior leader releases it"
    );

    runner.lock().await.shutdown_leader_state().await?;
    Ok(())
}

#[sinex_test]
async fn shutdown_join_result_rejects_panicked_tasks() -> TestResult<()> {
    let handle = tokio::spawn(async {
        panic!("runtime panic");
    });

    let error =
        NodeRunner::<RuntimeTestNode>::shutdown_join_result("runtime-task", handle.await)
            .expect_err("panicked runtime tasks must fail shutdown honestly");
    let message = format!("{error:#}");
    assert!(message.contains("Task failed during shutdown"));
    assert!(message.contains("runtime-task"));
    Ok(())
}

#[sinex_test]
async fn run_resubscribing_listener_retries_after_subscribe_error() -> TestResult<()> {
    let subscribe_attempts = Arc::new(AtomicU64::new(0));
    let handled_subscriptions = Arc::new(AtomicU64::new(0));
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    run_resubscribing_listener(
        "test listener",
        "sinex.test.subject",
        Duration::from_millis(1),
        shutdown_rx,
        {
            let subscribe_attempts = subscribe_attempts.clone();
            move || {
                let subscribe_attempts = subscribe_attempts.clone();
                async move {
                    let attempt = subscribe_attempts.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        Err(SinexError::processing("subscribe failed".to_string()))
                    } else {
                        Ok("subscription")
                    }
                }
            }
        },
        {
            let handled_subscriptions = handled_subscriptions.clone();
            move |subscription| {
                let handled_subscriptions = handled_subscriptions.clone();
                async move {
                    assert_eq!(subscription, "subscription");
                    handled_subscriptions.fetch_add(1, Ordering::SeqCst);
                    false
                }
            }
        },
    )
    .await;

    assert_eq!(subscribe_attempts.load(Ordering::SeqCst), 2);
    assert_eq!(handled_subscriptions.load(Ordering::SeqCst), 1);
    Ok(())
}

#[sinex_test]
async fn run_resubscribing_listener_retries_after_subscription_exit() -> TestResult<()> {
    let subscribe_attempts = Arc::new(AtomicU64::new(0));
    let handled_subscriptions = Arc::new(AtomicU64::new(0));
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    run_resubscribing_listener(
        "test listener",
        "sinex.test.subject",
        Duration::from_millis(1),
        shutdown_rx,
        {
            let subscribe_attempts = subscribe_attempts.clone();
            move || {
                let subscribe_attempts = subscribe_attempts.clone();
                async move {
                    let attempt = subscribe_attempts.fetch_add(1, Ordering::SeqCst);
                    Ok::<u64, SinexError>(attempt)
                }
            }
        },
        {
            let handled_subscriptions = handled_subscriptions.clone();
            move |_subscription| {
                let handled_subscriptions = handled_subscriptions.clone();
                async move {
                    let handled = handled_subscriptions.fetch_add(1, Ordering::SeqCst);
                    handled == 0
                }
            }
        },
    )
    .await;

    assert_eq!(subscribe_attempts.load(Ordering::SeqCst), 2);
    assert_eq!(handled_subscriptions.load(Ordering::SeqCst), 2);
    Ok(())
}

#[sinex_test]
async fn run_resubscribing_listener_stops_after_shutdown_signal() -> TestResult<()> {
    let subscribe_attempts = Arc::new(AtomicU64::new(0));
    let handled_subscriptions = Arc::new(AtomicU64::new(0));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handler_shutdown_tx = shutdown_tx.clone();

    let listener = tokio::spawn({
        let subscribe_attempts = subscribe_attempts.clone();
        let handled_subscriptions = handled_subscriptions.clone();
        async move {
            run_resubscribing_listener(
                "test listener",
                "sinex.test.subject",
                Duration::from_secs(1),
                shutdown_rx,
                move || {
                    let subscribe_attempts = subscribe_attempts.clone();
                    async move {
                        subscribe_attempts.fetch_add(1, Ordering::SeqCst);
                        Ok::<&'static str, SinexError>("subscription")
                    }
                },
                move |_subscription| {
                    let handled_subscriptions = handled_subscriptions.clone();
                    let mut shutdown_rx = handler_shutdown_tx.subscribe();
                    async move {
                        handled_subscriptions.fetch_add(1, Ordering::SeqCst);
                        shutdown_rx.changed().await.ok();
                        false
                    }
                },
            )
            .await;
        }
    });

    tokio::task::yield_now().await;
    shutdown_tx.send(true)?;
    tokio::time::timeout(Duration::from_secs(1), listener).await??;

    assert_eq!(subscribe_attempts.load(Ordering::SeqCst), 1);
    assert_eq!(handled_subscriptions.load(Ordering::SeqCst), 1);
    Ok(())
}

#[sinex_test]
async fn event_batcher_shutdown_result_rejects_join_panics() -> TestResult<()> {
    let handle = tokio::spawn(async move {
        panic!("batcher panic");
        #[allow(unreachable_code)]
        Ok::<(), SinexError>(())
    });

    let error = NodeRunner::<RuntimeTestNode>::event_batcher_shutdown_result(handle.await)
        .expect_err("panicked batcher tasks must fail shutdown honestly");
    let message = format!("{error:#}");
    assert!(message.contains("Event batcher failed during shutdown"));
    Ok(())
}

#[sinex_test]
async fn shutdown_task_waits_for_watch_signalled_exit() -> TestResult<()> {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let finished = Arc::new(AtomicBool::new(false));
    let finished_clone = finished.clone();
    let task = tokio::spawn(async move {
        shutdown_rx.changed().await.ok();
        finished_clone.store(true, Ordering::SeqCst);
    });

    let mut task = Some(task);
    NodeRunner::<RuntimeTestNode>::shutdown_task(&mut task, Some(shutdown_tx), "listener")
        .await?;

    assert!(finished.load(Ordering::SeqCst));
    assert!(task.is_none());
    Ok(())
}

#[sinex_test]
async fn collapse_shutdown_errors_preserves_additional_failures() -> TestResult<()> {
    let error = NodeRunner::<RuntimeTestNode>::collapse_shutdown_errors(vec![
        (
            "heartbeat".to_string(),
            SinexError::processing("primary shutdown failure"),
        ),
        (
            "event batcher".to_string(),
            SinexError::processing("secondary shutdown failure"),
        ),
    ])
    .expect_err("multiple shutdown failures must stay visible");
    let message = format!("{error:#}");
    assert!(message.contains("primary shutdown failure"));
    assert!(message.contains("event batcher"));
    assert!(message.contains("secondary shutdown failure"));
    Ok(())
}

#[sinex_test]
async fn shutdown_marks_runner_failed_when_cleanup_errors() -> TestResult<()> {
    let mut runner = NodeRunner::new(FailingShutdownNode);
    runner.lifecycle = RunnerLifecycle::Initialized;

    let error = runner
        .shutdown()
        .await
        .expect_err("failing shutdowns must surface as errors");

    assert!(error.to_string().contains("node shutdown failed"));
    assert_eq!(runner.lifecycle(), RunnerLifecycle::ShutdownFailed);
    Ok(())
}
