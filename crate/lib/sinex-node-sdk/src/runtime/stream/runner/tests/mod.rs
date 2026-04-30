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

mod pipeline;
mod runtime;
