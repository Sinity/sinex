//! Tests for `RuntimeRunner<T>` private control-plane and runtime helpers.
//! Inline because they cover items that are not exposed beyond the runner module.

// Inline because these cover private control-plane encoding helpers.
use super::*;
use crate::runtime::checkpoint::CheckpointManager;
use crate::runtime::{NatsPublisher, SourceDriver, SourceDriverRuntime};
use async_nats::jetstream;
use serde::Serialize;
use serde::ser::Error as _;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::builder::EventId;
use tempfile::tempdir;
use tokio::sync::Notify;
use xtask::sandbox::prelude::*;

#[derive(Default)]
struct RuntimeTestModule;

#[derive(Default)]
struct FailingShutdownModule;

#[derive(Default)]
struct FailingBatchModule;

#[derive(Debug, Clone, PartialEq)]
struct RecordedScan {
    from: Checkpoint,
    until: &'static str,
}

struct StartupSequenceTestModule {
    checkpoint: std::sync::Arc<tokio::sync::Mutex<Checkpoint>>,
    scans: std::sync::Arc<tokio::sync::Mutex<Vec<RecordedScan>>>,
    snapshot_checkpoint: Checkpoint,
    capabilities: RuntimeCapabilities,
}

#[cfg(feature = "messaging")]
struct DrainTestSource {
    started: Arc<Notify>,
    drain_observed: Arc<Notify>,
    release_exit: Arc<Notify>,
    final_checkpoint: Checkpoint,
}

#[cfg(feature = "messaging")]
impl Default for DrainTestSource {
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
struct DrainBridgeTestModule {
    processing_started: Arc<Notify>,
    release_processing: Arc<Notify>,
    processed_event_ids: Arc<tokio::sync::Mutex<Vec<Uuid>>>,
}

impl StartupSequenceTestModule {
    fn new(initial_checkpoint: Checkpoint, snapshot_checkpoint: Checkpoint) -> Self {
        Self {
            checkpoint: std::sync::Arc::new(tokio::sync::Mutex::new(initial_checkpoint)),
            scans: std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new())),
            snapshot_checkpoint,
            capabilities: RuntimeCapabilities {
                supports_continuous: false,
                supports_historical: true,
                supports_snapshot: true,
                ..RuntimeCapabilities::default()
            },
        }
    }
}

#[cfg(feature = "messaging")]
impl SourceDriver for DrainTestSource {
    type Config = ();
    type State = ();

    fn name(&self) -> &'static str {
        "drain-test-source"
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            supports_continuous: true,
            supports_historical: false,
            supports_snapshot: false,
            manages_own_continuous_loop: true,
            manages_own_checkpoints: true,
            ..RuntimeCapabilities::default()
        }
    }

    async fn initialize(
        &mut self,
        _config: Self::Config,
        _runtime: &RuntimeContext,
        _state: &mut Self::State,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    async fn scan_snapshot(
        &mut self,
        _state: &mut Self::State,
        _args: ScanArgs,
    ) -> RuntimeResult<ScanReport> {
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::ZERO,
            final_checkpoint: Checkpoint::None,
            time_range: None,
            runtime_stats: HashMap::new(),
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
    ) -> RuntimeResult<ScanReport> {
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::ZERO,
            final_checkpoint: Checkpoint::None,
            time_range: None,
            runtime_stats: HashMap::new(),
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
    ) -> RuntimeResult<ScanReport> {
        self.started.notify_one();
        shutdown_rx.changed().await.map_err(|error| {
            SinexError::lifecycle(format!(
                "drain-test-source shutdown channel dropped before drain: {error}"
            ))
        })?;
        self.drain_observed.notify_one();
        self.release_exit.notified().await;
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::ZERO,
            final_checkpoint: self.final_checkpoint.clone(),
            time_range: None,
            runtime_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }
}

impl RuntimeModule for RuntimeTestModule {
    type Config = ();

    async fn initialize(&mut self, _init: RuntimeInitContext<Self::Config>) -> RuntimeResult<()> {
        Ok(())
    }

    async fn scan(
        &mut self,
        _from: Checkpoint,
        _until: TimeHorizon,
        _args: ScanArgs,
    ) -> RuntimeResult<ScanReport> {
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::ZERO,
            final_checkpoint: Checkpoint::None,
            time_range: None,
            runtime_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn module_name(&self) -> &'static str {
        "runtime-test-module"
    }

    fn module_kind(&self) -> ModuleKind {
        ModuleKind::Automaton
    }

    async fn current_checkpoint(&self) -> RuntimeResult<Checkpoint> {
        Ok(Checkpoint::None)
    }
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn automaton_consumer_config_targets_confirmed_events_stream() -> TestResult<()> {
    // Option C: each automaton runs one durable consumer on the confirmed-events
    // stream. A type-specific automaton filters server-side to a single event
    // type; the consumer name is derived from the service name; delivery is
    // `New` because the per-automaton checkpoint + historical scan cover anything
    // before the consumer starts (#2187 / #2202).
    let config = RuntimeRunner::<RuntimeTestModule>::automaton_consumer_config(
        "sinex.entity-extractor",
        crate::runtime::automaton::traits::InputProvenanceFilter::MaterialOnly,
        vec!["entity.extracted"],
    );

    assert_eq!(
        config.event_type_filters,
        vec!["entity.extracted".to_string()]
    );
    assert_eq!(
        config.provenance_filter,
        crate::runtime::automaton::traits::InputProvenanceFilter::MaterialOnly
    );
    assert!(matches!(
        config.deliver_policy,
        async_nats::jetstream::consumer::DeliverPolicy::New
    ));
    assert_eq!(
        config.consumer_name,
        "sinex_entity-extractor-confirmed-events-material-filter-entity_d_extracted"
    );

    let wildcard_config = RuntimeRunner::<RuntimeTestModule>::automaton_consumer_config(
        "sinex.entity-extractor",
        crate::runtime::automaton::traits::InputProvenanceFilter::Any,
        Vec::new(),
    );

    assert!(wildcard_config.event_type_filters.is_empty());
    assert_eq!(
        wildcard_config.provenance_filter,
        crate::runtime::automaton::traits::InputProvenanceFilter::Any
    );
    assert_eq!(
        wildcard_config.consumer_name,
        "sinex_entity-extractor-confirmed-events"
    );
    assert!(matches!(
        wildcard_config.deliver_policy,
        async_nats::jetstream::consumer::DeliverPolicy::New
    ));
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn automaton_consumer_config_names_multi_type_filters() -> TestResult<()> {
    let config = RuntimeRunner::<RuntimeTestModule>::automaton_consumer_config(
        "sinex.entity-extractor",
        crate::runtime::automaton::traits::InputProvenanceFilter::Any,
        vec!["document.chunked", "command.executed", "command.canonical"],
    );

    assert_eq!(
        config.event_type_filters,
        vec![
            "document.chunked".to_string(),
            "command.executed".to_string(),
            "command.canonical".to_string(),
        ]
    );
    assert_eq!(
        config.consumer_name,
        "sinex_entity-extractor-confirmed-events-filter-document_d_chunked_or_command_d_executed_or_command_d_canonical"
    );
    Ok(())
}

#[sinex_test]
async fn checkpoint_consumer_name_is_stable_for_sources() -> TestResult<()> {
    let raw_config = HashMap::new();

    let consumer_name = RuntimeRunner::<RuntimeTestModule>::checkpoint_consumer_name(
        ModuleKind::Source,
        &raw_config,
        "system.journald",
        "host-a",
    );

    assert_eq!(consumer_name, "system.journald");
    Ok(())
}

#[sinex_test]
async fn checkpoint_consumer_name_is_stable_for_automata() -> TestResult<()> {
    let raw_config = HashMap::new();

    let consumer_name = RuntimeRunner::<RuntimeTestModule>::checkpoint_consumer_name(
        ModuleKind::Automaton,
        &raw_config,
        "sinex.entity-extractor",
        "host-a",
    );

    assert_eq!(consumer_name, "sinex.entity-extractor");
    Ok(())
}

#[sinex_test]
async fn configured_checkpoint_consumer_name_overrides_source_default() -> TestResult<()> {
    let raw_config = HashMap::from([(
        "consumer_name".to_string(),
        serde_json::json!("stable-consumer"),
    )]);

    let consumer_name = RuntimeRunner::<RuntimeTestModule>::checkpoint_consumer_name(
        ModuleKind::Source,
        &raw_config,
        "system.journald",
        "host-a",
    );

    assert_eq!(consumer_name, "stable-consumer");
    Ok(())
}

impl RuntimeModule for FailingShutdownModule {
    type Config = ();

    async fn initialize(&mut self, _init: RuntimeInitContext<Self::Config>) -> RuntimeResult<()> {
        Ok(())
    }

    async fn scan(
        &mut self,
        _from: Checkpoint,
        _until: TimeHorizon,
        _args: ScanArgs,
    ) -> RuntimeResult<ScanReport> {
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::ZERO,
            final_checkpoint: Checkpoint::None,
            time_range: None,
            runtime_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn module_name(&self) -> &'static str {
        "failing-shutdown-module"
    }

    fn module_kind(&self) -> ModuleKind {
        ModuleKind::Automaton
    }

    async fn current_checkpoint(&self) -> RuntimeResult<Checkpoint> {
        Ok(Checkpoint::None)
    }

    async fn shutdown(&mut self) -> RuntimeResult<()> {
        Err(SinexError::processing("module shutdown failed"))
    }
}

impl RuntimeModule for FailingBatchModule {
    type Config = ();

    async fn initialize(&mut self, _init: RuntimeInitContext<Self::Config>) -> RuntimeResult<()> {
        Ok(())
    }

    async fn scan(
        &mut self,
        _from: Checkpoint,
        _until: TimeHorizon,
        _args: ScanArgs,
    ) -> RuntimeResult<ScanReport> {
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::ZERO,
            final_checkpoint: Checkpoint::None,
            time_range: None,
            runtime_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn module_name(&self) -> &'static str {
        "runtime-failing-batch-module"
    }

    fn module_kind(&self) -> ModuleKind {
        ModuleKind::Automaton
    }

    async fn current_checkpoint(&self) -> RuntimeResult<Checkpoint> {
        Ok(Checkpoint::None)
    }

    async fn process_event_batch(
        &mut self,
        _events: Vec<Event<JsonValue>>,
    ) -> RuntimeResult<ProcessingStats> {
        Err(SinexError::processing("batch processing boom"))
    }
}

impl RuntimeModule for StartupSequenceTestModule {
    type Config = ();

    async fn initialize(&mut self, _init: RuntimeInitContext<Self::Config>) -> RuntimeResult<()> {
        Ok(())
    }

    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        _args: ScanArgs,
    ) -> RuntimeResult<ScanReport> {
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
            runtime_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn module_name(&self) -> &'static str {
        "startup-sequence-test-module"
    }

    fn module_kind(&self) -> ModuleKind {
        ModuleKind::Source
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        self.capabilities.clone()
    }

    async fn current_checkpoint(&self) -> RuntimeResult<Checkpoint> {
        Ok(self.checkpoint.lock().await.clone())
    }
}

#[cfg(feature = "messaging")]
impl RuntimeModule for DrainBridgeTestModule {
    type Config = ();

    async fn initialize(&mut self, _init: RuntimeInitContext<Self::Config>) -> RuntimeResult<()> {
        Ok(())
    }

    async fn scan(
        &mut self,
        _from: Checkpoint,
        _until: TimeHorizon,
        _args: ScanArgs,
    ) -> RuntimeResult<ScanReport> {
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::ZERO,
            final_checkpoint: Checkpoint::None,
            time_range: None,
            runtime_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn module_name(&self) -> &'static str {
        "drain-bridge-test-module"
    }

    fn module_kind(&self) -> ModuleKind {
        ModuleKind::Automaton
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            supports_historical: false,
            ..RuntimeCapabilities::default()
        }
    }

    async fn current_checkpoint(&self) -> RuntimeResult<Checkpoint> {
        Ok(Checkpoint::None)
    }

    async fn process_event_batch(
        &mut self,
        events: Vec<Event<JsonValue>>,
    ) -> RuntimeResult<ProcessingStats> {
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
        name: topology.events_stream.to_string(),
        subjects: vec![topology.events_subject.to_string()],
        storage: jetstream::stream::StorageType::Memory,
        ..Default::default()
    })
    .await?;
    js.get_or_create_stream(jetstream::stream::Config {
        name: topology.confirmations_stream.into(),
        subjects: vec![topology.confirmations_subject.into()],
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
    let subject = env.nats_subject(&format!("sinex.control.sources.{control_identity}.drain"));
    let payload = serde_json::to_vec(&sinex_primitives::rpc::runtime::RuntimeDrainRequest {
        module_name: control_identity.to_string().into(),
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
        module_run_id: None,
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
        automaton_model: None,
        ts_quality: None,
        anchor_payload_hash: None,
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
    let confirmation_subject = env.nats_subject(&format!(
        "events.confirmations.{}.{}",
        event.source.as_str(),
        event.event_type.as_str()
    ));
    let confirmation = serde_json::json!({
        "event_id": event_id.to_string(),
        "source": event.source.as_str(),
        "event_type": event.event_type.as_str(),
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
async fn module_run_status(pool: &sinex_db::DbPool, module_run_id: Uuid) -> TestResult<String> {
    let status =
        sqlx::query_scalar::<_, String>("SELECT status::text FROM core.runs WHERE id = $1")
            .bind(module_run_id)
            .fetch_one(pool)
            .await?;
    Ok(status)
}

mod pipeline;
mod runtime;
