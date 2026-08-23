//! Tests for `RuntimeRunner` private control-plane and runtime helpers.
//! Inline because they cover items that are not exposed beyond the runner module.

// Inline because these cover private control-plane encoding helpers.
use super::*;
use crate::runtime::checkpoint::CheckpointManager;
use crate::runtime::stream::{ContinuousStart, ProcessingStats, RuntimeInitContext};
use crate::runtime::{
    ConfirmedEventHandler, JetStreamEventConsumer, JetStreamEventConsumerConfig, NatsPublisher,
    SourceDriver, SourceDriverRuntime,
};
use async_nats::jetstream;
use async_trait::async_trait;
use serde::Serialize;
use serde::ser::Error as _;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::builder::EventId;
use tempfile::tempdir;
use tokio::sync::{Mutex, Notify, oneshot};
use xtask::sandbox::prelude::*;

#[derive(Default)]
struct RuntimeTestModule;

#[derive(Default)]
struct FailingShutdownModule;

/// `sinex-q102`: a module whose own `initialize()` fails AFTER
/// `initialize_with_transport` has already started the schema listener and
/// checkpoint-cleanup background tasks — used to prove those tasks are not
/// shut down on this early-return error path.
#[derive(Default)]
struct FailingInitModule;

#[derive(Default)]
struct FailingBatchModule;

#[cfg(feature = "messaging")]
struct RecordingConfirmedEventHandler {
    received: Notify,
    events: Mutex<Vec<Event<JsonValue>>>,
}

#[cfg(feature = "messaging")]
impl RecordingConfirmedEventHandler {
    fn new() -> Self {
        Self {
            received: Notify::new(),
            events: Mutex::new(Vec::new()),
        }
    }
}

#[cfg(feature = "messaging")]
#[async_trait]
impl ConfirmedEventHandler for RecordingConfirmedEventHandler {
    async fn handle_confirmed(
        &self,
        event: &Event<JsonValue>,
        completion: oneshot::Sender<crate::runtime::ConfirmedEventCompletion>,
    ) -> RuntimeResult<()> {
        self.events.lock().await.push(event.clone());
        self.received.notify_one();
        completion
            .send(crate::runtime::ConfirmedEventCompletion::Safe)
            .map_err(|_| crate::runtime::SinexError::lifecycle("completion receiver dropped"))?;
        Ok(())
    }
}

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
    snapshot_started: Option<Arc<Notify>>,
    snapshot_release: Option<Arc<Notify>>,
}

#[cfg(feature = "messaging")]
struct HistoricalCatchupTestAutomaton {
    scans: std::sync::Arc<tokio::sync::Mutex<Vec<RecordedScan>>>,
    supports_historical: bool,
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
            snapshot_started: None,
            snapshot_release: None,
        }
    }

    fn with_snapshot_gate() -> (Self, Arc<Notify>, Arc<Notify>) {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let mut module = Self::new(Checkpoint::None, Checkpoint::None);
        module.snapshot_started = Some(started.clone());
        module.snapshot_release = Some(release.clone());
        (module, started, release)
    }
}

#[cfg(feature = "messaging")]
impl HistoricalCatchupTestAutomaton {
    fn new(supports_historical: bool) -> Self {
        Self {
            scans: std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new())),
            supports_historical,
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
    let config = RuntimeRunner::automaton_consumer_config(
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

    let wildcard_config = RuntimeRunner::automaton_consumer_config(
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
    let config = RuntimeRunner::automaton_consumer_config(
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

/// sinex-ijz6 (vfy ruling): hourly-summarizer and daily-summarizer must
/// consume a narrow `filter_subjects` allowlist on the confirmed-events
/// stream instead of the broad activity wildcard. This exercises the REAL
/// registered `HourlySummarizerRuntime`/`DailySummarizerRuntime` types (the
/// same `AutomatonRuntime<WindowedWrapper<..>>` the supervisor spawns via
/// `automata::registry::AUTOMATA`) through the same
/// `automaton_consumer_config` helper the runtime uses to build the
/// JetStream consumer config. It fails if either summarizer's
/// `Windowed::input_event_type()` is ever widened (e.g. an accidental
/// `input_event_types()` override returning `"*"`), since that is exactly
/// the mutation that would silently re-widen the confirmed-stream fan-in
/// this ruling closed.
#[cfg(feature = "messaging")]
#[sinex_test]
async fn summarizer_confirmed_consumers_stay_narrowed_to_declared_input_types() -> TestResult<()> {
    use crate::automata::{DailySummarizerRuntime, HourlySummarizerRuntime};
    use crate::runtime::automaton::traits::InputProvenanceFilter;
    use crate::runtime::stream::RuntimeModule;

    // `RuntimeModule::event_type_filters`/`confirmed_event_provenance_filter`
    // are called via fully-qualified syntax: `AutomatonRuntime<N>` also gets a
    // blanket `ErasedRuntimeModule` impl (sinex-qabz type-erasure, #2498) with
    // methods of the same name, so plain `.event_type_filters()` is ambiguous.
    let hourly = HourlySummarizerRuntime::default();
    let hourly_types = RuntimeModule::event_type_filters(&hourly);
    let hourly_provenance = RuntimeModule::confirmed_event_provenance_filter(&hourly);
    assert_eq!(
        hourly_types,
        vec!["activity.window.summary"],
        "hourly-summarizer must consume exactly the analytics-produced \
         activity.window.summary type, not a wildcard"
    );
    assert_eq!(hourly_provenance, InputProvenanceFilter::SynthesizedOnly);
    let hourly_config =
        RuntimeRunner::automaton_consumer_config("sinex.hourly", hourly_provenance, hourly_types);
    assert_eq!(
        hourly_config.event_type_filters,
        vec!["activity.window.summary".to_string()]
    );

    let daily = DailySummarizerRuntime::default();
    let daily_types = RuntimeModule::event_type_filters(&daily);
    let daily_provenance = RuntimeModule::confirmed_event_provenance_filter(&daily);
    assert_eq!(
        daily_types,
        vec!["activity.summary.hourly"],
        "daily-summarizer must consume exactly the hourly-summarizer's \
         activity.summary.hourly output, not a wildcard"
    );
    assert_eq!(daily_provenance, InputProvenanceFilter::SynthesizedOnly);
    let daily_config =
        RuntimeRunner::automaton_consumer_config("sinex.daily", daily_provenance, daily_types);
    assert_eq!(
        daily_config.event_type_filters,
        vec!["activity.summary.hourly".to_string()]
    );

    Ok(())
}

/// sinex-i41y.1: prove that an event published during historical catch-up is
/// retained by the real `DeliverPolicy::New` durable consumer and delivered
/// once the production start gate opens. This deliberately exercises the
/// consumer API directly instead of `RuntimeRunner`'s cfg(test) readiness
/// hook, so moving consumer creation after catch-up or bypassing the start
/// gate makes the test fail.
#[cfg(feature = "messaging")]
#[sinex_test(timeout = 30)]
async fn confirmed_consumer_start_gate_captures_concurrent_publication(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let client = ctx.nats_client();
    let env = sinex_primitives::environment::environment().clone();
    let namespace = format!("i41y-start-gate-{}", Uuid::now_v7());
    let raw_stream = env.nats_stream_name_with_namespace(Some(&namespace), "SINEX_RAW_EVENTS");
    let confirmed_stream = format!("{raw_stream}_CONFIRMED");
    let confirmed_subject = env.nats_subject_with_namespace(Some(&namespace), "events.confirmed.>");
    let js = jetstream::new(client.clone());

    js.create_stream(jetstream::stream::Config {
        name: confirmed_stream.clone(),
        subjects: vec![confirmed_subject],
        storage: jetstream::stream::StorageType::Memory,
        ..Default::default()
    })
    .await?;

    let handler = Arc::new(RecordingConfirmedEventHandler::new());
    let consumer_name = format!("i41y-start-gate-consumer-{}", Uuid::now_v7());
    let consumer = Arc::new(JetStreamEventConsumer::new_with_namespace(
        client.clone(),
        env.clone(),
        JetStreamEventConsumerConfig {
            batch_size: 1,
            consumer_name: consumer_name.clone(),
            deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::New,
            ..Default::default()
        },
        handler.clone(),
        Some(namespace.clone()),
    ));
    let (ready_tx, ready_rx) = oneshot::channel();
    let (start_tx, start_rx) = oneshot::channel();
    let consumer_task = tokio::spawn({
        let consumer = consumer.clone();
        async move {
            consumer
                .run_with_ready_and_start_gate(Some(ready_tx), start_rx)
                .await
        }
    });

    tokio::time::timeout(Duration::from_secs(3), ready_rx)
        .await
        .map_err(|_| color_eyre::eyre::eyre!("confirmed consumer did not become ready"))??;

    let stream = js.get_stream(&confirmed_stream).await?;
    let consumer_info = stream.consumer_info(&consumer_name).await?;
    assert_eq!(
        consumer_info.config.deliver_policy,
        async_nats::jetstream::consumer::DeliverPolicy::New,
        "the test must observe the actual durable New consumer before catch-up release"
    );

    let event = runtime_test_material_event(
        Uuid::now_v7(),
        "i41y-start-gate-source",
        "i41y.start.gate",
        serde_json::json!({"published": "during-catch-up"}),
    )?;
    let publish_subject = env.nats_subject_with_namespace(
        Some(&namespace),
        &format!(
            "events.confirmed.material.{}.{}",
            sinex_primitives::environment::SinexEnvironment::nats_subject_token(
                event.source.as_str()
            ),
            sinex_primitives::environment::SinexEnvironment::nats_subject_token(
                event.event_type.as_str()
            ),
        ),
    );
    js.publish(publish_subject, serde_json::to_vec(&event)?.into())
        .await?
        .await?;

    assert!(
        tokio::time::timeout(Duration::from_millis(100), handler.received.notified())
            .await
            .is_err(),
        "consumer must not process publication while the historical catch-up gate is held"
    );

    start_tx
        .send(())
        .map_err(|_| color_eyre::eyre::eyre!("consumer start gate receiver was dropped"))?;
    tokio::time::timeout(Duration::from_secs(3), handler.received.notified())
        .await
        .map_err(|_| {
            color_eyre::eyre::eyre!("confirmed event was not delivered after gate release")
        })?;

    let received_events = handler.events.lock().await;
    assert_eq!(received_events.len(), 1);
    assert_eq!(received_events[0].id, event.id);
    drop(received_events);

    consumer.stop().await;
    tokio::time::timeout(Duration::from_secs(3), consumer_task)
        .await
        .map_err(|_| color_eyre::eyre::eyre!("confirmed consumer did not shut down"))???;
    js.delete_stream(&confirmed_stream).await?;
    Ok(())
}

#[sinex_test]
async fn checkpoint_consumer_name_is_stable_for_sources() -> TestResult<()> {
    let raw_config = HashMap::new();

    let consumer_name = RuntimeRunner::checkpoint_consumer_name(
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

    let consumer_name = RuntimeRunner::checkpoint_consumer_name(
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

    let consumer_name = RuntimeRunner::checkpoint_consumer_name(
        ModuleKind::Source,
        &raw_config,
        "system.journald",
        "host-a",
    );

    assert_eq!(consumer_name, "stable-consumer");
    Ok(())
}

impl RuntimeModule for FailingInitModule {
    type Config = ();

    async fn initialize(&mut self, _init: RuntimeInitContext<Self::Config>) -> RuntimeResult<()> {
        Err(SinexError::processing(
            "deliberate module init failure (sinex-q102 test)",
        ))
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
        "failing-init-module"
    }

    fn module_kind(&self) -> ModuleKind {
        ModuleKind::Automaton
    }

    async fn current_checkpoint(&self) -> RuntimeResult<Checkpoint> {
        Ok(Checkpoint::None)
    }
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
                if let Some(started) = &self.snapshot_started {
                    started.notify_one();
                }
                if let Some(release) = &self.snapshot_release {
                    release.notified().await;
                }
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
impl RuntimeModule for HistoricalCatchupTestAutomaton {
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
            TimeHorizon::Snapshot => "snapshot",
            TimeHorizon::Historical { .. } => "historical",
            TimeHorizon::Continuous => "continuous",
        };
        self.scans
            .lock()
            .await
            .push(RecordedScan { from, until: phase });

        Err(SinexError::processing(
            "intentional historical catch-up stop".to_string(),
        ))
    }

    fn module_name(&self) -> &'static str {
        "historical-catchup-test-automaton"
    }

    fn module_kind(&self) -> ModuleKind {
        ModuleKind::Automaton
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            supports_continuous: true,
            supports_historical: self.supports_historical,
            supports_snapshot: false,
            ..RuntimeCapabilities::default()
        }
    }

    async fn current_checkpoint(&self) -> RuntimeResult<Checkpoint> {
        Ok(Checkpoint::None)
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
        // sinex-li78: `run_automaton_event_bridge` (crate::runtime::stream::runner::
        // automaton_runtime) has required `supports_historical` since #2299
        // (sinex-r6d.6 / sinex-vxu crash-window hardening) — it hard-errors
        // before ever subscribing the confirmed-event consumer when this is
        // false, so `process_event_batch` never runs. This module goes through
        // the generic bridge (no `manages_own_continuous_loop`), so it must
        // advertise `supports_historical: true` like a real bridge-backed
        // automaton; the fixture's `scan()` stub above already satisfies that
        // capability. Plain `RuntimeCapabilities::default()` already has
        // `supports_historical: true`.
        RuntimeCapabilities::default()
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
        name: topology.confirmed_events_stream.into(),
        subjects: vec![topology.confirmed_events_subject.into()],
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
        product_class: None,
        claim_support: None,
        derivation_declaration_id: None,
        derivation_epoch_id: None,
        derivation_lane_id: None,
        adjudication_event_id: None,
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

    if event.id.is_none() {
        return Err(color_eyre::eyre::eyre!("test event is missing an id"));
    }
    let provenance = if event.is_synthesized_event() {
        "synthesized"
    } else {
        "material"
    };
    let confirmation_subject = env.nats_subject(&format!(
        "events.confirmed.{}.{}.{}",
        provenance,
        sinex_primitives::environment::SinexEnvironment::nats_subject_token(event.source.as_str()),
        sinex_primitives::environment::SinexEnvironment::nats_subject_token(
            event.event_type.as_str()
        )
    ));
    client
        .publish(confirmation_subject, serde_json::to_vec(event)?.into())
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

#[path = "tests/cancel.rs"]
mod cancel;
#[path = "tests/pipeline.rs"]
mod pipeline;
#[path = "tests/runtime.rs"]
mod runtime;
