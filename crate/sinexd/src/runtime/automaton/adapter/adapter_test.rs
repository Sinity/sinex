#[cfg(test)]
#[path = "tests/processing_replay.rs"]
mod processing_replay;
// Inline because these cover a private shutdown-signaling helper.
#[cfg(feature = "messaging")]
use super::log_self_observation_failure;
use super::{AutomatonRuntime, stale_output_ids_or_fail_scope};
use crate::runtime::automaton::{
    AutomatonAdapterConfig, AutomatonContext, DerivedOutput, InputProvenanceFilter,
    ScopeReconcilerWrapper, TransducerWrapper,
};
use crate::runtime::exploration::{ExplorationProvider, ExportFormat};
#[cfg(feature = "messaging")]
use crate::runtime::health_reporter::{HealthReporter, HealthThresholds};
#[cfg(feature = "messaging")]
use crate::runtime::self_observation::{SelfObservationError, SelfObserver, SelfObserverConfig};
use crate::runtime::shutdown::ShutdownConfig;
use crate::runtime::stream::{
    Checkpoint, EventEmitter, RuntimeContext, RuntimeHandles, RuntimeModule, ScanArgs, ServiceInfo,
};
use crate::runtime::{AutomatonLogicError, ScopeReconciler, Transducer};
use crate::runtime::{
    CheckpointManager, CheckpointState, EventTransport, NatsPublisher, SinexError,
};
use camino::Utf8PathBuf;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_primitives::domain::{
    EventSource, EventType, ProcessingMode, SanitizedPath, TriggerKind,
};
use sinex_primitives::events::{DynamicPayload, Event};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{HostName, Id, JsonValue, Uuid};
use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
#[cfg(feature = "messaging")]
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::mpsc;
use xtask::sandbox::prelude::*;

#[derive(Debug, Default, Serialize, Deserialize)]
struct TestDerivedState;

#[derive(Debug, Default, Serialize, Deserialize)]
struct WildcardMaterialOnlyState {
    processed: usize,
}

struct TestAutomaton;

impl Transducer for TestAutomaton {
    type State = TestDerivedState;
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "derived-adapter-test"
    }

    fn input_event_type(&self) -> &'static str {
        "test.input"
    }

    fn output_event_type(&self) -> &'static str {
        "test.output"
    }
    async fn process(
        &mut self,
        _state: &mut Self::State,
        _input: Self::Input,
        _context: &AutomatonContext,
    ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        Ok(None)
    }
}

struct WildcardMaterialOnlyNode;

impl Transducer for WildcardMaterialOnlyNode {
    type State = WildcardMaterialOnlyState;
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "wildcard-material-only"
    }

    fn input_event_type(&self) -> &'static str {
        "*"
    }

    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::MaterialOnly
    }

    fn output_event_type(&self) -> &'static str {
        "ignored.output"
    }
    async fn process(
        &mut self,
        state: &mut Self::State,
        _input: Self::Input,
        _context: &AutomatonContext,
    ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        state.processed += 1;
        Ok(None)
    }
}

struct RetryAutomaton {
    seen: Arc<AtomicUsize>,
}

impl Transducer for RetryAutomaton {
    type State = TestDerivedState;
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "derived-adapter-retry-test"
    }

    fn input_event_type(&self) -> &'static str {
        "test.input"
    }

    fn output_event_type(&self) -> &'static str {
        "test.output"
    }
    async fn process(
        &mut self,
        _state: &mut Self::State,
        _input: Self::Input,
        _context: &AutomatonContext,
    ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        self.seen.fetch_add(1, Ordering::SeqCst);
        Err(AutomatonLogicError::Processing(
            "retry requested".to_string(),
        ))
    }
}

struct EmittingAutomaton;

impl Transducer for EmittingAutomaton {
    type State = TestDerivedState;
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "derived-adapter-emitting-test"
    }

    fn input_event_type(&self) -> &'static str {
        "test.input"
    }

    fn output_event_type(&self) -> &'static str {
        "test.output"
    }
    async fn process(
        &mut self,
        _state: &mut Self::State,
        _input: Self::Input,
        context: &AutomatonContext,
    ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        Ok(Some(DerivedOutput::transduced(
            json!({"ok": true}),
            context.ts_orig.unwrap_or_else(Timestamp::now),
            context.trigger_uuid(),
        )))
    }
}

#[derive(Default, Deserialize)]
struct UnserializableDerivedState;

impl Serialize for UnserializableDerivedState {
    fn serialize<S>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(serde::ser::Error::custom("state serialization exploded"))
    }
}

struct UnserializableAutomaton;

impl Transducer for UnserializableAutomaton {
    type State = UnserializableDerivedState;
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "adapter-regression-unserializable-checkpoint"
    }

    fn input_event_type(&self) -> &'static str {
        "test.input"
    }

    fn output_event_type(&self) -> &'static str {
        "test.output"
    }
    async fn process(
        &mut self,
        _state: &mut Self::State,
        _input: Self::Input,
        _context: &AutomatonContext,
    ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        Ok(None)
    }
}

#[derive(Default, Serialize, Deserialize)]
struct TestScopeReconcilerState;

#[derive(Deserialize)]
struct ScopeReconcilerInput {
    value: i64,
}

#[derive(Serialize)]
struct ScopeReconcilerOutput {
    total: i64,
    count: usize,
}

struct TestScopeReconcilerAutomaton;

impl ScopeReconciler for TestScopeReconcilerAutomaton {
    type State = TestScopeReconcilerState;
    type Input = ScopeReconcilerInput;
    type Output = ScopeReconcilerOutput;

    fn name(&self) -> &'static str {
        "adapter-regression-scope-reconciler"
    }

    fn input_event_type(&self) -> &'static str {
        "measurement.taken"
    }

    fn output_event_type(&self) -> &'static str {
        "measurement.aggregate"
    }
    fn scope_keys(&self, _input: &Self::Input, _context: &AutomatonContext) -> Vec<String> {
        vec!["default".into()]
    }

    async fn reconcile(
        &mut self,
        _state: &mut Self::State,
        scope_key: &str,
        input: Self::Input,
        context: &AutomatonContext,
    ) -> Result<Vec<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        Ok(vec![DerivedOutput::reconciled(
            ScopeReconcilerOutput {
                total: input.value,
                count: 1,
            },
            context.ts_orig.unwrap_or_else(Timestamp::now),
            vec![*context.trigger_event_id.as_uuid()],
            scope_key.to_string(),
        )])
    }

    async fn recompute_scope(
        &mut self,
        _state: &mut Self::State,
        scope_key: &str,
        working_set: Vec<Self::Input>,
        context: &AutomatonContext,
    ) -> Result<Vec<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        if working_set.is_empty() {
            return Ok(Vec::new());
        }

        let total = working_set.iter().map(|input| input.value).sum();
        let count = working_set.len();

        Ok(vec![DerivedOutput::reconciled(
            ScopeReconcilerOutput { total, count },
            context.ts_orig.unwrap_or_else(Timestamp::now),
            vec![*context.trigger_event_id.as_uuid()],
            scope_key.to_string(),
        )])
    }
}

#[derive(Default, Serialize, Deserialize)]
struct StatefulInvalidationState {
    invalidations_applied: u64,
}

struct StatefulInvalidationNode;

impl ScopeReconciler for StatefulInvalidationNode {
    type State = StatefulInvalidationState;
    type Input = ScopeReconcilerInput;
    type Output = ScopeReconcilerOutput;

    fn name(&self) -> &'static str {
        "adapter-regression-stateful-invalidation"
    }

    fn input_event_type(&self) -> &'static str {
        "measurement.taken"
    }

    fn output_event_type(&self) -> &'static str {
        "measurement.aggregate"
    }
    fn scope_keys(&self, _input: &Self::Input, _context: &AutomatonContext) -> Vec<String> {
        vec!["default".into()]
    }

    async fn reconcile(
        &mut self,
        _state: &mut Self::State,
        _scope_key: &str,
        _input: Self::Input,
        _context: &AutomatonContext,
    ) -> Result<Vec<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        Ok(Vec::new())
    }

    async fn recompute_scope(
        &mut self,
        state: &mut Self::State,
        _scope_key: &str,
        _working_set: Vec<Self::Input>,
        _context: &AutomatonContext,
    ) -> Result<Vec<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        state.invalidations_applied += 1;
        Ok(Vec::new())
    }
}

struct DlqRetryAutomaton;

impl Transducer for DlqRetryAutomaton {
    type State = TestDerivedState;
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "derived-adapter-dlq-retry-test"
    }

    fn input_event_type(&self) -> &'static str {
        "test.input"
    }

    fn output_event_type(&self) -> &'static str {
        "test.output"
    }
    async fn process(
        &mut self,
        _state: &mut Self::State,
        _input: Self::Input,
        _context: &AutomatonContext,
    ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        Err(AutomatonLogicError::InputParsing(
            "route me to dlq".to_string(),
        ))
    }
}

fn make_input_event(value: &str) -> std::result::Result<Event<JsonValue>, SinexError> {
    let mut event = DynamicPayload::new("test.source", "test.input", json!({ "value": value }))
        .from_parents([Id::<Event<JsonValue>>::new()])?
        .build()?;
    event.id = Some(event.id.unwrap_or_else(Id::new));
    Ok(event)
}

fn make_material_input_event(
    event_type: &str,
    value: &str,
) -> std::result::Result<Event<JsonValue>, SinexError> {
    let mut event = DynamicPayload::new("test.source", event_type, json!({ "value": value }))
        .from_material(Uuid::now_v7())
        .build()?;
    event.id = Some(event.id.unwrap_or_else(Id::new));
    Ok(event)
}

async fn make_runtime_state(
    ctx: &TestContext,
    module_name: &str,
    module_run_id: Option<Uuid>,
) -> TestResult<RuntimeContext> {
    let kv = ctx.checkpoint_kv().await?;
    let checkpoint_manager = Arc::new(CheckpointManager::new(
        kv,
        module_name.to_string(),
        "test-group".to_string(),
        format!("test-consumer-{}", Uuid::now_v7().simple()),
    ));
    let (event_sender, _event_receiver) = mpsc::channel::<Event<JsonValue>>(32);
    let emitter = EventEmitter::new(event_sender, false);
    let publisher = Arc::new(NatsPublisher::new(ctx.nats_client()));
    let handles = RuntimeHandles::new_edge(
        checkpoint_manager,
        emitter,
        EventTransport::Nats(publisher),
        None,
    );
    let work_dir = tempdir()?;
    let work_dir_path = work_dir.keep();
    let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir_path.clone()).map_err(|path| {
        color_eyre::eyre::eyre!("temporary work dir should be utf-8: {}", path.display())
    })?;
    Ok(RuntimeContext::new(
        ServiceInfo::new(
            module_name.to_string(),
            module_name.to_string(),
            HostName::from_static("test-host"),
            work_dir_path,
            false,
            format!("instance-{}", Uuid::now_v7().simple()),
            env!("CARGO_PKG_VERSION").to_string(),
            module_run_id,
        ),
        handles,
        HashMap::new(),
        work_dir_utf8,
    ))
}

async fn make_runtime_state_with_db(
    ctx: &TestContext,
    module_name: &str,
    module_run_id: Option<Uuid>,
) -> TestResult<(RuntimeContext, mpsc::Receiver<Event<JsonValue>>)> {
    let kv = ctx.checkpoint_kv().await?;
    let checkpoint_manager = Arc::new(CheckpointManager::new(
        kv,
        module_name.to_string(),
        "test-group".to_string(),
        format!("test-consumer-{}", Uuid::now_v7().simple()),
    ));
    let (event_sender, event_receiver) = mpsc::channel::<Event<JsonValue>>(32);
    let emitter = EventEmitter::new(event_sender, false);
    let publisher = Arc::new(NatsPublisher::new(ctx.nats_client()));
    let handles = RuntimeHandles::new(
        ctx.pool().clone(),
        checkpoint_manager,
        emitter,
        EventTransport::Nats(publisher),
        None,
    );
    let work_dir = tempdir()?;
    let work_dir_path = work_dir.keep();
    let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir_path.clone()).map_err(|path| {
        color_eyre::eyre::eyre!("temporary work dir should be utf-8: {}", path.display())
    })?;
    Ok((
        RuntimeContext::new(
            ServiceInfo::new(
                module_name.to_string(),
                module_name.to_string(),
                HostName::from_static("test-host"),
                work_dir_path,
                false,
                format!("instance-{}", Uuid::now_v7().simple()),
                env!("CARGO_PKG_VERSION").to_string(),
                module_run_id,
            ),
            handles,
            HashMap::new(),
            work_dir_utf8,
        ),
        event_receiver,
    ))
}

#[cfg(feature = "messaging")]
async fn make_runtime_state_with_validator(
    ctx: &TestContext,
    module_name: &str,
    module_run_id: Option<Uuid>,
) -> TestResult<(RuntimeContext, mpsc::Receiver<Event<JsonValue>>, Uuid)> {
    let kv = ctx.checkpoint_kv().await?;
    let checkpoint_manager = Arc::new(CheckpointManager::new(
        kv,
        module_name.to_string(),
        "test-group".to_string(),
        format!("test-consumer-{}", Uuid::now_v7().simple()),
    ));
    let (event_sender, event_receiver) = mpsc::channel::<Event<JsonValue>>(32);
    let validator = Arc::new(crate::runtime::schema_validator::RuntimeSchemaValidator::new());
    let schema_id = Uuid::now_v7();
    validator.register_test_schema(
        schema_id,
        module_name,
        "test.output",
        &json!({
            "type": "object",
            "properties": {
                "ok": { "type": "boolean" }
            },
            "required": ["ok"]
        }),
    )?;
    let emitter = EventEmitter::with_validator(event_sender, false, validator);
    let publisher = Arc::new(NatsPublisher::new(ctx.nats_client()));
    let handles = RuntimeHandles::new_edge(
        checkpoint_manager,
        emitter,
        EventTransport::Nats(publisher),
        None,
    );
    let work_dir = tempdir()?;
    let work_dir_path = work_dir.keep();
    let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir_path.clone()).map_err(|path| {
        color_eyre::eyre::eyre!("temporary work dir should be utf-8: {}", path.display())
    })?;
    Ok((
        RuntimeContext::new(
            ServiceInfo::new(
                module_name.to_string(),
                module_name.to_string(),
                HostName::from_static("test-host"),
                work_dir_path,
                false,
                format!("instance-{}", Uuid::now_v7().simple()),
                env!("CARGO_PKG_VERSION").to_string(),
                module_run_id,
            ),
            handles,
            HashMap::new(),
            work_dir_utf8,
        ),
        event_receiver,
        schema_id,
    ))
}

#[sinex_test]
async fn request_runtime_drain_delivers_to_receiver() -> TestResult<()> {
    crate::runtime::stream::test_support::assert_request_drain_delivers_to_receiver("test-derived")
        .await
}

#[sinex_test]
async fn request_runtime_drain_is_idempotent() -> TestResult<()> {
    crate::runtime::stream::test_support::assert_request_drain_is_idempotent("test-derived");
    Ok(())
}

#[sinex_test]
async fn stale_output_ids_or_fail_scope_returns_empty_ids_on_success() -> TestResult<()> {
    let stale_ids = stale_output_ids_or_fail_scope("test-derived", "scope-a", Ok(Vec::new()))
        .expect("successful stale query should return ids");
    assert!(stale_ids.is_empty());
    Ok(())
}

#[sinex_test]
async fn stale_output_ids_or_fail_scope_surfaces_query_error() -> TestResult<()> {
    let error = stale_output_ids_or_fail_scope(
        "test-derived",
        "scope-a",
        Err(SinexError::invalid_state("corrupt stale output row")),
    )
    .expect_err("stale output query errors must fail the invalidation scope");

    let rendered = error.to_string();
    assert!(rendered.contains("Failed to query stale outputs"));
    assert!(rendered.contains("test-derived"));
    assert!(rendered.contains("scope-a"));
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn log_self_observation_failure_accepts_publish_errors() -> TestResult<()> {
    log_self_observation_failure(
        "test-derived",
        "invalidation.errors",
        &SelfObservationError::Publish("boom".to_string()),
    );
    Ok(())
}

#[sinex_test]
async fn derived_source_state_is_unhealthy_before_runtime_initialization() -> TestResult<()> {
    let adapter = AutomatonRuntime::new(TransducerWrapper(TestAutomaton));

    let state = ExplorationProvider::get_source_state(&adapter)?;

    assert!(!state.is_connected);
    assert!(!state.healthy);
    assert_eq!(state.last_updated, None);
    assert_eq!(state.total_items, Some(0));
    assert!(state.description.contains("runtime not initialized"));
    assert_eq!(
        state
            .metadata
            .get("runtime_initialized")
            .and_then(serde_json::Value::as_bool),
        Some(false)
    );
    assert_eq!(
        state
            .metadata
            .get("total_processed")
            .and_then(serde_json::Value::as_u64),
        Some(0)
    );
    assert_eq!(
        state
            .metadata
            .get("run_processed")
            .and_then(serde_json::Value::as_u64),
        Some(0)
    );
    Ok(())
}

#[sinex_test]
async fn derived_source_state_reports_processed_counters() -> TestResult<()> {
    let mut adapter = AutomatonRuntime::new(TransducerWrapper(TestAutomaton));
    adapter.persisted_state.events_processed = 7;
    adapter.run_events_processed = 3;

    let state = ExplorationProvider::get_source_state(&adapter)?;

    assert_eq!(state.total_items, Some(7));
    assert_eq!(
        state
            .metadata
            .get("total_processed")
            .and_then(serde_json::Value::as_u64),
        Some(7)
    );
    assert_eq!(
        state
            .metadata
            .get("run_processed")
            .and_then(serde_json::Value::as_u64),
        Some(3)
    );
    Ok(())
}

#[sinex_test]
async fn derived_ingestion_history_is_explicitly_unavailable() -> TestResult<()> {
    let adapter = AutomatonRuntime::new(TransducerWrapper(TestAutomaton));

    let error = ExplorationProvider::get_ingestion_history(&adapter, 10)
        .expect_err("automatons must not report an empty ingestion history as success");

    assert!(error.to_string().contains("automaton"));
    assert!(error.to_string().contains("ingestion history"));
    Ok(())
}

#[sinex_test]
async fn derived_export_is_explicitly_unavailable() -> TestResult<()> {
    let adapter = AutomatonRuntime::new(TransducerWrapper(TestAutomaton));
    let path = SanitizedPath::from_static("/tmp/derived-export.json");

    let error = ExplorationProvider::export_data(&adapter, &path, ExportFormat::Json)
        .expect_err("automatons must not report export success without writing data");

    assert!(error.to_string().contains("automaton"));
    assert!(error.to_string().contains("data export"));
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn derived_source_state_reflects_failed_health_reporter(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let mut adapter = AutomatonRuntime::new(TransducerWrapper(TestAutomaton));
    adapter.runtime = Some(make_runtime_state(&ctx, "test-derived", None).await?);

    let observer = Arc::new(SelfObserver::new(
        ctx.nats_client(),
        SelfObserverConfig {
            component: "derived-source-state".to_string(),
            namespace: None,
            enabled: true,
            min_emission_interval: Duration::from_millis(10),
        },
    ));
    let reporter = Arc::new(HealthReporter::new(
        "derived-source-state".to_string(),
        observer,
        HealthThresholds {
            error_rate_degraded: 0.05,
            error_rate_failed: 0.20,
            window_seconds: 60,
            emit_stall_seconds: 0,
            refresh_seconds: 900,
        },
    ));
    reporter.record_error(&SinexError::processing("automaton failure"));
    adapter.health_reporter = Some(reporter);

    let state = ExplorationProvider::get_source_state(&adapter)?;

    assert!(state.is_connected);
    assert!(!state.healthy);
    // current_status() is a HealthStatus, whose worst state is Unhealthy (Display:
    // "unhealthy"). The `error_rate_failed` threshold is an internal knob name, not
    // a status value — exceeding it yields HealthStatus::Unhealthy.
    assert!(state.description.contains("status=unhealthy"));
    assert_eq!(
        state
            .metadata
            .get("health_status")
            .and_then(serde_json::Value::as_str),
        Some("unhealthy")
    );
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn derived_health_check_reflects_failed_health_reporter(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let mut adapter = AutomatonRuntime::new(TransducerWrapper(TestAutomaton));
    adapter.runtime = Some(make_runtime_state(&ctx, "test-derived", None).await?);

    let observer = Arc::new(SelfObserver::new(
        ctx.nats_client(),
        SelfObserverConfig {
            component: "derived-health-check".to_string(),
            namespace: None,
            enabled: true,
            min_emission_interval: Duration::from_millis(10),
        },
    ));
    let reporter = Arc::new(HealthReporter::new(
        "derived-health-check".to_string(),
        observer,
        HealthThresholds {
            error_rate_degraded: 0.05,
            error_rate_failed: 0.20,
            window_seconds: 60,
            emit_stall_seconds: 0,
            refresh_seconds: 900,
        },
    ));
    reporter.record_error(&SinexError::processing("automaton failure"));
    adapter.health_reporter = Some(reporter);

    assert!(
        !crate::runtime::stream::RuntimeModule::health_check(&adapter).await?,
        "health_check should fail once the reporter marks the automaton failed"
    );
    Ok(())
}

#[sinex_test]
async fn try_restore_from_file_rejects_missing_state_payload() -> TestResult<()> {
    let temp_dir = tempdir()?;
    let checkpoint_path = temp_dir.path().join("derived-empty-state.checkpoint.json");
    CheckpointState {
        checkpoint: Checkpoint::None,
        processed_count: 0,
        last_activity: Timestamp::now(),
        data: None,
        version: 2,
        revision: 0,
    }
    .save_to_file(&checkpoint_path)
    .await?;

    let mut adapter = AutomatonRuntime::with_shutdown_config(
        TransducerWrapper(TestAutomaton),
        ShutdownConfig {
            checkpoint_path: Some(checkpoint_path.clone()),
            ..ShutdownConfig::default()
        },
    );

    let error = adapter
        .try_restore_from_file()
        .await
        .expect_err("empty hot reload state must not be treated as absent");
    let message = format!("{error:#}");
    assert!(message.contains("missing state data"));
    assert!(message.contains("derived-adapter-test"));
    assert!(message.contains(&checkpoint_path.display().to_string()));
    Ok(())
}

#[sinex_test]
async fn load_state_accepts_fresh_kv_checkpoint_without_state_payload(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let manager = CheckpointManager::new(
        kv,
        "derived-adapter-test".to_string(),
        "test-group".to_string(),
        "fresh-consumer".to_string(),
    );

    let mut adapter = AutomatonRuntime::new(TransducerWrapper(TestAutomaton));
    adapter.checkpoint_manager = Some(Arc::new(manager));
    adapter
        .load_state()
        .await
        .expect("fresh derived checkpoint state should be treated as a clean start");

    assert_eq!(adapter.persisted_state.events_processed, 0);
    assert_eq!(adapter.last_revision, 0);
    Ok(())
}

#[sinex_test]
async fn load_state_rejects_kv_checkpoint_without_state_payload(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let manager = CheckpointManager::new(
        kv.clone(),
        "derived-adapter-test".to_string(),
        "test-group".to_string(),
        "test-consumer".to_string(),
    );
    manager.save_checkpoint(&CheckpointState::default()).await?;

    let mut keys = kv.keys().await?;
    let key = keys.try_next().await?.expect("checkpoint key should exist");
    let corrupt = serde_json::to_vec(&CheckpointState {
        checkpoint: Checkpoint::stream("restored", None),
        processed_count: 0,
        last_activity: Timestamp::now(),
        data: None,
        version: 2,
        revision: 0,
    })?;
    kv.put(&key, corrupt.into()).await?;

    let mut adapter = AutomatonRuntime::new(TransducerWrapper(TestAutomaton));
    adapter.checkpoint_manager = Some(Arc::new(manager));

    let error = adapter
        .load_state()
        .await
        .expect_err("empty derived checkpoint KV state must not be treated as fresh");
    let message = format!("{error:#}");
    assert!(message.contains("missing state data"));
    assert!(message.contains("derived-adapter-test"));
    Ok(())
}

#[sinex_test]
async fn process_batch_halts_on_retry_error() -> TestResult<()> {
    let seen = Arc::new(AtomicUsize::new(0));
    let automaton = RetryAutomaton {
        seen: Arc::clone(&seen),
    };
    let mut adapter = AutomatonRuntime::new(TransducerWrapper(automaton));

    let error = adapter
        .process_batch(vec![
            make_input_event("first")?,
            make_input_event("second")?,
        ])
        .await
        .expect_err("retry errors must stop the batch");

    assert!(
        error.to_string().contains("retry"),
        "retryable batch failure should propagate an explicit error: {error:#}"
    );
    assert_eq!(
        seen.load(Ordering::SeqCst),
        1,
        "batch processing must stop at the first retryable error"
    );
    Ok(())
}

/// Env var carrying the parent's ephemeral NATS connection URL to the child.
/// Its presence ALSO switches this same test function into its child role
/// (the outer/parent role never sets it on itself, only on the spawned
/// child) — one test function serves both roles, so the child gets a real
/// `TestContext`/binary re-invocation for free, no separate harness binary.
/// `.shared()`/`.dedicated()` NATS provisioning
/// (`xtask::sandbox::nats::ephemeral`) is scoped by an in-process registry —
/// it reuses one server across tests WITHIN a process, but the child here is
/// a genuinely separate OS process (re-invoked via `current_exe()`), so
/// `.shared()` in the child would silently connect to a SEPARATE ephemeral
/// server the parent can never see. Passing the parent's already-running
/// server's URL explicitly is what makes cross-process state sharing work.
const R6D9_NATS_URL_ENV: &str = "SINEX_R6D9_NATS_URL";

/// Fixed (not per-test-random) checkpoint identity shared by both the outer
/// harness and inner scenario roles of
/// `r6d9_checkpoint_before_output_fail_point_fires` — the usual
/// `ctx.checkpoint_kv()` per-test-random namespace would prevent the parent
/// from ever finding the bucket the child wrote to, even connected to the
/// same server.
const R6D9_CHECKPOINT_BUCKET: &str = "sinex_r6d9_crash_window_test_checkpoints";
const R6D9_MODULE_NAME: &str = "derived-adapter-r6d9-crash-window-test";
const R6D9_CONSUMER_GROUP: &str = "r6d9-test-group";
const R6D9_CONSUMER_NAME: &str = "r6d9-test-consumer";

async fn r6d9_fixed_checkpoint_manager(
    js: &async_nats::jetstream::Context,
) -> TestResult<CheckpointManager> {
    let kv = sinex_primitives::nats::create_or_open_kv_store(
        js,
        async_nats::jetstream::kv::Config {
            bucket: R6D9_CHECKPOINT_BUCKET.to_string(),
            history: 8,
            ..Default::default()
        },
    )
    .await?;
    Ok(CheckpointManager::new(
        kv,
        R6D9_MODULE_NAME.to_string(),
        R6D9_CONSUMER_GROUP.to_string(),
        R6D9_CONSUMER_NAME.to_string(),
    ))
}

/// sinex-r6d.9 crash-window harness, first scenario: proves BOTH halves of
/// the sinex-vxu checkpoint-before-output data-loss contract using a real
/// NATS KV `CheckpointManager` shared (by fixed bucket/key, not the usual
/// per-test-random namespace — see `r6d9_fixed_checkpoint_manager`) between
/// a child process and its parent:
///
/// 1. INJECTION: the `fail_point_after_checkpoint` hook fires exactly at the
///    boundary sinex-vxu describes — checkpoint durably saved,
///    `process_batch` about to return outputs to its caller for emission
///    but hasn't yet — proven by the child process exiting(97) instead of
///    returning.
/// 2. IRREPARABILITY: after the crash, a fresh `CheckpointManager` pointed
///    at the SAME durable checkpoint (simulating what a restarted process's
///    catch-up would read) shows the input as already processed
///    (`processed_count == 1`) — proving this is not just "the process
///    crashed once" but the exact silent-loss shape sinex-vxu names: a
///    restart's catch-up would skip this input as already-done, yet its
///    derived output was never captured anywhere.
#[sinex_test]
async fn r6d9_checkpoint_before_output_fail_point_fires(ctx: TestContext) -> TestResult<()> {
    if let Ok(nats_url) = std::env::var(R6D9_NATS_URL_ENV) {
        // Child role: connect directly to the PARENT's already-running
        // ephemeral NATS server (see R6D9_NATS_URL_ENV doc) rather than
        // provisioning our own via ctx.with_nats() — this is what makes the
        // checkpoint write below visible to the parent after this process
        // exits.
        let client = async_nats::connect(&nats_url)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("child failed to connect to parent NATS: {e}"))?;
        let js = async_nats::jetstream::new(client);
        let checkpoint_manager = Arc::new(r6d9_fixed_checkpoint_manager(&js).await?);
        let mut adapter = AutomatonRuntime::with_config(
            TransducerWrapper(EmittingAutomaton),
            AutomatonAdapterConfig {
                checkpoint_interval: 1,
                ..AutomatonAdapterConfig::default()
            },
        )
        .with_fail_point_after_checkpoint(Arc::new(std::sync::atomic::AtomicBool::new(true)));
        adapter.checkpoint_manager = Some(checkpoint_manager);

        // The fail point exits the process inside this call, after the
        // checkpoint save succeeds and before outputs are returned. This
        // line intentionally never returns on a correctly-armed fail point.
        let _ = adapter.process_batch(vec![make_input_event("r6d9")?]).await;
        panic!(
            "fail point did not fire: process_batch returned instead of exiting the process \
             after the checkpoint save"
        );
    }

    let ctx = ctx.with_nats().shared().await?;
    let js = ctx.jetstream().await?;
    let nats_url = ctx.nats_handle()?.client_url().to_string();

    // Defensive: purge any stale checkpoint state a prior run of this fixed
    // bucket/key may have left behind, so the post-crash assertion below
    // reflects only THIS run's child.
    let pre_manager = r6d9_fixed_checkpoint_manager(&js).await?;
    let _ = pre_manager.reset_checkpoint().await;

    let exe = std::env::current_exe().map_err(|e| {
        color_eyre::eyre::eyre!("current_exe unavailable for r6d9 fail-point harness: {e}")
    })?;
    // libtest's --exact filter matches the fully qualified test name as
    // libtest itself reports it: the module path WITHOUT the leading crate
    // name (module_path!() includes the crate name; libtest's own test
    // identifiers, as seen in nextest failure output, do not), plus the
    // function name.
    let module_path_without_crate = module_path!()
        .split_once("::")
        .map_or(module_path!(), |(_, rest)| rest);
    let qualified_name = format!(
        "{module_path_without_crate}::r6d9_checkpoint_before_output_fail_point_fires"
    );
    let output = tokio::process::Command::new(exe)
        .arg(&qualified_name)
        .arg("--exact")
        .arg("--nocapture")
        .env(R6D9_NATS_URL_ENV, &nats_url)
        .output()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("failed to spawn r6d9 inner-scenario child: {e}"))?;

    assert_eq!(
        output.status.code(),
        Some(97),
        "sinex-r6d.9 fail point must fire exactly at the checkpoint-saved/outputs-not-yet-\
         returned boundary in process_batch (process.rs) — got exit status {:?} instead of the \
         expected exit(97).\n--- child stdout ---\n{}\n--- child stderr ---\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    // IRREPARABILITY: the crashed child's checkpoint write is durable and
    // visible to a fresh manager over the same bucket/key/NATS server —
    // exactly what a restarted process's catch-up would read. This is the
    // proof that the window is real data loss, not just a crashed process:
    // catch-up would see this input as done and skip it, yet
    // EmittingAutomaton's output for it was never returned to any emission
    // path.
    let post_manager = r6d9_fixed_checkpoint_manager(&js).await?;
    let restored = post_manager.load_checkpoint().await?;
    assert_eq!(
        restored.processed_count, 1,
        "the crashed child's checkpoint must durably record the input as processed \
         (processed_count == 1) even though its output was never returned for emission — \
         this is the silent-loss shape sinex-vxu describes: a restart's catch-up would skip \
         this input as already-done. Got: {restored:?}"
    );

    Ok(())
}
