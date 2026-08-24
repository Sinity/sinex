#[cfg(test)]
#[path = "tests/processing_replay.rs"]
mod processing_replay;
// Inline because these cover a private shutdown-signaling helper.
#[cfg(feature = "messaging")]
use super::log_self_observation_failure;
#[cfg(feature = "messaging")]
use super::recv_invalidation;
use super::{AutomatonRuntime, stale_output_ids_or_fail_scope};
use crate::runtime::automaton::traits::Automaton;
use crate::runtime::automaton::{
    AutomatonAdapterConfig, AutomatonContext, DerivedOutput, DerivedScopeInvalidation,
    INVALIDATION_SUBJECT, InputProvenanceFilter, ScopeReconcilerWrapper, TransducerWrapper,
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
use crate::runtime::{AutomatonLogicError, ScopeReconciler, Transducer, Windowed, WindowedWrapper};
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
struct TestDerivedState {
    processed: usize,
}

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

/// Derivation control-plane declaration for the `EmittingAutomaton` test
/// fixture (sinex-0vx.8: anonymous `declaration_id: None` derived outputs
/// have been rejected since PR #2552 — this fixture predates that gate and
/// needs its own declaration like every real automaton in
/// `automata/registry.rs`). `output_source: None` matches any source so
/// this doesn't need to track `Transducer`'s default
/// `output_event_source()`.
const EMITTING_AUTOMATON_OUTPUT_DECLARATIONS:
    &[sinex_primitives::derivation::DerivationOutputDeclaration] =
    &[sinex_primitives::derivation::DerivationOutputDeclaration {
        declaration_id: "test.derived-adapter-emitting-test.test.output",
        owner: "test",
        product_class: sinex_primitives::derivation::DerivedProductClass::CanonicalDerivedEvent,
        write_surface: sinex_primitives::derivation::DerivationWriteSurface::DerivedOutput,
        output_source: None,
        output_event_type: Some("test.output"),
        projection_kind: None,
        artifact_kind: None,
        proposal_kind: None,
        semantics_version: "1.0.0",
        input_eligibility: sinex_primitives::derivation::InputEligibility::ExplicitOnly,
        default_support: sinex_primitives::derivation::ClaimSupportTemplate::new(
            sinex_primitives::derivation::SupportLevel::Convergent,
            sinex_primitives::derivation::SourceCoverage::Partial,
            sinex_primitives::derivation::ClaimTemporalQuality::DeclaredEffective,
        ),
        verification_command: "xtask test -p sinexd -E 'test(process_one) or test(r6d9)'",
    }];

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

    const OUTPUT_DECLARATIONS:
        &'static [sinex_primitives::derivation::DerivationOutputDeclaration] =
        EMITTING_AUTOMATON_OUTPUT_DECLARATIONS;

    async fn process(
        &mut self,
        state: &mut Self::State,
        _input: Self::Input,
        context: &AutomatonContext,
    ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        state.processed += 1;
        Ok(Some(
            DerivedOutput::transduced(
                json!({"ok": true}),
                context.ts_orig.unwrap_or_else(Timestamp::now),
                context.trigger_uuid(),
            )
            .with_declaration_id(EMITTING_AUTOMATON_OUTPUT_DECLARATIONS[0].declaration_id)
            .with_product_class(EMITTING_AUTOMATON_OUTPUT_DECLARATIONS[0].product_class)
            .with_claim_support(sinex_primitives::derivation::ClaimSupport::unknown()),
        ))
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

const SCOPE_RECONCILER_OUTPUT_DECLARATION:
    sinex_primitives::derivation::DerivationOutputDeclaration =
    sinex_primitives::derivation::DerivationOutputDeclaration {
        declaration_id: "test.derived-adapter-scope-reconciler.measurement.aggregate",
        owner: "test",
        product_class: sinex_primitives::derivation::DerivedProductClass::CanonicalDerivedEvent,
        write_surface: sinex_primitives::derivation::DerivationWriteSurface::DerivedOutput,
        output_source: Some("adapter-regression-scope-reconciler"),
        output_event_type: Some("measurement.aggregate"),
        projection_kind: None,
        artifact_kind: None,
        proposal_kind: None,
        semantics_version: "1.0.0",
        input_eligibility: sinex_primitives::derivation::InputEligibility::ExplicitOnly,
        default_support: sinex_primitives::derivation::ClaimSupportTemplate::new(
            sinex_primitives::derivation::SupportLevel::Convergent,
            sinex_primitives::derivation::SourceCoverage::Partial,
            sinex_primitives::derivation::ClaimTemporalQuality::DeclaredEffective,
        ),
        verification_command: "xtask test -p sinexd -E 'test(scope_reconciler_invalidation)'",
    };

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
    const OUTPUT_DECLARATIONS:
        &'static [sinex_primitives::derivation::DerivationOutputDeclaration] =
        &[SCOPE_RECONCILER_OUTPUT_DECLARATION];
    fn scope_keys(&self, _input: &Self::Input, _context: &AutomatonContext) -> Vec<String> {
        vec!["default".into()]
    }

    fn supports_scope_invalidation_recompute(&self) -> bool {
        true
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

        Ok(vec![
            DerivedOutput::reconciled(
                ScopeReconcilerOutput { total, count },
                context.ts_orig.unwrap_or_else(Timestamp::now),
                vec![*context.trigger_event_id.as_uuid()],
                scope_key.to_string(),
            )
            .with_declaration_id(SCOPE_RECONCILER_OUTPUT_DECLARATION.declaration_id)
            .with_product_class(SCOPE_RECONCILER_OUTPUT_DECLARATION.product_class)
            .with_claim_support(sinex_primitives::derivation::ClaimSupport::unknown())
            .with_event_type("measurement.aggregate"),
        ])
    }
}

#[derive(Default, Serialize, Deserialize)]
struct StatefulInvalidationState {
    invalidations_applied: u64,
}

struct StatefulInvalidationNode {
    allow_scope_recompute: bool,
}

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

    fn supports_scope_invalidation_recompute(&self) -> bool {
        self.allow_scope_recompute
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

/// Like `make_runtime_state`, but wires an explicit `SettlementRegistry`
/// into the returned `RuntimeContext` (sinex-vxu) instead of the default
/// disconnected one, so a caller-side `emit_batch_durable` can actually
/// observe settlement. The context's own internal emitter/channel is
/// unused by callers that install their own `adapter.event_emitter`
/// (matching `make_runtime_state`'s existing `_event_receiver` pattern).
async fn make_runtime_state_with_registry(
    ctx: &TestContext,
    module_name: &str,
    registry: crate::runtime::durable_emission::SettlementRegistry,
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
    )
    .with_settlement_registry(registry);
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
            None,
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
    let settlement_registry = crate::runtime::durable_emission::SettlementRegistry::new();
    let _settler = auto_settle_events(event_receiver, settlement_registry.clone());
    let publisher = Arc::new(NatsPublisher::new(ctx.nats_client()));
    let handles = RuntimeHandles::new(
        ctx.pool().clone(),
        checkpoint_manager,
        emitter,
        EventTransport::Nats(publisher),
        None,
    )
    .with_settlement_registry(settlement_registry);
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
        // The helper's returned receiver is retained for API compatibility;
        // the settlement forwarder above owns the actual emitter receiver so
        // historical replay tests exercise the production receipt route.
        mpsc::channel::<Event<JsonValue>>(1).1,
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

#[sinex_test]
async fn scope_reconciler_invalidation_preserves_output_event_type() -> TestResult<()> {
    let input = DynamicPayload::new(
        "measurements",
        "measurement.taken",
        json!({ "value": 5_i64 }),
    )
    .from_material(Uuid::now_v7())
    .build()?;
    let context = AutomatonContext {
        trigger_event_id: Id::new(),
        source: EventSource::from_static("measurements"),
        event_type: EventType::from_static("measurement.taken"),
        ts_orig: None,
        ts_coided: Timestamp::now(),
        processing_mode: ProcessingMode::Replay,
        trigger_kind: TriggerKind::ScopeInvalidation,
        created_by_operation_id: None,
        trigger_material_id: None,
        trigger_anchor_byte: None,
    };
    let mut reconciler = ScopeReconcilerWrapper(TestScopeReconcilerAutomaton);
    let mut state = TestScopeReconcilerState;

    let outputs = reconciler
        .process_invalidation_derived(&mut state, "scope:measurements", vec![input], &context)
        .await?;

    assert_eq!(
        outputs.len(),
        1,
        "recomputation fixture must emit one output"
    );
    assert_eq!(
        outputs[0].event_type,
        Some("measurement.aggregate"),
        "the wrapper must preserve an event type selected by recompute_scope"
    );
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
#[ignore = "sinex-g9sy open: record_processed_input has no monotonicity guard on \
            last_input_event_id -- an out-of-order (older) input silently overwrites a \
            newer high-water mark, unlike the sibling max_input_ts_orig field which does guard"]
async fn record_processed_input_does_not_regress_last_input_event_id() -> TestResult<()> {
    let mut adapter = AutomatonRuntime::new(TransducerWrapper(TestAutomaton));

    let newer_id: Id<Event<JsonValue>> =
        Uuid::from_u128(0xffff_ffff_ffff_ffff_ffff_ffff_ffff_ffff).into();
    let older_id: Id<Event<JsonValue>> =
        Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_0001).into();

    adapter.record_processed_input(newer_id, None);
    adapter.record_processed_input(older_id, None);

    assert_eq!(
        adapter.persisted_state.last_input_event_id,
        Some(*newer_id.as_uuid()),
        "last_input_event_id regressed from the newer input to an older one -- \
         record_processed_input has no monotonicity guard, unlike max_input_ts_orig"
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

/// sinex-r6d.9 crash-window harness, first scenario — FLIPPED (sinex-vxu
/// fix): this test used to prove the checkpoint-before-output data-loss
/// window was reachable (child exits(97) after a checkpoint save that
/// preceded output durability, and a fresh `CheckpointManager` then showed
/// `processed_count == 1` even though the output was never captured
/// anywhere). After the sinex-vxu reorder (`process_batch` now routes every
/// input through `prepare_one` + `commit_prepared_inputs`, which durably
/// emits outputs via `emit_batch_durable` and only calls
/// `record_processed_input` for inputs whose receipt actually unlocked
/// progress — see `process.rs`), that window is no longer reachable at all:
///
/// 1. SAFETY: with no event emitter wired (deliberately reproducing the
///    OLD test's exact setup — nothing this automaton could possibly emit
///    through), `process_batch` can never durably confirm
///    `EmittingAutomaton`'s output, so it must never call
///    `record_processed_input` for it either. `should_checkpoint()` then
///    never sees a dirty counter to save, so the checkpoint save the fail
///    point is armed on top of is never reached — the fail point does NOT
///    fire, and the child exits normally (not via `std::process::exit(97)`).
/// 2. NO FALSE ADVANCEMENT: a fresh `CheckpointManager` pointed at the SAME
///    durable checkpoint (simulating what a restarted process's catch-up
///    would read) shows `processed_count == 0` — proving the checkpoint was
///    never falsely advanced past an input whose output could not be
///    durably confirmed. The crash window sinex-vxu described (checkpoint
///    durably advanced while its output was never captured anywhere) is now
///    structurally impossible: a checkpoint save can only be reached once
///    every input it reflects has an already-durable outcome.
///
/// See `process_batch_advances_checkpoint_only_after_durable_emission_settles`
/// below for the complementary POSITIVE-path proof (with a properly wired,
/// auto-settling registry, the checkpoint DOES advance correctly) — that
/// test exercises the exact same `commit_prepared_inputs` code path
/// in-process, without needing a second child-process scenario here.
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

        // sinex-vxu fix: no event_emitter/settlement registry is wired here
        // — deliberately reproducing the OLD test's exact setup. Under the
        // FIXED code, `process_batch` has no way to durably confirm
        // `EmittingAutomaton`'s output at all, so it must never mark the
        // input processed either — the checkpoint-before-output window the
        // fail point targets must be structurally unreachable, not merely
        // empirically avoided. A clean, non-crashing return (with nothing
        // committed) is therefore the CORRECT outcome now.
        let committed = adapter
            .process_batch(vec![make_input_event("r6d9")?])
            .await
            .expect(
                "process_batch must not error merely because durable emission cannot be \
                 attempted (no event emitter wired) — it should simply commit nothing",
            );
        assert!(
            committed.is_empty(),
            "sinex-vxu fix: with no event emitter wired, nothing can be durably confirmed, so \
             no input may be marked processed — got {committed:?}"
        );
        return Ok(());
    }

    let ctx = ctx.with_nats().shared().await?;
    let js = ctx.jetstream().await?;
    let nats_url = ctx.nats_handle()?.client_url().to_string();

    // Defensive: purge any stale checkpoint state a prior run of this fixed
    // bucket/key may have left behind, so the post-run assertion below
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
    let qualified_name =
        format!("{module_path_without_crate}::r6d9_checkpoint_before_output_fail_point_fires");
    let output = tokio::process::Command::new(exe)
        .arg(&qualified_name)
        .arg("--exact")
        .arg("--nocapture")
        .env(R6D9_NATS_URL_ENV, &nats_url)
        .output()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("failed to spawn r6d9 inner-scenario child: {e}"))?;

    assert!(
        output.status.success(),
        "sinex-vxu fix: the checkpoint-before-output crash window must no longer be reachable \
         — the child should return normally (having committed nothing) instead of the fail \
         point ever firing. Got exit status {:?}.\n--- child stdout ---\n{}\n--- child stderr ---\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    // NO FALSE ADVANCEMENT: the checkpoint this fixed bucket/key points at
    // must still be exactly what `reset_checkpoint` left it as — the
    // crashed-and-lost-output shape sinex-vxu described (checkpoint
    // durably advanced, output never captured anywhere) is now provably
    // impossible: `processed_count` never moved off zero because
    // `commit_prepared_inputs` never had a durable outcome to commit.
    let post_manager = r6d9_fixed_checkpoint_manager(&js).await?;
    let restored = post_manager.load_checkpoint().await?;
    assert_eq!(
        restored.processed_count, 0,
        "sinex-vxu fix: the checkpoint must NOT have advanced — no input's output was ever \
         durably confirmed, so none should be marked processed. Got: {restored:?}"
    );

    Ok(())
}

/// Complementary POSITIVE-path proof for the sinex-vxu fix, exercised
/// entirely in-process (no cross-process fail point needed): with a
/// properly wired, auto-settling `SettlementRegistry` — the same pattern
/// `adapter_source_test.rs::auto_settle_events` uses for the sinex-r6d.11
/// reference caller — `process_batch` DOES durably emit the output and DOES
/// advance the checkpoint, and it does so only AFTER the durable-emission
/// receipt actually unlocked progress (proven directly: withholding
/// settlement entirely, with a short timeout, leaves the checkpoint
/// untouched; wiring the auto-resolver lets it advance).
#[sinex_test]
async fn process_batch_advances_checkpoint_only_after_durable_emission_settles(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    // Negative half: an emitter IS wired, but nothing ever resolves the
    // registry — settlement must time out, and the checkpoint must stay
    // untouched (mirrors the r6d9 harness's safety property, but as a fast
    // in-process assertion instead of a cross-process crash).
    {
        let (event_sender, _event_receiver) = mpsc::channel::<Event<JsonValue>>(8);
        let emitter = EventEmitter::new(event_sender, false);
        let mut adapter = AutomatonRuntime::new(TransducerWrapper(EmittingAutomaton))
            .with_durable_emission_timeout(std::time::Duration::from_millis(100));
        adapter.event_emitter = Some(emitter);

        let committed = adapter
            .process_batch(vec![make_input_event("unsettled")?])
            .await?;
        assert!(
            committed.is_empty(),
            "an unresolved durable-emission receipt must not be treated as processed"
        );
        assert_eq!(
            adapter.persisted_state.events_processed, 0,
            "the checkpoint must not advance while durable emission is still unresolved"
        );
        assert_eq!(
            adapter.persisted_state.state.processed, 0,
            "unsettled output must roll back the automaton state delta as well as input progress"
        );
        assert_eq!(adapter.current_checkpoint_internal(), Checkpoint::None);
    }

    // Positive half: an auto-settling registry resolves every emitted event
    // as PersistedConfirmed the moment it's observed on the channel — the
    // checkpoint must then advance to reflect exactly that input.
    {
        let (event_sender, event_receiver) = mpsc::channel::<Event<JsonValue>>(8);
        let emitter = EventEmitter::new(event_sender, false);
        let registry = crate::runtime::durable_emission::SettlementRegistry::new();
        let _forwarder = auto_settle_events(event_receiver, registry.clone());
        let runtime =
            make_runtime_state_with_registry(&ctx, "derived-adapter-emitting-test", registry)
                .await?;

        let mut adapter = AutomatonRuntime::new(TransducerWrapper(EmittingAutomaton));
        adapter.event_emitter = Some(emitter);
        adapter.runtime = Some(runtime);

        let input = make_input_event("settled")?;
        let input_id = input.id.expect("test input must have an id");
        let committed = adapter.process_batch(vec![input]).await?;

        assert_eq!(
            committed.len(),
            1,
            "the input's output settled durably and must be reported as committed"
        );
        assert_eq!(
            adapter.persisted_state.events_processed, 1,
            "the checkpoint must advance once durable emission actually settles"
        );
        assert_eq!(
            adapter.current_checkpoint_internal(),
            Checkpoint::internal(*input_id.as_uuid(), 1)
        );
    }

    Ok(())
}

// -------------------------------------------------------------------------
// sinex-vxu remaining scope: timer_flush has no state-commit barrier
// -------------------------------------------------------------------------
//
// process_batch_advances_checkpoint_only_after_durable_emission_settles
// (above) proves the live-bridge path IS gated on durable-emission
// settlement. `AutomatonRuntime::timer_flush` (process.rs) is a completely
// separate code path used by every Windowed automaton's trailing-bucket
// flush -- it calls `emit_output_events` directly and returns the emitted
// count with NO settlement check at all, unlike `commit_prepared_inputs`'s
// per-input gating. If a shutdown/checkpoint-save happens right after
// `timer_flush` mutates `persisted_state.state` (e.g. resetting a window
// accumulator) but before the emitted event durably lands, that window's
// data is gone with nothing to replay it from -- CLAUDE.md's own note on
// this bead: "historical replay/timer_flush/shutdown still open".

#[derive(Debug, Default, Serialize, Deserialize)]
struct FlushBarrierState {
    has_pending_window: bool,
}

const FLUSH_BARRIER_OUTPUT_DECLARATIONS:
    &[sinex_primitives::derivation::DerivationOutputDeclaration] =
    &[sinex_primitives::derivation::DerivationOutputDeclaration {
        declaration_id: "test.derived-windowed-flush-barrier-test.test.output",
        owner: "test",
        product_class: sinex_primitives::derivation::DerivedProductClass::CanonicalDerivedEvent,
        write_surface: sinex_primitives::derivation::DerivationWriteSurface::DerivedOutput,
        output_source: None,
        output_event_type: Some("test.output"),
        projection_kind: None,
        artifact_kind: None,
        proposal_kind: None,
        semantics_version: "1.0.0",
        input_eligibility: sinex_primitives::derivation::InputEligibility::ExplicitOnly,
        default_support: sinex_primitives::derivation::ClaimSupportTemplate::new(
            sinex_primitives::derivation::SupportLevel::Convergent,
            sinex_primitives::derivation::SourceCoverage::Partial,
            sinex_primitives::derivation::ClaimTemporalQuality::DeclaredEffective,
        ),
        verification_command: "xtask test -p sinexd -E 'test(timer_flush_does_not_clear)'",
    }];

/// Minimal Windowed fixture: `flush_due` is true whenever a window is open;
/// `emit` unconditionally clears it (mirroring every real Windowed
/// automaton's `state.reset_hour()`-shaped emit, e.g.
/// `crate::automata::hourly::HourlySummarizer`).
struct EmittingWindowedAutomaton;

impl Windowed for EmittingWindowedAutomaton {
    type State = FlushBarrierState;
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "derived-windowed-flush-barrier-test"
    }

    fn input_event_type(&self) -> &'static str {
        "test.input"
    }

    fn output_event_type(&self) -> &'static str {
        "test.output"
    }

    const OUTPUT_DECLARATIONS:
        &'static [sinex_primitives::derivation::DerivationOutputDeclaration] =
        FLUSH_BARRIER_OUTPUT_DECLARATIONS;

    async fn accumulate(
        &mut self,
        state: &mut Self::State,
        _input: Self::Input,
        _context: &AutomatonContext,
    ) -> Result<(), AutomatonLogicError> {
        state.has_pending_window = true;
        Ok(())
    }

    fn window_complete(&self, _state: &Self::State) -> bool {
        false
    }

    fn flush_due(&self, state: &Self::State, _now: Timestamp) -> bool {
        state.has_pending_window
    }

    async fn emit(
        &mut self,
        state: &mut Self::State,
        _context: &AutomatonContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        // The exact shape of the bug: state is reset as part of emit, with
        // no check of whether the output below actually settles durably.
        state.has_pending_window = false;
        let declaration = &FLUSH_BARRIER_OUTPUT_DECLARATIONS[0];
        Ok(Some(
            DerivedOutput::windowed(json!({"ok": true}), Timestamp::now(), vec![Uuid::now_v7()])
                .with_declaration_id(declaration.declaration_id)
                .with_product_class(declaration.product_class)
                .with_claim_support(declaration.default_support.instantiate(1, 0, 1, 0)),
        ))
    }
}

/// sinex-vxu open (remaining scope): `timer_flush` must not clear
/// `persisted_state.state` past an emitted output whose durable-emission
/// receipt never settles -- the same barrier `process_batch` already has
/// via `commit_prepared_inputs`. Currently it has none: this proves
/// `has_pending_window` is cleared even though the registry below never
/// resolves the emitted event, so a shutdown/checkpoint-save landing here
/// would durably lose the window with no receipt ever having unlocked it.
#[sinex_test]
async fn timer_flush_does_not_clear_window_state_before_emission_settles(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let registry = crate::runtime::durable_emission::SettlementRegistry::new();
    // Deliberately never resolved -- the crash window this bead names.
    let (event_sender, _event_receiver) = mpsc::channel::<Event<JsonValue>>(8);
    let emitter = EventEmitter::new(event_sender, false);
    let runtime =
        make_runtime_state_with_registry(&ctx, "derived-windowed-flush-barrier-test", registry)
            .await?;

    let mut adapter = AutomatonRuntime::new(WindowedWrapper(EmittingWindowedAutomaton))
        .with_durable_emission_timeout(std::time::Duration::from_millis(100));
    adapter.event_emitter = Some(emitter);
    adapter.runtime = Some(runtime);
    adapter.persisted_state.state.has_pending_window = true;

    let error = adapter
        .timer_flush(Timestamp::now())
        .await
        .expect_err("unsettled timer output must not be reported as successful");

    assert!(
        error.to_string().contains("durable settlement"),
        "timer flush must surface the unsettled receipt: {error}"
    );
    assert!(
        adapter.persisted_state.state.has_pending_window,
        "timer_flush must not clear the window's state past an output whose durable-emission \
         receipt never settled -- doing so unconditionally is exactly sinex-vxu's remaining \
         crash-loss window"
    );
    Ok(())
}

#[sinex_test]
async fn timer_flush_checkpoints_state_only_after_durable_settlement(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let registry = crate::runtime::durable_emission::SettlementRegistry::new();
    let (event_sender, event_receiver) = mpsc::channel::<Event<JsonValue>>(8);
    let _settler = auto_settle_events(event_receiver, registry.clone());
    let emitter = EventEmitter::new(event_sender, false);
    let runtime =
        make_runtime_state_with_registry(&ctx, "derived-windowed-flush-checkpoint-test", registry)
            .await?;
    let checkpoint_manager = runtime.checkpoint_manager();

    let mut adapter = AutomatonRuntime::new(WindowedWrapper(EmittingWindowedAutomaton))
        .with_durable_emission_timeout(std::time::Duration::from_millis(500));
    adapter.checkpoint_manager = Some(checkpoint_manager.clone());
    adapter.event_emitter = Some(emitter);
    adapter.runtime = Some(runtime);
    adapter.persisted_state.state.has_pending_window = true;

    assert_eq!(adapter.timer_flush(Timestamp::now()).await?, 1);
    let checkpoint = checkpoint_manager.load_checkpoint().await?;
    assert_eq!(
        checkpoint
            .data
            .as_ref()
            .and_then(|data| data.get("state"))
            .and_then(|state| state.get("has_pending_window"))
            .and_then(JsonValue::as_bool),
        Some(false),
        "timer checkpoint must contain the post-flush state"
    );
    Ok(())
}

#[sinex_test]
async fn shutdown_saves_pre_flush_state_after_unsettled_timer_output(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let registry = crate::runtime::durable_emission::SettlementRegistry::new();
    let (event_sender, _event_receiver) = mpsc::channel::<Event<JsonValue>>(8);
    let emitter = EventEmitter::new(event_sender, false);
    let runtime =
        make_runtime_state_with_registry(&ctx, "derived-windowed-flush-shutdown-test", registry)
            .await?;
    let checkpoint_dir = tempdir()?;
    let checkpoint_path = checkpoint_dir.path().join("shutdown.checkpoint.json");
    let mut adapter = AutomatonRuntime::with_shutdown_config(
        WindowedWrapper(EmittingWindowedAutomaton),
        ShutdownConfig {
            checkpoint_path: Some(checkpoint_path.clone()),
            ..ShutdownConfig::default()
        },
    )
    .with_durable_emission_timeout(std::time::Duration::from_millis(100));
    adapter.event_emitter = Some(emitter);
    adapter.runtime = Some(runtime);
    adapter.persisted_state.state.has_pending_window = true;

    adapter
        .timer_flush(Timestamp::now())
        .await
        .expect_err("unsettled timer output must block the flush");
    RuntimeModule::shutdown(&mut adapter).await?;

    let checkpoint = CheckpointState::load_from_file(&checkpoint_path)
        .await?
        .expect("shutdown must leave a checkpoint file");
    assert_eq!(
        checkpoint
            .data
            .as_ref()
            .and_then(|data| data.get("state"))
            .and_then(|state| state.get("has_pending_window"))
            .and_then(JsonValue::as_bool),
        Some(true),
        "shutdown must not persist state past an unsettled timer receipt"
    );
    Ok(())
}

/// Drain `raw`, resolving every emitted event's id as `PersistedConfirmed`
/// in `registry` before forwarding it — a minimal stand-in for the real
/// event-engine's settlement call sites (`jetstream_consumer/persist.rs`),
/// matching `adapter_source_test.rs::auto_settle_events`'s pattern. Spawned
/// as a background task; returns the join handle purely so callers can keep
/// it alive for the scope that needs it (dropping the handle does not abort
/// the task).
fn auto_settle_events(
    mut raw: mpsc::Receiver<Event<JsonValue>>,
    registry: crate::runtime::durable_emission::SettlementRegistry,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = raw.recv().await {
            if let Some(id) = event.id {
                registry.resolve(
                    id,
                    crate::runtime::durable_emission::EmissionReceiptState::PersistedConfirmed {
                        lane: sinex_db::repositories::EventStorageLane::Activity,
                        inserted: true,
                        confirmed_sequence: None,
                    },
                );
            }
        }
    })
}

// -------------------------------------------------------------------------
// sinex-r6d.9 crash-window harness, second scenario: invalidation-ack
// -------------------------------------------------------------------------

const R6D9_INVALIDATION_DELIVER_GROUP: &str = "derived.invalidation.r6d9-crash-window-test";

async fn r6d9_invalidation_stream(
    js: &async_nats::jetstream::Context,
) -> TestResult<(async_nats::jetstream::stream::Stream, String)> {
    let env = sinex_primitives::environment::environment();
    let stream_name = env.nats_stream_name("SINEX_RAW_EVENTS_DERIVED_INVALIDATIONS");
    let invalidation_subject = env.nats_subject(INVALIDATION_SUBJECT);
    let stream = js
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: stream_name,
            subjects: vec![invalidation_subject.clone()],
            ..Default::default()
        })
        .await?;
    Ok((stream, invalidation_subject))
}

async fn r6d9_invalidation_push_consumer(
    stream: &async_nats::jetstream::stream::Stream,
    client: &async_nats::Client,
) -> TestResult<async_nats::jetstream::consumer::push::Messages> {
    let deliver_subject = client.new_inbox();
    let consumer = stream
        .create_consumer(async_nats::jetstream::consumer::push::Config {
            deliver_subject,
            deliver_group: Some(R6D9_INVALIDATION_DELIVER_GROUP.to_string()),
            ..Default::default()
        })
        .await?;
    Ok(consumer.messages().await?)
}

/// sinex-r6d.9 crash-window harness, second scenario: proves — empirically,
/// not just from reading `async-nats` source — whether the invalidation-ack
/// window (sinex-r6d.7, sinex-vxu) that `recv_invalidation` exposes is
/// PERMANENT data loss, or self-heals on any restart within the invalidation
/// stream's own retention window.
///
/// Same cross-process shape as `r6d9_checkpoint_before_output_fail_point_fires`:
/// one test function serves both an outer harness role (spawns a re-invoked
/// child sharing the parent's NATS server) and an inner scenario role
/// (switched by `R6D9_NATS_URL_ENV`'s presence).
///
/// 1. INJECTION: the child acks a real invalidation message via
///    `recv_invalidation`, then exits(98) — the exact sinex-r6d.7 window:
///    the message is durably, permanently removed from THIS consumer's
///    redelivery queue, before debounce/recompute/checkpoint ever runs.
/// 2. SELF-HEALING CHECK: the parent then creates a FRESH ephemeral push
///    consumer against the SAME stream (simulating `run_continuous`
///    resubscribing after a real restart) and polls for the same
///    invalidation payload. `async-nats` source review this session
///    concluded ephemeral consumers get a server-generated name each
///    creation, with no inherited ack/delivery state, and
///    `DeliverPolicy::All` (the config default) delivers everything still
///    present in the stream — so a restart's fresh consumer should see the
///    message again. This test proves that conclusion instead of trusting it.
#[sinex_test]
async fn r6d9_invalidation_ack_fail_point_fires(ctx: TestContext) -> TestResult<()> {
    if let Ok(nats_url) = std::env::var(R6D9_NATS_URL_ENV) {
        // Child role: connect directly to the parent's already-running
        // ephemeral NATS server (see R6D9_NATS_URL_ENV doc on the checkpoint
        // scenario above).
        let client = async_nats::connect(&nats_url)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("child failed to connect to parent NATS: {e}"))?;
        let js = async_nats::jetstream::new(client.clone());
        let (stream, _subject) = r6d9_invalidation_stream(&js).await?;
        let mut messages = Some(r6d9_invalidation_push_consumer(&stream, &client).await?);
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(true));

        // The fail point exits the process inside this call, right after the
        // ack succeeds and before the payload is returned to the caller for
        // debounce/recompute. This line intentionally never returns on a
        // correctly-armed fail point.
        let _ = recv_invalidation(&mut messages, Some(&flag)).await;
        panic!(
            "fail point did not fire: recv_invalidation returned instead of exiting the \
             process after the ack succeeded"
        );
    }

    let ctx = ctx.with_nats().shared().await?;
    let js = ctx.jetstream().await?;
    let nats_client = ctx.nats_client();
    let nats_url = ctx.nats_handle()?.client_url().to_string();

    let (stream, invalidation_subject) = r6d9_invalidation_stream(&js).await?;

    let invalidation = DerivedScopeInvalidation::archived(
        vec![Uuid::now_v7()],
        sinex_primitives::domain::EventSource::from_static("test.r6d9-invalidation-crash"),
        sinex_primitives::domain::EventType::new("test.r6d9_invalidation_crash")
            .expect("valid event type"),
    );
    let payload = serde_json::to_vec(&invalidation)?;
    js.publish(invalidation_subject, payload.clone().into())
        .await?
        .await?;

    let exe = std::env::current_exe().map_err(|e| {
        color_eyre::eyre::eyre!("current_exe unavailable for r6d9 fail-point harness: {e}")
    })?;
    let module_path_without_crate = module_path!()
        .split_once("::")
        .map_or(module_path!(), |(_, rest)| rest);
    let qualified_name =
        format!("{module_path_without_crate}::r6d9_invalidation_ack_fail_point_fires");
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
        Some(98),
        "sinex-r6d.9 fail point must fire exactly at the ack-succeeded/payload-not-yet-\
         returned boundary in recv_invalidation (adapter/mod.rs) — got exit status {:?} \
         instead of the expected exit(98).\n--- child stdout ---\n{}\n--- child stderr ---\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    // SELF-HEALING CHECK: a fresh ephemeral consumer, simulating a restart's
    // resubscription, must see the same invalidation payload again — proving
    // the ack-before-recompute window self-heals on restart rather than
    // permanently losing the invalidation.
    let mut restart_messages = r6d9_invalidation_push_consumer(&stream, &nats_client).await?;
    use futures::StreamExt;
    let redelivered = tokio::time::timeout(Duration::from_secs(10), restart_messages.next())
        .await
        .map_err(|_| {
            color_eyre::eyre::eyre!(
                "sinex-r6d.7: a fresh consumer (simulating restart) did NOT see the \
                 invalidation again within 10s — the ack-before-recompute window is \
                 PERMANENT data loss for this consumer shape, not self-healing"
            )
        })?;
    let redelivered_msg = redelivered
        .ok_or_else(|| color_eyre::eyre::eyre!("invalidation consumer message stream ended"))?
        .map_err(|e| color_eyre::eyre::eyre!("error receiving redelivered invalidation: {e}"))?;
    assert_eq!(
        redelivered_msg.payload.to_vec(),
        payload,
        "the redelivered message must be the SAME invalidation payload the child acked"
    );
    redelivered_msg.ack().await.map_err(|e| {
        color_eyre::eyre::eyre!("failed to ack redelivered invalidation in restart check: {e}")
    })?;

    Ok(())
}

/// sinex-r6d.7: closes the one gap `r6d9_invalidation_ack_fail_point_fires`
/// leaves open — that harness's consumer omits `deliver_group`, but
/// `run_continuous`'s real production config
/// (`adapter/run.rs::run_continuous`) sets `deliver_group:
/// Some(format!("derived.invalidation.{automaton_name}"))` on every
/// `create_consumer` call, with no `durable_name`. If a *shared* group name
/// caused the JetStream server to hand back the SAME underlying consumer
/// (and therefore the same ack floor) across restarts instead of a fresh
/// ephemeral one, the self-healing conclusion above would not actually hold
/// for production. This test creates two push consumers against the same
/// stream with the EXACT same `deliver_group` string (simulating a crash +
/// restart of `run_continuous` for one automaton), acks a message on the
/// first, and asserts the second still receives it — proving `deliver_group`
/// does not carry ack state across separate `create_consumer` calls.
#[sinex_test]
async fn r6d9_invalidation_deliver_group_does_not_share_ack_state(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let js = ctx.jetstream().await?;
    let nats_client = ctx.nats_client();

    let (stream, invalidation_subject) = r6d9_invalidation_stream(&js).await?;

    let invalidation = DerivedScopeInvalidation::archived(
        vec![Uuid::now_v7()],
        sinex_primitives::domain::EventSource::from_static("test.r6d9-invalidation-deliver-group"),
        sinex_primitives::domain::EventType::new("test.r6d9_invalidation_deliver_group")
            .expect("valid event type"),
    );
    let payload = serde_json::to_vec(&invalidation)?;
    js.publish(invalidation_subject, payload.clone().into())
        .await?
        .await?;

    // Exact production config shape from `run_continuous`: a fresh
    // `deliver_subject` inbox per call, but the SAME `deliver_group` string
    // both times — no `durable_name`, matching `async_nats::jetstream::
    // consumer::push::Config { deliver_subject, deliver_group, ..Default::default() }`.
    let queue_group = "derived.invalidation.r6d9-deliver-group-test".to_string();

    use futures::StreamExt;

    let first_deliver_subject = nats_client.new_inbox();
    let first_consumer = stream
        .create_consumer(async_nats::jetstream::consumer::push::Config {
            deliver_subject: first_deliver_subject,
            deliver_group: Some(queue_group.clone()),
            ..Default::default()
        })
        .await?;
    let mut first = first_consumer.messages().await?;
    let first_msg = tokio::time::timeout(Duration::from_secs(10), first.next())
        .await
        .map_err(|_| {
            color_eyre::eyre::eyre!(
                "first deliver_group consumer never received the published invalidation"
            )
        })?
        .ok_or_else(|| color_eyre::eyre::eyre!("first consumer message stream ended"))?
        .map_err(|e| color_eyre::eyre::eyre!("error receiving on first consumer: {e}"))?;
    assert_eq!(first_msg.payload.to_vec(), payload);
    first_msg
        .ack()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("failed to ack on first consumer: {e}"))?;
    // Drop the first consumer's message stream — simulates the process
    // exiting after ack, before the payload is handled (the sinex-r6d.7
    // window), then restarting.
    drop(first);

    let second_deliver_subject = nats_client.new_inbox();
    let second_consumer = stream
        .create_consumer(async_nats::jetstream::consumer::push::Config {
            deliver_subject: second_deliver_subject,
            deliver_group: Some(queue_group.clone()),
            ..Default::default()
        })
        .await?;
    let mut second = second_consumer.messages().await?;
    let second_msg = tokio::time::timeout(Duration::from_secs(10), second.next())
        .await
        .map_err(|_| {
            color_eyre::eyre::eyre!(
                "sinex-r6d.7: a second consumer created with the SAME deliver_group as an \
                 already-acked consumer did NOT see the invalidation again — deliver_group \
                 shares ack state across create_consumer calls in this async-nats/server \
                 version, so production's run_continuous restart would NOT self-heal an \
                 ack-before-recompute crash"
            )
        })?
        .ok_or_else(|| color_eyre::eyre::eyre!("second consumer message stream ended"))?
        .map_err(|e| color_eyre::eyre::eyre!("error receiving on second consumer: {e}"))?;
    assert_eq!(
        second_msg.payload.to_vec(),
        payload,
        "the second deliver_group consumer must see the SAME invalidation payload"
    );
    second_msg.ack().await.map_err(|e| {
        color_eyre::eyre::eyre!("failed to ack on second deliver_group consumer: {e}")
    })?;

    Ok(())
}

/// The production bridge uses one durable consumer per automaton and leaves
/// invalidations unacked until `process_invalidation_message` succeeds. An
/// unacked delivery must therefore redeliver after the ACK wait expires.
#[sinex_test]
async fn durable_invalidation_consumer_redelivers_unacked_message(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let js = ctx.jetstream().await?;
    let nats_client = ctx.nats_client();
    let (stream, invalidation_subject) = r6d9_invalidation_stream(&js).await?;
    let payload = serde_json::to_vec(&DerivedScopeInvalidation::archived(
        vec![Uuid::now_v7()],
        sinex_primitives::domain::EventSource::from_static("test.durable-invalidation"),
        sinex_primitives::domain::EventType::from_static("test.durable-invalidation"),
    ))?;
    let deliver_subject = nats_client.new_inbox();
    let durable_name = format!("r6d9-durable-invalidation-{}", Uuid::now_v7());
    let consumer = stream
        .create_consumer(async_nats::jetstream::consumer::push::Config {
            durable_name: Some(durable_name),
            deliver_subject,
            deliver_group: Some("derived.invalidation.r6d9-durable-test".to_string()),
            ack_wait: Duration::from_millis(100),
            deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::New,
            ..Default::default()
        })
        .await?;
    let mut messages = consumer.messages().await?;
    js.publish(invalidation_subject, payload.clone().into())
        .await?
        .await?;
    use futures::StreamExt;
    let first = tokio::time::timeout(Duration::from_secs(2), messages.next())
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("durable consumer ended before first delivery"))??;
    assert_eq!(first.payload.to_vec(), payload);

    let redelivered = tokio::time::timeout(Duration::from_secs(2), messages.next())
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("durable consumer ended before redelivery"))??;
    assert_eq!(redelivered.payload.to_vec(), payload);
    redelivered.ack().await?;
    Ok(())
}
