//! Derived node trait family.
//!
//! Three explicit processing models replace the monolithic `AutomatonNode`:
//! - [`Transducer`] — 1:1 event transform
//! - [`Windowed`] — accumulate + emit on window completion
//! - [`ScopeReconciler`] — scope-keyed reconciliation
//!
//! Each model is bridged to the shared adapter via wrapper types that implement
//! [`Automaton`].

use super::context::AutomatonContext;
use super::output::DerivedOutput;
use crate::runtime::processing::AutomatonLogicError;

use serde::{Serialize, de::DeserializeOwned};
use sinex_primitives::JsonValue;
use sinex_primitives::domain::AutomatonModel;
use sinex_primitives::events::Event;
use sinex_primitives::temporal::Timestamp;
use std::collections::HashMap;

const DEFAULT_CHECKPOINT_INTERVAL_EVENTS: u64 = 1000;
const DEFAULT_CHECKPOINT_TIMEOUT_SECS: u64 = 10;
const DEFAULT_PROCESSING_BATCH_SIZE: usize = 100;

/// Configuration for the automaton adapter.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct AutomatonAdapterConfig {
    /// How often to checkpoint (in events processed).
    pub checkpoint_interval: u64,
    /// Maximum time between checkpoints (seconds).
    pub checkpoint_timeout_secs: u64,
    /// Batch size for event processing.
    pub batch_size: usize,
    /// Optional consumer group for NATS.
    pub consumer_group: Option<String>,
    /// Extra configuration for node-specific use.
    pub extra: HashMap<String, String>,
}

impl Default for AutomatonAdapterConfig {
    fn default() -> Self {
        Self {
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL_EVENTS,
            checkpoint_timeout_secs: DEFAULT_CHECKPOINT_TIMEOUT_SECS,
            batch_size: DEFAULT_PROCESSING_BATCH_SIZE,
            consumer_group: None,
            extra: HashMap::new(),
        }
    }
}

fn serialize_output<T: Serialize>(
    output: DerivedOutput<T>,
) -> Result<DerivedOutput<JsonValue>, AutomatonLogicError> {
    let json_payload = serde_json::to_value(&output.payload).map_err(|e| {
        AutomatonLogicError::OutputSerialization(format!("Failed to serialize output: {e}"))
    })?;
    Ok(DerivedOutput {
        payload: json_payload,
        ts_orig: output.ts_orig,
        source_event_ids: output.source_event_ids,
        temporal_policy: output.temporal_policy,
        semantics_version: output.semantics_version,
        scope_key: output.scope_key,
        equivalence_key: output.equivalence_key,
        aggregation: output.aggregation,
        event_type: output.event_type,
    })
}

fn serialize_outputs<T: Serialize>(
    outputs: Vec<DerivedOutput<T>>,
) -> Result<Vec<DerivedOutput<JsonValue>>, AutomatonLogicError> {
    outputs.into_iter().map(serialize_output).collect()
}

/// Which provenance class a automaton consumes from its input stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputProvenanceFilter {
    /// Accept both material and synthesized events.
    Any,
    /// Accept only first-order material events.
    MaterialOnly,
    /// Accept only synthesized events.
    SynthesizedOnly,
}

impl InputProvenanceFilter {
    #[must_use]
    pub fn matches_event<T>(self, event: &Event<T>) -> bool {
        self.matches_lineage(event.is_synthesized_event())
    }

    #[must_use]
    pub fn matches_lineage(self, has_lineage: bool) -> bool {
        match self {
            Self::Any => true,
            Self::MaterialOnly => !has_lineage,
            Self::SynthesizedOnly => has_lineage,
        }
    }

    #[must_use]
    pub fn query_has_lineage(self) -> Option<bool> {
        match self {
            Self::Any => None,
            Self::MaterialOnly => Some(false),
            Self::SynthesizedOnly => Some(true),
        }
    }
}

// ── Transducer ─────────────────────────────────────────────────────

/// A 1:1 event transducer: one input event produces zero or one output event.
///
/// Transducers are deterministic transforms with inherited `ts_orig`.
/// The default `automaton_model` is `AutomatonModel::Transducer`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not implement `Transducer`",
    label = "missing Transducer implementation",
    note = "implement `name()`, `input_event_type()`, `output_event_type()`, and `process()`"
)]
pub trait Transducer: Send + Sync + 'static {
    /// Checkpoint state (use `()` if stateless).
    type State: Serialize + DeserializeOwned + Default + Send + Sync;
    /// Parsed input event type.
    type Input: DeserializeOwned + Send;
    /// Serialized output event type.
    type Output: Serialize + Send;

    fn name(&self) -> &'static str;
    fn input_event_type(&self) -> &'static str;
    fn output_event_type(&self) -> &'static str;
    fn output_event_source(&self) -> &'static str {
        self.name()
    }
    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::Any
    }
    fn automaton_model(&self) -> AutomatonModel {
        AutomatonModel::Transducer
    }

    /// Process a single input event into zero or one output events.
    fn process(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        context: &AutomatonContext,
    ) -> impl std::future::Future<
        Output = Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError>,
    > + Send;
    fn on_initialize(
        &mut self,
        _state: &Self::State,
    ) -> impl std::future::Future<Output = Result<(), AutomatonLogicError>> + Send {
        async { Ok(()) }
    }

    fn on_shutdown(
        &mut self,
        _state: &Self::State,
    ) -> impl std::future::Future<Output = Result<(), AutomatonLogicError>> + Send {
        async { Ok(()) }
    }
}

// ── Windowed ───────────────────────────────────────────────────────

/// A windowed aggregator: accumulates events, emits on window completion.
///
/// The SDK calls `accumulate()` for each event, checks `window_complete()`,
/// and calls `emit()` when the window is ready. A periodic timer calls
/// `flush_due()` so trailing (latest) buckets are emitted without waiting
/// for the next bucket's first event.
///
/// # Scope invalidation
///
/// Windowed nodes do **not** set `scope_key` on their outputs and are
/// **out of scope** for scope-based invalidation recompute (see #1569).
/// The input-driven accumulation model is authoritative; `recompute_window`
/// is provided for ad-hoc replay but is not called by the invalidation path.
///
/// # Ordering under historical imports
///
/// Live processing orders by arrival (`ts_coided`), so historical imports
/// whose `ts_orig` predates the current bucket can cross window boundaries
/// silently. This is a known limitation shared with `TimescaleDB` continuous
/// aggregates that refresh on `ts_coided`. Handling historical backfill
/// with correct `ts_orig`-ordered semantics is not supported; flag any
/// historical import as an explicit replay and rely on `recompute_window`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not implement `Windowed`",
    label = "missing Windowed implementation",
    note = "implement `accumulate()`, `window_complete()`, and `emit()`"
)]
pub trait Windowed: Send + Sync + 'static {
    /// Window state.
    type State: Serialize + DeserializeOwned + Default + Send + Sync;
    type Input: DeserializeOwned + Send;
    type Output: Serialize + Send;

    fn name(&self) -> &'static str;
    fn input_event_type(&self) -> &'static str;
    fn output_event_type(&self) -> &'static str;
    fn output_event_source(&self) -> &'static str {
        self.name()
    }
    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::Any
    }
    fn automaton_model(&self) -> AutomatonModel {
        AutomatonModel::Windowed
    }

    /// Accumulate an event into the window state.
    fn accumulate(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        context: &AutomatonContext,
    ) -> impl std::future::Future<Output = Result<(), AutomatonLogicError>> + Send;

    /// Check if the window is complete and should emit.
    fn window_complete(&self, state: &Self::State) -> bool;

    /// Clock-driven window-close predicate for the trailing-bucket flush.
    ///
    /// The SDK's periodic timer calls this method with the current wall time.
    /// Return `true` when the open accumulator has data AND the window boundary
    /// (hour end, day end, etc.) has elapsed, so the trailing bucket can be
    /// emitted without waiting for the next bucket's first event.
    ///
    /// Default: `false` — no clock-driven flush. Implementations that produce
    /// time-bucketed outputs (hourly, daily) should override this.
    ///
    /// If `flush_due` returns `true` the runtime calls `emit()` immediately,
    /// then resets the accumulator via the normal post-emit path. A bucket
    /// that was already emitted by this timer will not be re-emitted when the
    /// next-bucket event arrives (the accumulator reset prevents it).
    fn flush_due(&self, _state: &Self::State, _now: Timestamp) -> bool {
        false
    }

    /// Emit the output from the completed window.
    fn emit(
        &mut self,
        state: &mut Self::State,
        context: &AutomatonContext,
    ) -> impl std::future::Future<
        Output = Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError>,
    > + Send;

    /// Recompute a window from its full event set after invalidation.
    ///
    /// Called when a scope invalidation signal indicates the window's inputs changed.
    /// The SDK loads the current working set and passes it here. The implementation
    /// should accumulate all events and emit the result.
    ///
    /// Default: accumulate all events, then emit if window is complete.
    fn recompute_window(
        &mut self,
        state: &mut Self::State,
        events: Vec<Self::Input>,
        context: &AutomatonContext,
    ) -> impl std::future::Future<
        Output = Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError>,
    > + Send {
        async move {
            let mut rebuilt_state = Self::State::default();
            for event in events {
                self.accumulate(&mut rebuilt_state, event, context).await?;
            }
            let output = if self.window_complete(&rebuilt_state) {
                self.emit(&mut rebuilt_state, context).await?
            } else {
                None
            };
            *state = rebuilt_state;
            Ok(output)
        }
    }
    fn on_initialize(
        &mut self,
        _state: &Self::State,
    ) -> impl std::future::Future<Output = Result<(), AutomatonLogicError>> + Send {
        async { Ok(()) }
    }

    fn on_shutdown(
        &mut self,
        _state: &Self::State,
    ) -> impl std::future::Future<Output = Result<(), AutomatonLogicError>> + Send {
        async { Ok(()) }
    }
}

// ── ScopeReconciler ────────────────────────────────────────────────

/// A scope-based reconciler: derives a live scope from each trigger and reconciles per-scope
/// state.
///
/// Live event processing can emit at most one derived event per trigger, so implementations must
/// resolve to zero or one scope key on that path. Invalidation fan-out is handled separately by
/// the adapter, which calls [`ScopeReconciler::recompute_scope`] once per affected scope.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not implement `ScopeReconciler`",
    label = "missing ScopeReconciler implementation",
    note = "implement `scope_keys()` and `reconcile()`"
)]
pub trait ScopeReconciler: Send + Sync + 'static {
    type State: Serialize + DeserializeOwned + Default + Send + Sync;
    type Input: DeserializeOwned + Send;
    type Output: Serialize + Send;

    fn name(&self) -> &'static str;
    fn input_event_type(&self) -> &'static str;
    fn output_event_type(&self) -> &'static str;
    fn output_event_source(&self) -> &'static str {
        self.name()
    }
    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::Any
    }
    fn automaton_model(&self) -> AutomatonModel {
        AutomatonModel::ScopeReconciler
    }

    /// Derive the live scope key from a trigger event.
    ///
    /// Return an empty vector to skip live processing for this trigger. Returning more than one
    /// key is rejected by the adapter because the live path can reconcile at most one scope per
    /// trigger, even though that reconciliation may emit multiple output events.
    fn scope_keys(&self, input: &Self::Input, context: &AutomatonContext) -> Vec<String>;

    /// Reconcile a scope: given the trigger and current state, produce zero or more outputs.
    fn reconcile(
        &mut self,
        state: &mut Self::State,
        scope_key: &str,
        input: Self::Input,
        context: &AutomatonContext,
    ) -> impl std::future::Future<Output = Result<Vec<DerivedOutput<Self::Output>>, AutomatonLogicError>> + Send;

    /// Recompute a scope from its full working set after invalidation.
    ///
    /// Called when a `DerivedScopeInvalidation` signal indicates this scope's
    /// inputs changed (archive, backfill, replacement). The SDK queries the
    /// current persisted events for this scope and passes them here.
    ///
    /// The implementation should produce zero or more outputs that replace the
    /// previous scope outputs. The SDK handles archiving old outputs via
    /// `scope_key` + `equivalence_key`.
    ///
    /// Default: reconcile each event in the working set, collecting outputs.
    fn recompute_scope(
        &mut self,
        state: &mut Self::State,
        scope_key: &str,
        working_set: Vec<Self::Input>,
        context: &AutomatonContext,
    ) -> impl std::future::Future<Output = Result<Vec<DerivedOutput<Self::Output>>, AutomatonLogicError>> + Send
    {
        async move {
            let mut recomputed_state = Self::State::default();
            let mut outputs = Vec::new();
            for input in working_set {
                outputs.extend(
                    self.reconcile(&mut recomputed_state, scope_key, input, context)
                        .await?,
                );
            }
            *state = recomputed_state;
            Ok(outputs)
        }
    }
    fn on_initialize(
        &mut self,
        _state: &Self::State,
    ) -> impl std::future::Future<Output = Result<(), AutomatonLogicError>> + Send {
        async { Ok(()) }
    }

    fn on_shutdown(
        &mut self,
        _state: &Self::State,
    ) -> impl std::future::Future<Output = Result<(), AutomatonLogicError>> + Send {
        async { Ok(()) }
    }
}

// ── MultiOutputTransducer ───────────────────────────────────────────

/// A 1:N event transducer: one input event produces zero or more output events,
/// each potentially of a different event type.
///
/// Unlike [`Transducer`], which emits at most one output per input, this node
/// can emit multiple outputs with distinct event types — necessary when a single
/// logical operation (e.g. document parsing) produces events of multiple kinds
/// (`document.parsed` + N× `document.chunked`). Each output carries its own
/// event type via [`DerivedOutput::with_event_type`].
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not implement `MultiOutputTransducer`",
    label = "missing MultiOutputTransducer implementation",
    note = "implement `name()`, `input_event_type()`, `output_event_types()`, and `process()`"
)]
pub trait MultiOutputTransducer: Send + Sync + 'static {
    type State: Serialize + DeserializeOwned + Default + Send + Sync;
    type Input: DeserializeOwned + Send;
    type Output: Serialize + Send;

    fn name(&self) -> &'static str;
    fn input_event_type(&self) -> &'static str;
    /// The set of event types this node can produce. Callers stamp each output
    /// with the appropriate type from this list via
    /// [`DerivedOutput::with_event_type`].
    fn output_event_types(&self) -> &[&'static str];
    fn output_event_source(&self) -> &'static str {
        self.name()
    }
    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::Any
    }
    fn automaton_model(&self) -> AutomatonModel {
        AutomatonModel::Transducer
    }

    /// Process a single input event into zero or more output events.
    ///
    /// Each output should carry its event type set via
    /// `DerivedOutput::with_event_type(output_event_types()[idx])`.
    fn process(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        context: &AutomatonContext,
    ) -> impl std::future::Future<Output = Result<Vec<DerivedOutput<Self::Output>>, AutomatonLogicError>> + Send;
    fn on_initialize(
        &mut self,
        _state: &Self::State,
    ) -> impl std::future::Future<Output = Result<(), AutomatonLogicError>> + Send {
        async { Ok(()) }
    }

    fn on_shutdown(
        &mut self,
        _state: &Self::State,
    ) -> impl std::future::Future<Output = Result<(), AutomatonLogicError>> + Send {
        async { Ok(()) }
    }
}

// ── Automaton — unified dispatch trait ───────────────────────────

/// Internal trait that unifies all three automaton models for the adapter.
///
/// Implemented via wrapper types: `TransducerWrapper<N>`, `WindowedWrapper<N>`,
/// `ScopeReconcilerWrapper<N>`. Users never implement this directly.
pub trait Automaton: Send + Sync + 'static {
    type State: Serialize + DeserializeOwned + Default + Send + Sync;

    fn name(&self) -> &'static str;
    fn input_event_type(&self) -> &'static str;
    fn input_provenance_filter(&self) -> InputProvenanceFilter;
    fn output_event_type(&self) -> &'static str;
    fn output_event_source(&self) -> &'static str;
    fn automaton_model(&self) -> AutomatonModel;

    /// Process a single event through the node's model-specific logic.
    fn process_derived(
        &mut self,
        state: &mut Self::State,
        event: sinex_primitives::events::Event<JsonValue>,
        context: &AutomatonContext,
    ) -> impl std::future::Future<Output = Result<Vec<DerivedOutput<JsonValue>>, AutomatonLogicError>> + Send;

    /// Process a scope invalidation signal.
    ///
    /// Returns recomputed outputs for each affected scope. The adapter handles
    /// archiving old outputs and emitting new ones.
    ///
    /// - Transducers: return empty (invalidation not applicable, outputs archived with inputs)
    /// - Windowed: recompute from working set
    /// - Scope reconcilers: recompute each affected scope from working set
    fn process_invalidation_derived(
        &mut self,
        state: &mut Self::State,
        scope_key: &str,
        working_set: Vec<sinex_primitives::events::Event<JsonValue>>,
        context: &AutomatonContext,
    ) -> impl std::future::Future<Output = Result<Vec<DerivedOutput<JsonValue>>, AutomatonLogicError>> + Send;

    fn on_initialize_derived(
        &mut self,
        state: &Self::State,
    ) -> impl std::future::Future<Output = Result<(), AutomatonLogicError>> + Send;

    fn on_shutdown_derived(
        &mut self,
        state: &Self::State,
    ) -> impl std::future::Future<Output = Result<(), AutomatonLogicError>> + Send;

    /// Clock-driven flush for `Windowed` nodes (trailing-bucket emission).
    ///
    /// Called by the SDK periodic timer with the current wall time. Returns
    /// any output events produced by flushing the open accumulator.
    ///
    /// Default: returns empty — non-windowed models do not flush on a timer.
    fn timer_flush_derived(
        &mut self,
        _state: &mut Self::State,
        _now: Timestamp,
        _context: &AutomatonContext,
    ) -> impl std::future::Future<Output = Result<Vec<DerivedOutput<JsonValue>>, AutomatonLogicError>> + Send
    {
        async { Ok(Vec::new()) }
    }
}

// ── Wrapper types ──────────────────────────────────────────────────────

/// Wrapper that bridges `Transducer` to `Automaton`.
pub struct TransducerWrapper<N: Transducer>(pub N);

impl<N: Transducer + Default> Default for TransducerWrapper<N> {
    fn default() -> Self {
        Self(N::default())
    }
}

impl<N: Transducer> Automaton for TransducerWrapper<N> {
    type State = N::State;

    fn name(&self) -> &'static str {
        self.0.name()
    }
    fn input_event_type(&self) -> &'static str {
        self.0.input_event_type()
    }
    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        self.0.input_provenance_filter()
    }
    fn output_event_type(&self) -> &'static str {
        self.0.output_event_type()
    }
    fn output_event_source(&self) -> &'static str {
        self.0.output_event_source()
    }
    fn automaton_model(&self) -> AutomatonModel {
        self.0.automaton_model()
    }

    async fn process_derived(
        &mut self,
        state: &mut Self::State,
        event: sinex_primitives::events::Event<JsonValue>,
        context: &AutomatonContext,
    ) -> Result<Vec<DerivedOutput<JsonValue>>, AutomatonLogicError> {
        let input: N::Input = serde_json::from_value(event.payload)
            .map_err(|e| AutomatonLogicError::InputParsing(format!("Failed to parse input: {e}")))?;

        self.0
            .process(state, input, context)
            .await?
            .map(|output| vec![output])
            .map_or_else(|| Ok(Vec::new()), serialize_outputs)
    }

    /// Transducers ignore invalidation — their outputs are archived with inputs.
    async fn process_invalidation_derived(
        &mut self,
        _state: &mut Self::State,
        _scope_key: &str,
        _working_set: Vec<sinex_primitives::events::Event<JsonValue>>,
        _context: &AutomatonContext,
    ) -> Result<Vec<DerivedOutput<JsonValue>>, AutomatonLogicError> {
        Ok(Vec::new())
    }
    async fn on_initialize_derived(&mut self, state: &Self::State) -> Result<(), AutomatonLogicError> {
        self.0.on_initialize(state).await
    }

    async fn on_shutdown_derived(&mut self, state: &Self::State) -> Result<(), AutomatonLogicError> {
        self.0.on_shutdown(state).await
    }
}

/// Wrapper that bridges `Windowed` to `Automaton`.
pub struct WindowedWrapper<N: Windowed>(pub N);

impl<N: Windowed + Default> Default for WindowedWrapper<N> {
    fn default() -> Self {
        Self(N::default())
    }
}

impl<N: Windowed> Automaton for WindowedWrapper<N> {
    type State = N::State;

    fn name(&self) -> &'static str {
        self.0.name()
    }
    fn input_event_type(&self) -> &'static str {
        self.0.input_event_type()
    }
    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        self.0.input_provenance_filter()
    }
    fn output_event_type(&self) -> &'static str {
        self.0.output_event_type()
    }
    fn output_event_source(&self) -> &'static str {
        self.0.output_event_source()
    }
    fn automaton_model(&self) -> AutomatonModel {
        self.0.automaton_model()
    }

    async fn process_derived(
        &mut self,
        state: &mut Self::State,
        event: sinex_primitives::events::Event<JsonValue>,
        context: &AutomatonContext,
    ) -> Result<Vec<DerivedOutput<JsonValue>>, AutomatonLogicError> {
        let input: N::Input = serde_json::from_value(event.payload)
            .map_err(|e| AutomatonLogicError::InputParsing(format!("Failed to parse input: {e}")))?;

        // Accumulate into window
        self.0.accumulate(state, input, context).await?;

        // Check if window is complete
        if self.0.window_complete(state) {
            self.0
                .emit(state, context)
                .await?
                .map(|output| vec![output])
                .map_or_else(|| Ok(Vec::new()), serialize_outputs)
        } else {
            Ok(Vec::new())
        }
    }

    /// Windowed recomputation: parse working set, delegate to `recompute_window()`.
    async fn process_invalidation_derived(
        &mut self,
        state: &mut Self::State,
        _scope_key: &str,
        working_set: Vec<sinex_primitives::events::Event<JsonValue>>,
        context: &AutomatonContext,
    ) -> Result<Vec<DerivedOutput<JsonValue>>, AutomatonLogicError> {
        let inputs: Vec<N::Input> = working_set
            .into_iter()
            .map(|e| {
                serde_json::from_value(e.payload).map_err(|e| {
                    AutomatonLogicError::InputParsing(format!("Failed to parse input: {e}"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        match self.0.recompute_window(state, inputs, context).await? {
            Some(output) => {
                let json_payload = serde_json::to_value(&output.payload).map_err(|e| {
                    AutomatonLogicError::OutputSerialization(format!("Failed to serialize output: {e}"))
                })?;
                Ok(vec![DerivedOutput {
                    payload: json_payload,
                    ts_orig: output.ts_orig,
                    source_event_ids: output.source_event_ids,
                    temporal_policy: output.temporal_policy,
                    semantics_version: output.semantics_version,
                    scope_key: output.scope_key,
                    equivalence_key: output.equivalence_key,
                    aggregation: output.aggregation,
                    event_type: output.event_type,
                }])
            }
            None => Ok(Vec::new()),
        }
    }
    async fn on_initialize_derived(&mut self, state: &Self::State) -> Result<(), AutomatonLogicError> {
        self.0.on_initialize(state).await
    }

    async fn on_shutdown_derived(&mut self, state: &Self::State) -> Result<(), AutomatonLogicError> {
        self.0.on_shutdown(state).await
    }

    /// Clock-driven trailing-bucket flush for `Windowed` nodes.
    ///
    /// Calls `flush_due(state, now)`. If true, calls `emit()` and returns the
    /// serialized output. The accumulator is reset inside `emit()` by the
    /// normal post-emit path, preventing double-emission when the next bucket
    /// event arrives.
    async fn timer_flush_derived(
        &mut self,
        state: &mut Self::State,
        now: Timestamp,
        context: &AutomatonContext,
    ) -> Result<Vec<DerivedOutput<JsonValue>>, AutomatonLogicError> {
        if !self.0.flush_due(state, now) {
            return Ok(Vec::new());
        }

        match self.0.emit(state, context).await? {
            Some(output) => serialize_outputs(vec![output]),
            None => Ok(Vec::new()),
        }
    }
}

/// Wrapper that bridges `ScopeReconciler` to `Automaton`.
pub struct ScopeReconcilerWrapper<N: ScopeReconciler>(pub N);

impl<N: ScopeReconciler + Default> Default for ScopeReconcilerWrapper<N> {
    fn default() -> Self {
        Self(N::default())
    }
}

impl<N> Automaton for ScopeReconcilerWrapper<N>
where
    N: ScopeReconciler,
{
    type State = N::State;

    fn name(&self) -> &'static str {
        self.0.name()
    }
    fn input_event_type(&self) -> &'static str {
        self.0.input_event_type()
    }
    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        self.0.input_provenance_filter()
    }
    fn output_event_type(&self) -> &'static str {
        self.0.output_event_type()
    }
    fn output_event_source(&self) -> &'static str {
        self.0.output_event_source()
    }
    fn automaton_model(&self) -> AutomatonModel {
        self.0.automaton_model()
    }

    async fn process_derived(
        &mut self,
        state: &mut Self::State,
        event: sinex_primitives::events::Event<JsonValue>,
        context: &AutomatonContext,
    ) -> Result<Vec<DerivedOutput<JsonValue>>, AutomatonLogicError> {
        let input: N::Input = serde_json::from_value(event.payload)
            .map_err(|e| AutomatonLogicError::InputParsing(format!("Failed to parse input: {e}")))?;

        let scope_keys = self.0.scope_keys(&input, context);

        match scope_keys.as_slice() {
            [] => Ok(Vec::new()),
            [scope_key] => {
                serialize_outputs(self.0.reconcile(state, scope_key, input, context).await?)
            }
            _ => Err(AutomatonLogicError::Processing(format!(
                "ScopeReconciler '{}' returned {} live scope keys; derived-node live processing supports at most one scope per trigger",
                self.0.name(),
                scope_keys.len()
            ))),
        }
    }

    /// Scope reconciler recomputation: parse working set, delegate to `recompute_scope()`.
    async fn process_invalidation_derived(
        &mut self,
        state: &mut Self::State,
        scope_key: &str,
        working_set: Vec<sinex_primitives::events::Event<JsonValue>>,
        context: &AutomatonContext,
    ) -> Result<Vec<DerivedOutput<JsonValue>>, AutomatonLogicError> {
        let inputs: Vec<N::Input> = working_set
            .into_iter()
            .map(|e| {
                serde_json::from_value(e.payload).map_err(|e| {
                    AutomatonLogicError::InputParsing(format!("Failed to parse input: {e}"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let typed_outputs = self
            .0
            .recompute_scope(state, scope_key, inputs, context)
            .await?;

        typed_outputs
            .into_iter()
            .map(|output| {
                let json_payload = serde_json::to_value(&output.payload).map_err(|e| {
                    AutomatonLogicError::OutputSerialization(format!("Failed to serialize output: {e}"))
                })?;
                Ok(DerivedOutput {
                    payload: json_payload,
                    ts_orig: output.ts_orig,
                    source_event_ids: output.source_event_ids,
                    temporal_policy: output.temporal_policy,
                    semantics_version: output.semantics_version,
                    scope_key: output.scope_key,
                    equivalence_key: output.equivalence_key,
                    aggregation: output.aggregation,
                    event_type: None,
                })
            })
            .collect()
    }
    async fn on_initialize_derived(&mut self, state: &Self::State) -> Result<(), AutomatonLogicError> {
        self.0.on_initialize(state).await
    }

    async fn on_shutdown_derived(&mut self, state: &Self::State) -> Result<(), AutomatonLogicError> {
        self.0.on_shutdown(state).await
    }
}

/// Wrapper that bridges `MultiOutputTransducer` to `Automaton`.
pub struct MultiOutputTransducerWrapper<N: MultiOutputTransducer>(pub N);

impl<N: MultiOutputTransducer + Default> Default for MultiOutputTransducerWrapper<N> {
    fn default() -> Self {
        Self(N::default())
    }
}

impl<N: MultiOutputTransducer> Automaton for MultiOutputTransducerWrapper<N> {
    type State = N::State;

    fn name(&self) -> &'static str {
        self.0.name()
    }
    fn input_event_type(&self) -> &'static str {
        self.0.input_event_type()
    }
    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        self.0.input_provenance_filter()
    }
    fn output_event_type(&self) -> &'static str {
        self.0
            .output_event_types()
            .first()
            .copied()
            .unwrap_or("unknown")
    }
    fn output_event_source(&self) -> &'static str {
        self.0.output_event_source()
    }
    fn automaton_model(&self) -> AutomatonModel {
        self.0.automaton_model()
    }

    async fn process_derived(
        &mut self,
        state: &mut Self::State,
        event: sinex_primitives::events::Event<JsonValue>,
        context: &AutomatonContext,
    ) -> Result<Vec<DerivedOutput<JsonValue>>, AutomatonLogicError> {
        let input: N::Input = serde_json::from_value(event.payload)
            .map_err(|e| AutomatonLogicError::InputParsing(format!("Failed to parse input: {e}")))?;

        let outputs = self.0.process(state, input, context).await?;
        serialize_outputs(outputs)
    }

    async fn process_invalidation_derived(
        &mut self,
        _state: &mut Self::State,
        _scope_key: &str,
        _working_set: Vec<sinex_primitives::events::Event<JsonValue>>,
        _context: &AutomatonContext,
    ) -> Result<Vec<DerivedOutput<JsonValue>>, AutomatonLogicError> {
        Ok(Vec::new())
    }
    async fn on_initialize_derived(&mut self, state: &Self::State) -> Result<(), AutomatonLogicError> {
        self.0.on_initialize(state).await
    }

    async fn on_shutdown_derived(&mut self, state: &Self::State) -> Result<(), AutomatonLogicError> {
        self.0.on_shutdown(state).await
    }
}
