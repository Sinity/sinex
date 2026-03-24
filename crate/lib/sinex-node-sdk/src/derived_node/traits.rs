//! Derived node trait family.
//!
//! Three explicit processing models replace the monolithic `AutomatonNode`:
//! - [`TransducerNode`] — 1:1 event transform
//! - [`WindowedNode`] — accumulate + emit on window completion
//! - [`ScopeReconcilerNode`] — scope-keyed reconciliation
//!
//! Each model is bridged to the shared adapter via wrapper types that implement
//! [`DerivedNodeImpl`].

use super::context::DerivedTriggerContext;
use super::output::DerivedOutput;
use crate::automaton_node::{ErrorAction, NodeLogicError};

use serde::{Serialize, de::DeserializeOwned};
use sinex_primitives::JsonValue;
use sinex_primitives::domain::DerivedNodeModel;
use std::collections::HashMap;

/// Configuration for the derived node adapter.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct DerivedNodeConfig {
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

impl Default for DerivedNodeConfig {
    fn default() -> Self {
        Self {
            checkpoint_interval: 1000,
            checkpoint_timeout_secs: 10,
            batch_size: 100,
            consumer_group: None,
            extra: HashMap::new(),
        }
    }
}

// ── TransducerNode ─────────────────────────────────────────────────────

/// A 1:1 event transducer: one input event produces zero or one output event.
///
/// Transducers are deterministic transforms with inherited `ts_orig`.
/// The default `node_model` is `DerivedNodeModel::Transducer`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not implement `TransducerNode`",
    label = "missing TransducerNode implementation",
    note = "implement `name()`, `input_event_type()`, `output_event_type()`, and `process()`"
)]
pub trait TransducerNode: Send + Sync + 'static {
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
    fn node_model(&self) -> DerivedNodeModel {
        DerivedNodeModel::Transducer
    }

    /// Process a single input event into zero or one output events.
    fn process(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        context: &DerivedTriggerContext,
    ) -> impl std::future::Future<
        Output = Result<Option<DerivedOutput<Self::Output>>, NodeLogicError>,
    > + Send;

    fn handle_error(&self, _error: &NodeLogicError) -> ErrorAction {
        ErrorAction::SendToDLQ
    }

    fn on_initialize(
        &mut self,
        _state: &Self::State,
    ) -> impl std::future::Future<Output = Result<(), NodeLogicError>> + Send {
        async { Ok(()) }
    }

    fn on_shutdown(
        &mut self,
        _state: &Self::State,
    ) -> impl std::future::Future<Output = Result<(), NodeLogicError>> + Send {
        async { Ok(()) }
    }
}

// ── WindowedNode ───────────────────────────────────────────────────────

/// A windowed aggregator: accumulates events, emits on window completion.
///
/// The SDK calls `accumulate()` for each event, checks `window_complete()`,
/// and calls `emit()` when the window is ready.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not implement `WindowedNode`",
    label = "missing WindowedNode implementation",
    note = "implement `accumulate()`, `window_complete()`, and `emit()`"
)]
pub trait WindowedNode: Send + Sync + 'static {
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
    fn node_model(&self) -> DerivedNodeModel {
        DerivedNodeModel::Windowed
    }

    /// Accumulate an event into the window state.
    fn accumulate(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        context: &DerivedTriggerContext,
    ) -> impl std::future::Future<Output = Result<(), NodeLogicError>> + Send;

    /// Check if the window is complete and should emit.
    fn window_complete(&self, state: &Self::State) -> bool;

    /// Emit the output from the completed window.
    fn emit(
        &mut self,
        state: &mut Self::State,
        context: &DerivedTriggerContext,
    ) -> impl std::future::Future<
        Output = Result<Option<DerivedOutput<Self::Output>>, NodeLogicError>,
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
        context: &DerivedTriggerContext,
    ) -> impl std::future::Future<
        Output = Result<Option<DerivedOutput<Self::Output>>, NodeLogicError>,
    > + Send {
        async move {
            for event in events {
                self.accumulate(state, event, context).await?;
            }
            if self.window_complete(state) {
                self.emit(state, context).await
            } else {
                Ok(None)
            }
        }
    }

    fn handle_error(&self, _error: &NodeLogicError) -> ErrorAction {
        ErrorAction::SendToDLQ
    }

    fn on_initialize(
        &mut self,
        _state: &Self::State,
    ) -> impl std::future::Future<Output = Result<(), NodeLogicError>> + Send {
        async { Ok(()) }
    }

    fn on_shutdown(
        &mut self,
        _state: &Self::State,
    ) -> impl std::future::Future<Output = Result<(), NodeLogicError>> + Send {
        async { Ok(()) }
    }
}

// ── ScopeReconcilerNode ────────────────────────────────────────────────

/// A scope-based reconciler: derives a live scope from each trigger and reconciles per-scope
/// state.
///
/// Live event processing can emit at most one derived event per trigger, so implementations must
/// resolve to zero or one scope key on that path. Invalidation fan-out is handled separately by
/// the adapter, which calls [`ScopeReconcilerNode::recompute_scope`] once per affected scope.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not implement `ScopeReconcilerNode`",
    label = "missing ScopeReconcilerNode implementation",
    note = "implement `scope_keys()` and `reconcile()`"
)]
pub trait ScopeReconcilerNode: Send + Sync + 'static {
    type State: Serialize + DeserializeOwned + Default + Send + Sync;
    type Input: DeserializeOwned + Send;
    type Output: Serialize + Send;

    fn name(&self) -> &'static str;
    fn input_event_type(&self) -> &'static str;
    fn output_event_type(&self) -> &'static str;
    fn output_event_source(&self) -> &'static str {
        self.name()
    }
    fn node_model(&self) -> DerivedNodeModel {
        DerivedNodeModel::ScopeReconciler
    }

    /// Derive the live scope key from a trigger event.
    ///
    /// Return an empty vector to skip live processing for this trigger. Returning more than one
    /// key is rejected by the adapter because the live path can emit at most one output event per
    /// trigger.
    fn scope_keys(&self, input: &Self::Input, context: &DerivedTriggerContext) -> Vec<String>;

    /// Reconcile a scope: given the trigger and current state, produce output.
    fn reconcile(
        &mut self,
        state: &mut Self::State,
        scope_key: &str,
        input: Self::Input,
        context: &DerivedTriggerContext,
    ) -> impl std::future::Future<
        Output = Result<Option<DerivedOutput<Self::Output>>, NodeLogicError>,
    > + Send;

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
        context: &DerivedTriggerContext,
    ) -> impl std::future::Future<Output = Result<Vec<DerivedOutput<Self::Output>>, NodeLogicError>> + Send
    {
        async move {
            let mut outputs = Vec::new();
            for input in working_set {
                if let Some(output) = self.reconcile(state, scope_key, input, context).await? {
                    outputs.push(output);
                }
            }
            Ok(outputs)
        }
    }

    fn handle_error(&self, _error: &NodeLogicError) -> ErrorAction {
        ErrorAction::SendToDLQ
    }

    fn on_initialize(
        &mut self,
        _state: &Self::State,
    ) -> impl std::future::Future<Output = Result<(), NodeLogicError>> + Send {
        async { Ok(()) }
    }

    fn on_shutdown(
        &mut self,
        _state: &Self::State,
    ) -> impl std::future::Future<Output = Result<(), NodeLogicError>> + Send {
        async { Ok(()) }
    }
}

// ── DerivedNodeImpl — unified dispatch trait ───────────────────────────

/// Internal trait that unifies all three derived node models for the adapter.
///
/// Implemented via wrapper types: `TransducerWrapper<N>`, `WindowedWrapper<N>`,
/// `ScopeReconcilerWrapper<N>`. Users never implement this directly.
pub trait DerivedNodeImpl: Send + Sync + 'static {
    type State: Serialize + DeserializeOwned + Default + Send + Sync;

    fn name(&self) -> &'static str;
    fn input_event_type(&self) -> &'static str;
    fn output_event_type(&self) -> &'static str;
    fn output_event_source(&self) -> &'static str;
    fn node_model(&self) -> DerivedNodeModel;

    /// Process a single event through the node's model-specific logic.
    fn process_derived(
        &mut self,
        state: &mut Self::State,
        event: sinex_primitives::events::Event<JsonValue>,
        context: &DerivedTriggerContext,
    ) -> impl std::future::Future<Output = Result<Option<DerivedOutput<JsonValue>>, NodeLogicError>> + Send;

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
        context: &DerivedTriggerContext,
    ) -> impl std::future::Future<Output = Result<Vec<DerivedOutput<JsonValue>>, NodeLogicError>> + Send;

    fn handle_error_derived(&self, error: &NodeLogicError) -> ErrorAction;

    fn on_initialize_derived(
        &mut self,
        state: &Self::State,
    ) -> impl std::future::Future<Output = Result<(), NodeLogicError>> + Send;

    fn on_shutdown_derived(
        &mut self,
        state: &Self::State,
    ) -> impl std::future::Future<Output = Result<(), NodeLogicError>> + Send;
}

// ── Wrapper types ──────────────────────────────────────────────────────

/// Wrapper that bridges `TransducerNode` to `DerivedNodeImpl`.
pub struct TransducerWrapper<N: TransducerNode>(pub N);

impl<N: TransducerNode + Default> Default for TransducerWrapper<N> {
    fn default() -> Self {
        Self(N::default())
    }
}

impl<N: TransducerNode> DerivedNodeImpl for TransducerWrapper<N> {
    type State = N::State;

    fn name(&self) -> &'static str {
        self.0.name()
    }
    fn input_event_type(&self) -> &'static str {
        self.0.input_event_type()
    }
    fn output_event_type(&self) -> &'static str {
        self.0.output_event_type()
    }
    fn output_event_source(&self) -> &'static str {
        self.0.output_event_source()
    }
    fn node_model(&self) -> DerivedNodeModel {
        self.0.node_model()
    }

    async fn process_derived(
        &mut self,
        state: &mut Self::State,
        event: sinex_primitives::events::Event<JsonValue>,
        context: &DerivedTriggerContext,
    ) -> Result<Option<DerivedOutput<JsonValue>>, NodeLogicError> {
        let input: N::Input = serde_json::from_value(event.payload)
            .map_err(|e| NodeLogicError::Processing(format!("Failed to parse input: {e}")))?;

        match self.0.process(state, input, context).await? {
            Some(output) => {
                let json_payload = serde_json::to_value(&output.payload).map_err(|e| {
                    NodeLogicError::Processing(format!("Failed to serialize output: {e}"))
                })?;
                Ok(Some(DerivedOutput {
                    payload: json_payload,
                    ts_orig: output.ts_orig,
                    source_event_ids: output.source_event_ids,
                    temporal_policy: output.temporal_policy,
                    semantics_version: output.semantics_version,
                    scope_key: output.scope_key,
                    equivalence_key: output.equivalence_key,
                }))
            }
            None => Ok(None),
        }
    }

    /// Transducers ignore invalidation — their outputs are archived with inputs.
    async fn process_invalidation_derived(
        &mut self,
        _state: &mut Self::State,
        _scope_key: &str,
        _working_set: Vec<sinex_primitives::events::Event<JsonValue>>,
        _context: &DerivedTriggerContext,
    ) -> Result<Vec<DerivedOutput<JsonValue>>, NodeLogicError> {
        Ok(Vec::new())
    }

    fn handle_error_derived(&self, error: &NodeLogicError) -> ErrorAction {
        self.0.handle_error(error)
    }

    async fn on_initialize_derived(&mut self, state: &Self::State) -> Result<(), NodeLogicError> {
        self.0.on_initialize(state).await
    }

    async fn on_shutdown_derived(&mut self, state: &Self::State) -> Result<(), NodeLogicError> {
        self.0.on_shutdown(state).await
    }
}

/// Wrapper that bridges `WindowedNode` to `DerivedNodeImpl`.
pub struct WindowedWrapper<N: WindowedNode>(pub N);

impl<N: WindowedNode + Default> Default for WindowedWrapper<N> {
    fn default() -> Self {
        Self(N::default())
    }
}

impl<N: WindowedNode> DerivedNodeImpl for WindowedWrapper<N> {
    type State = N::State;

    fn name(&self) -> &'static str {
        self.0.name()
    }
    fn input_event_type(&self) -> &'static str {
        self.0.input_event_type()
    }
    fn output_event_type(&self) -> &'static str {
        self.0.output_event_type()
    }
    fn output_event_source(&self) -> &'static str {
        self.0.output_event_source()
    }
    fn node_model(&self) -> DerivedNodeModel {
        self.0.node_model()
    }

    async fn process_derived(
        &mut self,
        state: &mut Self::State,
        event: sinex_primitives::events::Event<JsonValue>,
        context: &DerivedTriggerContext,
    ) -> Result<Option<DerivedOutput<JsonValue>>, NodeLogicError> {
        let input: N::Input = serde_json::from_value(event.payload)
            .map_err(|e| NodeLogicError::Processing(format!("Failed to parse input: {e}")))?;

        // Accumulate into window
        self.0.accumulate(state, input, context).await?;

        // Check if window is complete
        if self.0.window_complete(state) {
            match self.0.emit(state, context).await? {
                Some(output) => {
                    let json_payload = serde_json::to_value(&output.payload).map_err(|e| {
                        NodeLogicError::Processing(format!("Failed to serialize output: {e}"))
                    })?;
                    Ok(Some(DerivedOutput {
                        payload: json_payload,
                        ts_orig: output.ts_orig,
                        source_event_ids: output.source_event_ids,
                        temporal_policy: output.temporal_policy,
                        semantics_version: output.semantics_version,
                        scope_key: output.scope_key,
                        equivalence_key: output.equivalence_key,
                    }))
                }
                None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    /// Windowed recomputation: parse working set, delegate to `recompute_window()`.
    async fn process_invalidation_derived(
        &mut self,
        state: &mut Self::State,
        _scope_key: &str,
        working_set: Vec<sinex_primitives::events::Event<JsonValue>>,
        context: &DerivedTriggerContext,
    ) -> Result<Vec<DerivedOutput<JsonValue>>, NodeLogicError> {
        let inputs: Vec<N::Input> = working_set
            .into_iter()
            .map(|e| {
                serde_json::from_value(e.payload)
                    .map_err(|e| NodeLogicError::Processing(format!("Failed to parse input: {e}")))
            })
            .collect::<Result<Vec<_>, _>>()?;

        match self.0.recompute_window(state, inputs, context).await? {
            Some(output) => {
                let json_payload = serde_json::to_value(&output.payload).map_err(|e| {
                    NodeLogicError::Processing(format!("Failed to serialize output: {e}"))
                })?;
                Ok(vec![DerivedOutput {
                    payload: json_payload,
                    ts_orig: output.ts_orig,
                    source_event_ids: output.source_event_ids,
                    temporal_policy: output.temporal_policy,
                    semantics_version: output.semantics_version,
                    scope_key: output.scope_key,
                    equivalence_key: output.equivalence_key,
                }])
            }
            None => Ok(Vec::new()),
        }
    }

    fn handle_error_derived(&self, error: &NodeLogicError) -> ErrorAction {
        self.0.handle_error(error)
    }

    async fn on_initialize_derived(&mut self, state: &Self::State) -> Result<(), NodeLogicError> {
        self.0.on_initialize(state).await
    }

    async fn on_shutdown_derived(&mut self, state: &Self::State) -> Result<(), NodeLogicError> {
        self.0.on_shutdown(state).await
    }
}

/// Wrapper that bridges `ScopeReconcilerNode` to `DerivedNodeImpl`.
pub struct ScopeReconcilerWrapper<N: ScopeReconcilerNode>(pub N);

impl<N: ScopeReconcilerNode + Default> Default for ScopeReconcilerWrapper<N> {
    fn default() -> Self {
        Self(N::default())
    }
}

impl<N> DerivedNodeImpl for ScopeReconcilerWrapper<N>
where
    N: ScopeReconcilerNode,
{
    type State = N::State;

    fn name(&self) -> &'static str {
        self.0.name()
    }
    fn input_event_type(&self) -> &'static str {
        self.0.input_event_type()
    }
    fn output_event_type(&self) -> &'static str {
        self.0.output_event_type()
    }
    fn output_event_source(&self) -> &'static str {
        self.0.output_event_source()
    }
    fn node_model(&self) -> DerivedNodeModel {
        self.0.node_model()
    }

    async fn process_derived(
        &mut self,
        state: &mut Self::State,
        event: sinex_primitives::events::Event<JsonValue>,
        context: &DerivedTriggerContext,
    ) -> Result<Option<DerivedOutput<JsonValue>>, NodeLogicError> {
        let input: N::Input = serde_json::from_value(event.payload)
            .map_err(|e| NodeLogicError::Processing(format!("Failed to parse input: {e}")))?;

        let scope_keys = self.0.scope_keys(&input, context);

        match scope_keys.as_slice() {
            [] => Ok(None),
            [scope_key] => match self.0.reconcile(state, scope_key, input, context).await? {
                Some(output) => {
                    let json_payload = serde_json::to_value(&output.payload).map_err(|e| {
                        NodeLogicError::Processing(format!("Failed to serialize output: {e}"))
                    })?;
                    Ok(Some(DerivedOutput {
                        payload: json_payload,
                        ts_orig: output.ts_orig,
                        source_event_ids: output.source_event_ids,
                        temporal_policy: output.temporal_policy,
                        semantics_version: output.semantics_version,
                        scope_key: output.scope_key,
                        equivalence_key: output.equivalence_key,
                    }))
                }
                None => Ok(None),
            },
            _ => Err(NodeLogicError::Processing(format!(
                "ScopeReconcilerNode '{}' returned {} live scope keys; derived-node live processing supports at most one scope per trigger",
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
        context: &DerivedTriggerContext,
    ) -> Result<Vec<DerivedOutput<JsonValue>>, NodeLogicError> {
        let inputs: Vec<N::Input> = working_set
            .into_iter()
            .map(|e| {
                serde_json::from_value(e.payload)
                    .map_err(|e| NodeLogicError::Processing(format!("Failed to parse input: {e}")))
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
                    NodeLogicError::Processing(format!("Failed to serialize output: {e}"))
                })?;
                Ok(DerivedOutput {
                    payload: json_payload,
                    ts_orig: output.ts_orig,
                    source_event_ids: output.source_event_ids,
                    temporal_policy: output.temporal_policy,
                    semantics_version: output.semantics_version,
                    scope_key: output.scope_key,
                    equivalence_key: output.equivalence_key,
                })
            })
            .collect()
    }

    fn handle_error_derived(&self, error: &NodeLogicError) -> ErrorAction {
        self.0.handle_error(error)
    }

    async fn on_initialize_derived(&mut self, state: &Self::State) -> Result<(), NodeLogicError> {
        self.0.on_initialize(state).await
    }

    async fn on_shutdown_derived(&mut self, state: &Self::State) -> Result<(), NodeLogicError> {
        self.0.on_shutdown(state).await
    }
}
