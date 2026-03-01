//! AutomatonNode trait for LLM-friendly node development.
//!
//! This module provides a high-level abstraction that reduces typical node
//! implementations from 200+ lines to ~10 lines. The trait is designed to be
//! simple enough that LLMs can reliably generate correct implementations.
//!
//! # Example
//!
//! ```rust,ignore
//! use sinex_node_sdk::{AutomatonNode, NodeAdapterConfig};
//! use serde::{Deserialize, Serialize};
//! use async_trait::async_trait;
//!
//! #[derive(Serialize, Deserialize, Default)]
//! struct GitActivityState {
//!     commands_seen: u64,
//! }
//!
//! struct GitActivityDetector;
//!
//! #[async_trait]
//! impl AutomatonNode for GitActivityDetector {
//!     type State = GitActivityState;
//!     type Input = TerminalCommandEvent;
//!     type Output = GitActivityEvent;
//!
//!     fn name(&self) -> &'static str {
//!         "git-activity-detector"
//!     }
//!
//!     fn input_event_type(&self) -> &'static str {
//!         "terminal.command.executed"
//!     }
//!
//!     fn output_event_type(&self) -> &'static str {
//!         "git.activity.detected"
//!     }
//!
//!     async fn process(
//!         &mut self,
//!         state: &mut Self::State,
//!         input: Self::Input,
//!     ) -> Result<Option<Self::Output>, NodeLogicError> {
//!         if !input.command.starts_with("git ") {
//!             return Ok(None);
//!         }
//!         state.commands_seen += 1;
//!         Ok(Some(GitActivityEvent { ... }))
//!     }
//! }
//! ```

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinex_primitives::{
    JsonValue, Ulid,
    domain::{EventSource, EventType, HostName},
    events::{Event, Provenance},
    ids::Id,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::checkpoint::{CheckpointManager, CheckpointState};
use crate::runtime::stream::{
    Checkpoint, EventSender, NodeCapabilities, NodeInitContext, NodeRuntimeState, NodeType,
    ScanArgs, ScanEstimate, ScanReport, TimeHorizon,
};
use crate::shutdown::ShutdownConfig;
use crate::{NodeResult, SinexError};

/// Errors specific to AutomatonNode
#[derive(Debug, Error)]
pub enum NodeLogicError {
    #[error("Processing error: {0}")]
    Processing(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Input parsing error: {0}")]
    InputParsing(String),

    #[error("Output serialization error: {0}")]
    OutputSerialization(String),
}

impl From<NodeLogicError> for SinexError {
    fn from(err: NodeLogicError) -> Self {
        match &err {
            NodeLogicError::Processing(msg) => SinexError::processing(msg),
            NodeLogicError::Serialization(e) => {
                SinexError::serialization("node serialization error")
                    .with_std_error(e as &(dyn std::error::Error + 'static))
            }
            NodeLogicError::InputParsing(msg) => SinexError::validation(msg),
            NodeLogicError::OutputSerialization(msg) => SinexError::serialization(msg),
        }
    }
}

/// Context provided to AutomatonNode::process
#[derive(Debug, Clone)]
pub struct NodeEventContext {
    pub source: EventSource,
    pub event_type: EventType,
    pub ts_orig: Option<sinex_primitives::temporal::Timestamp>,
    pub event_id: Ulid,
}

/// Action to take when an error occurs during processing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorAction {
    /// Retry the event (with backoff)
    Retry,
    /// Send to dead-letter queue
    SendToDLQ,
    /// Skip the event and continue
    Skip,
}

/// Configuration for `AutomatonNodeAdapter`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeAdapterConfig {
    /// How often to persist state checkpoint (in events)
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval: u64,

    /// How often to persist state checkpoint (in seconds)
    #[serde(default = "default_checkpoint_timeout_secs")]
    pub checkpoint_timeout_secs: u64,

    /// Maximum batch size for event processing
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Consumer group name (defaults to node name)
    #[serde(default)]
    pub consumer_group: Option<String>,

    /// Additional node-specific configuration
    #[serde(default, flatten)]
    pub extra: HashMap<String, JsonValue>,
}

fn default_checkpoint_interval() -> u64 {
    1000
}

fn default_checkpoint_timeout_secs() -> u64 {
    10
}

fn default_batch_size() -> usize {
    100
}

impl Default for NodeAdapterConfig {
    fn default() -> Self {
        Self {
            checkpoint_interval: default_checkpoint_interval(),
            checkpoint_timeout_secs: default_checkpoint_timeout_secs(),
            batch_size: default_batch_size(),
            consumer_group: None,
            extra: HashMap::new(),
        }
    }
}

/// Persisted state wrapper that includes both user state and checkpoint info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedState<S> {
    /// User-defined state
    pub state: S,
    /// Number of events processed
    pub events_processed: u64,
    /// Last checkpoint time
    pub last_checkpoint: sinex_primitives::temporal::Timestamp,
    /// State version for migration support
    pub version: u32,
}

impl<S: Default> Default for PersistedState<S> {
    fn default() -> Self {
        Self {
            state: S::default(),
            events_processed: 0,
            last_checkpoint: sinex_primitives::temporal::now(),
            version: 1,
        }
    }
}

/// The main trait for stateful automaton nodes.
///
/// This trait is designed to be:
/// - **Minimal**: Only implement what matters (business logic)
/// - **LLM-friendly**: Constrained enough that LLMs generate correct code
/// - **State-aware**: Custom state with automatic persistence
/// - **Hot-reload-ready**: State survives process restarts
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not implement `AutomatonNode`",
    label = "missing AutomatonNode implementation",
    note = "implement `name()`, `input_event_type()`, `output_event_type()`, and `process()` — see crate/lib/sinex-node-sdk/docs/overview.md"
)]
pub trait AutomatonNode: Send + Sync + 'static {
    /// Custom state that will be automatically persisted and restored.
    /// Must implement Serialize, Deserialize, Default, and Send + Sync.
    type State: Serialize + DeserializeOwned + Default + Send + Sync;

    /// Input event type. Parsed from incoming JSON events.
    type Input: DeserializeOwned + Send;

    /// Output event type. Serialized to JSON for outgoing events.
    type Output: Serialize + Send;

    /// Node name (used for logging and checkpoints)
    fn name(&self) -> &'static str;

    /// Input event type to subscribe to (e.g., "terminal.command.executed")
    fn input_event_type(&self) -> &'static str;

    /// Output event type to emit (e.g., "git.activity.detected")
    fn output_event_type(&self) -> &'static str;

    /// Output event source (defaults to node name)
    fn output_event_source(&self) -> &'static str {
        self.name()
    }

    /// Process a single event.
    ///
    /// # Arguments
    /// - `state`: Mutable reference to your custom state (auto-persisted)
    /// - `input`: The parsed input event
    ///
    /// # Returns
    /// - `Ok(Some(output))`: Emit an output event
    /// - `Ok(None)`: No output for this input (filtered)
    /// - `Err(e)`: Processing failed
    fn process(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        context: &NodeEventContext,
    ) -> impl std::future::Future<Output = Result<Option<Self::Output>, NodeLogicError>> + Send;

    /// Handle processing errors (default: send to DLQ)
    fn handle_error(&self, _error: &NodeLogicError) -> ErrorAction {
        ErrorAction::SendToDLQ
    }

    /// Called when the node initializes (optional hook)
    fn on_initialize(
        &mut self,
        _state: &Self::State,
    ) -> impl std::future::Future<Output = Result<(), NodeLogicError>> + Send {
        async { Ok(()) }
    }

    /// Called before shutdown (optional hook)
    fn on_shutdown(
        &mut self,
        _state: &Self::State,
    ) -> impl std::future::Future<Output = Result<(), NodeLogicError>> + Send {
        async { Ok(()) }
    }
}

/// Adapter that implements the full `Node` trait for an `AutomatonNode`.
pub struct AutomatonNodeAdapter<P>
where
    P: AutomatonNode,
{
    node: P,
    persisted_state: PersistedState<P::State>,
    config: NodeAdapterConfig,
    shutdown_config: ShutdownConfig,
    runtime: Option<NodeRuntimeState>,
    checkpoint_manager: Option<Arc<CheckpointManager>>,
    event_sender: Option<EventSender>,
    shutdown_tx: Option<watch::Sender<bool>>,
    host: String,
    events_since_checkpoint: u64,
    last_checkpoint_time: Instant,
    last_revision: u64,
    #[cfg(feature = "messaging")]
    health_reporter: Option<Arc<crate::health_reporter::HealthReporter>>,
}

impl<P> AutomatonNodeAdapter<P>
where
    P: AutomatonNode,
{
    /// Create a new adapter from node logic.
    pub fn with_node(node_logic: P) -> Self {
        Self {
            node: node_logic,
            persisted_state: PersistedState::default(),
            config: NodeAdapterConfig::default(),
            shutdown_config: ShutdownConfig::default(),
            runtime: None,
            checkpoint_manager: None,
            event_sender: None,
            shutdown_tx: None,
            host: gethostname::gethostname().to_string_lossy().to_string(),
            events_since_checkpoint: 0,
            last_checkpoint_time: Instant::now(),
            last_revision: 0,
            #[cfg(feature = "messaging")]
            health_reporter: None,
        }
    }

    /// Create a new adapter from node logic.
    pub fn new(node_logic: P) -> Self {
        Self::with_node(node_logic)
    }

    /// Create with custom config
    pub fn with_config(node_logic: P, config: NodeAdapterConfig) -> Self {
        let mut node = Self::with_node(node_logic);
        node.config = config;
        node
    }

    /// Create with custom shutdown config
    pub fn with_shutdown_config(node_logic: P, shutdown_config: ShutdownConfig) -> Self {
        let mut node = Self::with_node(node_logic);
        node.shutdown_config = shutdown_config;
        node
    }

    /// Create with both configs
    pub fn with_configs(
        node_logic: P,
        config: NodeAdapterConfig,
        shutdown_config: ShutdownConfig,
    ) -> Self {
        let mut node = Self::with_node(node_logic);
        node.config = config;
        node.shutdown_config = shutdown_config;
        node
    }
}

/// Default implementation for `AutomatonNodeAdapter` when node logic implements `Default`.
/// This enables the `node_entrypoint!` macro to work with type aliases.
impl<P> Default for AutomatonNodeAdapter<P>
where
    P: AutomatonNode + Default,
{
    fn default() -> Self {
        Self::with_node(P::default())
    }
}

impl<P> AutomatonNodeAdapter<P>
where
    P: AutomatonNode,
{
    /// Load state from checkpoint.
    ///
    /// Priority order:
    /// 1. File-based checkpoint (for hot reload state continuity)
    /// 2. NATS KV checkpoint (primary storage)
    /// 3. Default state (fresh start)
    async fn load_state(&mut self) -> NodeResult<()> {
        // Priority 1: file-based checkpoint (hot reload scenario)
        if self.shutdown_config.restore_state_on_startup {
            if let Some(persisted) = self.try_restore_from_file().await {
                self.persisted_state = persisted;
                return Ok(());
            }
        }

        // Priority 2: NATS KV checkpoint
        let Some(checkpoint_mgr) = &self.checkpoint_manager else {
            return Ok(());
        };

        let checkpoint_state = checkpoint_mgr.load_checkpoint().await?;
        match checkpoint_state
            .data
            .and_then(|data| serde_json::from_value::<PersistedState<P::State>>(data).ok())
        {
            Some(persisted) => {
                info!(
                    node = %self.node.name(),
                    events_processed = persisted.events_processed,
                    "Restored state from NATS KV checkpoint"
                );
                self.persisted_state = persisted;
                self.last_revision = checkpoint_state.revision;
            }
            None => {
                info!(
                    node = %self.node.name(),
                    "No valid checkpoint data found, starting fresh"
                );
                self.persisted_state = PersistedState::default();
            }
        }

        Ok(())
    }

    /// Try to restore persisted state from a hot-reload file checkpoint.
    ///
    /// Returns the deserialized state on success, cleaning up the file.
    /// Returns `None` if no file exists, data is missing, or deserialization fails.
    async fn try_restore_from_file(&self) -> Option<PersistedState<P::State>> {
        let checkpoint_path = self.shutdown_config.checkpoint_path(self.node.name());
        let file_state = CheckpointState::load_from_file(&checkpoint_path).await?;
        let data = file_state.data?;

        match serde_json::from_value::<PersistedState<P::State>>(data) {
            Ok(persisted) => {
                info!(
                    node = %self.node.name(),
                    events_processed = persisted.events_processed,
                    "Restored state from hot reload file"
                );
                if let Err(e) = CheckpointState::delete_file(&checkpoint_path).await {
                    error!(
                        node = %self.node.name(),
                        error = %e,
                        "Failed to delete hot reload file after loading state"
                    );
                }
                Some(persisted)
            }
            Err(e) => {
                warn!(
                    node = %self.node.name(),
                    error = %e,
                    "Failed to deserialize file checkpoint state"
                );
                None
            }
        }
    }

    /// Save state to file for hot reload.
    ///
    /// Called when SIGTERM is received to preserve state before exit.
    pub async fn save_state_to_file(&self) -> std::io::Result<()> {
        if !self.shutdown_config.save_state_on_shutdown {
            return Ok(());
        }

        let checkpoint_path = self.shutdown_config.checkpoint_path(self.node.name());

        let state_json = serde_json::to_value(&self.persisted_state)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let checkpoint_state = CheckpointState {
            checkpoint: Checkpoint::external(
                serde_json::json!({"version": self.persisted_state.version}),
                format!("automaton_node_{}", self.node.name()),
            ),
            processed_count: self.persisted_state.events_processed,
            last_activity: sinex_primitives::temporal::now(),
            data: Some(state_json),
            version: 2,
            revision: self.last_revision,
        };

        checkpoint_state.save_to_file(&checkpoint_path).await
    }

    /// Save state to checkpoint
    async fn save_state(&mut self) -> NodeResult<()> {
        let Some(checkpoint_mgr) = &self.checkpoint_manager else {
            return Ok(());
        };

        self.persisted_state.last_checkpoint = sinex_primitives::temporal::now();
        let state_json = serde_json::to_value(&self.persisted_state)
            .map_err(|e| SinexError::processing(format!("Failed to serialize state: {e}")))?;

        let checkpoint_state = CheckpointState {
            checkpoint: Checkpoint::external(
                serde_json::json!({"version": self.persisted_state.version}),
                format!("automaton_node_{}", self.node.name()),
            ),
            processed_count: self.persisted_state.events_processed,
            last_activity: sinex_primitives::temporal::now(),
            data: Some(state_json),
            version: 2,
            revision: self.last_revision,
        };

        // Update revision on successful save
        self.last_revision = checkpoint_mgr.save_checkpoint(&checkpoint_state).await?;

        self.events_since_checkpoint = 0;
        self.last_checkpoint_time = Instant::now();

        debug!(
            node = %self.node.name(),
            events_processed = self.persisted_state.events_processed,
            revision = self.last_revision,
            "Saved checkpoint"
        );

        Ok(())
    }

    /// Check if checkpoint should be saved
    fn should_checkpoint(&self) -> bool {
        self.events_since_checkpoint >= self.config.checkpoint_interval
            || self.last_checkpoint_time.elapsed()
                >= Duration::from_secs(self.config.checkpoint_timeout_secs)
    }

    /// Process a single event and return the output event if any
    pub async fn process_one(
        &mut self,
        event: Event<JsonValue>,
    ) -> NodeResult<Option<Event<JsonValue>>> {
        // Parse input
        let input: P::Input = serde_json::from_value(event.payload.clone()).map_err(|e| {
            SinexError::processing(format!(
                "Failed to parse input event {}: {}",
                event.event_type, e
            ))
        })?;

        // Get source event ID for provenance (clone to avoid partial move)
        let source_event_id = event.id.unwrap_or_default();

        // Build context
        let context = NodeEventContext {
            source: event.source.clone(),
            event_type: event.event_type.clone(),
            ts_orig: event.ts_orig,
            event_id: source_event_id.into(),
        };

        // Process
        let result = self
            .node
            .process(&mut self.persisted_state.state, input, &context)
            .await;

        // Track health (success/error)
        #[cfg(feature = "messaging")]
        if let Some(ref reporter) = self.health_reporter {
            match &result {
                Ok(_) => reporter.record_success(),
                Err(e) => {
                    // Convert NodeLogicError to SinexError for health tracking
                    let sinex_error = SinexError::processing(e.to_string());
                    reporter.record_error(&sinex_error);
                }
            }

            // Periodic health check (every 100 events)
            if self.persisted_state.events_processed.is_multiple_of(100) {
                if let Err(e) = reporter.check_and_emit().await {
                    warn!(
                        node = %self.node.name(),
                        error = %e,
                        "Failed to emit health status"
                    );
                }
            }
        }

        match result {
            Ok(Some(output)) => {
                // Build output event
                let output_payload = serde_json::to_value(&output).map_err(|e| {
                    SinexError::processing(format!("Failed to serialize output: {e}"))
                })?;

                let output_event = Event {
                    id: Some(Id::new()),
                    source: EventSource::new(self.node.output_event_source()),
                    event_type: EventType::new(self.node.output_event_type()),
                    payload: output_payload,
                    ts_orig: Some(sinex_primitives::temporal::now()),
                    host: HostName::new(&self.host),
                    node_version: None,
                    payload_schema_id: None,
                    provenance: Provenance::Synthesis {
                        source_event_ids: sinex_primitives::non_empty::NonEmptyVec::from_head_tail(
                            source_event_id,
                            vec![],
                        ),
                        operation_id: None,
                    },
                    associated_blob_ids: None,
                };

                self.persisted_state.events_processed += 1;
                self.events_since_checkpoint += 1;

                Ok(Some(output_event))
            }
            Ok(None) => {
                // Filtered out, no output
                self.persisted_state.events_processed += 1;
                self.events_since_checkpoint += 1;
                Ok(None)
            }
            Err(e) => {
                let action = self.node.handle_error(&e);
                match action {
                    ErrorAction::Skip => {
                        warn!(
                            node = %self.node.name(),
                            error = %e,
                            "Skipping event due to processing error"
                        );
                        self.persisted_state.events_processed += 1;
                        self.events_since_checkpoint += 1;
                        Ok(None)
                    }
                    ErrorAction::SendToDLQ => {
                        // Send to DLQ via transport if available
                        if let Some(ref runtime) = self.runtime {
                            let transport = runtime.handles().transport();
                            if let Err(dlq_err) = transport
                                .send_to_dlq(&event, &e.to_string(), self.node.name())
                                .await
                            {
                                error!(
                                    node = %self.node.name(),
                                    error = %e,
                                    dlq_error = %dlq_err,
                                    "Failed to send event to DLQ"
                                );
                            }
                        } else {
                            // No runtime available (e.g., during testing) - just log
                            warn!(
                                node = %self.node.name(),
                                error = %e,
                                "Event would be sent to DLQ but no transport available"
                            );
                        }
                        self.persisted_state.events_processed += 1;
                        self.events_since_checkpoint += 1;
                        Ok(None)
                    }
                    ErrorAction::Retry => {
                        // Return error to trigger retry
                        Err(e.into())
                    }
                }
            }
        }
    }

    /// Process a batch of events
    pub async fn process_batch(
        &mut self,
        events: Vec<Event<JsonValue>>,
    ) -> NodeResult<Vec<Event<JsonValue>>> {
        let mut outputs = Vec::new();

        for event in events {
            match self.process_one(event).await {
                Ok(Some(output_event)) => {
                    outputs.push(output_event);
                }
                Ok(None) => {
                    // Filtered, no output
                }
                Err(e) => {
                    error!(
                        node = %self.node.name(),
                        error = %e,
                        "Error processing event in batch"
                    );
                    // Continue with next event
                }
            }
        }

        // Checkpoint if needed
        if self.should_checkpoint() {
            if let Err(e) = self.save_state().await {
                warn!(
                    node = %self.node.name(),
                    error = %e,
                    "Failed to save checkpoint after batch"
                );
            }
        }

        Ok(outputs)
    }

    /// Run continuous processing loop (called by the stream node runner)
    ///
    /// Note: For Phase 1, this is a placeholder. The actual continuous loop
    /// will be implemented in Phase 2/3 with the sx dev orchestrator.
    async fn run_continuous(&mut self, _from: Checkpoint) -> NodeResult<ScanReport> {
        let start = Instant::now();
        let events_processed = 0u64;

        // For Phase 1, we just signal that this requires external event delivery.
        // The sx dev orchestrator (Phase 3) will handle the actual NATS subscription
        // and event delivery to process_batch().

        info!(
            node = %self.node.name(),
            input_type = %self.node.input_event_type(),
            output_type = %self.node.output_event_type(),
            "AutomatonNode initialized - awaiting events via process_batch()"
        );

        // Set up shutdown channel for external control
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        // Wait for shutdown signal
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!(node = %self.node.name(), "Shutdown signal received");
                        break;
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(60)) => {
                    // Periodic checkpoint even when idle
                    if self.events_since_checkpoint > 0 {
                        if let Err(e) = self.save_state().await {
                            warn!(
                                node = %self.node.name(),
                                error = %e,
                                "Failed to save periodic checkpoint"
                            );
                        }
                    }
                }
            }
        }

        // Final checkpoint
        if let Err(e) = self.save_state().await {
            warn!(
                node = %self.node.name(),
                error = %e,
                "Failed to save final checkpoint"
            );
        }

        Ok(ScanReport {
            events_processed,
            duration: start.elapsed(),
            final_checkpoint: self.current_checkpoint_internal(),
            time_range: None,
            node_stats: HashMap::from([(
                "total_processed".to_string(),
                self.persisted_state.events_processed,
            )]),
            successful_targets: vec![],
            failed_targets: vec![],
            warnings: vec![],
        })
    }

    fn current_checkpoint_internal(&self) -> Checkpoint {
        let state_json = serde_json::to_value(&self.persisted_state).unwrap_or(JsonValue::Null);
        Checkpoint::external(state_json, format!("automaton_node_{}", self.node.name()))
    }

    /// Get the node's current state (for testing/debugging)
    pub fn state(&self) -> &P::State {
        &self.persisted_state.state
    }

    /// Get the number of events processed
    pub fn events_processed(&self) -> u64 {
        self.persisted_state.events_processed
    }

    /// Signal shutdown
    pub fn signal_shutdown(&self) {
        if let Some(tx) = &self.shutdown_tx {
            let _ = tx.send(true);
        }
    }
}

/// Node trait implementation for AutomatonNodeAdapter
impl<P> crate::runtime::stream::Node for AutomatonNodeAdapter<P>
where
    P: AutomatonNode,
{
    type Config = NodeAdapterConfig;

    async fn initialize(&mut self, init: NodeInitContext<Self::Config>) -> NodeResult<()> {
        let (config, runtime) = init.into_runtime();
        self.config = config;

        // Get checkpoint manager
        self.checkpoint_manager = Some(runtime.checkpoint_manager().clone());
        self.event_sender = Some(runtime.event_sender().clone());

        // Store host from runtime
        self.host = runtime.service_info().host().to_string();

        // Auto-enable health monitoring if NATS is available
        #[cfg(feature = "messaging")]
        {
            if let Some(nats_client) = runtime.nats_client() {
                use crate::health_reporter::{HealthReporter, HealthThresholds};
                use crate::self_observation::{SelfObserver, SelfObserverConfig};
                use std::time::Duration;

                // Check if health monitoring is enabled (default: yes)
                let health_enabled = std::env::var("SINEX_HEALTH_MONITORING_ENABLED")
                    .map_or(true, |v| v != "false" && v != "0");

                if health_enabled {
                    let config = SelfObserverConfig {
                        component: self.node.name().to_string(),
                        subject_prefix: "sinex.telemetry".to_string(),
                        enabled: true,
                        min_emission_interval: Duration::from_secs(1),
                    };

                    let observer = Arc::new(SelfObserver::new(nats_client, config));
                    let thresholds = HealthThresholds::from_env().unwrap_or_default();

                    self.health_reporter = Some(Arc::new(HealthReporter::new(
                        self.node.name().to_string(),
                        observer,
                        thresholds,
                    )));

                    info!(
                        node = %self.node.name(),
                        "Health monitoring auto-enabled"
                    );
                }
            }
        }

        // Load state from checkpoint
        self.runtime = Some(runtime);
        self.load_state().await?;

        // Call user hook
        self.node
            .on_initialize(&self.persisted_state.state)
            .await
            .map_err(|e| SinexError::processing(format!("Initialize hook failed: {e}")))?;

        info!(
            node = %self.node.name(),
            events_processed = self.persisted_state.events_processed,
            "AutomatonNode initialized"
        );

        Ok(())
    }

    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        match until {
            TimeHorizon::Continuous => self.run_continuous(from).await,
            TimeHorizon::Snapshot | TimeHorizon::Historical { .. } => {
                // AutomatonNode only supports continuous mode
                Err(SinexError::unknown(
                    "AutomatonNode only supports continuous mode",
                ))
            }
        }
    }

    fn node_name(&self) -> &str {
        self.node.name()
    }

    fn node_type(&self) -> NodeType {
        NodeType::Automaton
    }

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            supports_continuous: true,
            supports_historical: false,
            supports_snapshot: false,
            supports_interactive: false,
            max_scan_size: None,
            supports_concurrent: false,
            manages_own_continuous_loop: true,
        }
    }

    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
        Ok(self.current_checkpoint_internal())
    }

    async fn health_check(&self) -> NodeResult<bool> {
        Ok(true)
    }

    async fn shutdown(&mut self) -> NodeResult<()> {
        info!(node = %self.node.name(), "Shutting down AutomatonNode");

        // Signal shutdown
        self.signal_shutdown();

        // Call user hook
        if let Err(e) = self.node.on_shutdown(&self.persisted_state.state).await {
            warn!(
                node = %self.node.name(),
                error = %e,
                "Shutdown hook failed"
            );
        }

        let mut file_save_success = true;
        // Save state to file for hot reload (fast, no network required)
        if let Err(e) = self.save_state_to_file().await {
            warn!(
                node = %self.node.name(),
                error = %e,
                "Failed to save state to file for hot reload"
            );
            file_save_success = false;
        }

        let mut nats_save_success = true;
        // Also save to NATS KV (primary checkpoint)
        if let Err(e) = self.save_state().await {
            warn!(
                node = %self.node.name(),
                error = %e,
                "Failed to save final checkpoint on shutdown"
            );
            nats_save_success = false;
        }

        if !file_save_success && !nats_save_success {
            return Err(SinexError::checkpoint(
                "Failed to save final state to both file and NATS KV on shutdown".to_string(),
            ));
        }

        Ok(())
    }

    async fn estimate_scan_scope(
        &self,
        _from: &Checkpoint,
        _until: &TimeHorizon,
        _args: &ScanArgs,
    ) -> NodeResult<ScanEstimate> {
        Ok(ScanEstimate::default())
    }
}

/// ExplorationProvider implementation for AutomatonNodeAdapter
///
/// Automatons don't have traditional "ingestion" semantics, so this provides
/// stub implementations that report basic health status.
impl<P> crate::exploration::ExplorationProvider for AutomatonNodeAdapter<P>
where
    P: AutomatonNode,
{
    fn get_source_state(&self) -> NodeResult<crate::exploration::SourceState> {
        Ok(crate::exploration::SourceState {
            is_connected: true,
            healthy: true,
            description: format!("{} automaton", self.node.name()),
            last_updated: sinex_primitives::temporal::now(),
            lag_seconds: None,
            recent_activity: Vec::new(),
            total_items: None,
            metadata: std::collections::HashMap::new(),
        })
    }

    fn get_ingestion_history(
        &self,
        _limit: u64,
    ) -> NodeResult<Vec<crate::exploration::IngestionHistoryEntry>> {
        // Automatons process events rather than ingest from sources
        Ok(Vec::new())
    }

    fn get_coverage_analysis(
        &self,
        _time_range: Option<(
            sinex_primitives::temporal::Timestamp,
            sinex_primitives::temporal::Timestamp,
        )>,
    ) -> NodeResult<crate::exploration::CoverageAnalysis> {
        Ok(crate::exploration::CoverageAnalysis {
            time_range: (
                sinex_primitives::temporal::now(),
                sinex_primitives::temporal::now(),
            ),
            source_total: 0,
            sinex_total: 0,
            coverage_percentage: 100.0,
            missing_count: 0,
            duplicate_count: 0,
            missing_samples: Vec::new(),
            recommendations: Vec::new(),
        })
    }

    fn export_data(
        &self,
        _path: &sinex_primitives::domain::SanitizedPath,
        _format: crate::exploration::ExportFormat,
    ) -> NodeResult<()> {
        // Automatons don't have data to export in the traditional sense
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::*;

    #[derive(Serialize, Deserialize, Default)]
    struct TestState {
        count: u64,
    }

    #[derive(Deserialize)]
    struct TestInput {
        value: String,
    }

    #[derive(Serialize)]
    struct TestOutput {
        processed_value: String,
    }

    struct TestNodeLogic;

    impl AutomatonNode for TestNodeLogic {
        type State = TestState;
        type Input = TestInput;
        type Output = TestOutput;

        fn name(&self) -> &'static str {
            "test-node"
        }

        fn input_event_type(&self) -> &'static str {
            "test.input"
        }

        fn output_event_type(&self) -> &'static str {
            "test.output"
        }

        async fn process(
            &mut self,
            state: &mut Self::State,
            input: Self::Input,
            _context: &NodeEventContext,
        ) -> Result<Option<Self::Output>, NodeLogicError> {
            state.count += 1;
            Ok(Some(TestOutput {
                processed_value: input.value.to_uppercase(),
            }))
        }
    }

    #[sinex_test]
    async fn test_automaton_node_creation() -> TestResult<()> {
        let node_logic = TestNodeLogic;
        let node = AutomatonNodeAdapter::new(node_logic);
        assert_eq!(node.node.name(), "test-node");
        assert_eq!(node.events_processed(), 0);
        Ok(())
    }

    #[sinex_test]
    async fn test_config_defaults() -> TestResult<()> {
        let config = NodeAdapterConfig::default();
        assert_eq!(config.checkpoint_interval, 1000);
        assert_eq!(config.checkpoint_timeout_secs, 10);
        assert_eq!(config.batch_size, 100);
        Ok(())
    }
}
