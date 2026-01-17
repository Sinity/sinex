//! SimpleProcessor trait for LLM-friendly node development.
//!
//! This module provides a high-level abstraction that reduces typical node
//! implementations from 200+ lines to ~10 lines. The trait is designed to be
//! simple enough that LLMs can reliably generate correct implementations.
//!
//! # Example
//!
//! ```rust,ignore
//! use sinex_node_sdk::simple_processor::{SimpleProcessor, SimpleProcessorConfig};
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
//! impl SimpleProcessor for GitActivityDetector {
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
//!     ) -> Result<Option<Self::Output>, SimpleProcessorError> {
//!         if !input.command.starts_with("git ") {
//!             return Ok(None);
//!         }
//!         state.commands_seen += 1;
//!         Ok(Some(GitActivityEvent { ... }))
//!     }
//! }
//! ```

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sinex_core::db::models::{Event, EventId, Provenance};
use sinex_core::types::non_empty::NonEmptyVec;
use sinex_core::{EventSource, EventType, HostName, JsonValue};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::checkpoint::{CheckpointManager, CheckpointState};
use crate::shutdown::ShutdownConfig;
use crate::stream_processor::{
    Checkpoint, EventSender, ProcessorCapabilities, ProcessorInitContext, ProcessorRuntimeState,
    ProcessorType, ScanArgs, ScanEstimate, ScanReport, TimeHorizon,
};
use crate::{NodeError, NodeResult};

/// Errors specific to SimpleProcessor
#[derive(Debug, Error)]
pub enum SimpleProcessorError {
    #[error("Processing error: {0}")]
    Processing(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Input parsing error: {0}")]
    InputParsing(String),

    #[error("Output serialization error: {0}")]
    OutputSerialization(String),
}

impl From<SimpleProcessorError> for NodeError {
    fn from(err: SimpleProcessorError) -> Self {
        NodeError::Processing(err.to_string())
    }
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

/// Configuration for SimpleProcessor wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleProcessorConfig {
    /// How often to persist state checkpoint (in events)
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval: u64,

    /// How often to persist state checkpoint (in seconds)
    #[serde(default = "default_checkpoint_timeout_secs")]
    pub checkpoint_timeout_secs: u64,

    /// Maximum batch size for event processing
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Consumer group name (defaults to processor name)
    #[serde(default)]
    pub consumer_group: Option<String>,

    /// Additional processor-specific configuration
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

impl Default for SimpleProcessorConfig {
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
    pub last_checkpoint: DateTime<Utc>,
    /// State version for migration support
    pub version: u32,
}

impl<S: Default> Default for PersistedState<S> {
    fn default() -> Self {
        Self {
            state: S::default(),
            events_processed: 0,
            last_checkpoint: Utc::now(),
            version: 1,
        }
    }
}

/// The main trait for simple event processors.
///
/// This trait is designed to be:
/// - **Minimal**: Only implement what matters (business logic)
/// - **LLM-friendly**: Constrained enough that LLMs generate correct code
/// - **State-aware**: Custom state with automatic persistence
/// - **Hot-reload-ready**: State survives process restarts
#[async_trait]
pub trait SimpleProcessor: Send + Sync + 'static {
    /// Custom state that will be automatically persisted and restored.
    /// Must implement Serialize, Deserialize, Default, and Send + Sync.
    type State: Serialize + DeserializeOwned + Default + Send + Sync;

    /// Input event type. Parsed from incoming JSON events.
    type Input: DeserializeOwned + Send;

    /// Output event type. Serialized to JSON for outgoing events.
    type Output: Serialize + Send;

    /// Processor name (used for logging and checkpoints)
    fn name(&self) -> &'static str;

    /// Input event type to subscribe to (e.g., "terminal.command.executed")
    fn input_event_type(&self) -> &'static str;

    /// Output event type to emit (e.g., "git.activity.detected")
    fn output_event_type(&self) -> &'static str;

    /// Output event source (defaults to processor name)
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
    async fn process(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
    ) -> Result<Option<Self::Output>, SimpleProcessorError>;

    /// Handle processing errors (default: send to DLQ)
    fn handle_error(&self, _error: &SimpleProcessorError) -> ErrorAction {
        ErrorAction::SendToDLQ
    }

    /// Called when processor initializes (optional hook)
    async fn on_initialize(&mut self, _state: &Self::State) -> Result<(), SimpleProcessorError> {
        Ok(())
    }

    /// Called before shutdown (optional hook)
    async fn on_shutdown(&mut self, _state: &Self::State) -> Result<(), SimpleProcessorError> {
        Ok(())
    }
}

/// Wrapper that implements the full Node trait for a SimpleProcessor
pub struct SimpleProcessorNode<P>
where
    P: SimpleProcessor,
{
    processor: P,
    persisted_state: PersistedState<P::State>,
    config: SimpleProcessorConfig,
    shutdown_config: ShutdownConfig,
    runtime: Option<ProcessorRuntimeState>,
    checkpoint_manager: Option<Arc<CheckpointManager>>,
    event_sender: Option<EventSender>,
    shutdown_tx: Option<watch::Sender<bool>>,
    host: String,
    events_since_checkpoint: u64,
    last_checkpoint_time: Instant,
}

impl<P> SimpleProcessorNode<P>
where
    P: SimpleProcessor,
{
    /// Create a new SimpleProcessorNode wrapping the given processor
    pub fn new(processor: P) -> Self {
        Self {
            processor,
            persisted_state: PersistedState::default(),
            config: SimpleProcessorConfig::default(),
            shutdown_config: ShutdownConfig::default(),
            runtime: None,
            checkpoint_manager: None,
            event_sender: None,
            shutdown_tx: None,
            host: gethostname::gethostname().to_string_lossy().to_string(),
            events_since_checkpoint: 0,
            last_checkpoint_time: Instant::now(),
        }
    }

    /// Create with custom config
    pub fn with_config(processor: P, config: SimpleProcessorConfig) -> Self {
        let mut node = Self::new(processor);
        node.config = config;
        node
    }

    /// Create with custom shutdown config
    pub fn with_shutdown_config(processor: P, shutdown_config: ShutdownConfig) -> Self {
        let mut node = Self::new(processor);
        node.shutdown_config = shutdown_config;
        node
    }

    /// Create with both configs
    pub fn with_configs(
        processor: P,
        config: SimpleProcessorConfig,
        shutdown_config: ShutdownConfig,
    ) -> Self {
        let mut node = Self::new(processor);
        node.config = config;
        node.shutdown_config = shutdown_config;
        node
    }

    /// Load state from checkpoint.
    ///
    /// Priority order:
    /// 1. File-based checkpoint (for hot reload state continuity)
    /// 2. NATS KV checkpoint (primary storage)
    /// 3. Default state (fresh start)
    async fn load_state(&mut self) -> NodeResult<()> {
        // First, try to load from file (hot reload scenario)
        if self.shutdown_config.restore_state_on_startup {
            let checkpoint_path = self.shutdown_config.checkpoint_path(self.processor.name());
            if let Some(file_state) = CheckpointState::load_from_file(&checkpoint_path) {
                // Try to restore our state from the file's data field
                if let Some(data) = file_state.data {
                    match serde_json::from_value::<PersistedState<P::State>>(data) {
                        Ok(persisted) => {
                            info!(
                                processor = %self.processor.name(),
                                events_processed = persisted.events_processed,
                                "Restored state from hot reload file"
                            );
                            self.persisted_state = persisted;

                            // Clean up the file since we've loaded it
                            let _ = CheckpointState::delete_file(&checkpoint_path);
                            return Ok(());
                        }
                        Err(e) => {
                            warn!(
                                processor = %self.processor.name(),
                                error = %e,
                                "Failed to deserialize file checkpoint state"
                            );
                        }
                    }
                }
            }
        }

        // Fall back to NATS KV checkpoint
        let Some(checkpoint_mgr) = &self.checkpoint_manager else {
            return Ok(());
        };

        let checkpoint_state = checkpoint_mgr.load_checkpoint().await?;

        // Try to restore state from the checkpoint's data field
        if let Some(data) = checkpoint_state.data {
            match serde_json::from_value::<PersistedState<P::State>>(data) {
                Ok(persisted) => {
                    info!(
                        processor = %self.processor.name(),
                        events_processed = persisted.events_processed,
                        "Restored state from NATS KV checkpoint"
                    );
                    self.persisted_state = persisted;
                }
                Err(e) => {
                    warn!(
                        processor = %self.processor.name(),
                        error = %e,
                        "Failed to deserialize checkpoint state, starting fresh"
                    );
                    self.persisted_state = PersistedState::default();
                }
            }
        } else {
            info!(
                processor = %self.processor.name(),
                "No checkpoint data found, starting fresh"
            );
            self.persisted_state = PersistedState::default();
        }

        Ok(())
    }

    /// Save state to file for hot reload.
    ///
    /// Called when SIGTERM is received to preserve state before exit.
    pub fn save_state_to_file(&self) -> std::io::Result<()> {
        if !self.shutdown_config.save_state_on_shutdown {
            return Ok(());
        }

        let checkpoint_path = self.shutdown_config.checkpoint_path(self.processor.name());

        let state_json = serde_json::to_value(&self.persisted_state)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let checkpoint_state = CheckpointState {
            checkpoint: Checkpoint::external(
                serde_json::json!({"version": self.persisted_state.version}),
                format!("simple_processor_{}", self.processor.name()),
            ),
            processed_count: self.persisted_state.events_processed,
            last_activity: chrono::Utc::now(),
            data: Some(state_json),
            version: 2,
        };

        checkpoint_state.save_to_file(&checkpoint_path)
    }

    /// Save state to checkpoint
    async fn save_state(&mut self) -> NodeResult<()> {
        let Some(checkpoint_mgr) = &self.checkpoint_manager else {
            return Ok(());
        };

        self.persisted_state.last_checkpoint = Utc::now();
        let state_json = serde_json::to_value(&self.persisted_state)
            .map_err(|e| NodeError::Processing(format!("Failed to serialize state: {}", e)))?;

        let checkpoint_state = CheckpointState {
            checkpoint: Checkpoint::external(
                serde_json::json!({"version": self.persisted_state.version}),
                format!("simple_processor_{}", self.processor.name()),
            ),
            processed_count: self.persisted_state.events_processed,
            last_activity: Utc::now(),
            data: Some(state_json),
            version: 2,
        };

        checkpoint_mgr.save_checkpoint(&checkpoint_state).await?;

        self.events_since_checkpoint = 0;
        self.last_checkpoint_time = Instant::now();

        debug!(
            processor = %self.processor.name(),
            events_processed = self.persisted_state.events_processed,
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
            NodeError::Processing(format!(
                "Failed to parse input event {}: {}",
                event.event_type, e
            ))
        })?;

        // Get source event ID for provenance
        let source_event_id = event.id.unwrap_or_else(EventId::new);

        // Process
        match self
            .processor
            .process(&mut self.persisted_state.state, input)
            .await
        {
            Ok(Some(output)) => {
                // Build output event
                let output_payload = serde_json::to_value(&output).map_err(|e| {
                    NodeError::Processing(format!("Failed to serialize output: {}", e))
                })?;

                let output_event = Event {
                    id: Some(EventId::new()),
                    source: EventSource::new(self.processor.output_event_source()),
                    event_type: EventType::new(self.processor.output_event_type()),
                    payload: output_payload,
                    ts_orig: Some(Utc::now()),
                    host: HostName::new(&self.host),
                    ingestor_version: None,
                    payload_schema_id: None,
                    provenance: Provenance::Synthesis {
                        source_event_ids: NonEmptyVec::single(source_event_id),
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
                let action = self.processor.handle_error(&e);
                match action {
                    ErrorAction::Skip => {
                        warn!(
                            processor = %self.processor.name(),
                            error = %e,
                            "Skipping event due to processing error"
                        );
                        self.persisted_state.events_processed += 1;
                        self.events_since_checkpoint += 1;
                        Ok(None)
                    }
                    ErrorAction::SendToDLQ => {
                        // TODO: Actually send to DLQ
                        error!(
                            processor = %self.processor.name(),
                            error = %e,
                            "Sending event to DLQ due to processing error"
                        );
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
                        processor = %self.processor.name(),
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
                    processor = %self.processor.name(),
                    error = %e,
                    "Failed to save checkpoint after batch"
                );
            }
        }

        Ok(outputs)
    }

    /// Run continuous processing loop (called by the stream processor runner)
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
            processor = %self.processor.name(),
            input_type = %self.processor.input_event_type(),
            output_type = %self.processor.output_event_type(),
            "SimpleProcessor initialized - awaiting events via process_batch()"
        );

        // Set up shutdown channel for external control
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        // Wait for shutdown signal
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!(processor = %self.processor.name(), "Shutdown signal received");
                        break;
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(60)) => {
                    // Periodic checkpoint even when idle
                    if self.events_since_checkpoint > 0 {
                        if let Err(e) = self.save_state().await {
                            warn!(
                                processor = %self.processor.name(),
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
                processor = %self.processor.name(),
                error = %e,
                "Failed to save final checkpoint"
            );
        }

        Ok(ScanReport {
            events_processed,
            duration: start.elapsed(),
            final_checkpoint: self.current_checkpoint_internal(),
            time_range: None,
            processor_stats: HashMap::from([(
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
        Checkpoint::external(
            state_json,
            format!("simple_processor_{}", self.processor.name()),
        )
    }

    /// Get the processor's current state (for testing/debugging)
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

/// Node trait implementation for SimpleProcessorNode
#[async_trait]
impl<P> crate::stream_processor::Node for SimpleProcessorNode<P>
where
    P: SimpleProcessor,
{
    type Config = SimpleProcessorConfig;

    async fn initialize(&mut self, init: ProcessorInitContext<Self::Config>) -> NodeResult<()> {
        let (config, runtime) = init.into_runtime();
        self.config = config;

        // Get checkpoint manager
        self.checkpoint_manager = Some(runtime.checkpoint_manager().clone());
        self.event_sender = Some(runtime.event_sender().clone());

        // Store host from runtime
        self.host = runtime.service_info().host().to_string();

        // Load state from checkpoint
        self.runtime = Some(runtime);
        self.load_state().await?;

        // Call user hook
        self.processor
            .on_initialize(&self.persisted_state.state)
            .await
            .map_err(|e| NodeError::Processing(format!("Initialize hook failed: {}", e)))?;

        info!(
            processor = %self.processor.name(),
            events_processed = self.persisted_state.events_processed,
            "SimpleProcessor initialized"
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
                // SimpleProcessor only supports continuous mode
                Err(NodeError::General(color_eyre::eyre::eyre!(
                    "SimpleProcessor only supports continuous mode"
                )))
            }
        }
    }

    fn processor_name(&self) -> &str {
        self.processor.name()
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Automaton
    }

    fn capabilities(&self) -> ProcessorCapabilities {
        ProcessorCapabilities {
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
        info!(processor = %self.processor.name(), "Shutting down SimpleProcessor");

        // Signal shutdown
        self.signal_shutdown();

        // Call user hook
        if let Err(e) = self
            .processor
            .on_shutdown(&self.persisted_state.state)
            .await
        {
            warn!(
                processor = %self.processor.name(),
                error = %e,
                "Shutdown hook failed"
            );
        }

        // Save state to file for hot reload (fast, no network required)
        if let Err(e) = self.save_state_to_file() {
            warn!(
                processor = %self.processor.name(),
                error = %e,
                "Failed to save state to file for hot reload"
            );
        }

        // Also save to NATS KV (primary checkpoint)
        if let Err(e) = self.save_state().await {
            warn!(
                processor = %self.processor.name(),
                error = %e,
                "Failed to save final checkpoint on shutdown"
            );
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

#[cfg(test)]
mod tests {
    use super::*;

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

    struct TestProcessor;

    #[async_trait]
    impl SimpleProcessor for TestProcessor {
        type State = TestState;
        type Input = TestInput;
        type Output = TestOutput;

        fn name(&self) -> &'static str {
            "test-processor"
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
        ) -> Result<Option<Self::Output>, SimpleProcessorError> {
            state.count += 1;
            Ok(Some(TestOutput {
                processed_value: input.value.to_uppercase(),
            }))
        }
    }

    #[test]
    fn test_simple_processor_creation() {
        let processor = TestProcessor;
        let node = SimpleProcessorNode::new(processor);
        assert_eq!(node.processor.name(), "test-processor");
        assert_eq!(node.events_processed(), 0);
    }

    #[test]
    fn test_config_defaults() {
        let config = SimpleProcessorConfig::default();
        assert_eq!(config.checkpoint_interval, 1000);
        assert_eq!(config.checkpoint_timeout_secs, 10);
        assert_eq!(config.batch_size, 100);
    }
}
