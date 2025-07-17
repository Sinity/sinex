//! Automaton trait and context for satellite automata

use crate::{
    checkpoint::CheckpointManager,
    grpc_client::IngestClient,
    redis_client::{RedisStreamClient, StreamMessage},
    SatelliteError, SatelliteResult,
};
use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sinex_db::SqlxPgPool as PgPool;
use sinex_events::RawEvent;
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

/// Result of processing an event
#[derive(Debug, Clone)]
pub enum ProcessingResult {
    /// Event processed successfully
    Success {
        /// Optional data to include in checkpoint
        checkpoint_data: Option<serde_json::Value>,
    },
    /// Event processing failed but can be retried
    Retry {
        /// Error message
        error: String,
        /// Delay before retry (seconds)
        retry_after_secs: u64,
    },
    /// Event processing failed permanently
    Failed {
        /// Error message
        error: String,
        /// Whether to send to dead letter queue
        dead_letter: bool,
    },
    /// Event should be skipped (e.g., duplicate)
    Skip {
        /// Reason for skipping
        reason: String,
    },
}

// ============================================================================
// New Unified Hotlog-based Automaton System
// ============================================================================

/// Context for unified hotlog-based automata
#[derive(Debug)]
pub struct HotlogAutomatonContext {
    /// Service name
    pub service_name: String,

    /// Hostname where the service is running
    pub host: String,

    /// Working directory for temporary files
    pub work_dir: std::path::PathBuf,

    /// Whether running in dry-run mode
    pub dry_run: bool,

    /// Database connection pool for reading events
    pub db_pool: PgPool,

    /// Redis client for message bus
    pub redis_client: RedisStreamClient,

    /// gRPC client for writing synthesis events
    pub ingest_client: IngestClient,

    /// Consumer group name
    pub consumer_group: String,

    /// Consumer name
    pub consumer_name: String,

    /// Event filters for this automaton
    pub event_filters: Vec<EventFilter>,

    /// Automaton-specific configuration
    pub config: HashMap<String, serde_json::Value>,

    /// Checkpoint manager
    pub checkpoint_manager: CheckpointManager,
}

impl HotlogAutomatonContext {
    /// Create a synthesis event and send it to ingestd
    pub async fn emit_synthesis_event(&self, synthesis_event: RawEvent) -> SatelliteResult<String> {
        // Create a mutable copy of the ingest client for this call
        let mut ingest_client = self.ingest_client.clone();
        ingest_client.ingest_event(&synthesis_event).await
    }

    /// Create multiple synthesis events and send them to ingestd
    pub async fn emit_synthesis_events(
        &self,
        synthesis_events: Vec<RawEvent>,
    ) -> SatelliteResult<()> {
        let mut ingest_client = self.ingest_client.clone();
        let result = ingest_client.ingest_batch(&synthesis_events).await?;

        if !result.success {
            return Err(SatelliteError::General(anyhow::anyhow!(
                "Failed to ingest synthesis events: {}",
                result.error.unwrap_or_else(|| "Unknown error".to_string())
            )));
        }

        Ok(())
    }
}

/// Event filter for automaton event selection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventFilter {
    /// Filter by event source (e.g., "fs", "shell.kitty")
    pub source: Option<String>,

    /// Filter by event type (e.g., "file.created", "command.executed")
    pub event_type: Option<String>,

    /// Filter by host name
    pub host: Option<String>,

    /// Additional JSON path filters on payload
    pub payload_filters: Vec<PayloadFilter>,
}

/// JSON path filter for event payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PayloadFilter {
    /// JSON path expression (e.g., "$.command")
    pub path: String,

    /// Expected value or pattern
    pub value: serde_json::Value,

    /// Filter operation type
    pub operation: FilterOperation,
}

/// Filter operation types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterOperation {
    /// Exact match
    Equals,
    /// Contains substring (for strings)
    Contains,
    /// Regex match (for strings)
    Regex,
    /// Exists (check if path exists)
    Exists,
    /// Greater than (for numbers)
    GreaterThan,
    /// Less than (for numbers)
    LessThan,
}

impl EventFilter {
    /// Create a simple source + event_type filter
    pub fn new(source: Option<String>, event_type: Option<String>) -> Self {
        Self {
            source,
            event_type,
            host: None,
            payload_filters: vec![],
        }
    }

    /// Check if a RawEvent matches this filter
    pub fn matches(&self, event: &RawEvent) -> bool {
        // Check source filter
        if let Some(ref filter_source) = self.source {
            if &event.source != filter_source {
                return false;
            }
        }

        // Check event_type filter
        if let Some(ref filter_event_type) = self.event_type {
            if &event.event_type != filter_event_type {
                return false;
            }
        }

        // Check host filter
        if let Some(ref filter_host) = self.host {
            if &event.host != filter_host {
                return false;
            }
        }

        // Check payload filters
        for payload_filter in &self.payload_filters {
            if !payload_filter.matches(&event.payload) {
                return false;
            }
        }

        true
    }
}

impl PayloadFilter {
    /// Check if a JSON payload matches this filter
    pub fn matches(&self, payload: &serde_json::Value) -> bool {
        // Simple implementation - could be enhanced with jsonpath library
        match self.operation {
            FilterOperation::Exists => {
                // Simple path existence check
                self.path_exists(payload, &self.path)
            }
            FilterOperation::Equals => {
                if let Some(value) = self.get_path_value(payload, &self.path) {
                    value == &self.value
                } else {
                    false
                }
            }
            FilterOperation::Contains => {
                if let Some(serde_json::Value::String(s)) = self.get_path_value(payload, &self.path)
                {
                    if let serde_json::Value::String(pattern) = &self.value {
                        s.contains(pattern)
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            // TODO: Implement other operations
            _ => false,
        }
    }

    /// Simple path existence check
    fn path_exists(&self, payload: &serde_json::Value, path: &str) -> bool {
        // Basic implementation for simple paths like "$.command"
        if let Some(key) = path.strip_prefix("$.") {
            payload.get(key).is_some()
        } else {
            false
        }
    }

    /// Get value at path
    fn get_path_value<'a>(
        &self,
        payload: &'a serde_json::Value,
        path: &str,
    ) -> Option<&'a serde_json::Value> {
        // Basic implementation for simple paths like "$.command"
        if let Some(key) = path.strip_prefix("$.") {
            payload.get(key)
        } else {
            None
        }
    }
}

/// Enhanced automaton event from hotlog stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotlogAutomatonEvent {
    /// Message ID from Redis Stream
    pub message_id: String,

    /// Stream name (always "sinex:streams:hotlog")
    pub stream: String,

    /// Parsed RawEvent
    pub event: RawEvent,

    /// Original stream fields for reference
    pub stream_fields: HashMap<String, String>,
}

impl HotlogAutomatonEvent {
    /// Create from a Redis Stream message from hotlog
    pub fn from_hotlog_message(message: StreamMessage) -> SatelliteResult<Self> {
        let event_data = message.get_string("data").ok_or_else(|| {
            SatelliteError::General(anyhow::anyhow!("Missing event data in hotlog message"))
        })?;

        let event: RawEvent =
            serde_json::from_str(&event_data).map_err(SatelliteError::Serialization)?;

        // Convert redis::Value fields to strings
        let mut stream_fields = HashMap::new();
        for (key, value) in message.fields {
            let string_value = match value {
                redis::Value::Data(data) => String::from_utf8_lossy(&data).to_string(),
                redis::Value::Okay => "OK".to_string(),
                redis::Value::Status(status) => status,
                redis::Value::Int(i) => i.to_string(),
                _ => format!("{:?}", value),
            };
            stream_fields.insert(key, string_value);
        }

        Ok(Self {
            message_id: message.id.clone(),
            stream: message.stream.clone(),
            event,
            stream_fields,
        })
    }
}

/// New trait for unified hotlog-based automata
#[async_trait]
pub trait HotlogAutomaton: Send + Sync {
    /// Initialize the automaton with the given context
    async fn initialize(&mut self, ctx: HotlogAutomatonContext) -> SatelliteResult<()>;

    /// Process a single event from the hotlog stream
    async fn process_event(
        &mut self,
        event: HotlogAutomatonEvent,
    ) -> SatelliteResult<ProcessingResult>;

    /// Process a batch of events (default implementation processes one by one)
    async fn process_batch(
        &mut self,
        events: Vec<HotlogAutomatonEvent>,
    ) -> SatelliteResult<Vec<ProcessingResult>> {
        let mut results = Vec::new();
        for event in events {
            let result = self.process_event(event).await?;
            results.push(result);
        }
        Ok(results)
    }

    /// Define event filters for this automaton
    fn event_filters(&self) -> Vec<EventFilter>;

    /// Graceful shutdown
    async fn shutdown(&mut self) -> SatelliteResult<()> {
        info!("Hotlog automaton shutting down");
        Ok(())
    }

    /// Health check
    async fn health_check(&self) -> SatelliteResult<bool> {
        Ok(true)
    }

    /// Get automaton name (used for identification)
    fn automaton_name(&self) -> &str;
}

/// Runner for unified hotlog-based automata
pub struct HotlogAutomatonRunner<T: HotlogAutomaton> {
    automaton: T,
    context: Option<HotlogAutomatonContext>,
    _shutdown_receiver: Option<tokio::sync::oneshot::Receiver<()>>,
}

impl<T: HotlogAutomaton> HotlogAutomatonRunner<T> {
    /// Create a new hotlog automaton runner
    pub fn new(automaton: T) -> Self {
        Self {
            automaton,
            context: None,
            _shutdown_receiver: None,
        }
    }

    /// Initialize the automaton with configuration
    pub async fn initialize(
        &mut self,
        service_name: String,
        consumer_group: String,
        consumer_name: String,
        event_filters: Vec<EventFilter>,
        config: HashMap<String, serde_json::Value>,
        db_pool: PgPool,
        redis_client: RedisStreamClient,
        ingest_client: IngestClient,
        work_dir: std::path::PathBuf,
        dry_run: bool,
    ) -> SatelliteResult<()> {
        let host = gethostname::gethostname().to_string_lossy().to_string();

        // Initialize checkpoint manager
        let checkpoint_manager = CheckpointManager::new(
            db_pool.clone(),
            service_name.clone(),
            consumer_group.clone(),
            consumer_name.clone(),
        );

        // Create context
        let context = HotlogAutomatonContext {
            service_name: service_name.clone(),
            host,
            work_dir,
            dry_run,
            db_pool,
            redis_client,
            ingest_client,
            consumer_group,
            consumer_name,
            event_filters,
            config,
            checkpoint_manager,
        };

        // Initialize the automaton
        self.automaton.initialize(context).await?;

        info!(
            service = %service_name,
            automaton = %self.automaton.automaton_name(),
            "Hotlog automaton initialized"
        );

        Ok(())
    }

    /// Run the automaton, consuming from unified hotlog stream
    pub async fn run(&mut self) -> SatelliteResult<()> {
        if self.context.is_none() {
            return Err(SatelliteError::Lifecycle(
                "Hotlog automaton not initialized".to_string(),
            ));
        }

        let context = self.context.as_ref().unwrap();

        info!(
            automaton = %self.automaton.automaton_name(),
            consumer_group = %context.consumer_group,
            consumer_name = %context.consumer_name,
            "Starting hotlog automaton"
        );

        // Create consumer group for unified hotlog stream
        const HOTLOG_STREAM: &str = "sinex:streams:hotlog";

        if let Err(e) = context
            .redis_client
            .create_consumer_group(
                HOTLOG_STREAM,
                &context.consumer_group,
                "0", // Start from beginning
            )
            .await
        {
            warn!(
                error = %e,
                "Failed to create consumer group (may already exist)"
            );
        }

        // Main processing loop
        if let Err(e) = self.process_hotlog_events(HOTLOG_STREAM).await {
            error!(error = %e, "Hotlog processing failed");
        }

        info!("Hotlog automaton stopped");
        Ok(())
    }

    /// Process events from the unified hotlog stream
    async fn process_hotlog_events(&mut self, stream: &str) -> SatelliteResult<()> {
        let context = self.context.as_ref().unwrap();
        let event_filters = self.automaton.event_filters();

        loop {
            // Read messages from the hotlog stream
            let messages = context
                .redis_client
                .read_group(
                    &[stream.to_string()],
                    &context.consumer_group,
                    &context.consumer_name,
                    Some(10),   // Read up to 10 messages
                    Some(5000), // 5 second timeout
                )
                .await?;

            if messages.is_empty() {
                // No messages, continue loop
                continue;
            }

            // Filter and convert messages to automaton events
            let mut filtered_events = Vec::new();
            let mut message_ids = Vec::new();

            for message in messages {
                let message_id = message.id.clone();

                // Parse event from hotlog message
                match HotlogAutomatonEvent::from_hotlog_message(message) {
                    Ok(automaton_event) => {
                        // Check if event matches our filters
                        let matches = event_filters
                            .iter()
                            .any(|filter| filter.matches(&automaton_event.event));

                        if matches {
                            filtered_events.push(automaton_event);
                            message_ids.push(message_id);
                        } else {
                            // Event doesn't match filters, ACK it immediately
                            if let Err(e) = context
                                .redis_client
                                .ack_messages(stream, &context.consumer_group, &[message_id])
                                .await
                            {
                                warn!(error = %e, "Failed to ACK filtered message");
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            error = %e,
                            message_id = %message_id,
                            "Failed to parse hotlog message"
                        );
                        // ACK malformed messages to avoid infinite retries
                        if let Err(e) = context
                            .redis_client
                            .ack_messages(stream, &context.consumer_group, &[message_id])
                            .await
                        {
                            warn!(error = %e, "Failed to ACK malformed message");
                        }
                    }
                }
            }

            // Process filtered events
            if !filtered_events.is_empty() {
                debug!(
                    automaton = %self.automaton.automaton_name(),
                    count = filtered_events.len(),
                    "Processing filtered events"
                );

                // Process batch of filtered events
                let results = self.automaton.process_batch(filtered_events).await?;

                // Handle results and ACK successful messages
                let mut successful_ids = Vec::new();

                for (i, result) in results.iter().enumerate() {
                    let message_id = &message_ids[i];

                    match result {
                        ProcessingResult::Success { .. } => {
                            successful_ids.push(message_id.clone());
                        }
                        ProcessingResult::Skip { .. } => {
                            successful_ids.push(message_id.clone());
                        }
                        ProcessingResult::Retry { .. } => {
                            // Don't ACK, let it retry
                        }
                        ProcessingResult::Failed { .. } => {
                            // ACK to prevent infinite retries
                            successful_ids.push(message_id.clone());
                        }
                    }
                }

                // ACK successfully processed messages
                if !successful_ids.is_empty() {
                    if let Err(e) = context
                        .redis_client
                        .ack_messages(stream, &context.consumer_group, &successful_ids)
                        .await
                    {
                        error!(error = %e, "Failed to ACK processed messages");
                    } else {
                        // CRITICAL FIX: Save checkpoint to database after successful ACK
                        // Aggregate checkpoint data from successful results
                        let mut checkpoint_data_values = Vec::new();
                        for (i, result) in results.iter().enumerate() {
                            if successful_ids.contains(&message_ids[i]) {
                                if let ProcessingResult::Success {
                                    checkpoint_data: Some(data),
                                } = result
                                {
                                    checkpoint_data_values.push(data.clone());
                                }
                            }
                        }

                        // Combine checkpoint data (could be enhanced based on automaton needs)
                        let combined_checkpoint_data = if checkpoint_data_values.is_empty() {
                            None
                        } else {
                            Some(serde_json::json!({
                                "batch_data": checkpoint_data_values,
                                "last_processed_time": chrono::Utc::now()
                            }))
                        };

                        // Get the highest message ID for this batch (Redis stream IDs are naturally ordered)
                        let last_message_id = successful_ids.iter().max().cloned();

                        // Create checkpoint state for database persistence
                        if let Some(message_id) = last_message_id {
                            use crate::checkpoint::CheckpointState;
                            use crate::stream_processor::Checkpoint;

                            let checkpoint_state = CheckpointState {
                                checkpoint: Checkpoint::Stream {
                                    message_id: message_id.clone(),
                                    event_id: None, // Redis stream message ID doesn't map to specific event ULID
                                },
                                processed_count: successful_ids.len() as u64,
                                last_activity: Utc::now(),
                                data: combined_checkpoint_data,
                                version: 1,
                            };

                            // Save checkpoint to database
                            if let Err(e) = context
                                .checkpoint_manager
                                .save_checkpoint(&checkpoint_state)
                                .await
                            {
                                error!(
                                    error = %e,
                                    "Failed to save checkpoint to database - this could cause reprocessing on restart"
                                );
                            } else {
                                debug!(
                                    automaton = %self.automaton.automaton_name(),
                                    processed_count = successful_ids.len(),
                                    last_id = ?message_id,
                                    "Checkpoint saved to database"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    /// Graceful shutdown
    pub async fn shutdown(&mut self) -> SatelliteResult<()> {
        info!("Shutting down hotlog automaton runner");
        self.automaton.shutdown().await
    }
}
