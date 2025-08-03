//! Unified StatefulStreamProcessor implementation for Terminal Command Canonicalizer
//!
//! This automaton creates canonical command events as synthesis events based on terminal
//! command events from multiple sources (kitty, atuin, shell history).

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Value};
use sinex_types::error::SinexError;
use tokio::time::Duration;
use sinex_db::models::{Event, EventSource, EventType, CanonicalCommandPayload};
use sinex_db::repositories::DbPoolExt;
use sinex_satellite_sdk::{
    nats_stream_consumer::{
        BatchProcessingResult as NatsBatchProcessingResult, EventBatchProcessor as NatsEventBatchProcessor,
        EventFilter as NatsEventFilter, NatsConsumerConfig, NatsStreamConsumer},
    stream_processor::{
        Checkpoint, ProcessorType, ScanArgs, ScanReport, StatefulStreamProcessor,
        StreamProcessorContext, TimeHorizon},
    SatelliteError, SatelliteResult};
use sinex_types::ulid::Ulid;
use sqlx::PgPool;
use std::collections::HashMap;
use tracing::{debug, info, warn};

// Helper trait for extracting values from JSON
trait JsonExtractor {
    fn get_string(&self, key: &str) -> Option<String>;
    fn get_i32(&self, key: &str) -> Option<i32>;
    fn get_i64(&self, key: &str) -> Option<i64>;
    fn get_datetime(&self, key: &str) -> Option<DateTime<Utc>>;
}

impl JsonExtractor for Value {
    fn get_string(&self, key: &str) -> Option<String> {
        self.get(key)?.as_str().map(Into::into)
    }
    
    fn get_i32(&self, key: &str) -> Option<i32> {
        self.get(key)?.as_i64().map(|v| v as i32)
    }
    
    fn get_i64(&self, key: &str) -> Option<i64> {
        self.get(key)?.as_i64()
    }
    
    fn get_datetime(&self, key: &str) -> Option<DateTime<Utc>> {
        self.get(key)?
            .as_str()?
            .parse::<DateTime<Utc>>()
            .ok()
    }
}

/// Command data extracted from terminal events
#[derive(Debug, Clone)]
struct CommandData {
    command: String,
    working_directory: Option<String>,
    exit_code: Option<i32>,
    duration_ms: Option<i64>,
    start_time: DateTime<Utc>,
    end_time: Option<DateTime<Utc>>,
    user: Option<String>,
    session_id: Option<String>,
    environment_hash: Option<String>,
    source_events: Vec<Ulid>}

/// Terminal Command Canonicalizer as a unified StatefulStreamProcessor
pub struct TerminalCommandCanonicalizer {
    context: Option<StreamProcessorContext>,
    deduplication_window_secs: i64}

impl TerminalCommandCanonicalizer {
    pub fn new() -> Self {
        Self {
            context: None,
            deduplication_window_secs: 5, // 5 second window for deduplication
        }
    }

    /// Find existing canonical command near the given timestamp
    async fn find_existing_canonical_command(
        &self,
        pool: &PgPool,
        command_text: &str,
        timestamp: DateTime<Utc>,
        window_secs: i64,
    ) -> Result<Option<String>, SinexError> {
        let start_time = timestamp - chrono::Duration::seconds(window_secs);
        let end_time = timestamp + chrono::Duration::seconds(window_secs);

        // Search for canonical command events within the time window
        let events = pool.events()
            .get_events_by_type_and_time_range(
                "automaton.terminal_command_canonicalizer",
                "command.canonical",
                start_time,
                end_time,
                Some(1000),
                None,
            )
            .await?;
        
        // Find matching command text
        for event in events {
            if let Some(cmd) = event.payload.get("command").and_then(|v| v.as_str()) {
                if cmd == command_text {
                    return Ok(event.id.map(|id| id.to_string()));
                }
            }
        }
        
        Ok(None)
    }

    /// Extract command data from a terminal event
    fn extract_command_data(&self, event: &Event) -> Option<CommandData> {
        let payload = &event.payload;

        // Extract command text based on source
        let command = match event.source.as_str() {
            "shell.kitty" => payload.get("command")?.as_str()?.to_string(),
            "shell.atuin" => payload.get("command")?.as_str()?.to_string(),
            "shell.history.bash" | "shell.history.zsh" | "shell.history.fish" => {
                payload.get("command")?.as_str()?.to_string()
            }
            _ => return None};

        // Skip empty commands
        if command.trim().is_empty() {
            return None;
        }

        Some(CommandData {
            command,
            working_directory: payload.get_string("working_directory"),
            exit_code: payload.get_i32("exit_code"),
            duration_ms: payload.get_i64("duration_ms"),
            start_time: event.ts_orig,
            end_time: payload.get_datetime("end_time"),
            user: payload.get_string("user"),
            session_id: payload.get_string("session_id"),
            environment_hash: payload.get_string("environment_hash"),
            source_events: vec![event.id.unwrap_or_else(|| Ulid::new())]})
    }

    /// Create a canonical command synthesis event
    async fn create_canonical_command(&self, command_data: &CommandData) -> SatelliteResult<Event> {
        let ctx = self.context.as_ref().ok_or_else(|| {
            SatelliteError::General(eyre!("Context not initialized"))
        })?;

        // Create synthesis event  
        let payload = serde_json::json!({
            "command": command_data.command,
            "working_directory": command_data.working_directory,
            "exit_code": command_data.exit_code,
            "duration_ms": command_data.duration_ms,
            "start_time": command_data.start_time,
            "end_time": command_data.end_time,
            "user": command_data.user,
            "session_id": command_data.session_id,
            "environment_hash": command_data.environment_hash,
            "source_events": command_data.source_events.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
            "enrichment_history": Vec::<String>::new(),
        });
        
        let event = Event {
            id: None,
            source: EventSource::from_static("automaton.terminal_command_canonicalizer"),
            event_type: EventType::from_static("command.canonical"),
            payload,
            ts_orig: command_data.start_time,
            ts_ingest: Utc::now(),
            source_event_ids: Some(command_data.source_events.clone()),
            host: sinex_db::models::HostName::current(),
            ingestor_version: "1.0.0".to_string(),
            schema_id: None,
        };

        Ok(event)
    }

    /// Get event filters for this automaton
    fn event_filters() -> Vec<NatsEventFilter> {
        vec![
            // All shell command execution events
            NatsEventFilter::new()
                .with_source("shell.kitty")
                .with_event_type("command.executed"),
            NatsEventFilter::new()
                .with_source("shell.atuin")
                .with_event_type("command.executed"),
            NatsEventFilter::new()
                .with_source("shell.history.bash")
                .with_event_type("command.executed"),
            NatsEventFilter::new()
                .with_source("shell.history.zsh")
                .with_event_type("command.executed"),
            NatsEventFilter::new()
                .with_source("shell.history.fish")
                .with_event_type("command.executed"),
        ]
    }
}

#[async_trait]
impl NatsEventBatchProcessor for TerminalCommandCanonicalizer {
    async fn process_batch(&mut self, events: Vec<Event>) -> SatelliteResult<NatsBatchProcessingResult> {
        let mut successful_ids = Vec::new();
        let mut failed_ids = Vec::new();

        for event in events {
            let event_id = event.id.map(|id| id.to_string()).unwrap_or_else(|| "unknown".into());

            // Extract command data
            let command_data = match self.extract_command_data(&event) {
                Some(data) => data,
                None => {
                    debug!("Skipping event {} - no command data", event_id);
                    successful_ids.push(event_id);
                    continue;
                }
            };

            // Check for existing canonical command
            if let Some(ctx) = &self.context {
                match self.find_existing_canonical_command(
                    &ctx.db_pool,
                    &command_data.command,
                    command_data.start_time,
                    self.deduplication_window_secs,
                ).await {
                    Ok(Some(existing_id)) => {
                        debug!(
                            "Found existing canonical command {} for '{}'",
                            existing_id, command_data.command
                        );
                        successful_ids.push(event_id);
                        continue;
                    }
                    Ok(None) => {
                        // No existing canonical command, create one
                    }
                    Err(e) => {
                        warn!("Error checking for existing command: {}", e);
                        // Continue anyway - better to have duplicates than miss commands
                    }
                }

                // Create canonical command
                match self.create_canonical_command(&command_data).await {
                    Ok(synthesis_event) => {
                        // For now, we'll just log the event creation
                        // In a real implementation, this would be sent via NATS or stored in DB
                        info!(
                            "Created canonical command for '{}'",
                            command_data.command
                        );
                        successful_ids.push(event_id);
                    }
                    Err(e) => {
                        warn!("Failed to create canonical command: {}", e);
                        failed_ids.push((event_id, e.to_string()));
                    }
                }
            }
        }

        Ok(NatsBatchProcessingResult {
            processed: successful_ids.len(),
            skipped: 0,
            failed: failed_ids.len(),
            duration: Duration::from_millis(0),
            errors: failed_ids.into_iter().map(|(_, e)| e).collect(),
        })
    }

}

#[async_trait]
impl StatefulStreamProcessor for TerminalCommandCanonicalizer {
    async fn initialize(&mut self, ctx: StreamProcessorContext) -> SatelliteResult<()> {
        info!("Initializing Terminal Command Canonicalizer");
        self.context = Some(ctx);
        Ok(())
    }

    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        let start_time = Utc::now();
        let mut events_processed = 0;

        match until {
            TimeHorizon::Continuous => {
                info!("Starting continuous terminal command canonicalization");
                
                let ctx = self.context.as_ref().unwrap();
                
                // Configure NATS consumer
                let config = NatsConsumerConfig {
                    group_name: "terminal-command-canonicalizer".to_string(),
                    consumer_name: "canonicalizer-1".to_string(),
                    stream_name: "SINEX_EVENTS".to_string(),
                    batch_size: 100,
                    block_timeout: Duration::from_secs(1),
                    filters: Self::event_filters(),
                    nats_servers: vec!["nats://localhost:4222".to_string()],
                };
                
                let mut nats_consumer = NatsStreamConsumer::new(config);
                nats_consumer.initialize(None).await?;

                // This will run continuously - we'd need to modify this to support shutdown signals
                // For now, this is a simplified implementation
                nats_consumer.run(self).await?;

                Ok(ScanReport {
                    events_processed,
                    duration: Utc::now() - start_time,
                    final_checkpoint: Some(Checkpoint::None),
                    time_range: Some((start_time, Utc::now())),
                    processor_stats: HashMap::new(),
                    successful_targets: vec!["nats-jetstream".to_string()],
                    failed_targets: HashMap::new(),
                    warnings: Vec::new()})
            }
            TimeHorizon::Historical { end_time } => {
                info!("Processing historical terminal commands up to {}", end_time);

                let ctx = self.context.as_ref().unwrap();
                
                // Determine start time
                let start_time = match from {
                    Checkpoint::Timestamp { timestamp, .. } => timestamp,
                    _ => end_time - chrono::Duration::days(7)};

                // Query all terminal command events
                let mut all_events = Vec::new();
                
                for source in &["shell.kitty", "shell.atuin", "shell.history.bash", 
                               "shell.history.zsh", "shell.history.fish"] {
                    let events = ctx.db_pool.events()
                        .get_events_by_type_and_time_range(
                            source,
                            "command.executed",
                            start_time,
                            end_time,
                            Some(10000),
                            None,
                        )
                        .await?;
                    
                    all_events.extend(events);
                }

                // Sort by timestamp
                all_events.sort_by_key(|e| e.ts_orig);

                events_processed = all_events.len();

                // Process events in batches using the batch processor
                let batch_size = 100;
                for chunk in all_events.chunks(batch_size) {
                    match self.process_batch(chunk.to_vec()).await {
                        Ok(result) => {
                            debug!("Processed batch of {} events", result.processed);
                        }
                        Err(e) => {
                            warn!("Failed to process batch: {}", e);
                        }
                    }
                }

                Ok(ScanReport {
                    events_processed,
                    duration: Utc::now() - start_time,
                    final_checkpoint: Some(Checkpoint::None),
                    time_range: Some((start_time, end_time)),
                    processor_stats: HashMap::new(),
                    successful_targets: vec!["postgresql".to_string()],
                    failed_targets: HashMap::new(),
                    warnings: Vec::new()})
            }
            TimeHorizon::Snapshot => {
                // No snapshot mode for canonicalizer
                Ok(ScanReport {
                    events_processed: 0,
                    duration: Utc::now() - start_time,
                    final_checkpoint: Some(Checkpoint::None),
                    time_range: None,
                    processor_stats: HashMap::new(),
                    successful_targets: Vec::new(),
                    failed_targets: HashMap::new(),
                    warnings: vec!["Terminal command canonicalizer does not support snapshot mode".to_string()]})
            }
        }
    }

    fn processor_name(&self) -> &str {
        "terminal-command-canonicalizer"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Automaton
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        Ok(Checkpoint::None)
    }
}

impl Default for TerminalCommandCanonicalizer {
    fn default() -> Self {
        Self::new()
    }
}