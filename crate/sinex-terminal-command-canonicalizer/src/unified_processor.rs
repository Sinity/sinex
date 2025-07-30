//! Unified StatefulStreamProcessor implementation for Terminal Command Canonicalizer
//!
//! This automaton creates canonical command events as synthesis events based on terminal
//! command events from multiple sources (kitty, atuin, shell history).

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Value};
use sinex_error::SinexError;
use sinex_db::repositories::{EventRepository, Repository};
use sinex_core_types::event_constants::{sources as typed_sources, types as typed_types};
use sinex_events::{EventFactory, RawEvent, event_types, sources};
use sinex_satellite_sdk::{
    redis_stream_consumer::{
        BatchProcessingResult, EventBatchProcessor, RedisStreamConsumer,
        EventFilter as StreamEventFilter,
    },
    stream_processor::{
        Checkpoint, ProcessorType, ScanArgs, ScanReport, StatefulStreamProcessor,
        StreamProcessorContext, TimeHorizon,
    },
    SatelliteError, SatelliteResult,
};
use sinex_ulid::Ulid;
use sqlx::PgPool;
use std::collections::HashMap;
use tracing::{debug, info, warn};

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
    source_events: Vec<Ulid>,
}

/// Terminal Command Canonicalizer as a unified StatefulStreamProcessor
pub struct TerminalCommandCanonicalizer {
    context: Option<StreamProcessorContext>,
    deduplication_window_secs: i64,
}

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
        let start_time = timestamp - Duration::seconds(window_secs);
        let end_time = timestamp + Duration::seconds(window_secs);

        let event_repo = EventRepository::new(pool);
        
        // Search for canonical command events within the time window
        let events = event_repo
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
                    return Ok(Some(event.id));
                }
            }
        }
        
        Ok(None)
    }

    /// Extract command data from a terminal event
    fn extract_command_data(&self, event: &RawEvent) -> Option<CommandData> {
        let payload = &event.payload;

        // Extract command text based on source
        let command = match event.source.as_str() {
            "shell.kitty" => payload.get("command")?.as_str()?.to_string(),
            "shell.atuin" => payload.get("command")?.as_str()?.to_string(),
            "shell.history.bash" | "shell.history.zsh" | "shell.history.fish" => {
                payload.get("command")?.as_str()?.to_string()
            }
            _ => return None,
        };

        // Skip empty commands
        if command.trim().is_empty() {
            return None;
        }

        Some(CommandData {
            command,
            working_directory: payload.get("working_directory").and_then(|v| v.as_str()).map(String::from),
            exit_code: payload.get("exit_code").and_then(|v| v.as_i64()).map(|v| v as i32),
            duration_ms: payload.get("duration_ms").and_then(|v| v.as_i64()),
            start_time: event.ts_orig,
            end_time: payload.get("end_time")
                .and_then(|v| v.as_str())
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&Utc)),
            user: payload.get("user").and_then(|v| v.as_str()).map(String::from),
            session_id: payload.get("session_id").and_then(|v| v.as_str()).map(String::from),
            environment_hash: payload.get("environment_hash").and_then(|v| v.as_str()).map(String::from),
            source_events: vec![event.id],
        })
    }

    /// Create a canonical command synthesis event
    async fn create_canonical_command(&self, command_data: &CommandData) -> SatelliteResult<RawEvent> {
        let ctx = self.context.as_ref().ok_or_else(|| {
            SatelliteError::General(anyhow::anyhow!("Context not initialized"))
        })?;

        let payload = json!({
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
            "enrichment_history": []
        });

        // Create synthesis event
        let mut event = EventFactory::new("canonical.terminal")
            .create_event("command.canonical", payload);
        
        // Set source event IDs for provenance
        event.source_event_ids = Some(command_data.source_events.clone());
        event.host = ctx.host.clone();

        Ok(event)
    }

    /// Get event filters for this automaton
    fn event_filters() -> Vec<StreamEventFilter> {
        vec![
            // Kitty terminal events
            StreamEventFilter::new(
                Some("shell.kitty".to_string()),
                Some(typed_types::shell::COMMAND_EXECUTED.as_str().to_string()),
            ),
            // Atuin history events
            StreamEventFilter::new(
                Some("shell.atuin".to_string()),
                Some(typed_types::shell::COMMAND_EXECUTED.as_str().to_string()),
            ),
            // Shell history events
            StreamEventFilter::new(
                Some("shell.history.bash".to_string()),
                Some(typed_types::shell::COMMAND_EXECUTED.as_str().to_string()),
            ),
            StreamEventFilter::new(
                Some("shell.history.zsh".to_string()),
                Some(typed_types::shell::COMMAND_EXECUTED.as_str().to_string()),
            ),
            StreamEventFilter::new(
                Some("shell.history.fish".to_string()),
                Some(typed_types::shell::COMMAND_EXECUTED.as_str().to_string()),
            ),
        ]
    }
}

#[async_trait]
impl EventBatchProcessor for TerminalCommandCanonicalizer {
    async fn process_batch(&mut self, events: Vec<RawEvent>) -> SatelliteResult<BatchProcessingResult> {
        let mut successful_ids = Vec::new();
        let mut failed_ids = Vec::new();

        for event in events {
            let event_id = event.id.to_string();

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
                        ctx.send_event(synthesis_event).await?;
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

        Ok(BatchProcessingResult {
            successful_ids,
            failed_ids,
            retry_ids: Vec::new(),
            checkpoint_data: None,
        })
    }

    async fn get_checkpoint_data(&self) -> Option<serde_json::Value> {
        None
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
                let mut redis_consumer = RedisStreamConsumer::from_context(
                    ctx.redis_client.clone(),
                    "terminal-command-canonicalizer".to_string(),
                    Self::event_filters(),
                );

                let final_checkpoint = redis_consumer
                    .consume_continuous(from, self, args.shutdown_signal)
                    .await?;

                Ok(ScanReport {
                    events_processed,
                    duration: Utc::now() - start_time,
                    final_checkpoint: Some(final_checkpoint),
                    time_range: Some((start_time, Utc::now())),
                    processor_stats: HashMap::new(),
                    successful_targets: vec!["redis-stream".to_string()],
                    failed_targets: HashMap::new(),
                    warnings: Vec::new(),
                })
            }
            TimeHorizon::Historical { end_time } => {
                info!("Processing historical terminal commands up to {}", end_time);

                let ctx = self.context.as_ref().unwrap();
                
                // Determine start time
                let start_time = match from {
                    Checkpoint::Timestamp { timestamp, .. } => timestamp,
                    _ => end_time - Duration::days(7),
                };

                // Query all terminal command events
                let mut all_events = Vec::new();
                
                for source in &["shell.kitty", "shell.atuin", "shell.history.bash", 
                               "shell.history.zsh", "shell.history.fish"] {
                    let event_repo = EventRepository::new(&ctx.db_pool);
                    let events = event_repo
                        .get_events_by_type_and_time_range(
                            source,
                            typed_types::shell::COMMAND_EXECUTED.as_str(),
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

                // Process using batch processor
                let mut redis_consumer = RedisStreamConsumer::from_context(
                    ctx.redis_client.clone(),
                    "terminal-command-canonicalizer".to_string(),
                    Self::event_filters(),
                );

                let final_checkpoint = redis_consumer
                    .consume_historical(all_events, self, 100)
                    .await?;

                Ok(ScanReport {
                    events_processed,
                    duration: Utc::now() - start_time,
                    final_checkpoint: Some(final_checkpoint),
                    time_range: Some((start_time, end_time)),
                    processor_stats: HashMap::new(),
                    successful_targets: vec!["postgresql".to_string()],
                    failed_targets: HashMap::new(),
                    warnings: Vec::new(),
                })
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
                    warnings: vec!["Terminal command canonicalizer does not support snapshot mode".to_string()],
                })
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