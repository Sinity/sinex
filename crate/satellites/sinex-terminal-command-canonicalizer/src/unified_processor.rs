//! Unified StatefulStreamProcessor implementation for Terminal Command Canonicalizer
//!
//! This automaton creates canonical command events as synthesis events based on terminal
//! command events from multiple sources (kitty, atuin, shell history).

use async_trait::async_trait;
use camino::Utf8PathBuf;
use chrono::{DateTime, Duration, Utc};
use color_eyre::eyre::eyre;
use serde_json::{json, Value};
use sinex_core::db::models::{Provenance, RawEvent};
use sinex_core::db::repositories::DbPoolExt;
use sinex_core::types::domain::{EventSource, EventType, HostName};
use sinex_core::types::error::SinexError;
use sinex_core::types::ulid::Ulid;
use sinex_satellite_sdk::{
    cli::{
        ActivityEntry, CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry,
        MissingItem, SourceState,
    },
    stream_processor::{
        Checkpoint, ProcessingStats, ProcessorType, ScanArgs, ScanReport, StatefulStreamProcessor,
        StreamProcessorContext, TimeHorizon,
    },
    SatelliteError, SatelliteResult,
};
use sqlx::PgPool;
use std::collections::HashMap;
use tokio::time::Duration as TokioDuration;
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
        self.get(key)?.as_str()?.parse::<DateTime<Utc>>().ok()
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
        let start_time = timestamp - chrono::Duration::seconds(window_secs);
        let end_time = timestamp + chrono::Duration::seconds(window_secs);

        // Search for canonical command events within the time window
        let event_type = EventType::from_static("command.canonical");
        let events = pool
            .events()
            .get_events_by_type_and_time_range(&event_type, start_time, end_time, Some(1000))
            .await?;

        // Find matching command text
        for event in events {
            if let Some(cmd) = event.payload.get("command").and_then(|v| v.as_str()) {
                if cmd == command_text {
                    return Ok(event.id.map(|id| id.as_ulid().to_string()));
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
            working_directory: payload.get_string("working_directory"),
            exit_code: payload.get_i32("exit_code"),
            duration_ms: payload.get_i64("duration_ms"),
            start_time: event.ts_orig.unwrap_or_else(|| Utc::now()),
            end_time: payload.get_datetime("end_time"),
            user: payload.get_string("user"),
            session_id: payload.get_string("session_id"),
            environment_hash: payload.get_string("environment_hash"),
            source_events: vec![event
                .id
                .as_ref()
                .map(|id| id.as_ulid().clone())
                .unwrap_or_else(|| Ulid::new())],
        })
    }

    /// Create a canonical command synthesis event
    async fn create_canonical_command(
        &self,
        command_data: &CommandData,
    ) -> SatelliteResult<RawEvent> {
        let ctx = self
            .context
            .as_ref()
            .ok_or_else(|| SatelliteError::General(eyre!("Context not initialized")))?;

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

        use sinex_core::types::{Id, Ulid as CoreUlid};

        let event = RawEvent {
            id: None,
            source: EventSource::from_static("automaton.terminal_command_canonicalizer"),
            event_type: EventType::from_static("command.canonical"),
            payload,
            ts_orig: Some(command_data.start_time),
            ts_ingest: Utc::now(),
            host: HostName::new("localhost"), // Will be set by builder default
            ingestor_version: Some("1.0.0".to_string()),
            payload_schema_id: None,
            provenance: Some(Provenance::Events(
                command_data
                    .source_events
                    .iter()
                    .map(|ulid| Id::<RawEvent>::from_ulid(*ulid))
                    .collect(),
            )),
            anchor_byte: None,
            associated_blob_ids: None,
        };

        Ok(event)
    }
}

#[async_trait]
impl StatefulStreamProcessor for TerminalCommandCanonicalizer {
    type Config = ();

    async fn initialize(
        &mut self,
        ctx: StreamProcessorContext,
        _config: Self::Config,
    ) -> SatelliteResult<()> {
        info!("Initializing Terminal Command Canonicalizer");
        self.context = Some(ctx);
        Ok(())
    }

    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        _args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        let start_time = Utc::now();
        let mut events_processed = 0;

        match until {
            TimeHorizon::Continuous => {
                // For automata, continuous mode is now handled by StreamProcessorRunner
                // This shouldn't be called directly anymore
                warn!(
                    "Continuous mode should be handled by StreamProcessorRunner, not scan method"
                );
                Ok(ScanReport {
                    events_processed: 0,
                    duration: std::time::Duration::from_secs(0),
                    final_checkpoint: Checkpoint::None,
                    time_range: None,
                    processor_stats: HashMap::new(),
                    successful_targets: Vec::new(),
                    failed_targets: Vec::new(),
                    warnings: vec![
                        "Continuous mode handled externally by StreamProcessorRunner".to_string(),
                    ],
                })
            }
            TimeHorizon::Historical { end_time } => {
                info!("Processing historical terminal commands up to {}", end_time);

                let ctx = self.context.as_ref().unwrap();

                // Determine start time
                let start_time = match from {
                    Checkpoint::Timestamp { timestamp, .. } => timestamp,
                    _ => end_time - chrono::Duration::days(7),
                };

                // Query all terminal command events and convert to RawEvent
                let mut all_raw_events = Vec::new();

                for source in &[
                    "shell.kitty",
                    "shell.atuin",
                    "shell.history.bash",
                    "shell.history.zsh",
                    "shell.history.fish",
                ] {
                    let event_type = EventType::from_static("command.executed");
                    let events = ctx
                        .db_pool
                        .events()
                        .get_events_by_type_and_time_range(
                            &event_type,
                            start_time,
                            end_time,
                            Some(10000),
                        )
                        .await?;

                    // Events are already RawEvent type
                    for raw_event in events {
                        all_raw_events.push(raw_event);
                    }
                }

                // Sort by timestamp
                all_raw_events.sort_by_key(|e| e.ts_orig);
                events_processed = all_raw_events.len() as u64;

                // Process events in batches using the new unified method
                let batch_size = 100;
                for chunk in all_raw_events.chunks(batch_size) {
                    match self.process_event_batch(chunk.to_vec()).await {
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
                    duration: (Utc::now() - start_time).to_std().unwrap_or_default(),
                    final_checkpoint: Checkpoint::None,
                    time_range: Some((start_time, end_time)),
                    processor_stats: HashMap::new(),
                    successful_targets: vec!["postgresql".to_string()],
                    failed_targets: Vec::new(),
                    warnings: Vec::new(),
                })
            }
            TimeHorizon::Snapshot => {
                // No snapshot mode for canonicalizer
                Ok(ScanReport {
                    events_processed: 0,
                    duration: std::time::Duration::from_secs(0),
                    final_checkpoint: Checkpoint::None,
                    time_range: None,
                    processor_stats: HashMap::new(),
                    successful_targets: Vec::new(),
                    failed_targets: Vec::new(),
                    warnings: vec![
                        "Terminal command canonicalizer does not support snapshot mode".to_string(),
                    ],
                })
            }
        }
    }

    /// Process a batch of events from NATS (unified method)
    async fn process_event_batch(
        &mut self,
        events: Vec<RawEvent>,
    ) -> SatelliteResult<ProcessingStats> {
        let start_time = std::time::Instant::now();
        let mut processed = 0;
        let mut skipped = 0;
        let mut failed = 0;
        let mut errors = Vec::new();

        for event in events {
            // Work directly with RawEvent

            let event_id = event
                .id
                .as_ref()
                .map(|id| id.as_ulid().to_string())
                .unwrap_or_else(|| "unknown".to_string());

            // Extract command data
            let command_data = match self.extract_command_data(&event) {
                Some(data) => data,
                None => {
                    debug!("Skipping event {} - no command data", event_id);
                    skipped += 1;
                    continue;
                }
            };

            // Check for existing canonical command
            if let Some(ctx) = &self.context {
                match self
                    .find_existing_canonical_command(
                        &ctx.db_pool,
                        &command_data.command,
                        command_data.start_time,
                        self.deduplication_window_secs,
                    )
                    .await
                {
                    Ok(Some(existing_id)) => {
                        debug!(
                            "Found existing canonical command {} for '{}'",
                            existing_id, command_data.command
                        );
                        processed += 1;
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
                        // In a real implementation, this would be sent via the event channel
                        info!("Created canonical command for '{}'", command_data.command);
                        processed += 1;
                    }
                    Err(e) => {
                        warn!("Failed to create canonical command: {}", e);
                        failed += 1;
                        errors.push(format!("Event {}: {}", event_id, e));
                    }
                }
            } else {
                failed += 1;
                errors.push(format!("Event {}: Context not initialized", event_id));
            }
        }

        Ok(ProcessingStats {
            processed,
            skipped,
            failed,
            duration: start_time.elapsed(),
            errors,
        })
    }

    // TODO: Remove event_filters after NatsStreamConsumer removal
    // /// Get event filters for NATS consumption
    // fn event_filters(&self) -> Vec<NatsEventFilter> {
    //     vec![
    //         // All shell command execution events
    //         NatsEventFilter::new()
    //             .with_source("shell.kitty")
    //             .with_event_type("command.executed"),
    //         NatsEventFilter::new()
    //             .with_source("shell.atuin")
    //             .with_event_type("command.executed"),
    //         NatsEventFilter::new()
    //             .with_source("shell.history.bash")
    //             .with_event_type("command.executed"),
    //         NatsEventFilter::new()
    //             .with_source("shell.history.zsh")
    //             .with_event_type("command.executed"),
    //         NatsEventFilter::new()
    //             .with_source("shell.history.fish")
    //             .with_event_type("command.executed"),
    //     ]
    // }

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

impl ExplorationProvider for TerminalCommandCanonicalizer {
    fn get_source_state(&self) -> color_eyre::eyre::Result<SourceState> {
        Ok(SourceState {
            description: "Terminal command canonicalizer - creates synthesis events".to_string(),
            last_updated: Utc::now(),
            total_items: None,
            metadata: HashMap::new(),
            healthy: true,
            recent_activity: vec![],
        })
    }

    fn get_ingestion_history(
        &self,
        _limit: u64,
    ) -> color_eyre::eyre::Result<Vec<IngestionHistoryEntry>> {
        // Automata don't ingest from external sources
        Ok(vec![])
    }

    fn get_coverage_analysis(
        &self,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> color_eyre::eyre::Result<CoverageAnalysis> {
        let (start, end) = time_range.unwrap_or_else(|| {
            let now = Utc::now();
            (now - chrono::Duration::days(7), now)
        });
        Ok(CoverageAnalysis {
            time_range: (start, end),
            source_total: 0,
            sinex_total: 0,
            coverage_percentage: 100.0,
            missing_count: 0,
            missing_samples: vec![],
            duplicate_count: 0,
            recommendations: vec![],
        })
    }

    fn export_data(
        &self,
        _path: &Utf8PathBuf,
        _format: ExportFormat,
    ) -> color_eyre::eyre::Result<()> {
        Err(eyre!("Export not supported for automata"))
    }
}
