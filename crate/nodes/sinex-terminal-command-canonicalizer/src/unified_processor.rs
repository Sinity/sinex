#![doc = include_str!("../docs/unified_processor.md")]

//! Unified `Node` implementation for the terminal command canonicalizer.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use color_eyre::eyre::eyre;
use serde_json::Value;
use sinex_core::events::CanonicalCommandPayload;
use sinex_core::types::error::SinexError;
use sinex_core::types::ulid::Ulid;
use sinex_core::DbPoolExt;
use sinex_core::EventType;
use sinex_core::{Event, JsonValue};
use sinex_node_sdk::{
    stream_processor::{
        Checkpoint, Node, NodeInitContext, NodeRuntimeState, NodeType, ProcessingStats, ScanArgs,
        ScanReport, TimeHorizon,
    },
    NodeError, NodeResult,
};
use sinex_processor_runtime::{
    CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
};
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
        self.get(key)?.as_str()?.parse::<DateTime<Utc>>().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_core::db::models::{Event, SourceMaterial};
    use sinex_core::types::Id;
    use sinex_core::EventBuilder;
    use sinex_test_utils::sinex_test;

    #[sinex_test]
    fn json_extractor_reads_fields() -> sinex_test_utils::TestResult<()> {
        let now = Utc::now();
        let payload = serde_json::json!({
            "command": "ls",
            "exit_code": 0,
            "duration_ms": 42,
            "end_time": now.to_rfc3339(),
        });

        assert_eq!(payload.get_string("command"), Some("ls".to_string()));
        assert_eq!(payload.get_i32("exit_code"), Some(0));
        assert_eq!(payload.get_i64("duration_ms"), Some(42));

        let parsed = payload
            .get_datetime("end_time")
            .expect("datetime should parse");
        assert_eq!(parsed.timestamp(), now.timestamp());
        Ok(())
    }

    fn test_event(
        source: &str,
        payload: serde_json::Value,
    ) -> sinex_test_utils::TestResult<Event<JsonValue>> {
        let material_id = Id::<SourceMaterial>::from_ulid(Ulid::new());
        EventBuilder::dynamic(source, "command.executed", payload)
            .from_material(material_id, 0)
            .build()
            .map_err(Into::into)
    }

    #[sinex_test]
    fn extract_command_data_rejects_unknown_source() -> sinex_test_utils::TestResult<()> {
        let event = test_event("unknown.source", serde_json::json!({ "command": "ls" }))?;
        let processor = TerminalCommandCanonicalizer::new();
        assert!(processor.extract_command_data(&event).is_none());
        Ok(())
    }

    #[sinex_test]
    fn extract_command_data_rejects_empty_command() -> sinex_test_utils::TestResult<()> {
        let event = test_event("shell.kitty", serde_json::json!({ "command": "   " }))?;
        let processor = TerminalCommandCanonicalizer::new();
        assert!(processor.extract_command_data(&event).is_none());
        Ok(())
    }

    #[sinex_test]
    fn extract_command_data_extracts_fields() -> sinex_test_utils::TestResult<()> {
        let payload = serde_json::json!({
            "command": "ls -la",
            "working_directory": "/tmp",
            "exit_code": 0,
            "duration_ms": 120,
            "user": "tester",
        });
        let event = test_event("shell.kitty", payload)?;
        let processor = TerminalCommandCanonicalizer::new();
        let data = processor
            .extract_command_data(&event)
            .expect("command data should be parsed");

        assert_eq!(data.command, "ls -la");
        assert_eq!(data.working_directory.as_deref(), Some("/tmp"));
        assert_eq!(data.exit_code, Some(0));
        assert_eq!(data.duration_ms, Some(120));
        assert_eq!(data.user.as_deref(), Some("tester"));
        assert_eq!(data.source_events.len(), 1);
        Ok(())
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

/// Terminal Command Canonicalizer as a unified Node
pub struct TerminalCommandCanonicalizer {
    runtime: Option<NodeRuntimeState>,
    deduplication_window_secs: i64,
}

impl TerminalCommandCanonicalizer {
    pub fn new() -> Self {
        Self {
            runtime: None,
            deduplication_window_secs: 5, // 5 second window for deduplication
        }
    }

    fn runtime(&self) -> NodeResult<&NodeRuntimeState> {
        self.runtime.as_ref().ok_or_else(|| {
            NodeError::General(eyre!("Terminal canonicalizer runtime not initialized"))
        })
    }

    async fn initialise_with_runtime_state(&mut self, runtime: NodeRuntimeState) -> NodeResult<()> {
        info!(
            processor = "terminal-command-canonicalizer-automaton",
            service = %runtime.service_info().service_name(),
            "Initializing Terminal Command Canonicalizer"
        );

        self.runtime = Some(runtime);
        Ok(())
    }

    /// Safely extract ULID from event ID with proper error handling
    fn extract_ulid_safely(id: &Option<sinex_core::types::Id<Event<JsonValue>>>) -> Ulid {
        match id {
            Some(id) => *id.as_ulid(),
            None => {
                warn!("Event missing ID, generating new ULID");
                Ulid::new()
            }
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
            .get_events_by_type_and_time_range(
                &event_type,
                start_time,
                end_time,
                sinex_core::types::Pagination::new(Some(1000), None),
            )
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
    fn extract_command_data(&self, event: &Event<JsonValue>) -> Option<CommandData> {
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
            source_events: vec![Self::extract_ulid_safely(&event.id)],
        })
    }

    /// Create a canonical command synthesis event
    async fn create_canonical_command(
        &self,
        command_data: &CommandData,
    ) -> NodeResult<Event<CanonicalCommandPayload>> {
        if self.runtime.is_none() {
            return Err(NodeError::General(eyre!(
                "Canonicalizer runtime not initialized"
            )));
        }

        use sinex_core::types::events::payloads::shell::CanonicalCommandPayload;
        use sinex_core::types::Id;
        use sinex_core::{Event, Provenance};

        let source_event_ids: Vec<Id<Event<JsonValue>>> = command_data
            .source_events
            .iter()
            .map(|ulid| Id::from_ulid(*ulid))
            .collect();

        // Create typed payload
        let payload = CanonicalCommandPayload {
            command: command_data.command.clone(),
            working_directory: command_data.working_directory.clone().unwrap_or_default(),
            exit_code: command_data.exit_code.unwrap_or(0),
            duration_ms: command_data.duration_ms.unwrap_or(0) as u64,
            start_time: command_data.start_time,
            end_time: command_data.end_time.unwrap_or(command_data.start_time),
            user: command_data.user.clone().unwrap_or_default(),
            session_id: command_data.session_id.clone().unwrap_or_default(),
            environment_hash: command_data.environment_hash.clone().unwrap_or_default(),
            source_events: command_data
                .source_events
                .iter()
                .map(|id| id.to_string())
                .collect(),
            enrichment_history: Vec::new(),
        };

        // Create provenance from source events
        let provenance = Provenance::from_synthesis(source_event_ids)
            .ok_or_else(|| SinexError::invalid_state("No source events for canonical command"))?;

        let event = Event::new(payload, provenance).at_time(command_data.start_time);

        Ok(event)
    }
}

#[async_trait]
impl Node for TerminalCommandCanonicalizer {
    type Config = ();

    async fn initialize(&mut self, init: NodeInitContext<Self::Config>) -> NodeResult<()> {
        let (_config, runtime) = init.into_runtime();
        self.initialise_with_runtime_state(runtime).await
    }

    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
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
                let runtime = self.runtime()?;
                let db_pool = runtime.db_pool();

                // Determine start time
                let start_time = match from {
                    Checkpoint::Timestamp { timestamp, .. } => timestamp,
                    _ => end_time - chrono::Duration::days(7),
                };

                // Query all terminal command events
                let mut all_raw_events = Vec::new();

                let event_type = EventType::from_static("command.executed");
                let events = db_pool
                    .events()
                    .get_events_by_type_and_time_range(
                        &event_type,
                        start_time,
                        end_time,
                        sinex_core::types::Pagination::new(Some(10_000), None),
                    )
                    .await?;

                all_raw_events.extend(events);

                // Sort by timestamp
                all_raw_events.sort_by_key(|e| e.ts_orig);
                let events_processed = all_raw_events.len() as u64;

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
        events: Vec<Event<JsonValue>>,
    ) -> NodeResult<ProcessingStats> {
        let start_time = std::time::Instant::now();
        let mut processed = 0;
        let mut skipped = 0;
        let mut failed = 0;
        let mut errors = Vec::new();
        let db_pool = self.runtime()?.db_pool().clone();

        for event in events {
            // Work directly with Event<JsonValue>

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

            match self
                .find_existing_canonical_command(
                    &db_pool,
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
                Ok(None) => {}
                Err(e) => {
                    warn!("Error checking for existing command: {}", e);
                }
            }

            match self.create_canonical_command(&command_data).await {
                Ok(_synthesis_event) => {
                    info!("Created canonical command for '{}'", command_data.command);
                    processed += 1;
                }
                Err(e) => {
                    warn!("Failed to create canonical command: {}", e);
                    failed += 1;
                    errors.push(format!("Event {}: {}", event_id, e));
                }
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

    fn processor_name(&self) -> &str {
        "terminal-command-canonicalizer-automaton"
    }

    fn processor_type(&self) -> NodeType {
        NodeType::Automaton
    }

    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
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
        _path: &sinex_core::SanitizedPath,
        _format: ExportFormat,
    ) -> color_eyre::eyre::Result<()> {
        Err(eyre!("Export not supported for automata"))
    }
}
