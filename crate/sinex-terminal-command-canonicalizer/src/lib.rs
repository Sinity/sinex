//! Terminal Command Canonicalizer Automaton
//!
//! This automaton creates canonical command events as synthesis events based on terminal
//! command events from multiple sources (kitty, atuin, shell history). Uses the unified
//! architecture where all events are stored in core.events with source_event_ids for provenance.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Value};
use sinex_core_types::CoreError;
use sinex_db::queries::EventQueries;
use sinex_events::{EventFactory, event_types, sources};
use sinex_macros::with_context;
use sinex_satellite_sdk::{
    automaton::{
        EventFilter, HotlogAutomaton, HotlogAutomatonContext, HotlogAutomatonEvent,
        ProcessingResult,
    },
    SatelliteError, SatelliteResult,
};
use sinex_ulid::Ulid;
use sqlx::PgPool;
use tracing::{debug, info};
// use sinex_events::constants::{event_types, sources}; // already imported above

/// Terminal command canonicalizer automaton
pub struct TerminalCommandCanonicalizer {
    context: Option<HotlogAutomatonContext>,
}

impl TerminalCommandCanonicalizer {
    /// Create a new terminal command canonicalizer
    pub fn new() -> Self {
        Self { context: None }
    }

    /// Find existing canonical command near the given timestamp
    #[with_context(
        operation = "find_existing_canonical_command",
        retry_count = 2,
        timeout_ms = 5000,
        enable_metrics
    )]
    async fn find_existing_canonical_command(
        &self,
        pool: &PgPool,
        command_text: &str,
        timestamp: DateTime<Utc>,
        window_secs: i64,
    ) -> Result<Option<String>, CoreError> {
        let start_time = timestamp - Duration::seconds(window_secs);
        let end_time = timestamp + Duration::seconds(window_secs);

        let event_id = EventQueries::find_canonical_command_by_time_and_text(
            pool,
            start_time,
            end_time,
            command_text.to_string(),
        )
        .await?;

        Ok(event_id)
    }

    /// Create a new canonical command synthesis event
    #[with_context(
        operation = "create_canonical_command",
        retry_count = 3,
        timeout_ms = 10000,
        enable_metrics,
        context = "component=synthesis"
    )]
    async fn create_canonical_command(
        &self,
        command_data: &CommandData,
    ) -> Result<String, CoreError> {
        let context = self
            .context
            .as_ref()
            .ok_or_else(|| CoreError::Service("Context not initialized".to_string()))?;

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
            "source_events": command_data.source_events,
            "enrichment_history": []
        });

        // Create synthesis event with provenance links
        let factory = EventFactory::new("canonical.terminal");
        let mut synthesis_event = factory.create_event("command.canonical", payload);
        
        // Set additional fields that EventFactory doesn't handle
        synthesis_event.ts_orig = Some(command_data.timestamp);
        synthesis_event.source_event_ids = Some(command_data.source_events.clone());

        // Submit synthesis event via ingest client
        let event_id = context.emit_synthesis_event(synthesis_event).await
            .map_err(|e| CoreError::Service(e.to_string()))?;

        info!(
            event_id = %event_id,
            command = %command_data.command,
            "Created canonical command synthesis event"
        );

        Ok(event_id)
    }

    /// Enrich existing canonical command with new data
    #[with_context(
        operation = "enrich_canonical_command",
        retry_count = 2,
        timeout_ms = 8000,
        enable_metrics,
        context = "component=enrichment"
    )]
    async fn enrich_canonical_command(
        &self,
        pool: &PgPool,
        event_id: &str,
        enrichment_data: &EnrichmentData,
    ) -> SatelliteResult<()> {
        // First, get the current event payload
        let mut payload: Value =
            EventQueries::get_payload_by_event_id_text(pool, event_id.to_string()).await?;

        // Add enrichment to history
        let enrichment_entry = json!({
            "timestamp": enrichment_data.timestamp.to_rfc3339(),
            "source": enrichment_data.source,
            "data": enrichment_data.data
        });

        // Update enrichment history
        if let Some(history) = payload.get_mut("enrichment_history") {
            if let Some(history_array) = history.as_array_mut() {
                history_array.push(enrichment_entry);
            }
        }

        // Merge additional data directly into payload
        if let Some(exit_code) = enrichment_data.data.get("exit_code") {
            payload["exit_code"] = exit_code.clone();
        }
        if let Some(duration) = enrichment_data.data.get("duration_ms") {
            payload["duration_ms"] = duration.clone();
        }
        if let Some(end_time) = enrichment_data.data.get("end_time") {
            payload["end_time"] = end_time.clone();
        }

        // Update the event in core.events (this is allowed for synthesis events)
        EventQueries::update_payload_by_event_id_text(pool, event_id.to_string(), payload).await?;

        info!(
            event_id = %event_id,
            source = %enrichment_data.source,
            "Enriched canonical command event in core.events"
        );

        Ok(())
    }

    /// Extract command data from an event
    #[with_context(operation = "extract_command_data", context = "component=extraction")]
    fn extract_command_data(
        &self,
        event: &HotlogAutomatonEvent,
    ) -> SatelliteResult<Option<CommandData>> {
        let data = &event.event.payload;

        // Extract command text - this is required
        let command = match data.get("command").or_else(|| data.get("command_line")) {
            Some(Value::String(cmd)) => cmd.clone(),
            _ => return Ok(None), // Skip events without command
        };

        // Skip empty commands
        if command.trim().is_empty() {
            return Ok(None);
        }

        let command_data = CommandData {
            command,
            working_directory: data
                .get("cwd")
                .or_else(|| data.get("working_directory"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            exit_code: data
                .get("exit_code")
                .and_then(|v| v.as_i64())
                .map(|c| c as i32),
            duration_ms: data
                .get("duration")
                .or_else(|| data.get("duration_ms"))
                .and_then(|v| v.as_u64()),
            start_time: data
                .get("start_time")
                .and_then(|v| v.as_str())
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&Utc)),
            end_time: data
                .get("end_time")
                .and_then(|v| v.as_str())
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&Utc)),
            user: data
                .get("user")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            session_id: data
                .get("session_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            environment_hash: data
                .get("env_hash")
                .or_else(|| data.get("environment_hash"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            source_events: vec![event.event.id],
            timestamp: event.event.ts_ingest,
            source: event.event.source.clone(),
            host: Some(event.event.host.clone()),
        };

        Ok(Some(command_data))
    }

    /// Extract enrichment data from an event
    #[with_context(
        operation = "extract_enrichment_data",
        context = "component=extraction"
    )]
    fn extract_enrichment_data(
        &self,
        event: &HotlogAutomatonEvent,
    ) -> SatelliteResult<Option<EnrichmentData>> {
        let data = &event.event.payload;

        // Check if this event can enrich an existing command
        if data.get("command").is_none() && data.get("command_line").is_none() {
            return Ok(None);
        }

        let enrichment_data = EnrichmentData {
            source: event.event.source.clone(),
            data: data.clone(),
            timestamp: event.event.ts_ingest,
        };

        Ok(Some(enrichment_data))
    }
}

impl Default for TerminalCommandCanonicalizer {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl HotlogAutomaton for TerminalCommandCanonicalizer {
    async fn initialize(&mut self, ctx: HotlogAutomatonContext) -> SatelliteResult<()> {
        info!("Initializing terminal command canonicalizer");

        self.context = Some(ctx);

        info!("Terminal command canonicalizer initialized");
        Ok(())
    }

    async fn process_event(
        &mut self,
        event: HotlogAutomatonEvent,
    ) -> SatelliteResult<ProcessingResult> {
        debug!(
            event_id = %event.event.id,
            source = %event.event.source,
            event_type = %event.event.event_type,
            "Processing event for terminal command canonicalization"
        );

        let context = self
            .context
            .as_ref()
            .ok_or_else(|| SatelliteError::Automaton("Context not initialized".to_string()))?;

        let pool = &context.db_pool;

        // Try to extract command data for synthesis
        if let Some(command_data) = self.extract_command_data(&event)? {
            // Look for existing canonical command
            match self
                .find_existing_canonical_command(
                    pool,
                    &command_data.command,
                    command_data.timestamp,
                    30, // 30 second window
                )
                .await
                .map_err(|e| SatelliteError::Processing(e.to_string()))?
            {
                Some(event_id) => {
                    // Existing command found - enrich it
                    if let Some(enrichment_data) = self.extract_enrichment_data(&event)? {
                        self.enrich_canonical_command(pool, &event_id, &enrichment_data)
                            .await
                            .map_err(|e| SatelliteError::Processing(e.to_string()))?;

                        return Ok(ProcessingResult::Success {
                            checkpoint_data: Some(json!({
                                "action": "enriched",
                                "event_id": event_id,
                                "command": command_data.command
                            })),
                        });
                    }
                }
                None => {
                    // No existing command - create new canonical event (synthesis)
                    let event_id = self.create_canonical_command(&command_data).await
                        .map_err(|e| SatelliteError::Processing(e.to_string()))?;

                    return Ok(ProcessingResult::Success {
                        checkpoint_data: Some(json!({
                            "action": "synthesized",
                            "event_id": event_id,
                            "command": command_data.command
                        })),
                    });
                }
            }
        }

        // Try to extract enrichment data for existing commands
        if let Some(enrichment_data) = self.extract_enrichment_data(&event)? {
            if let Some(command) = enrichment_data.data.get("command").and_then(|v| v.as_str()) {
                // Look for existing canonical command to enrich
                if let Some(event_id) = self
                    .find_existing_canonical_command(
                        pool,
                        command,
                        enrichment_data.timestamp,
                        60, // Larger window for enrichment
                    )
                    .await
                    .map_err(|e| SatelliteError::Processing(e.to_string()))?
                {
                    self.enrich_canonical_command(pool, &event_id, &enrichment_data)
                        .await
                        .map_err(|e| SatelliteError::Processing(e.to_string()))?;

                    return Ok(ProcessingResult::Success {
                        checkpoint_data: Some(json!({
                            "action": "enriched",
                            "event_id": event_id,
                            "command": command
                        })),
                    });
                }
            }
        }

        // Event doesn't contain command data or doesn't match existing commands
        Ok(ProcessingResult::Skip {
            reason: "Event does not contain processable command data".to_string(),
        })
    }

    fn automaton_name(&self) -> &str {
        "terminal-command-canonicalizer"
    }

    fn event_filters(&self) -> Vec<EventFilter> {
        vec![
            // Terminal command events from various sources
            EventFilter::new(
                Some(sources::SHELL_KITTY.to_string()),
                Some(event_types::shell::COMMAND_EXECUTED.to_string()),
            ),
            EventFilter::new(
                Some(sources::SHELL_ATUIN.to_string()),
                Some(event_types::shell::COMMAND_IMPORTED.to_string()),
            ),
            EventFilter::new(
                Some(sources::SHELL_HISTORY.to_string()),
                Some(event_types::shell::COMMAND_IMPORTED.to_string()),
            ),
            EventFilter::new(
                Some(sources::SHELL_RECORDING.to_string()),
                Some(event_types::shell::COMMAND_EXECUTED.to_string()),
            ),
            // Legacy event types for backward compatibility
            EventFilter::new(None, Some(event_types::shell::SHELL_COMMAND_EXECUTED.to_string())),
            EventFilter::new(None, Some(event_types::shell::SHELL_COMMAND_COMPLETED.to_string())),
            EventFilter::new(None, Some(event_types::shell::COMMAND_EXECUTED.to_string())),
        ]
    }
}

/// Command data extracted from events
#[derive(Debug, Clone)]
struct CommandData {
    command: String,
    working_directory: Option<String>,
    exit_code: Option<i32>,
    duration_ms: Option<u64>,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    user: Option<String>,
    session_id: Option<String>,
    environment_hash: Option<String>,
    source_events: Vec<Ulid>,
    timestamp: DateTime<Utc>,
    #[allow(dead_code)]
    source: String,
    host: Option<String>,
}

/// Enrichment data for existing canonical commands
#[derive(Debug, Clone)]
struct EnrichmentData {
    source: String,
    data: Value,
    timestamp: DateTime<Utc>,
}
