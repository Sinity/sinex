//! Terminal Command Canonicalizer Automaton
//!
//! This automaton creates canonical command events as synthesis events based on terminal
//! command events from multiple sources (kitty, atuin, shell history). Uses the new 
//! dual-log architecture where synthesis events are stored in synthesis.events.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Value};
use sqlx::PgPool;
use sinex_satellite_sdk::{
    automaton::{HotlogAutomaton, HotlogAutomatonContext, HotlogAutomatonEvent, ProcessingResult, EventFilter},
    SatelliteError, SatelliteResult,
};
use sinex_events::RawEventBuilder;
use sinex_ulid::Ulid;
use tracing::{debug, info};

/// Terminal command canonicalizer automaton
pub struct TerminalCommandCanonicalizer {
    context: Option<HotlogAutomatonContext>,
}

impl TerminalCommandCanonicalizer {
    /// Create a new terminal command canonicalizer
    pub fn new() -> Self {
        Self {
            context: None,
        }
    }

    /// Find existing canonical command near the given timestamp
    async fn find_existing_canonical_command(
        &self,
        pool: &PgPool,
        command_text: &str,
        timestamp: DateTime<Utc>,
        window_secs: i64,
    ) -> SatelliteResult<Option<String>> {
        let start_time = timestamp - Duration::seconds(window_secs);
        let end_time = timestamp + Duration::seconds(window_secs);

        let row = sqlx::query!(
            r#"
            SELECT id::text as event_id
            FROM synthesis.events
            WHERE source = 'canonical.terminal'
                AND event_type = 'command.canonical'
                AND ts_ingest >= $1
                AND ts_ingest <= $2
                AND payload->>'command' = $3
            ORDER BY ts_ingest ASC
            LIMIT 1
            "#,
            start_time,
            end_time,
            command_text
        )
        .fetch_optional(pool)
        .await?;

        Ok(row.map(|r| r.event_id.unwrap_or_default()))
    }

    /// Create a new canonical command synthesis event
    async fn create_canonical_command(
        &self,
        command_data: &CommandData,
    ) -> SatelliteResult<String> {
        let context = self.context.as_ref().ok_or_else(|| {
            SatelliteError::Automaton("Context not initialized".to_string())
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
            "source_events": command_data.source_events,
            "enrichment_history": []
        });

        // Create synthesis event with provenance links
        let synthesis_event = RawEventBuilder::new(
            "canonical.terminal",
            "command.canonical",
            payload,
        )
        .with_host(command_data.host.as_deref().unwrap_or("unknown"))
        .with_orig_timestamp(command_data.timestamp)
        .with_ingestor_version("0.4.2")
        .with_source_events(command_data.source_events.clone())
        .build();

        // Submit synthesis event via ingest client
        let event_id = context.emit_synthesis_event(synthesis_event).await?;

        info!(
            event_id = %event_id,
            command = %command_data.command,
            "Created canonical command synthesis event"
        );

        Ok(event_id)
    }

    /// Enrich existing canonical command with new data
    async fn enrich_canonical_command(
        &self,
        pool: &PgPool,
        event_id: &str,
        enrichment_data: &EnrichmentData,
    ) -> SatelliteResult<()> {
        // First, get the current event payload
        let current_event = sqlx::query!(
            r#"
            SELECT payload
            FROM synthesis.events
            WHERE id::text = $1
            "#,
            event_id
        )
        .fetch_one(pool)
        .await?;

        let mut payload: Value = current_event.payload;
        
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

        // Update the event in synthesis.events (this is allowed for synthesis events)
        sqlx::query!(
            r#"
            UPDATE synthesis.events 
            SET 
                payload = $1
            WHERE id::text = $2
            "#,
            payload,
            event_id
        )
        .execute(pool)
        .await?;

        info!(
            event_id = %event_id,
            source = %enrichment_data.source,
            "Enriched canonical command event in raw.events"
        );

        Ok(())
    }

    /// Extract command data from an event
    fn extract_command_data(&self, event: &HotlogAutomatonEvent) -> SatelliteResult<Option<CommandData>> {
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
    fn extract_enrichment_data(&self, event: &HotlogAutomatonEvent) -> SatelliteResult<Option<EnrichmentData>> {
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

    async fn process_event(&mut self, event: HotlogAutomatonEvent) -> SatelliteResult<ProcessingResult> {
        debug!(
            event_id = %event.event.id,
            source = %event.event.source,
            event_type = %event.event.event_type,
            "Processing event for terminal command canonicalization"
        );

        let context = self.context.as_ref().ok_or_else(|| {
            SatelliteError::Automaton("Context not initialized".to_string())
        })?;

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
                .await?
            {
                Some(event_id) => {
                    // Existing command found - enrich it
                    if let Some(enrichment_data) = self.extract_enrichment_data(&event)? {
                        self.enrich_canonical_command(pool, &event_id, &enrichment_data).await?;
                        
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
                    let event_id = self.create_canonical_command(&command_data).await?;
                    
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
                    .await?
                {
                    self.enrich_canonical_command(pool, &event_id, &enrichment_data).await?;
                    
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
            EventFilter::new(Some("shell.kitty".to_string()), Some("command.executed".to_string())),
            EventFilter::new(Some("shell.atuin".to_string()), Some("command.imported".to_string())),
            EventFilter::new(Some("shell.history".to_string()), Some("command.imported".to_string())),
            EventFilter::new(Some("shell.recording".to_string()), Some("command.executed".to_string())),
            // Legacy event types for backward compatibility
            EventFilter::new(None, Some("shell.command.executed".to_string())),
            EventFilter::new(None, Some("shell.command.completed".to_string())),
            EventFilter::new(None, Some("command.executed".to_string())),
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