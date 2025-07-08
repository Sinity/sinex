use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::process::Command;
use tracing::{debug, error, info};

use sinex_core::{
    sources, ChannelSenderExt, EventSender, EventSource, EventSourceBase, EventSourceContext, EventType, JsonValue,
    OptionalTimestamp, Result, Timestamp, EventFactory, ErrorContext, CoreError, RawEvent,
};

// ============================================================================
// Event Payloads
// ============================================================================

/// Systemd journal entry event
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct JournalEntryPayload {
    /// Journal cursor for this entry (unique identifier)
    pub cursor: String,
    /// Timestamp from journal (microseconds since epoch)
    pub timestamp_us: i64,
    /// Parsed timestamp
    pub timestamp: Timestamp,
    /// Hostname
    pub hostname: Option<String>,
    /// Unit name (for systemd services)
    pub unit: Option<String>,
    /// Syslog identifier
    pub syslog_identifier: Option<String>,
    /// Process ID
    pub pid: Option<u32>,
    /// User ID
    pub uid: Option<u32>,
    /// Group ID
    pub gid: Option<u32>,
    /// Command line
    pub cmdline: Option<String>,
    /// Executable path
    pub exe: Option<String>,
    /// systemd unit type (service, socket, etc)
    pub unit_type: Option<String>,
    /// Priority/severity level (0-7, emergency to debug)
    pub priority: Option<u8>,
    /// Facility (kernel, mail, etc)
    pub facility: Option<String>,
    /// Message content
    pub message: String,
    /// Additional fields from journal
    pub fields: HashMap<String, String>,
}

/// Journal sync/import status event
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct JournalSyncPayload {
    /// Sync operation type (initial_import, incremental_sync)
    pub sync_type: String,
    /// Starting cursor
    pub start_cursor: Option<String>,
    /// Ending cursor
    pub end_cursor: String,
    /// Number of entries processed
    pub entries_count: u64,
    /// Time range start
    pub time_start: OptionalTimestamp,
    /// Time range end
    pub time_end: OptionalTimestamp,
    /// Duration in milliseconds
    pub duration_ms: u64,
}

// ============================================================================
// Event Types
// ============================================================================

pub struct JournalEntry;
impl EventType for JournalEntry {
    type Payload = JournalEntryPayload;
    type SourceImpl = JournalMonitor;
    const EVENT_NAME: &'static str = "entry.written";
}

pub struct JournalSync;
impl EventType for JournalSync {
    type Payload = JournalSyncPayload;
    type SourceImpl = JournalMonitor;
    const EVENT_NAME: &'static str = "sync.completed";
}

// ============================================================================
// Event Source Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalConfig {
    /// Follow journal in real-time
    pub follow: bool,
    /// Import historical entries on startup
    pub import_on_startup: bool,
    /// How far back to import (in hours, 0 = all)
    pub import_hours: u32,
    /// Units to monitor (empty = all)
    pub units: Vec<String>,
    /// Priority levels to capture (0-7, empty = all)
    pub priorities: Vec<u8>,
    /// Include kernel messages
    pub include_kernel: bool,
    /// Include user session messages
    pub include_user: bool,
    /// Fields to exclude from additional fields
    pub exclude_fields: Vec<String>,
    /// Cursor file to track position
    pub cursor_file: Option<String>,
    /// Batch size for imports
    pub batch_size: usize,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self {
            follow: true,
            import_on_startup: true, // Changed: Import by default
            import_hours: 0,         // Changed: Import all history (0 = all)
            units: vec![],           // Empty = capture all units
            priorities: vec![],      // Empty = capture all priorities
            include_kernel: true,
            include_user: true,
            exclude_fields: vec![
                "__CURSOR".to_string(),
                "__REALTIME_TIMESTAMP".to_string(),
                "__MONOTONIC_TIMESTAMP".to_string(),
                "_TRANSPORT".to_string(),
            ],
            cursor_file: Some("/var/lib/sinex/journal.cursor".to_string()),
            batch_size: 1000,
        }
    }
}

// ============================================================================
// Event Source Implementation
// ============================================================================

pub struct JournalMonitor {
    config: JournalConfig,
    last_cursor: Option<String>,
    event_factory: EventFactory,
}

// Implement EventSourceBase to get common functionality
impl EventSourceBase for JournalMonitor {}

#[async_trait]
impl EventSource for JournalMonitor {
    type Config = JournalConfig;

    const SOURCE_NAME: &'static str = sources::JOURNALD;

    async fn initialize(ctx: EventSourceContext) -> Result<Self> {
        let config = <Self as EventSourceBase>::parse_config::<Self::Config>(&ctx).await?;

        info!("Initializing journal monitor");

        // Check journalctl availability
        let check = Command::new("journalctl")
            .arg("--version")
            .output()
            .await
            .map_err(|e| 
                ErrorContext::new(CoreError::Configuration(format!("journalctl not found: {}", e)))
                    .with_operation("initialize_journal_monitor")
                    .with_context("tool", "journalctl")
                    .with_context("command", "--version")
                    .build())?;

        if !check.status.success() {
            return Err(ErrorContext::new(CoreError::Configuration("journalctl command failed".to_string()))
                .with_operation("initialize_journal_monitor")
                .with_context("tool", "journalctl")
                .with_context("exit_status", check.status.to_string())
                .build());
        }

        // Load last cursor if cursor file exists
        let last_cursor = if let Some(ref cursor_file) = config.cursor_file {
            tokio::fs::read_to_string(cursor_file)
                .await
                .ok()
                .map(|s| s.trim().to_string())
        } else {
            None
        };

        info!(
            "Journal monitor initialized, last cursor: {:?}",
            last_cursor
        );

        Ok(Self {
            config,
            last_cursor,
            event_factory: EventFactory::new(Self::SOURCE_NAME),
        })
    }

    async fn stream_events(&mut self, tx: EventSender) -> Result<()> {
        info!("Starting journal monitoring");

        // Import historical entries if configured
        if self.config.import_on_startup {
            if let Err(e) = self.import_historical(&tx).await {
                error!("Failed to import historical journal entries: {}", e);
            }
        }

        // Follow journal if configured
        if self.config.follow {
            self.follow_journal(tx).await?;
        }

        Ok(())
    }
}

impl JournalMonitor {
    async fn import_historical(&mut self, tx: &EventSender) -> Result<()> {
        info!("Starting historical journal import");
        let start_time = std::time::Instant::now();

        let mut args = vec!["--output=json".to_string(), "--no-pager".to_string()];

        // Add time filter
        if self.config.import_hours > 0 {
            args.push(format!("--since=-{}h", self.config.import_hours));
        }

        // Add cursor position if we have one
        if let Some(ref cursor) = self.last_cursor {
            args.push(format!("--after-cursor={}", cursor));
        }

        // Add unit filters
        for unit in &self.config.units {
            args.push(format!("--unit={}", unit));
        }

        // Add priority filter
        if !self.config.priorities.is_empty() {
            let priorities: Vec<String> = self
                .config
                .priorities
                .iter()
                .map(|p| p.to_string())
                .collect();
            args.push(format!("--priority={}", priorities.join("..")));
        }

        let output = Command::new("journalctl")
            .args(&args)
            .output()
            .await
            .map_err(|e| 
                ErrorContext::new(CoreError::Io(format!("Failed to run journalctl: {}", e)))
                    .with_operation("import_historical")
                    .with_context("args", format!("{:?}", args))
                    .build())?;

        if !output.status.success() {
            return Err(ErrorContext::new(CoreError::Io("journalctl failed".to_string()))
                .with_operation("import_historical")
                .with_context("exit_status", output.status.to_string())
                .with_context("stderr", String::from_utf8_lossy(&output.stderr))
                .with_context("args", format!("{:?}", args))
                .build());
        }

        let mut entries_count = 0u64;
        let mut first_cursor = None;
        let mut last_cursor = None;
        let mut batch = Vec::new();

        for line in output.stdout.split(|&b| b == b'\n') {
            if line.is_empty() {
                continue;
            }

            match serde_json::from_slice::<JsonValue>(line) {
                Ok(entry) => {
                    if let Some(event) = self.parse_journal_entry(&entry)? {
                        if first_cursor.is_none() {
                            first_cursor =
                                Some(event.payload["cursor"].as_str().unwrap_or("").to_string());
                        }
                        last_cursor =
                            Some(event.payload["cursor"].as_str().unwrap_or("").to_string());

                        batch.push(event);
                        entries_count += 1;

                        if batch.len() >= self.config.batch_size {
                            for event in batch.drain(..) {
                                tx.send_or_log(event, "journal_batch").await?;
                            }
                        }
                    }
                }
                Err(e) => {
                    debug!("Failed to parse journal entry: {}", e);
                }
            }
        }

        // Send remaining batch
        for event in batch {
            tx.send_or_log(event, "journal_final_batch").await?;
        }

        // Update cursor
        if let Some(ref cursor) = last_cursor {
            self.last_cursor = Some(cursor.clone());
            self.save_cursor(cursor).await?;
        }

        // Send sync event
        if entries_count > 0 {
            let sync_payload = JournalSyncPayload {
                sync_type: "initial_import".to_string(),
                start_cursor: first_cursor,
                end_cursor: last_cursor.unwrap_or_default(),
                entries_count,
                time_start: None,
                time_end: None,
                duration_ms: start_time.elapsed().as_millis() as u64,
            };

            let sync_event =
                self.create_event(JournalSync::EVENT_NAME, serde_json::to_value(sync_payload)?);
            tx.send_or_log(sync_event, "journal_sync_event").await?;
        }

        info!(
            "Historical import complete: {} entries in {:?}",
            entries_count,
            start_time.elapsed()
        );

        Ok(())
    }

    async fn follow_journal(&mut self, tx: EventSender) -> Result<()> {
        let mut args = vec!["--output=json", "--no-pager", "--follow"];

        // Add cursor position if we have one
        let cursor_arg;
        if let Some(ref cursor) = self.last_cursor {
            cursor_arg = format!("--after-cursor={}", cursor);
            args.push(&cursor_arg);
        }

        // Add unit filters
        let unit_args: Vec<String> = self
            .config
            .units
            .iter()
            .map(|u| format!("--unit={}", u))
            .collect();
        let unit_refs: Vec<&str> = unit_args.iter().map(|s| s.as_str()).collect();
        args.extend(unit_refs);

        // Add priority filter
        let priority_arg;
        if !self.config.priorities.is_empty() {
            let priorities: Vec<String> = self
                .config
                .priorities
                .iter()
                .map(|p| p.to_string())
                .collect();
            priority_arg = format!("--priority={}", priorities.join(".."));
            args.push(&priority_arg);
        }

        let mut child = Command::new("journalctl")
            .args(&args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| 
                ErrorContext::new(CoreError::Io(format!("Failed to spawn journalctl: {}", e)))
                    .with_operation("follow_journal")
                    .with_context("args", format!("{:?}", args))
                    .build())?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| 
                ErrorContext::new(CoreError::Io("No stdout".to_string()))
                    .with_operation("follow_journal")
                    .with_context("process", "journalctl")
                    .build())?;

        use tokio::io::{AsyncBufReadExt, BufReader};
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();

        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => break, // EOF
                Ok(_) => {
                    if line.trim().is_empty() {
                        continue;
                    }

                    match serde_json::from_str::<JsonValue>(&line) {
                        Ok(entry) => {
                            if let Some(event) = self.parse_journal_entry(&entry)? {
                                // Update cursor
                                if let Some(cursor) = event.payload["cursor"].as_str() {
                                    self.last_cursor = Some(cursor.to_string());
                                    self.save_cursor(cursor).await?;
                                }

                                tx.send_or_log(event, "journal_follow_event").await?;
                            }
                        }
                        Err(e) => {
                            debug!("Failed to parse journal entry: {}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("Error reading journal output: {}", e);
                    break;
                }
            }
        }

        // Wait for child process
        let _ = child.wait().await;

        Ok(())
    }

    fn parse_journal_entry(&self, entry: &JsonValue) -> Result<Option<RawEvent>> {
        let obj = entry
            .as_object()
            .ok_or_else(|| 
                ErrorContext::new(CoreError::Serialization("Invalid journal entry".to_string()))
                    .with_operation("parse_journal_entry")
                    .with_context("entry_type", "not_object")
                    .build())?;

        // Extract required fields
        let cursor = obj
            .get("__CURSOR")
            .and_then(|v| v.as_str())
            .ok_or_else(|| 
                ErrorContext::new(CoreError::Validation("Missing cursor".to_string()))
                    .with_operation("parse_journal_entry")
                    .with_context("field", "__CURSOR")
                    .build())?;

        let timestamp_us = obj
            .get("__REALTIME_TIMESTAMP")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or_else(|| 
                ErrorContext::new(CoreError::Validation("Missing timestamp".to_string()))
                    .with_operation("parse_journal_entry")
                    .with_context("field", "__REALTIME_TIMESTAMP")
                    .build())?;

        let message = obj
            .get("MESSAGE")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // Parse timestamp
        let timestamp = sinex_core::timestamp_micros_to_datetime(timestamp_us)
            .ok_or_else(|| 
                ErrorContext::new(CoreError::Validation("Invalid timestamp".to_string()))
                    .with_operation("parse_journal_entry")
                    .with_context("timestamp_us", timestamp_us.to_string())
                    .build())?;

        // Extract optional fields
        let hostname = obj
            .get("_HOSTNAME")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let unit = obj
            .get("_SYSTEMD_UNIT")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let syslog_identifier = obj
            .get("SYSLOG_IDENTIFIER")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let pid = obj
            .get("_PID")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok());
        let uid = obj
            .get("_UID")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok());
        let gid = obj
            .get("_GID")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok());
        let cmdline = obj
            .get("_CMDLINE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let exe = obj
            .get("_EXE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let priority = obj
            .get("PRIORITY")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok());
        let facility = obj
            .get("SYSLOG_FACILITY")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // Determine unit type
        let unit_type = unit.as_ref().and_then(|u| {
            if u.ends_with(".service") {
                Some("service".to_string())
            } else if u.ends_with(".socket") {
                Some("socket".to_string())
            } else if u.ends_with(".timer") {
                Some("timer".to_string())
            } else if u.ends_with(".mount") {
                Some("mount".to_string())
            } else if u.ends_with(".device") {
                Some("device".to_string())
            } else if u.ends_with(".scope") {
                Some("scope".to_string())
            } else if u.ends_with(".slice") {
                Some("slice".to_string())
            } else {
                None
            }
        });

        // Collect additional fields
        let mut fields = HashMap::new();
        for (key, value) in obj {
            if !self.config.exclude_fields.contains(key)
                && !matches!(
                    key.as_str(),
                    "__CURSOR"
                        | "__REALTIME_TIMESTAMP"
                        | "MESSAGE"
                        | "_HOSTNAME"
                        | "_SYSTEMD_UNIT"
                        | "SYSLOG_IDENTIFIER"
                        | "_PID"
                        | "_UID"
                        | "_GID"
                        | "_CMDLINE"
                        | "_EXE"
                        | "PRIORITY"
                        | "SYSLOG_FACILITY"
                )
            {
                if let Some(s) = value.as_str() {
                    fields.insert(key.clone(), s.to_string());
                }
            }
        }

        let payload = JournalEntryPayload {
            cursor: cursor.to_string(),
            timestamp_us,
            timestamp,
            hostname,
            unit,
            syslog_identifier,
            pid,
            uid,
            gid,
            cmdline,
            exe,
            unit_type,
            priority,
            facility,
            message,
            fields,
        };

        let event = self.create_event(JournalEntry::EVENT_NAME, serde_json::to_value(payload)?);

        Ok(Some(event))
    }

    async fn save_cursor(&self, cursor: &str) -> Result<()> {
        if let Some(ref cursor_file) = self.config.cursor_file {
            // Create parent directory if needed
            if let Some(parent) = std::path::Path::new(cursor_file).parent() {
                tokio::fs::create_dir_all(parent).await.ok();
            }

            tokio::fs::write(cursor_file, cursor).await.map_err(|e| 
                ErrorContext::new(CoreError::Io(format!("Failed to save cursor: {}", e)))
                    .with_operation("save_cursor")
                    .with_context("cursor_file", cursor_file)
                    .with_context("cursor", cursor)
                    .build())?;
        }
        Ok(())
    }

    fn create_event(&self, event_type: &str, payload: JsonValue) -> RawEvent {
        self.event_factory.create_event(event_type, payload)
    }
}
