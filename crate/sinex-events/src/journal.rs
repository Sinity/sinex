use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::process::Command;
use tracing::{error, info, debug};

use sinex_core::{EventType, EventSource, EventSourceContext, Result};
use sinex_db::models::RawEvent;

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
    const EVENT_NAME: &'static str = "system.journal.entry";
}

pub struct JournalSync;
impl EventType for JournalSync {
    type Payload = JournalSyncPayload;
    type SourceImpl = JournalMonitor;
    const EVENT_NAME: &'static str = "system.journal.sync";
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
            import_on_startup: true,     // Changed: Import by default
            import_hours: 0,             // Changed: Import all history (0 = all)
            units: vec![],               // Empty = capture all units
            priorities: vec![],          // Empty = capture all priorities
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
}

#[async_trait]
impl EventSource for JournalMonitor {
    type Config = JournalConfig;
    
    const SOURCE_NAME: &'static str = "journal.monitor";
    
    async fn initialize(ctx: EventSourceContext) -> Result<Self> {
        let config: Self::Config = serde_json::from_value(ctx.config)
            .map_err(|e| sinex_core::CoreError::Configuration(format!("Failed to parse config: {}", e)))?;
        
        info!("Initializing journal monitor");
        
        // Check journalctl availability
        let check = Command::new("journalctl")
            .arg("--version")
            .output()
            .await
            .map_err(|e| sinex_core::CoreError::Other(
                format!("journalctl not found: {}", e)
            ))?;
            
        if !check.status.success() {
            return Err(sinex_core::CoreError::Other(
                "journalctl command failed".to_string()
            ));
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
        
        info!("Journal monitor initialized, last cursor: {:?}", last_cursor);
        
        Ok(Self {
            config,
            last_cursor,
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
        
        let mut args = vec![
            "--output=json".to_string(),
            "--no-pager".to_string(),
        ];
        
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
            let priorities: Vec<String> = self.config.priorities
                .iter()
                .map(|p| p.to_string())
                .collect();
            args.push(format!("--priority={}", priorities.join("..")));
        }
        
        let output = Command::new("journalctl")
            .args(&args)
            .output()
            .await
            .map_err(|e| sinex_core::CoreError::Other(
                format!("Failed to run journalctl: {}", e)
            ))?;
        
        if !output.status.success() {
            return Err(sinex_core::CoreError::Other(
                format!("journalctl failed: {}", String::from_utf8_lossy(&output.stderr))
            ));
        }
        
        let mut entries_count = 0u64;
        let mut first_cursor = None;
        let mut last_cursor = None;
        let mut batch = Vec::new();
        
        for line in output.stdout.split(|&b| b == b'\n') {
            if line.is_empty() {
                continue;
            }
            
            match serde_json::from_slice::<serde_json::Value>(line) {
                Ok(entry) => {
                    if let Some(event) = self.parse_journal_entry(&entry)? {
                        if first_cursor.is_none() {
                            first_cursor = Some(event.payload["cursor"].as_str().unwrap_or("").to_string());
                        }
                        last_cursor = Some(event.payload["cursor"].as_str().unwrap_or("").to_string());
                        
                        batch.push(event);
                        entries_count += 1;
                        
                        if batch.len() >= self.config.batch_size {
                            for event in batch.drain(..) {
                                tx.send(event).await.map_err(|_| sinex_core::CoreError::Other(
                                    "Channel closed".to_string()
                                ))?;
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
            tx.send(event).await.map_err(|_| sinex_core::CoreError::Other(
                "Channel closed".to_string()
            ))?;
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
            
            let sync_event = self.create_event(
                JournalSync::EVENT_NAME,
                serde_json::to_value(sync_payload)?
            );
            tx.send(sync_event).await.map_err(|_| sinex_core::CoreError::Other(
                "Channel closed".to_string()
            ))?;
        }
        
        info!("Historical import complete: {} entries in {:?}", 
              entries_count, start_time.elapsed());
        
        Ok(())
    }
    
    async fn follow_journal(&mut self, tx: EventSender) -> Result<()> {
        let mut args = vec![
            "--output=json",
            "--no-pager",
            "--follow",
        ];
        
        // Add cursor position if we have one
        let cursor_arg;
        if let Some(ref cursor) = self.last_cursor {
            cursor_arg = format!("--after-cursor={}", cursor);
            args.push(&cursor_arg);
        }
        
        // Add unit filters
        let unit_args: Vec<String> = self.config.units
            .iter()
            .map(|u| format!("--unit={}", u))
            .collect();
        let unit_refs: Vec<&str> = unit_args.iter().map(|s| s.as_str()).collect();
        args.extend(unit_refs);
        
        // Add priority filter
        let priority_arg;
        if !self.config.priorities.is_empty() {
            let priorities: Vec<String> = self.config.priorities
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
            .map_err(|e| sinex_core::CoreError::Other(
                format!("Failed to spawn journalctl: {}", e)
            ))?;
        
        let stdout = child.stdout.take()
            .ok_or_else(|| sinex_core::CoreError::Other("No stdout".to_string()))?;
        
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
                    
                    match serde_json::from_str::<serde_json::Value>(&line) {
                        Ok(entry) => {
                            if let Some(event) = self.parse_journal_entry(&entry)? {
                                // Update cursor
                                if let Some(cursor) = event.payload["cursor"].as_str() {
                                    self.last_cursor = Some(cursor.to_string());
                                    self.save_cursor(cursor).await?;
                                }
                                
                                tx.send(event).await.map_err(|_| sinex_core::CoreError::Other(
                                    "Channel closed".to_string()
                                ))?;
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
    
    fn parse_journal_entry(&self, entry: &serde_json::Value) -> Result<Option<RawEvent>> {
        let obj = entry.as_object()
            .ok_or_else(|| sinex_core::CoreError::Other("Invalid journal entry".to_string()))?;
        
        // Extract required fields
        let cursor = obj.get("__CURSOR")
            .and_then(|v| v.as_str())
            .ok_or_else(|| sinex_core::CoreError::Other("Missing cursor".to_string()))?;
            
        let timestamp_us = obj.get("__REALTIME_TIMESTAMP")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or_else(|| sinex_core::CoreError::Other("Missing timestamp".to_string()))?;
            
        let message = obj.get("MESSAGE")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
            
        // Parse timestamp
        let timestamp = DateTime::from_timestamp_micros(timestamp_us)
            .ok_or_else(|| sinex_core::CoreError::Other("Invalid timestamp".to_string()))?;
        
        // Extract optional fields
        let hostname = obj.get("_HOSTNAME").and_then(|v| v.as_str()).map(|s| s.to_string());
        let unit = obj.get("_SYSTEMD_UNIT").and_then(|v| v.as_str()).map(|s| s.to_string());
        let syslog_identifier = obj.get("SYSLOG_IDENTIFIER").and_then(|v| v.as_str()).map(|s| s.to_string());
        let pid = obj.get("_PID").and_then(|v| v.as_str()).and_then(|s| s.parse().ok());
        let uid = obj.get("_UID").and_then(|v| v.as_str()).and_then(|s| s.parse().ok());
        let gid = obj.get("_GID").and_then(|v| v.as_str()).and_then(|s| s.parse().ok());
        let cmdline = obj.get("_CMDLINE").and_then(|v| v.as_str()).map(|s| s.to_string());
        let exe = obj.get("_EXE").and_then(|v| v.as_str()).map(|s| s.to_string());
        let priority = obj.get("PRIORITY").and_then(|v| v.as_str()).and_then(|s| s.parse().ok());
        let facility = obj.get("SYSLOG_FACILITY").and_then(|v| v.as_str()).map(|s| s.to_string());
        
        // Determine unit type
        let unit_type = unit.as_ref().and_then(|u| {
            if u.ends_with(".service") { Some("service".to_string()) }
            else if u.ends_with(".socket") { Some("socket".to_string()) }
            else if u.ends_with(".timer") { Some("timer".to_string()) }
            else if u.ends_with(".mount") { Some("mount".to_string()) }
            else if u.ends_with(".device") { Some("device".to_string()) }
            else if u.ends_with(".scope") { Some("scope".to_string()) }
            else if u.ends_with(".slice") { Some("slice".to_string()) }
            else { None }
        });
        
        // Collect additional fields
        let mut fields = HashMap::new();
        for (key, value) in obj {
            if !self.config.exclude_fields.contains(key) &&
               !matches!(key.as_str(), "__CURSOR" | "__REALTIME_TIMESTAMP" | "MESSAGE" | 
                        "_HOSTNAME" | "_SYSTEMD_UNIT" | "SYSLOG_IDENTIFIER" | "_PID" | 
                        "_UID" | "_GID" | "_CMDLINE" | "_EXE" | "PRIORITY" | "SYSLOG_FACILITY") {
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
        
        let event = self.create_event(
            JournalEntry::EVENT_NAME,
            serde_json::to_value(payload)?
        );
        
        Ok(Some(event))
    }
    
    async fn save_cursor(&self, cursor: &str) -> Result<()> {
        if let Some(ref cursor_file) = self.config.cursor_file {
            // Create parent directory if needed
            if let Some(parent) = std::path::Path::new(cursor_file).parent() {
                tokio::fs::create_dir_all(parent).await.ok();
            }
            
            tokio::fs::write(cursor_file, cursor)
                .await
                .map_err(|e| sinex_core::CoreError::Other(
                    format!("Failed to save cursor: {}", e)
                ))?;
        }
        Ok(())
    }
    
    fn create_event(&self, event_type: &str, payload: serde_json::Value) -> RawEvent {
        RawEvent {
            id: sinex_ulid::Ulid::new(),
            source: Self::SOURCE_NAME.to_string(),
            event_type: event_type.to_string(),
            ts_ingest: Utc::now(),
            ts_orig: Some(Utc::now()),
            host: gethostname::gethostname().to_string_lossy().to_string(),
            ingestor_version: Some(env!("CARGO_PKG_VERSION").to_string()),
            payload_schema_id: None,
            payload,
        }
    }
}