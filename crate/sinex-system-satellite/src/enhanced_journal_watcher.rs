//! Enhanced journal watcher with historical import and cursor tracking
//!
//! This module provides advanced systemd journal monitoring with historical import,
//! cursor-based position tracking, rich metadata extraction, and batch processing.
//! Ported from the legacy sinex-events-system implementation with satellite support.

use crate::payloads::*;
use sinex_events::{EventFactory, RawEvent};
use sinex_satellite_sdk::SatelliteResult;
use std::collections::HashMap;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Enhanced journal watcher with historical import and cursor tracking
pub struct EnhancedJournalWatcher {
    config: JournalConfig,
    last_cursor: Option<String>,
}

impl EnhancedJournalWatcher {
    /// Create new enhanced journal watcher
    pub async fn new(config: JournalConfig) -> SatelliteResult<Self> {
        info!(
            "Enhanced journal watcher initialized with config: {:?}",
            config
        );

        // Check journalctl availability
        let check = Command::new("journalctl")
            .arg("--version")
            .output()
            .await
            .map_err(|e| {
                sinex_satellite_sdk::SatelliteError::Processing(format!(
                    "journalctl not found: {}",
                    e
                ))
            })?;

        if !check.status.success() {
            return Err(sinex_satellite_sdk::SatelliteError::Processing(
                "journalctl command failed".to_string(),
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

        info!(
            "Enhanced journal watcher initialized, last cursor: {:?}",
            last_cursor
        );

        Ok(Self {
            config,
            last_cursor,
        })
    }

    /// Start streaming events with optional historical import
    pub async fn start_streaming(
        &mut self,
        tx: mpsc::UnboundedSender<RawEvent>,
    ) -> SatelliteResult<()> {
        info!("Starting enhanced journal monitoring");

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

    /// Import historical journal entries with cursor tracking
    async fn import_historical(
        &mut self,
        tx: &mpsc::UnboundedSender<RawEvent>,
    ) -> SatelliteResult<()> {
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

        // Add kernel filter
        if !self.config.include_kernel {
            args.push("--no-kernel".to_string());
        }

        // Add user filter
        if !self.config.include_user {
            args.push("--system".to_string());
        }

        let output = Command::new("journalctl")
            .args(&args)
            .output()
            .await
            .map_err(|e| {
                sinex_satellite_sdk::SatelliteError::Processing(format!(
                    "Failed to run journalctl: {}",
                    e
                ))
            })?;

        if !output.status.success() {
            return Err(sinex_satellite_sdk::SatelliteError::Processing(format!(
                "journalctl failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
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
                            first_cursor = event
                                .payload
                                .get("cursor")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string());
                        }
                        last_cursor = event
                            .payload
                            .get("cursor")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());

                        batch.push(event);
                        entries_count += 1;

                        if batch.len() >= self.config.batch_size {
                            for event in batch.drain(..) {
                                Self::send_event(tx, event, "journal_batch").await?;
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
            Self::send_event(tx, event, "journal_final_batch").await?;
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
                Self::create_event("sync.completed", serde_json::to_value(sync_payload)?);
            Self::send_event(tx, sync_event, "journal_sync_event").await?;
        }

        info!(
            "Historical import complete: {} entries in {:?}",
            entries_count,
            start_time.elapsed()
        );

        Ok(())
    }

    /// Follow journal in real-time with cursor tracking
    async fn follow_journal(&mut self, tx: mpsc::UnboundedSender<RawEvent>) -> SatelliteResult<()> {
        loop {
            match self.follow_journal_inner(&tx).await {
                Ok(()) => {
                    warn!("Journal following ended normally");
                }
                Err(e) => {
                    error!("Journal following failed: {}", e);
                }
            }

            // Wait before restarting
            tokio::time::sleep(Duration::from_secs(5)).await;
            info!("Restarting journal following");
        }
    }

    /// Inner journal following loop with proper error handling
    async fn follow_journal_inner(
        &mut self,
        tx: &mpsc::UnboundedSender<RawEvent>,
    ) -> SatelliteResult<()> {
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

        // Add kernel filter
        if !self.config.include_kernel {
            args.push("--no-kernel");
        }

        // Add user filter
        if !self.config.include_user {
            args.push("--system");
        }

        let mut child = Command::new("journalctl")
            .args(&args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| {
                sinex_satellite_sdk::SatelliteError::Processing(format!(
                    "Failed to spawn journalctl: {}",
                    e
                ))
            })?;

        let stdout = child.stdout.take().ok_or_else(|| {
            sinex_satellite_sdk::SatelliteError::Processing("No stdout".to_string())
        })?;

        let mut reader = BufReader::new(stdout);
        let mut line = String::new();

        info!("Journal real-time following started");

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
                                if let Some(cursor) =
                                    event.payload.get("cursor").and_then(|v| v.as_str())
                                {
                                    self.last_cursor = Some(cursor.to_string());
                                    self.save_cursor(cursor).await?;
                                }

                                Self::send_event(tx, event, "journal_follow_event").await?;
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

    /// Parse journal entry with comprehensive metadata extraction
    fn parse_journal_entry(&self, entry: &serde_json::Value) -> SatelliteResult<Option<RawEvent>> {
        let obj = entry.as_object().ok_or_else(|| {
            sinex_satellite_sdk::SatelliteError::Processing("Invalid journal entry".to_string())
        })?;

        // Extract required fields
        let cursor = obj
            .get("__CURSOR")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                sinex_satellite_sdk::SatelliteError::Processing("Missing cursor".to_string())
            })?;

        let timestamp_us = obj
            .get("__REALTIME_TIMESTAMP")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or_else(|| {
                sinex_satellite_sdk::SatelliteError::Processing("Missing timestamp".to_string())
            })?;

        let message = obj
            .get("MESSAGE")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // Parse timestamp
        let timestamp = if timestamp_us > 0 {
            chrono::DateTime::from_timestamp_micros(timestamp_us)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| chrono::Utc::now().to_rfc3339())
        } else {
            chrono::Utc::now().to_rfc3339()
        };

        // Extract optional fields with rich metadata
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

        let event = Self::create_event("entry.written", serde_json::to_value(payload)?);

        Ok(Some(event))
    }

    /// Save cursor to file for position tracking
    async fn save_cursor(&self, cursor: &str) -> SatelliteResult<()> {
        if let Some(ref cursor_file) = self.config.cursor_file {
            // Create parent directory if needed
            if let Some(parent) = std::path::Path::new(cursor_file).parent() {
                tokio::fs::create_dir_all(parent).await.ok();
            }

            tokio::fs::write(cursor_file, cursor).await.map_err(|e| {
                sinex_satellite_sdk::SatelliteError::Processing(format!(
                    "Failed to save cursor: {}",
                    e
                ))
            })?;
        }
        Ok(())
    }

    /// Create event using standard pattern
    fn create_event(event_type: &str, payload: serde_json::Value) -> RawEvent {
        let factory = EventFactory::new(sinex_events::sources::JOURNALD);
        factory.create_event(event_type, payload)
    }

    /// Send event with error logging
    async fn send_event(
        tx: &mpsc::UnboundedSender<RawEvent>,
        event: RawEvent,
        context: &str,
    ) -> SatelliteResult<()> {
        if tx.send(event).is_err() {
            warn!("Event channel closed while sending {}", context);
        }
        Ok(())
    }
}
