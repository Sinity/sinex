#![doc = include_str!("../docs/unified_journal_watcher.md")]

//! Unified journal watcher that consolidates journal and systemd monitoring.
//!
//! Previously, the system satellite spawned two separate `journalctl` processes:
//! - One for general journal entries (journal_watcher.rs)
//! - One for systemd unit events (systemd_watcher.rs)
//!
//! This unified watcher uses a single `journalctl -f -o json` process and filters
//! events based on the presence of `_SYSTEMD_UNIT` field to emit both journal
//! and systemd-specific events, reducing process overhead by 50%.

use sinex_core::fs::atomic_write;
use sinex_core::{Event, JsonValue};

use crate::payloads::*;
use crate::systemd_watcher::{SystemdUnitState, SystemdUnitType};
use crate::WatcherMaterialContext;
use sinex_core::types::events::{
    JournalEntryWrittenPayload as EventJournalEntryWrittenPayload,
    JournalSyncCompletedPayload as EventJournalSyncCompletedPayload, SystemdTimerTriggeredPayload,
    SystemdUnitFailedPayload, SystemdUnitReloadedPayload, SystemdUnitStartedPayload,
    SystemdUnitStoppedPayload,
};
use sinex_node_sdk::NodeResult;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::watcher_lifecycle::{WatcherHealth, WatcherLifecycle};

/// Unified journal watcher with systemd event filtering
pub struct UnifiedJournalWatcher {
    journal_config: JournalConfig,
    systemd_enabled: bool,
    systemd_units: HashSet<String>,
    last_cursor: Option<String>,
    // Lifecycle tracking
    cancel_token: CancellationToken,
    last_event_time: Arc<Mutex<Option<Instant>>>,
    event_count: Arc<AtomicU64>,
    last_error: Arc<Mutex<Option<String>>>,
    child_process: Option<Child>,
}

impl UnifiedJournalWatcher {
    /// Create new unified journal watcher
    pub async fn new(journal_config: JournalConfig, systemd_enabled: bool) -> NodeResult<Self> {
        info!(
            "Unified journal watcher initialized (journal: {}, systemd: {})",
            true, systemd_enabled
        );

        // Check journalctl availability
        let check = Command::new("journalctl")
            .arg("--version")
            .output()
            .await
            .map_err(|e| {
                sinex_node_sdk::NodeError::Processing(format!("journalctl not found: {}", e))
            })?;

        if !check.status.success() {
            return Err(sinex_node_sdk::NodeError::Processing(
                "journalctl command failed".to_string(),
            ));
        }

        // Load last cursor if cursor file exists
        let last_cursor = if let Some(ref cursor_file) = journal_config.cursor_file {
            tokio::fs::read_to_string(cursor_file)
                .await
                .ok()
                .map(|s| s.trim().to_string())
        } else {
            None
        };

        info!(
            "Unified journal watcher initialized, last cursor: {:?}",
            last_cursor
        );

        Ok(Self {
            journal_config,
            systemd_enabled,
            systemd_units: HashSet::new(),
            last_cursor,
            cancel_token: CancellationToken::new(),
            last_event_time: Arc::new(Mutex::new(None)),
            event_count: Arc::new(AtomicU64::new(0)),
            last_error: Arc::new(Mutex::new(None)),
            child_process: None,
        })
    }

    /// Add systemd units to track
    pub fn track_systemd_units(&mut self, units: impl IntoIterator<Item = String>) {
        self.systemd_units.extend(units);
    }

    /// Start streaming events with optional historical import
    pub(crate) async fn start_streaming(
        &mut self,
        journal_tx: mpsc::Sender<Event<JsonValue>>,
        systemd_tx: Option<mpsc::Sender<Event<JsonValue>>>,
        material: WatcherMaterialContext,
    ) -> NodeResult<()> {
        info!("Starting unified journal monitoring");

        // Import historical entries if configured
        if self.journal_config.import_on_startup {
            if let Err(e) = self
                .import_historical(&journal_tx, &systemd_tx, &material)
                .await
            {
                error!("Failed to import historical journal entries: {}", e);
            }
        }

        // Follow journal if configured
        if self.journal_config.follow {
            self.follow_journal(journal_tx, systemd_tx, &material)
                .await?;
        }

        Ok(())
    }

    /// Import historical journal entries with cursor tracking
    pub(crate) async fn import_historical(
        &mut self,
        journal_tx: &mpsc::Sender<Event<JsonValue>>,
        systemd_tx: &Option<mpsc::Sender<Event<JsonValue>>>,
        material: &WatcherMaterialContext,
    ) -> NodeResult<u64> {
        info!("Starting historical journal import");
        let start_time = std::time::Instant::now();

        let mut args = vec!["--output=json".to_string(), "--no-pager".to_string()];

        // Add time filter
        if self.journal_config.import_hours > 0 {
            args.push(format!("--since=-{}h", self.journal_config.import_hours));
        }

        // Add cursor position if we have one
        if let Some(ref cursor) = self.last_cursor {
            args.push(format!("--after-cursor={}", cursor));
        }

        // Add unit filters
        for unit in &self.journal_config.units {
            args.push(format!("--unit={}", unit));
        }

        // Add priority filter
        if !self.journal_config.priorities.is_empty() {
            let priorities: Vec<String> = self
                .journal_config
                .priorities
                .iter()
                .map(|p| p.to_string())
                .collect();
            args.push(format!("--priority={}", priorities.join("..")));
        }

        // Add kernel filter
        if !self.journal_config.include_kernel {
            args.push("--no-kernel".to_string());
        }

        // Add user filter
        if !self.journal_config.include_user {
            args.push("--system".to_string());
        }

        let output = Command::new("journalctl")
            .args(&args)
            .output()
            .await
            .map_err(|e| {
                sinex_node_sdk::NodeError::Processing(format!("Failed to run journalctl: {}", e))
            })?;

        if !output.status.success() {
            return Err(sinex_node_sdk::NodeError::Processing(format!(
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
                    // Process entry and emit both journal and systemd events if applicable
                    if let Some(journal_event) = self.parse_journal_entry(&entry, material)? {
                        if first_cursor.is_none() {
                            first_cursor = journal_event
                                .payload
                                .get("cursor")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string());
                        }
                        last_cursor = journal_event
                            .payload
                            .get("cursor")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());

                        batch.push(journal_event);
                        entries_count += 1;

                        if batch.len() >= self.journal_config.batch_size {
                            for event in batch.drain(..) {
                                Self::send_event(journal_tx, event, "journal_batch", material)
                                    .await?;
                            }
                        }
                    }

                    // Check if this is a systemd event and emit systemd-specific event
                    if self.systemd_enabled {
                        if let Some(systemd_event) = self.parse_systemd_entry(&entry, material) {
                            if let Some(ref tx) = systemd_tx {
                                Self::send_event(tx, systemd_event, "systemd_batch", material)
                                    .await?;
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
            Self::send_event(journal_tx, event, "journal_final_batch", material).await?;
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
                duration_ms: start_time.elapsed().as_millis().min(u64::MAX as u128) as u64,
            };

            let sync_event = Event::new(
                EventJournalSyncCompletedPayload {
                    sync_type: sync_payload.sync_type,
                    start_cursor: sync_payload.start_cursor,
                    end_cursor: sync_payload.end_cursor,
                    entries_count: sync_payload.entries_count,
                    time_start: sync_payload.time_start,
                    time_end: sync_payload.time_end,
                    duration_ms: sync_payload.duration_ms,
                },
                material.initial_provenance(),
            )
            .to_json_event()
            .expect("serializing journal sync event should not fail");
            Self::send_event(journal_tx, sync_event, "journal_sync_event", material).await?;
        }

        info!(
            "Historical import complete: {} entries in {:?}",
            entries_count,
            start_time.elapsed()
        );

        Ok(entries_count)
    }

    /// Follow journal in real-time with cursor tracking
    async fn follow_journal(
        &mut self,
        journal_tx: mpsc::Sender<Event<JsonValue>>,
        systemd_tx: Option<mpsc::Sender<Event<JsonValue>>>,
        material: &WatcherMaterialContext,
    ) -> NodeResult<()> {
        self.follow_journal_inner(&journal_tx, systemd_tx, material)
            .await
    }

    /// Inner journal following loop
    async fn follow_journal_inner(
        &mut self,
        journal_tx: &mpsc::Sender<Event<JsonValue>>,
        systemd_tx: Option<mpsc::Sender<Event<JsonValue>>>,
        material: &WatcherMaterialContext,
    ) -> NodeResult<()> {
        let mut args = vec!["--output=json", "--no-pager", "--follow"];

        // Add cursor position if we have one
        let cursor_arg;
        if let Some(ref cursor) = self.last_cursor {
            cursor_arg = format!("--after-cursor={}", cursor);
            args.push(&cursor_arg);
        }

        // Add unit filters
        let unit_args: Vec<String> = self
            .journal_config
            .units
            .iter()
            .map(|u| format!("--unit={}", u))
            .collect();
        let unit_refs: Vec<&str> = unit_args.iter().map(|s| s.as_str()).collect();
        args.extend(unit_refs);

        // Add priority filter
        let priority_arg;
        if !self.journal_config.priorities.is_empty() {
            let priorities: Vec<String> = self
                .journal_config
                .priorities
                .iter()
                .map(|p| p.to_string())
                .collect();
            priority_arg = format!("--priority={}", priorities.join(".."));
            args.push(&priority_arg);
        }

        // Add kernel filter
        if !self.journal_config.include_kernel {
            args.push("--no-kernel");
        }

        // Add user filter
        if !self.journal_config.include_user {
            args.push("--system");
        }

        let mut child = Command::new("journalctl")
            .args(&args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| {
                sinex_node_sdk::NodeError::Processing(format!("Failed to spawn journalctl: {}", e))
            })?;

        // Store child process for lifecycle management
        let child_id = child.id();
        info!("Spawned journalctl process with PID: {:?}", child_id);

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| sinex_node_sdk::NodeError::Processing("No stdout".to_string()))?;

        let mut reader = BufReader::new(stdout);
        let mut line = String::new();

        info!("Unified journal real-time following started");

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
                            // Emit journal event
                            if let Some(event) = self.parse_journal_entry(&entry, material)? {
                                // Update cursor
                                if let Some(cursor) =
                                    event.payload.get("cursor").and_then(|v| v.as_str())
                                {
                                    self.last_cursor = Some(cursor.to_string());
                                    self.save_cursor(cursor).await?;
                                }

                                Self::send_event(
                                    journal_tx,
                                    event,
                                    "journal_follow_event",
                                    material,
                                )
                                .await?;
                            }

                            // Emit systemd event if applicable
                            if self.systemd_enabled {
                                if let Some(systemd_event) =
                                    self.parse_systemd_entry(&entry, material)
                                {
                                    if let Some(ref tx) = systemd_tx {
                                        Self::send_event(
                                            tx,
                                            systemd_event,
                                            "systemd_follow_event",
                                            material,
                                        )
                                        .await?;
                                    }
                                }
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
    fn parse_journal_entry(
        &self,
        entry: &serde_json::Value,
        material: &WatcherMaterialContext,
    ) -> NodeResult<Option<Event<JsonValue>>> {
        let obj = entry.as_object().ok_or_else(|| {
            sinex_node_sdk::NodeError::Processing("Invalid journal entry".to_string())
        })?;

        // Extract required fields
        let cursor = obj
            .get("__CURSOR")
            .and_then(|v| v.as_str())
            .ok_or_else(|| sinex_node_sdk::NodeError::Processing("Missing cursor".to_string()))?;

        let timestamp_us = obj
            .get("__REALTIME_TIMESTAMP")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or_else(|| {
                sinex_node_sdk::NodeError::Processing("Missing timestamp".to_string())
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
        let mut fields = HashMap::with_capacity(16);
        for (key, value) in obj {
            if !self.journal_config.exclude_fields.contains(key)
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

        let event = Event::new(
            EventJournalEntryWrittenPayload {
                cursor: payload.cursor,
                timestamp_us: payload.timestamp_us,
                timestamp: payload.timestamp,
                hostname: payload.hostname,
                unit: payload.unit,
                syslog_identifier: payload.syslog_identifier,
                pid: payload.pid,
                uid: payload.uid,
                gid: payload.gid,
                cmdline: payload.cmdline,
                exe: payload.exe,
                unit_type: payload.unit_type,
                priority: payload.priority,
                facility: payload.facility,
                message: payload.message,
                fields: payload.fields,
            },
            material.initial_provenance(),
        )
        .to_json_event()
        .map_err(|e| {
            sinex_node_sdk::NodeError::Processing(format!(
                "Failed to serialize journal entry: {}",
                e
            ))
        })?;

        Ok(Some(event))
    }

    /// Parse systemd-specific event from journal entry
    fn parse_systemd_entry(
        &self,
        entry: &serde_json::Value,
        material: &WatcherMaterialContext,
    ) -> Option<Event<JsonValue>> {
        let message = entry["MESSAGE"].as_str().unwrap_or("");
        let unit_name = entry["_SYSTEMD_UNIT"].as_str()?;
        let cursor = entry["__CURSOR"].as_str().unwrap_or("unknown");

        // Filter by tracked units if configured
        if !self.systemd_units.is_empty() && !self.systemd_units.contains(unit_name) {
            return None;
        }

        // Look for systemd state change messages
        if message.contains("Started ") {
            let unit_type = SystemdUnitType::from_unit_name(unit_name);

            Some(
                Event::new(
                    SystemdUnitStartedPayload {
                        unit_name: unit_name.to_string(),
                        unit_type: unit_type.to_string(),
                        main_pid: entry["_PID"].as_str().and_then(|s| s.parse().ok()),
                        active_state: SystemdUnitState::Active.to_string(),
                        sub_state: "running".to_string(),
                    },
                    material.initial_provenance(),
                )
                .to_json_event()
                .ok()?,
            )
        } else if message.contains("Stopped ") {
            let unit_type = SystemdUnitType::from_unit_name(unit_name);

            Some(
                Event::new(
                    SystemdUnitStoppedPayload {
                        unit_name: unit_name.to_string(),
                        unit_type: unit_type.to_string(),
                        exit_code: None,
                        active_state: SystemdUnitState::Inactive.to_string(),
                        sub_state: "dead".to_string(),
                    },
                    material.initial_provenance(),
                )
                .to_json_event()
                .ok()?,
            )
        } else if message.contains("Failed ") {
            Some(
                Event::new(
                    SystemdUnitFailedPayload {
                        unit_name: unit_name.to_string(),
                        message: message.to_string(),
                        cursor: cursor.to_string(),
                        pid: entry["_PID"].as_str().map(String::from),
                        uid: entry["_UID"].as_str().map(String::from),
                        timestamp: chrono::Utc::now().to_rfc3339(),
                        journal_timestamp: entry["__REALTIME_TIMESTAMP"].as_str().map(String::from),
                    },
                    material.initial_provenance(),
                )
                .to_json_event()
                .ok()?,
            )
        } else if message.contains("Reloaded ") {
            Some(
                Event::new(
                    SystemdUnitReloadedPayload {
                        unit_name: Some(unit_name.to_string()),
                        message: message.to_string(),
                        cursor: cursor.to_string(),
                        pid: entry["_PID"].as_str().map(String::from),
                        uid: entry["_UID"].as_str().map(String::from),
                        timestamp: chrono::Utc::now().to_rfc3339(),
                        journal_timestamp: entry["__REALTIME_TIMESTAMP"].as_str().map(String::from),
                    },
                    material.initial_provenance(),
                )
                .to_json_event()
                .ok()?,
            )
        } else if message.contains("Triggered ") {
            Some(
                Event::new(
                    SystemdTimerTriggeredPayload {
                        unit_name: Some(unit_name.to_string()),
                        message: message.to_string(),
                        cursor: cursor.to_string(),
                        pid: entry["_PID"].as_str().map(String::from),
                        uid: entry["_UID"].as_str().map(String::from),
                        timestamp: chrono::Utc::now().to_rfc3339(),
                        journal_timestamp: entry["__REALTIME_TIMESTAMP"].as_str().map(String::from),
                    },
                    material.initial_provenance(),
                )
                .to_json_event()
                .ok()?,
            )
        } else {
            None // Not a state change we care about
        }
    }

    /// Save cursor to file for position tracking
    async fn save_cursor(&self, cursor: &str) -> NodeResult<()> {
        if let Some(ref cursor_file) = self.journal_config.cursor_file {
            // Create parent directory if needed
            if let Some(parent) = camino::Utf8Path::new(cursor_file).parent() {
                tokio::fs::create_dir_all(parent).await.ok();
            }

            atomic_write(std::path::Path::new(cursor_file), cursor.as_bytes())
                .await
                .map_err(|e| {
                    sinex_node_sdk::NodeError::Processing(format!("Failed to save cursor: {}", e))
                })?;
        }
        Ok(())
    }

    /// Send event with error logging
    async fn send_event(
        tx: &mpsc::Sender<Event<JsonValue>>,
        mut event: Event<JsonValue>,
        context: &str,
        material: &WatcherMaterialContext,
    ) -> NodeResult<()> {
        material.decorate_event(&mut event).await?;
        if let Err(err) = tx.send(event).await {
            warn!(
                "Event channel unavailable while sending {}: {}",
                context, err
            );
        }
        Ok(())
    }

    /// Update event tracking
    fn record_event(&self) {
        if let Ok(mut last_event) = self.last_event_time.lock() {
            *last_event = Some(Instant::now());
        }
        self.event_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an error
    fn record_error(&self, error: String) {
        if let Ok(mut last_error) = self.last_error.lock() {
            *last_error = Some(error);
        }
    }
}

#[async_trait::async_trait]
impl WatcherLifecycle for UnifiedJournalWatcher {
    fn health_snapshot(&self) -> WatcherHealth {
        WatcherHealth {
            active: !self.cancel_token.is_cancelled(),
            last_event: self.last_event_time.lock().ok().and_then(|t| *t),
            last_error: self.last_error.lock().ok().and_then(|e| e.clone()),
            events_processed: self.event_count.load(Ordering::Relaxed),
        }
    }

    async fn shutdown(&mut self, graceful: bool) -> NodeResult<()> {
        info!(
            "Shutting down unified journal watcher (graceful: {})",
            graceful
        );

        // Signal cancellation
        self.cancel_token.cancel();

        // Kill the child process
        if let Some(ref mut child) = self.child_process {
            if graceful {
                // Try graceful shutdown with timeout
                match tokio::time::timeout(tokio::time::Duration::from_secs(30), child.wait()).await
                {
                    Ok(Ok(status)) => {
                        info!("Journal watcher process exited: {:?}", status);
                    }
                    Ok(Err(e)) => {
                        warn!("Error waiting for journal watcher process: {}", e);
                    }
                    Err(_) => {
                        warn!("Journal watcher process did not exit within 30s, killing");
                        let _ = child.kill().await;
                    }
                }
            } else {
                // Force kill
                if let Err(e) = child.kill().await {
                    warn!("Error killing journal watcher process: {}", e);
                }
            }
        }

        Ok(())
    }

    fn last_event_timestamp(&self) -> Option<Instant> {
        self.last_event_time.lock().ok().and_then(|t| *t)
    }

    fn cancellation_token(&self) -> &CancellationToken {
        &self.cancel_token
    }
}
