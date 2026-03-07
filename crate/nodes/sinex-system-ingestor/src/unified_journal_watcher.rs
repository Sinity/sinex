#![doc = include_str!("../docs/unified_journal_watcher.md")]

//! Unified journal watcher that consolidates journal and systemd monitoring.
//!
//! This watcher uses a single `journalctl -f -o json` process and filters events
//! by `_SYSTEMD_UNIT` presence to emit both journal and systemd-specific events.

use sinex_db::models::Event;
use sinex_primitives::JsonValue;
use sinex_primitives::fs::atomic_write;
use sinex_primitives::privacy::{self, ProcessingContext};
use sinex_primitives::temporal::Timestamp;

use crate::WatcherMaterialContext;
use crate::payloads::{JournalConfig, JournalEntryPayload, JournalSyncPayload, SystemdUnitType};
use sha2::{Digest, Sha256};
use sinex_node_sdk::NodeResult;
use sinex_primitives::events::{
    JournalEntryWrittenPayload as EventJournalEntryWrittenPayload,
    JournalSyncCompletedPayload as EventJournalSyncCompletedPayload, SystemdTimerTriggeredPayload,
    SystemdUnitFailedPayload, SystemdUnitReloadedPayload, SystemdUnitStartedPayload,
    SystemdUnitStoppedPayload,
};
use sinex_primitives::{
    events::enums::{
        JournalSyncType, SystemdActiveState as CoreSystemdActiveState,
        SystemdUnitType as CoreSystemdUnitType,
    },
    units::{Microseconds, ProcessId, SyslogPriority, UnixGid, UnixUid},
};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::watcher_lifecycle::{WatcherActivitySnapshot, WatcherLifecycle};

/// Default maximum line length from journalctl output (256 KB).
/// Protects against memory exhaustion from corrupted/malicious journal entries.
/// Can be overridden via `SINEX_JOURNAL_MAX_LINE_BYTES` environment variable.
const DEFAULT_MAX_JOURNAL_LINE_BYTES: usize = 256 * 1024;

/// Required keys in a systemd journal cursor string.
/// Format: `s=<hex>;i=<hex>;b=<boot_id>;m=<monotonic>;t=<realtime>;x=<xor_hash>`
const CURSOR_REQUIRED_KEYS: &[&str] = &["s", "i", "b", "m", "t", "x"];

/// Validate that a string looks like a legitimate systemd journal cursor.
///
/// Cursors are opaque to applications but have a well-defined internal format:
/// semicolon-separated `key=value` pairs with the required keys s, i, b, m, t, x.
/// Accepting garbage cursors causes `journalctl --after-cursor` to fail silently
/// or error out, so we validate before use and fall back to no-cursor (full replay).
fn validate_journal_cursor(cursor: &str) -> bool {
    if cursor.is_empty() {
        return false;
    }

    let parts: HashMap<&str, &str> = cursor
        .split(';')
        .filter_map(|segment| segment.split_once('='))
        .collect();

    CURSOR_REQUIRED_KEYS
        .iter()
        .all(|key| parts.contains_key(key))
}

/// Convert local `SystemdUnitType` to core `SystemdUnitType`
fn convert_unit_type(local: SystemdUnitType) -> CoreSystemdUnitType {
    match local {
        SystemdUnitType::Service => CoreSystemdUnitType::Service,
        SystemdUnitType::Timer => CoreSystemdUnitType::Timer,
        SystemdUnitType::Socket => CoreSystemdUnitType::Socket,
        SystemdUnitType::Target => CoreSystemdUnitType::Target,
        SystemdUnitType::Mount => CoreSystemdUnitType::Mount,
        SystemdUnitType::Other => CoreSystemdUnitType::Other,
    }
}

/// Parse unit type string to core `SystemdUnitType`
fn parse_systemd_unit_type(s: &str) -> CoreSystemdUnitType {
    if s.ends_with(".service") {
        CoreSystemdUnitType::Service
    } else if s.ends_with(".timer") {
        CoreSystemdUnitType::Timer
    } else if s.ends_with(".socket") {
        CoreSystemdUnitType::Socket
    } else if s.ends_with(".target") {
        CoreSystemdUnitType::Target
    } else if s.ends_with(".mount") {
        CoreSystemdUnitType::Mount
    } else {
        CoreSystemdUnitType::Other
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SystemdEventKind {
    Started,
    Stopped,
    Failed,
    Reloaded,
    Triggered,
}

fn classify_systemd_event(entry: &serde_json::Value, message: &str) -> Option<SystemdEventKind> {
    let job_result = entry
        .get("JOB_RESULT")
        .and_then(serde_json::Value::as_str)
        .or_else(|| entry.get("RESULT").and_then(serde_json::Value::as_str));
    if job_result.is_some_and(|r| r.eq_ignore_ascii_case("failed")) {
        return Some(SystemdEventKind::Failed);
    }

    if let Some(job_type) = entry.get("JOB_TYPE").and_then(serde_json::Value::as_str) {
        let kind = match job_type.to_ascii_lowercase().as_str() {
            "start" => Some(SystemdEventKind::Started),
            "stop" => Some(SystemdEventKind::Stopped),
            "reload" => Some(SystemdEventKind::Reloaded),
            "trigger" | "triggered" => Some(SystemdEventKind::Triggered),
            _ => None,
        };
        if kind.is_some() {
            return kind;
        }
    }

    let trimmed = message.trim_start();
    if trimmed.starts_with("Started ") {
        Some(SystemdEventKind::Started)
    } else if trimmed.starts_with("Stopped ") {
        Some(SystemdEventKind::Stopped)
    } else if trimmed.starts_with("Failed ") {
        Some(SystemdEventKind::Failed)
    } else if trimmed.starts_with("Reloaded ") {
        Some(SystemdEventKind::Reloaded)
    } else if trimmed.starts_with("Triggered ") {
        Some(SystemdEventKind::Triggered)
    } else {
        None
    }
}

/// Unified journal watcher with systemd event filtering
pub struct UnifiedJournalWatcher {
    journal_config: JournalConfig,
    systemd_enabled: bool,
    systemd_units: HashSet<String>,
    last_cursor: Option<String>,
    max_line_bytes: usize,
    // Lifecycle tracking
    cancel_token: CancellationToken,
    last_event_time: Arc<Mutex<Option<Instant>>>,
    event_count: Arc<AtomicU64>,
    last_error: Arc<Mutex<Option<String>>>,
    child_process: Option<Child>,
    // Cursor batching state
    pending_cursor: Arc<Mutex<Option<String>>>,
    cursor_save_count: Arc<AtomicU64>,
    last_cursor_save: Arc<Mutex<Instant>>,
    // Backpressure metrics
    channel_drops: Arc<AtomicU64>,
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
                sinex_node_sdk::SinexError::processing("journalctl not found").with_source(e)
            })?;

        if !check.status.success() {
            return Err(sinex_node_sdk::SinexError::processing(
                "journalctl command failed".to_string(),
            ));
        }

        // Load max line bytes from environment or use default
        let max_line_bytes = std::env::var("SINEX_JOURNAL_MAX_LINE_BYTES")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_JOURNAL_LINE_BYTES);

        info!("Journal max line size configured: {} bytes", max_line_bytes);

        // Load last cursor if cursor file exists, validating format before use.
        // A corrupted cursor file would cause `journalctl --after-cursor` to fail,
        // so we fall back to no-cursor (which replays from the configured time window).
        let last_cursor = if let Some(ref cursor_file) = journal_config.cursor_file {
            match tokio::fs::read_to_string(cursor_file).await {
                Ok(contents) => {
                    let trimmed = contents.trim().to_string();
                    if validate_journal_cursor(&trimmed) {
                        Some(trimmed)
                    } else {
                        warn!(
                            cursor_file,
                            cursor = %trimmed,
                            "Ignoring invalid journal cursor (corrupt file?), starting fresh"
                        );
                        None
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(e) => {
                    warn!(cursor_file, error = %e, "Failed to read cursor file, starting fresh");
                    None
                }
            }
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
            max_line_bytes,
            cancel_token: CancellationToken::new(),
            last_event_time: Arc::new(Mutex::new(None)),
            event_count: Arc::new(AtomicU64::new(0)),
            last_error: Arc::new(Mutex::new(None)),
            child_process: None,
            pending_cursor: Arc::new(Mutex::new(None)),
            cursor_save_count: Arc::new(AtomicU64::new(0)),
            last_cursor_save: Arc::new(Mutex::new(Instant::now())),
            channel_drops: Arc::new(AtomicU64::new(0)),
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
        if self.journal_config.import_on_startup
            && let Err(e) = self
                .import_historical(&journal_tx, &systemd_tx, &material)
                .await
        {
            error!("Failed to import historical journal entries: {}", e);
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
            args.push(format!("--after-cursor={cursor}"));
        }

        // Add unit filters
        for unit in &self.journal_config.units {
            args.push(format!("--unit={unit}"));
        }

        // Add priority filter
        if !self.journal_config.priorities.is_empty() {
            let priorities: Vec<String> = self
                .journal_config
                .priorities
                .iter()
                .map(std::string::ToString::to_string)
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
                sinex_node_sdk::SinexError::processing("Failed to run journalctl").with_source(e)
            })?;

        if !output.status.success() {
            return Err(sinex_node_sdk::SinexError::processing(format!(
                "journalctl failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        let mut entries_count = 0u64;
        let mut first_cursor = None;
        let mut last_cursor = None;
        let mut batch = Vec::new();

        for line in output.stdout.split(|&b| b == b'\n') {
            if !line.is_empty() {
                match serde_json::from_slice::<serde_json::Value>(line) {
                    Ok(entry) => {
                        // Process entry and emit both journal and systemd events if applicable
                        if let Some(journal_event) = self.parse_journal_entry(&entry, material)? {
                            if first_cursor.is_none() {
                                first_cursor = journal_event
                                    .payload
                                    .get("cursor")
                                    .and_then(|v| v.as_str())
                                    .map(std::string::ToString::to_string);
                            }
                            last_cursor = journal_event
                                .payload
                                .get("cursor")
                                .and_then(|v| v.as_str())
                                .map(std::string::ToString::to_string);

                            batch.push(journal_event);
                            entries_count += 1;

                            if batch.len() >= self.journal_config.batch_size {
                                for event in batch.drain(..) {
                                    self.send_event(journal_tx, event, "journal_batch", material)
                                        .await?;
                                }
                            }
                        }

                        // Check if this is a systemd event and emit systemd-specific event
                        if self.systemd_enabled
                            && let Some(systemd_event) = self.parse_systemd_entry(&entry, material)
                            && let Some(tx) = systemd_tx.as_ref()
                        {
                            self.send_event(tx, systemd_event, "systemd_batch", material)
                                .await?;
                        }
                    }
                    Err(e) => {
                        debug!("Failed to parse journal entry: {}", e);
                    }
                }
            }
        }

        // Send remaining batch
        for event in batch {
            self.send_event(journal_tx, event, "journal_final_batch", material)
                .await?;
        }

        // Update cursor
        if let Some(ref cursor) = last_cursor {
            self.last_cursor = Some(cursor.clone());
            self.save_cursor(cursor).await?;
        }

        // Send sync event
        if entries_count > 0 {
            let sync_payload = JournalSyncPayload {
                sync_type: JournalSyncType::InitialImport,
                start_cursor: first_cursor,
                end_cursor: last_cursor.unwrap_or_default(),
                entries_count,
                time_start: None,
                time_end: None,
                duration_ms: start_time.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
            };

            #[allow(clippy::expect_used)] // Typed payload serialization is infallible
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
            self.send_event(journal_tx, sync_event, "journal_sync_event", material)
                .await?;
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
            cursor_arg = format!("--after-cursor={cursor}");
            args.push(&cursor_arg);
        }

        // Add unit filters
        let unit_args: Vec<String> = self
            .journal_config
            .units
            .iter()
            .map(|u| format!("--unit={u}"))
            .collect();
        let unit_refs: Vec<&str> = unit_args.iter().map(std::string::String::as_str).collect();
        args.extend(unit_refs);

        // Add priority filter
        let priority_arg;
        if !self.journal_config.priorities.is_empty() {
            let priorities: Vec<String> = self
                .journal_config
                .priorities
                .iter()
                .map(std::string::ToString::to_string)
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
                sinex_node_sdk::SinexError::processing("Failed to spawn journalctl").with_source(e)
            })?;

        // Store child process for lifecycle management
        let child_id = child.id();
        info!("Spawned journalctl process with PID: {:?}", child_id);

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| sinex_node_sdk::SinexError::processing("No stdout".to_string()))?;
        self.child_process = Some(child);

        let mut reader = BufReader::new(stdout);
        let mut line = String::new();

        info!("Unified journal real-time following started");

        loop {
            line.clear();
            let read_result = tokio::select! {
                result = reader.read_line(&mut line) => result,
                () = self.cancel_token.cancelled() => {
                    info!("Journal follow cancelled by shutdown signal");
                    break;
                }
            };
            match read_result {
                Ok(0) => break, // EOF
                Ok(_) => {
                    // Guard against oversized lines from corrupted journal
                    if line.len() > self.max_line_bytes {
                        // Extract cursor and unit name for DLQ metadata if possible
                        let (cursor, unit) = line
                            .trim()
                            .parse::<serde_json::Value>()
                            .ok()
                            .and_then(|entry| {
                                let cursor = entry
                                    .get("__CURSOR")
                                    .and_then(|v| v.as_str())
                                    .map(String::from);
                                let unit = entry
                                    .get("_SYSTEMD_UNIT")
                                    .and_then(|v| v.as_str())
                                    .map(String::from);
                                cursor.map(|c| (c, unit))
                            })
                            .unwrap_or_else(|| ("unknown".to_string(), None));

                        // TODO: Route to DLQ when NatsPublisher is available in watcher context
                        // DLQ entry should include: event_id, error="journal_line_too_large",
                        // metadata with original_size, limit, cursor, journal_unit
                        warn!(
                            line_bytes = line.len(),
                            limit = self.max_line_bytes,
                            cursor = %cursor,
                            journal_unit = ?unit,
                            reason = "journal_line_too_large",
                            "Oversized journal line - would route to DLQ (not yet integrated)"
                        );
                        continue;
                    }
                    if !line.trim().is_empty() {
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

                                    self.send_event(
                                        journal_tx,
                                        event,
                                        "journal_follow_event",
                                        material,
                                    )
                                    .await?;
                                }

                                // Emit systemd event if applicable
                                if self.systemd_enabled
                                    && let Some(systemd_event) =
                                        self.parse_systemd_entry(&entry, material)
                                    && let Some(ref tx) = systemd_tx
                                {
                                    self.send_event(
                                        tx,
                                        systemd_event,
                                        "systemd_follow_event",
                                        material,
                                    )
                                    .await?;
                                }
                            }
                            Err(e) => {
                                debug!("Failed to parse journal entry: {}", e);
                            }
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
        if let Some(mut child) = self.child_process.take() {
            let _ = child.wait().await;
        }

        Ok(())
    }

    /// Parse journal entry with comprehensive metadata extraction
    fn parse_journal_entry(
        &self,
        entry: &serde_json::Value,
        material: &WatcherMaterialContext,
    ) -> NodeResult<Option<Event<JsonValue>>> {
        let obj = entry.as_object().ok_or_else(|| {
            sinex_node_sdk::SinexError::processing("Invalid journal entry".to_string())
        })?;

        // Extract required fields
        let cursor = obj
            .get("__CURSOR")
            .and_then(|v| v.as_str())
            .ok_or_else(|| sinex_node_sdk::SinexError::processing("Missing cursor".to_string()))?;

        let timestamp_us = obj
            .get("__REALTIME_TIMESTAMP")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or_else(|| {
                sinex_node_sdk::SinexError::processing("Missing timestamp".to_string())
            })?;

        let message = obj
            .get("MESSAGE")
            .and_then(|v| v.as_str())
            .map(|s| {
                privacy::engine()
                    .process(s, ProcessingContext::Journal)
                    .text
                    .into_owned()
            })
            .unwrap_or_default();

        // Parse timestamp
        let timestamp: Timestamp = if timestamp_us > 0 {
            Timestamp::from_unix_timestamp_nanos(i128::from(timestamp_us) * 1000)
                .unwrap_or_else(Timestamp::now)
        } else {
            sinex_primitives::temporal::now()
        };

        // Extract optional fields
        let hostname = obj
            .get("_HOSTNAME")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string);
        let unit = obj
            .get("_SYSTEMD_UNIT")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string);
        let syslog_identifier = obj
            .get("SYSLOG_IDENTIFIER")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string);
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
        let cmdline = obj.get("_CMDLINE").and_then(|v| v.as_str()).map(|s| {
            privacy::engine()
                .process(s, ProcessingContext::Command)
                .text
                .into_owned()
        });
        let exe = obj
            .get("_EXE")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string);
        let priority = obj
            .get("PRIORITY")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok());
        let facility = obj
            .get("SYSLOG_FACILITY")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string);

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
                && let Some(s) = value.as_str()
            {
                fields.insert(
                    key.clone(),
                    privacy::engine()
                        .process(s, ProcessingContext::Journal)
                        .text
                        .into_owned(),
                );
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

        let cursor_str = payload.cursor.clone();
        let timestamp_dt = payload.timestamp;

        let mut event = Event::new(
            EventJournalEntryWrittenPayload {
                cursor: payload.cursor,
                timestamp_us: Microseconds::from_micros(payload.timestamp_us),
                timestamp: payload.timestamp,
                hostname: payload.hostname,
                unit: payload.unit,
                syslog_identifier: payload.syslog_identifier,
                pid: payload.pid.map(ProcessId::from_raw),
                uid: payload.uid.map(UnixUid::from_raw),
                gid: payload.gid.map(UnixGid::from_raw),
                cmdline: payload.cmdline,
                exe: payload.exe,
                unit_type: payload.unit_type.as_deref().map(parse_systemd_unit_type),
                priority: payload.priority.map(SyslogPriority::from_raw),
                facility: payload.facility,
                message: payload.message,
                fields: payload.fields,
            },
            material.initial_provenance(),
        );

        // Set deterministic ID based on cursor to prevent duplicates (discriminator 0 for generic entry)
        let id_entropy = Self::calculate_entropy(cursor_str.as_str(), 0);
        let timestamp_ms = payload.timestamp_us / 1000;
        let id_val = (timestamp_ms as u128) << 80 | (id_entropy & 0xFFFF_FFFF_FFFF_FFFF_FFFF);
        let uuid = uuid::Uuid::from_bytes(id_val.to_be_bytes());

        let id = sinex_primitives::Id::from_uuid(uuid);
        event.id = Some(id);

        // Ensure ts_orig matches journal timestamp
        event.ts_orig = Some(timestamp_dt);

        let json_event = event.to_json_event().map_err(|e| {
            sinex_node_sdk::SinexError::processing("Failed to serialize journal entry")
                .with_source(e)
        })?;

        Ok(Some(json_event))
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

        // Parse timestamp once
        let timestamp_us = entry["__REALTIME_TIMESTAMP"]
            .as_str()
            .and_then(|t| t.parse::<u64>().ok())
            .unwrap_or_else(|| {
                (sinex_primitives::temporal::now().unix_timestamp_nanos() / 1000) as u64
            });

        // Helper to construct deterministic ID
        // timestamp (48 bits) | entropy (80 bits)
        let id_entropy = Self::calculate_entropy(cursor, 1);
        let timestamp_ms = timestamp_us / 1000;
        let id_val = u128::from(timestamp_ms) << 80 | (id_entropy & 0xFFFF_FFFF_FFFF_FFFF_FFFF);
        let uuid = uuid::Uuid::from_bytes(id_val.to_be_bytes());

        // Note: We create typed IDs inside each branch to satisfy type inference

        let ts_orig = Some(sinex_primitives::temporal::now());

        // Construct payload based on classified systemd event kind
        let event_kind = classify_systemd_event(entry, message)?;
        let event = match event_kind {
            SystemdEventKind::Started => {
                let unit_type = convert_unit_type(SystemdUnitType::from_unit_name(unit_name));
                let main_pid = entry["_PID"]
                    .as_str()
                    .and_then(|s| s.parse::<u32>().ok())
                    .map(ProcessId::from_raw);
                let mut e = Event::new(
                    SystemdUnitStartedPayload {
                        unit_name: unit_name.to_string(),
                        unit_type,
                        main_pid,
                        active_state: CoreSystemdActiveState::Active,
                        sub_state: "running".to_string(),
                    },
                    material.initial_provenance(),
                );
                e.id = Some(sinex_primitives::Id::from_uuid(uuid));
                e.ts_orig = ts_orig;
                e.to_json_event().ok()?
            }
            SystemdEventKind::Stopped => {
                let unit_type = convert_unit_type(SystemdUnitType::from_unit_name(unit_name));
                let mut e = Event::new(
                    SystemdUnitStoppedPayload {
                        unit_name: unit_name.to_string(),
                        unit_type,
                        exit_code: None,
                        active_state: CoreSystemdActiveState::Inactive,
                        sub_state: "dead".to_string(),
                    },
                    material.initial_provenance(),
                );
                e.id = Some(sinex_primitives::Id::from_uuid(uuid));
                e.ts_orig = ts_orig;
                e.to_json_event().ok()?
            }
            SystemdEventKind::Failed => {
                let mut e = Event::new(
                    SystemdUnitFailedPayload {
                        unit_name: unit_name.to_string(),
                        message: message.to_string(),
                        cursor: cursor.to_string(),
                        pid: entry["_PID"].as_str().map(String::from),
                        uid: entry["_UID"].as_str().map(String::from),
                        timestamp: sinex_primitives::temporal::now(),
                        journal_timestamp: entry["__REALTIME_TIMESTAMP"]
                            .as_str()
                            .and_then(|s| s.parse::<i64>().ok())
                            .map(|us| {
                                Timestamp::from_unix_timestamp_nanos(i128::from(us) * 1000)
                                    .unwrap_or_else(Timestamp::now)
                            }),
                    },
                    material.initial_provenance(),
                );
                e.id = Some(sinex_primitives::Id::from_uuid(uuid));
                e.ts_orig = ts_orig;
                e.to_json_event().ok()?
            }
            SystemdEventKind::Reloaded => {
                let mut e = Event::new(
                    SystemdUnitReloadedPayload {
                        unit_name: Some(unit_name.to_string()),
                        message: message.to_string(),
                        cursor: cursor.to_string(),
                        pid: entry["_PID"].as_str().map(String::from),
                        uid: entry["_UID"].as_str().map(String::from),
                        timestamp: sinex_primitives::temporal::now(),
                        journal_timestamp: entry["__REALTIME_TIMESTAMP"]
                            .as_str()
                            .and_then(|s| s.parse::<i64>().ok())
                            .map(|us| {
                                Timestamp::from_unix_timestamp_nanos(i128::from(us) * 1000)
                                    .unwrap_or_else(Timestamp::now)
                            }),
                    },
                    material.initial_provenance(),
                );
                e.id = Some(sinex_primitives::Id::from_uuid(uuid));
                e.ts_orig = ts_orig;
                e.to_json_event().ok()?
            }
            SystemdEventKind::Triggered => {
                let mut e = Event::new(
                    SystemdTimerTriggeredPayload {
                        unit_name: Some(unit_name.to_string()),
                        message: message.to_string(),
                        cursor: cursor.to_string(),
                        pid: entry["_PID"].as_str().map(String::from),
                        uid: entry["_UID"].as_str().map(String::from),
                        timestamp: sinex_primitives::temporal::now(),
                        journal_timestamp: entry["__REALTIME_TIMESTAMP"]
                            .as_str()
                            .and_then(|s| s.parse::<i64>().ok())
                            .map(|us| {
                                Timestamp::from_unix_timestamp_nanos(i128::from(us) * 1000)
                                    .unwrap_or_else(Timestamp::now)
                            }),
                    },
                    material.initial_provenance(),
                );
                e.id = Some(sinex_primitives::Id::from_uuid(uuid));
                e.ts_orig = ts_orig;
                e.to_json_event().ok()?
            }
        };

        Some(event)
    }

    /// Calculate deterministic entropy from cursor and discriminator
    fn calculate_entropy(cursor: &str, discriminator: u8) -> u128 {
        let mut hasher = Sha256::new();
        hasher.update(cursor.as_bytes());
        hasher.update([discriminator]);
        let hash = hasher.finalize();

        // Use first 16 bytes for 128-bit entropy
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&hash[0..16]);
        u128::from_be_bytes(bytes)
    }

    /// Save cursor to file for position tracking (batched)
    /// Saves based on configured event threshold and interval (defaults: 100 events or 10 seconds)
    async fn save_cursor(&self, cursor: &str) -> NodeResult<()> {
        // Update pending cursor
        if let Ok(mut pending) = self.pending_cursor.lock() {
            *pending = Some(cursor.to_string());
        }

        // Increment cursor save counter
        let count = self.cursor_save_count.fetch_add(1, Ordering::Relaxed) + 1;

        // Check if we should flush
        let should_flush = {
            let elapsed = if let Ok(last_save) = self.last_cursor_save.lock() {
                last_save.elapsed()
            } else {
                std::time::Duration::from_secs(0)
            };

            // Flush based on configured thresholds
            let event_threshold = self.journal_config.cursor_flush_event_threshold;
            let time_threshold = std::time::Duration::from_secs(
                self.journal_config.cursor_flush_interval_secs.as_secs(),
            );

            count >= event_threshold || elapsed >= time_threshold
        };

        if should_flush {
            self.flush_cursor().await?;
        }

        Ok(())
    }

    /// Flush pending cursor to disk
    async fn flush_cursor(&self) -> NodeResult<()> {
        let cursor_to_save = if let Ok(mut pending) = self.pending_cursor.lock() {
            pending.take()
        } else {
            None
        };

        if let Some(cursor) = cursor_to_save
            && let Some(ref cursor_file) = self.journal_config.cursor_file
        {
            // Create parent directory if needed
            if let Some(parent) = camino::Utf8Path::new(cursor_file).parent() {
                tokio::fs::create_dir_all(parent).await.ok();
            }

            atomic_write(std::path::Path::new(cursor_file), cursor.as_bytes())
                .await
                .map_err(|e| {
                    sinex_node_sdk::SinexError::processing("Failed to save cursor").with_source(e)
                })?;

            // Reset counters
            self.cursor_save_count.store(0, Ordering::Relaxed);
            if let Ok(mut last_save) = self.last_cursor_save.lock() {
                *last_save = Instant::now();
            }

            debug!("Cursor flushed to disk: {}", cursor);
        }

        Ok(())
    }

    /// Send event with error logging and backpressure metrics
    async fn send_event(
        &self,
        tx: &mpsc::Sender<Event<JsonValue>>,
        mut event: Event<JsonValue>,
        context: &str,
        material: &WatcherMaterialContext,
    ) -> NodeResult<()> {
        material.decorate_event(&mut event).await?;
        if let Err(err) = tx.send(event).await {
            let drops = self.channel_drops.fetch_add(1, Ordering::Relaxed) + 1;
            // Rate-limit drop warnings: log at 1, 10, 100, 1000, then every 1000
            if drops == 1 || drops == 10 || drops == 100 || drops.is_multiple_of(1000) {
                warn!(
                    channel_drops = drops,
                    context = context,
                    "Event channel backpressure: dropped event ({})",
                    err
                );
            }
        }
        Ok(())
    }

    /// Update event tracking.
    /// Reserved for metrics and diagnostics integration.
    #[allow(dead_code)]
    fn record_event(&self) {
        if let Ok(mut last_event) = self.last_event_time.lock() {
            *last_event = Some(Instant::now());
        }
        self.event_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an error.
    /// Reserved for metrics and diagnostics integration.
    #[allow(dead_code)]
    fn record_error(&self, error: String) {
        if let Ok(mut last_error) = self.last_error.lock() {
            *last_error = Some(error);
        }
    }
}

#[async_trait::async_trait]
impl WatcherLifecycle for UnifiedJournalWatcher {
    fn health_snapshot(&self) -> WatcherActivitySnapshot {
        WatcherActivitySnapshot {
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

        // Flush any pending cursor before shutdown
        if graceful && let Err(e) = self.flush_cursor().await {
            warn!("Failed to flush cursor during shutdown: {}", e);
        }

        // Kill the child process
        if let Some(ref mut child) = self.child_process {
            if graceful {
                // Try graceful shutdown with 30s timeout
                // journalctl should respond quickly to SIGTERM, but we allow time for buffered writes
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
