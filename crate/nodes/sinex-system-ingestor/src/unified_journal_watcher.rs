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
use serde_json::json;
use sha2::{Digest, Sha256};
use sinex_node_sdk::{NatsPublisher, NodeResult};
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

/// Preview length for malformed or oversized journal lines in logs and DLQ payloads.
const JOURNAL_LINE_PREVIEW_LIMIT: usize = 512;

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
    dlq_publisher: Option<Arc<NatsPublisher>>,
}

impl UnifiedJournalWatcher {
    fn parse_optional_field<T>(
        entry: &serde_json::Map<String, serde_json::Value>,
        field: &str,
        cursor: &str,
    ) -> NodeResult<Option<T>>
    where
        T: std::str::FromStr,
        T::Err: std::error::Error + Send + Sync + 'static,
    {
        let Some(raw) = entry.get(field).and_then(serde_json::Value::as_str) else {
            return Ok(None);
        };
        raw.parse::<T>().map(Some).map_err(|error| {
            sinex_node_sdk::SinexError::processing(format!(
                "Journal entry {cursor} has invalid {field}"
            ))
            .with_context("cursor", cursor.to_string())
            .with_context("field", field.to_string())
            .with_context("value", raw.to_string())
            .with_source(error)
        })
    }

    fn require_entry_string_field(
        entry: &serde_json::Map<String, serde_json::Value>,
        field: &str,
        context: &str,
    ) -> NodeResult<String> {
        entry
            .get(field)
            .and_then(serde_json::Value::as_str)
            .map(std::string::ToString::to_string)
            .ok_or_else(|| {
                sinex_node_sdk::SinexError::processing(format!(
                    "{context} is missing required {field}"
                ))
            })
    }

    fn require_nonempty_entry_string_field(
        entry: &serde_json::Map<String, serde_json::Value>,
        field: &str,
        context: &str,
    ) -> NodeResult<String> {
        let value = Self::require_entry_string_field(entry, field, context)?;
        if value.trim().is_empty() {
            return Err(sinex_node_sdk::SinexError::processing(format!(
                "{context} has empty {field}"
            )));
        }
        Ok(value)
    }

    fn parse_journal_timestamp_us(
        entry: &serde_json::Map<String, serde_json::Value>,
        cursor: &str,
    ) -> NodeResult<u64> {
        let raw = entry
            .get("__REALTIME_TIMESTAMP")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                sinex_node_sdk::SinexError::processing(format!(
                    "Journal entry {cursor} is missing __REALTIME_TIMESTAMP"
                ))
                .with_context("cursor", cursor.to_string())
            })?;
        raw.parse::<u64>().map_err(|error| {
            sinex_node_sdk::SinexError::processing(format!(
                "Journal entry {cursor} has invalid __REALTIME_TIMESTAMP"
            ))
            .with_context("cursor", cursor.to_string())
            .with_context("timestamp_us", raw.to_string())
            .with_source(error)
        })
    }

    fn journal_timestamp_from_micros(timestamp_us: u64, cursor: &str) -> NodeResult<Timestamp> {
        Timestamp::from_unix_timestamp_nanos(i128::from(timestamp_us) * 1000).ok_or_else(|| {
            sinex_node_sdk::SinexError::processing(format!(
                "Journal entry {cursor} has out-of-range __REALTIME_TIMESTAMP"
            ))
            .with_context("cursor", cursor.to_string())
            .with_context("timestamp_us", timestamp_us.to_string())
        })
    }

    fn parse_realtime_timestamp_us(
        entry: &serde_json::Value,
        unit_name: &str,
    ) -> NodeResult<u64> {
        let raw = entry["__REALTIME_TIMESTAMP"].as_str().ok_or_else(|| {
            sinex_node_sdk::SinexError::processing(format!(
                "Systemd journal entry for {unit_name} is missing __REALTIME_TIMESTAMP"
            ))
        })?;
        raw.parse::<u64>().map_err(|error| {
            sinex_node_sdk::SinexError::processing(format!(
                "Systemd journal entry for {unit_name} has invalid __REALTIME_TIMESTAMP"
            ))
            .with_context("unit_name", unit_name.to_string())
            .with_context("timestamp_us", raw.to_string())
            .with_source(error)
        })
    }

    fn timestamp_from_micros(timestamp_us: u64, unit_name: &str) -> NodeResult<Timestamp> {
        Timestamp::from_unix_timestamp_nanos(i128::from(timestamp_us) * 1000).ok_or_else(|| {
            sinex_node_sdk::SinexError::processing(format!(
                "Systemd journal entry for {unit_name} has out-of-range __REALTIME_TIMESTAMP"
            ))
            .with_context("unit_name", unit_name.to_string())
            .with_context("timestamp_us", timestamp_us.to_string())
        })
    }

    fn serialize_systemd_event<T: serde::Serialize>(
        mut event: Event<T>,
        uuid: uuid::Uuid,
        ts_orig: Timestamp,
    ) -> NodeResult<Event<JsonValue>> {
        event.id = Some(sinex_primitives::Id::from_uuid(uuid));
        event.ts_orig = Some(ts_orig);
        event.to_json_event().map_err(|error| {
            sinex_node_sdk::SinexError::processing("Failed to serialize systemd journal entry")
                .with_source(error)
        })
    }

    /// Create new unified journal watcher
    pub async fn new(
        journal_config: JournalConfig,
        systemd_enabled: bool,
        dlq_publisher: Option<Arc<NatsPublisher>>,
    ) -> NodeResult<Self> {
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
            dlq_publisher,
        })
    }

    fn journal_line_preview(line: &str) -> (String, bool) {
        let preview: String = line.chars().take(JOURNAL_LINE_PREVIEW_LIMIT).collect();
        let preview_truncated = line.chars().count() > JOURNAL_LINE_PREVIEW_LIMIT;
        (preview, preview_truncated)
    }

    fn parse_oversized_line_metadata(
        line: &str,
    ) -> Result<(String, Option<String>), serde_json::Error> {
        let entry: serde_json::Value = serde_json::from_str(line)?;
        let cursor = entry
            .get("__CURSOR")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown")
            .to_string();
        let unit = entry
            .get("_SYSTEMD_UNIT")
            .and_then(|value| value.as_str())
            .map(str::to_owned);
        Ok((cursor, unit))
    }

    fn require_sync_end_cursor(entries_count: u64, last_cursor: Option<String>) -> NodeResult<String> {
        last_cursor.ok_or_else(|| {
            sinex_node_sdk::SinexError::processing(format!(
                "Historical journal import processed {entries_count} entries without a terminal cursor"
            ))
        })
    }

    fn record_malformed_journal_line(
        &self,
        phase: &'static str,
        line: &str,
        error: &serde_json::Error,
    ) {
        let (line_preview, line_preview_truncated) = Self::journal_line_preview(line);
        let message = format!("Failed to parse journal {phase} line: {error}");
        self.record_error(message);
        warn!(
            phase,
            line_bytes = line.len(),
            line_preview = %line_preview,
            line_preview_truncated,
            error = %error,
            "Ignoring malformed journal line"
        );
    }

    async fn route_oversized_line_to_dlq(
        &self,
        material: &WatcherMaterialContext,
        line: &str,
        cursor: &str,
        unit: Option<&str>,
        metadata_parse_error: Option<&str>,
    ) -> NodeResult<bool> {
        let Some(publisher) = self.dlq_publisher.as_ref() else {
            return Ok(false);
        };

        let (preview, preview_truncated) = Self::journal_line_preview(line);
        let event = Event::new_json(
            "system-watcher",
            "journal.line.rejected",
            json!({
                "reason": "journal_line_too_large",
                "original_size": line.len(),
                "limit": self.max_line_bytes,
                "cursor": cursor,
                "journal_unit": unit,
                "line_preview": preview,
                "line_preview_truncated": preview_truncated,
                "metadata_parse_error": metadata_parse_error,
            }),
            material.initial_provenance(),
        )
        .with_timestamp(Timestamp::now());

        publisher
            .publish_to_dlq(&event, "journal_line_too_large", "system-watcher")
            .await?;
        Ok(true)
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
                            && let Some(systemd_event) = self.parse_systemd_entry(&entry, material)?
                            && let Some(tx) = systemd_tx.as_ref()
                        {
                            self.send_event(tx, systemd_event, "systemd_batch", material)
                                .await?;
                        }
                    }
                    Err(e) => {
                        let raw_line = String::from_utf8_lossy(line);
                        self.record_malformed_journal_line("historical", raw_line.as_ref(), &e);
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
            let end_cursor = Self::require_sync_end_cursor(entries_count, last_cursor)?;
            let sync_payload = JournalSyncPayload {
                sync_type: JournalSyncType::InitialImport,
                start_cursor: first_cursor,
                end_cursor,
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
                        let (cursor, unit, metadata_parse_error) =
                            match Self::parse_oversized_line_metadata(line.trim()) {
                                Ok((cursor, unit)) => (cursor, unit, None),
                                Err(error) => {
                                    self.record_malformed_journal_line(
                                        "oversized",
                                        line.trim(),
                                        &error,
                                    );
                                    ("unknown".to_string(), None, Some(error.to_string()))
                                }
                            };

                        match self
                            .route_oversized_line_to_dlq(
                                material,
                                &line,
                                &cursor,
                                unit.as_deref(),
                                metadata_parse_error.as_deref(),
                            )
                            .await
                        {
                            Ok(true) => {
                                warn!(
                                    line_bytes = line.len(),
                                    limit = self.max_line_bytes,
                                    cursor = %cursor,
                                    journal_unit = ?unit,
                                    reason = "journal_line_too_large",
                                    "Oversized journal line routed to DLQ"
                                );
                            }
                            Ok(false) => {
                                warn!(
                                    line_bytes = line.len(),
                                    limit = self.max_line_bytes,
                                    cursor = %cursor,
                                    journal_unit = ?unit,
                                    reason = "journal_line_too_large",
                                    "Oversized journal line skipped because no DLQ publisher is configured"
                                );
                            }
                            Err(err) => {
                                warn!(
                                    line_bytes = line.len(),
                                    limit = self.max_line_bytes,
                                    cursor = %cursor,
                                    journal_unit = ?unit,
                                    error = %err,
                                    reason = "journal_line_too_large",
                                    "Failed to route oversized journal line to DLQ"
                                );
                            }
                        }
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
                                        self.parse_systemd_entry(&entry, material)?
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
                                self.record_malformed_journal_line("follow", &line, &e);
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
        let cursor =
            Self::require_nonempty_entry_string_field(obj, "__CURSOR", "Journal entry")?;

        let timestamp_us = Self::parse_journal_timestamp_us(obj, &cursor)?;

        let message = privacy::engine()
            .process(
                &Self::require_entry_string_field(obj, "MESSAGE", "Journal entry")?,
                ProcessingContext::Journal,
            )
            .text
            .into_owned();

        let timestamp = Self::journal_timestamp_from_micros(timestamp_us, &cursor)?;

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
        let pid = Self::parse_optional_field(obj, "_PID", &cursor)?;
        let uid = Self::parse_optional_field(obj, "_UID", &cursor)?;
        let gid = Self::parse_optional_field(obj, "_GID", &cursor)?;
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
        let priority = Self::parse_optional_field(obj, "PRIORITY", &cursor)?;
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

        let timestamp_us_i64 = i64::try_from(timestamp_us).map_err(|error| {
            sinex_node_sdk::SinexError::processing(format!(
                "Journal entry {cursor} has out-of-range microsecond timestamp"
            ))
            .with_context("cursor", cursor.to_string())
            .with_context("timestamp_us", timestamp_us.to_string())
            .with_source(error)
        })?;

        let payload = JournalEntryPayload {
            cursor: cursor.to_string(),
            timestamp_us: timestamp_us_i64,
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
                timestamp_us: Microseconds::from_micros(timestamp_us_i64),
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
    ) -> NodeResult<Option<Event<JsonValue>>> {
        let Some(unit_name) = entry["_SYSTEMD_UNIT"].as_str() else {
            return Ok(None);
        };
        let obj = entry.as_object().ok_or_else(|| {
            sinex_node_sdk::SinexError::processing("Invalid systemd journal entry".to_string())
        })?;
        let message =
            Self::require_entry_string_field(obj, "MESSAGE", "Systemd journal entry")?;
        let cursor =
            Self::require_nonempty_entry_string_field(obj, "__CURSOR", "Systemd journal entry")?;

        // Filter by tracked units if configured
        if !self.systemd_units.is_empty() && !self.systemd_units.contains(unit_name) {
            return Ok(None);
        }

        let timestamp_us = Self::parse_realtime_timestamp_us(entry, unit_name)?;
        let ts_orig = Self::timestamp_from_micros(timestamp_us, unit_name)?;

        // Helper to construct deterministic ID
        // timestamp (48 bits) | entropy (80 bits)
        let id_entropy = Self::calculate_entropy(&cursor, 1);
        let timestamp_ms = timestamp_us / 1000;
        let id_val = u128::from(timestamp_ms) << 80 | (id_entropy & 0xFFFF_FFFF_FFFF_FFFF_FFFF);
        let uuid = uuid::Uuid::from_bytes(id_val.to_be_bytes());

        // Note: We create typed IDs inside each branch to satisfy type inference

        // Construct payload based on classified systemd event kind
        let Some(event_kind) = classify_systemd_event(entry, &message) else {
            return Ok(None);
        };
        let event = match event_kind {
            SystemdEventKind::Started => {
                let unit_type = convert_unit_type(SystemdUnitType::from_unit_name(unit_name));
                let main_pid = entry["_PID"]
                    .as_str()
                    .and_then(|s| s.parse::<u32>().ok())
                    .map(ProcessId::from_raw);
                let e = Event::new(
                    SystemdUnitStartedPayload {
                        unit_name: unit_name.to_string(),
                        unit_type,
                        main_pid,
                        active_state: CoreSystemdActiveState::Active,
                        sub_state: "running".to_string(),
                    },
                    material.initial_provenance(),
                );
                Self::serialize_systemd_event(e, uuid, ts_orig)?
            }
            SystemdEventKind::Stopped => {
                let unit_type = convert_unit_type(SystemdUnitType::from_unit_name(unit_name));
                let e = Event::new(
                    SystemdUnitStoppedPayload {
                        unit_name: unit_name.to_string(),
                        unit_type,
                        exit_code: None,
                        active_state: CoreSystemdActiveState::Inactive,
                        sub_state: "dead".to_string(),
                    },
                    material.initial_provenance(),
                );
                Self::serialize_systemd_event(e, uuid, ts_orig)?
            }
            SystemdEventKind::Failed => {
                let e = Event::new(
                    SystemdUnitFailedPayload {
                        unit_name: unit_name.to_string(),
                        message: message.to_string(),
                        cursor: cursor.to_string(),
                        pid: entry["_PID"].as_str().map(String::from),
                        uid: entry["_UID"].as_str().map(String::from),
                        timestamp: ts_orig,
                        journal_timestamp: Some(ts_orig),
                    },
                    material.initial_provenance(),
                );
                Self::serialize_systemd_event(e, uuid, ts_orig)?
            }
            SystemdEventKind::Reloaded => {
                let e = Event::new(
                    SystemdUnitReloadedPayload {
                        unit_name: Some(unit_name.to_string()),
                        message: message.to_string(),
                        cursor: cursor.to_string(),
                        pid: entry["_PID"].as_str().map(String::from),
                        uid: entry["_UID"].as_str().map(String::from),
                        timestamp: ts_orig,
                        journal_timestamp: Some(ts_orig),
                    },
                    material.initial_provenance(),
                );
                Self::serialize_systemd_event(e, uuid, ts_orig)?
            }
            SystemdEventKind::Triggered => {
                let e = Event::new(
                    SystemdTimerTriggeredPayload {
                        unit_name: Some(unit_name.to_string()),
                        message: message.to_string(),
                        cursor: cursor.to_string(),
                        pid: entry["_PID"].as_str().map(String::from),
                        uid: entry["_UID"].as_str().map(String::from),
                        timestamp: ts_orig,
                        journal_timestamp: Some(ts_orig),
                    },
                    material.initial_provenance(),
                );
                Self::serialize_systemd_event(e, uuid, ts_orig)?
            }
        };

        Ok(Some(event))
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
                tokio::fs::create_dir_all(parent).await.map_err(|error| {
                    sinex_node_sdk::SinexError::processing(format!(
                        "Failed to create cursor directory {}",
                        parent
                    ))
                    .with_source(error)
                })?;
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
        } else {
            self.record_event();
        }
        Ok(())
    }

    /// Update event tracking.
    /// Reserved for metrics and diagnostics integration.
    fn record_event(&self) {
        if let Ok(mut last_event) = self.last_event_time.lock() {
            *last_event = Some(Instant::now());
        }
        self.event_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an error.
    /// Reserved for metrics and diagnostics integration.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::material_context::MaterialContext;
    use async_trait::async_trait;
    use sinex_primitives::events::Provenance;
    use sinex_primitives::{Id, JsonValue};
    use xtask::sandbox::prelude::*;

    #[derive(Debug)]
    struct TestMaterialContext;

    #[async_trait]
    impl MaterialContext for TestMaterialContext {
        fn initial_provenance(&self) -> Provenance {
            Provenance::Material {
                id: Id::new(),
                anchor_byte: 0,
                offset_start: None,
                offset_end: None,
                offset_kind: sinex_primitives::events::OffsetKind::Byte,
            }
        }

        async fn decorate_event(&self, _event: &mut Event<JsonValue>) -> NodeResult<()> {
            Ok(())
        }

        async fn finalize(&self, _reason: &str) -> NodeResult<()> {
            Ok(())
        }

        fn event_count(&self) -> u64 {
            0
        }
    }

    fn test_watcher() -> UnifiedJournalWatcher {
        UnifiedJournalWatcher {
            journal_config: JournalConfig::default(),
            systemd_enabled: true,
            systemd_units: HashSet::new(),
            last_cursor: None,
            max_line_bytes: DEFAULT_MAX_JOURNAL_LINE_BYTES,
            cancel_token: CancellationToken::new(),
            last_event_time: Arc::new(Mutex::new(None)),
            event_count: Arc::new(AtomicU64::new(0)),
            last_error: Arc::new(Mutex::new(None)),
            child_process: None,
            pending_cursor: Arc::new(Mutex::new(None)),
            cursor_save_count: Arc::new(AtomicU64::new(0)),
            last_cursor_save: Arc::new(Mutex::new(Instant::now())),
            channel_drops: Arc::new(AtomicU64::new(0)),
            dlq_publisher: None,
        }
    }

    fn test_material() -> WatcherMaterialContext {
        Arc::new(TestMaterialContext)
    }

    #[sinex_test]
    async fn parse_journal_entry_rejects_invalid_timestamp(ctx: TestContext) -> TestResult<()> {
        let _ = ctx;
        let watcher = test_watcher();
        let material = test_material();
        let entry = json!({
            "MESSAGE": "hello from the journal",
            "__CURSOR": "s=abc;i=1;b=boot;m=1;t=1;x=1",
            "__REALTIME_TIMESTAMP": "not-a-timestamp",
        });

        let error = watcher
            .parse_journal_entry(&entry, &material)
            .expect_err("invalid journal timestamps must fail honestly");

        assert!(error.to_string().contains("invalid __REALTIME_TIMESTAMP"));
        assert!(error.to_string().contains("s=abc;i=1;b=boot;m=1;t=1;x=1"));
        Ok(())
    }

    #[sinex_test]
    async fn parse_journal_entry_rejects_invalid_pid(ctx: TestContext) -> TestResult<()> {
        let _ = ctx;
        let watcher = test_watcher();
        let material = test_material();
        let entry = json!({
            "MESSAGE": "hello from the journal",
            "__CURSOR": "s=abc;i=1;b=boot;m=1;t=1;x=1",
            "__REALTIME_TIMESTAMP": "1710000000000000",
            "_PID": "not-a-pid",
        });

        let error = watcher
            .parse_journal_entry(&entry, &material)
            .expect_err("invalid journal pid must fail honestly");

        assert!(error.to_string().contains("invalid _PID"));
        assert!(error.to_string().contains("s=abc;i=1;b=boot;m=1;t=1;x=1"));
        Ok(())
    }

    #[sinex_test]
    async fn parse_journal_entry_preserves_journal_timestamp(ctx: TestContext) -> TestResult<()> {
        let _ = ctx;
        let watcher = test_watcher();
        let material = test_material();
        let entry = json!({
            "MESSAGE": "hello from the journal",
            "__CURSOR": "s=abc;i=1;b=boot;m=1;t=1;x=1",
            "__REALTIME_TIMESTAMP": "1710000000000000",
            "_PID": "123",
        });

        let event = watcher
            .parse_journal_entry(&entry, &material)?
            .expect("valid journal entry should produce an event");

        let expected =
            Timestamp::from_unix_timestamp_nanos(1_710_000_000_000_000_000).expect("valid timestamp");
        assert_eq!(event.ts_orig, Some(expected));
        Ok(())
    }

    #[sinex_test]
    async fn parse_journal_entry_rejects_missing_message(ctx: TestContext) -> TestResult<()> {
        let _ = ctx;
        let watcher = test_watcher();
        let material = test_material();
        let entry = json!({
            "__CURSOR": "s=abc;i=1;b=boot;m=1;t=1;x=1",
            "__REALTIME_TIMESTAMP": "1710000000000000",
        });

        let error = watcher
            .parse_journal_entry(&entry, &material)
            .expect_err("missing journal MESSAGE must fail honestly");

        assert!(error.to_string().contains("missing required MESSAGE"));
        Ok(())
    }

    #[sinex_test]
    async fn parse_systemd_entry_rejects_invalid_timestamp(ctx: TestContext) -> TestResult<()> {
        let _ = ctx;
        let watcher = test_watcher();
        let material = test_material();
        let entry = json!({
            "MESSAGE": "Started test.service",
            "_SYSTEMD_UNIT": "test.service",
            "__CURSOR": "s=abc",
            "__REALTIME_TIMESTAMP": "not-a-timestamp",
        });

        let error = watcher
            .parse_systemd_entry(&entry, &material)
            .expect_err("invalid systemd timestamps must fail honestly");

        assert!(error.to_string().contains("invalid __REALTIME_TIMESTAMP"));
        assert!(error.to_string().contains("test.service"));
        Ok(())
    }

    #[sinex_test]
    async fn parse_systemd_entry_rejects_missing_cursor(ctx: TestContext) -> TestResult<()> {
        let _ = ctx;
        let watcher = test_watcher();
        let material = test_material();
        let entry = json!({
            "MESSAGE": "Started test.service",
            "_SYSTEMD_UNIT": "test.service",
            "__REALTIME_TIMESTAMP": "1710000000000000",
        });

        let error = watcher
            .parse_systemd_entry(&entry, &material)
            .expect_err("missing systemd cursor must fail honestly");

        assert!(error.to_string().contains("missing required __CURSOR"));
        Ok(())
    }

    #[sinex_test]
    async fn parse_systemd_entry_preserves_journal_timestamp(ctx: TestContext) -> TestResult<()> {
        let _ = ctx;
        let watcher = test_watcher();
        let material = test_material();
        let entry = json!({
            "MESSAGE": "Started test.service",
            "_SYSTEMD_UNIT": "test.service",
            "__CURSOR": "s=abc",
            "__REALTIME_TIMESTAMP": "1710000000000000",
            "_PID": "123",
        });

        let event = watcher
            .parse_systemd_entry(&entry, &material)?
            .expect("matching systemd entry should produce an event");

        let expected =
            Timestamp::from_unix_timestamp_nanos(1_710_000_000_000_000_000).expect("valid timestamp");
        assert_eq!(event.ts_orig, Some(expected));
        Ok(())
    }

    #[sinex_test]
    async fn parse_oversized_line_metadata_preserves_cursor_and_unit(
        ctx: TestContext,
    ) -> TestResult<()> {
        let _ = ctx;
        let (cursor, unit) = UnifiedJournalWatcher::parse_oversized_line_metadata(
            r#"{"__CURSOR":"s=abc","_SYSTEMD_UNIT":"test.service"}"#,
        )?;

        assert_eq!(cursor, "s=abc");
        assert_eq!(unit.as_deref(), Some("test.service"));
        Ok(())
    }

    #[sinex_test]
    async fn require_sync_end_cursor_rejects_missing_cursor_after_entries(
        ctx: TestContext,
    ) -> TestResult<()> {
        let _ = ctx;
        let error = UnifiedJournalWatcher::require_sync_end_cursor(3, None)
            .expect_err("sync events must not fabricate an empty end cursor");

        assert!(error
            .to_string()
            .contains("processed 3 entries without a terminal cursor"));
        Ok(())
    }

    #[sinex_test]
    async fn flush_cursor_reports_parent_directory_creation_failure(
        ctx: TestContext,
    ) -> TestResult<()> {
        let _ = ctx;
        let mut watcher = test_watcher();
        let runtime_dir = std::env::temp_dir().join(format!("sinex-journal-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&runtime_dir)?;
        let blocker = runtime_dir.join("blocker");
        std::fs::write(&blocker, b"not-a-directory")?;
        watcher.journal_config.cursor_file =
            Some(blocker.join("cursor.state").to_string_lossy().into_owned());
        watcher
            .pending_cursor
            .lock()
            .expect("pending cursor lock should not be poisoned")
            .replace("s=abc".to_string());

        let error = watcher
            .flush_cursor()
            .await
            .expect_err("directory creation failures must surface honestly");

        assert!(error
            .to_string()
            .contains("Failed to create cursor directory"));

        let _ = std::fs::remove_dir_all(&runtime_dir);
        Ok(())
    }

    #[sinex_test]
    async fn send_event_updates_health_snapshot(ctx: TestContext) -> TestResult<()> {
        let _ = ctx;
        let watcher = test_watcher();
        let material = test_material();
        let (tx, mut rx) = mpsc::channel(1);
        let event = Event::new_json(
            "system-watcher",
            "journal.entry.written",
            json!({"cursor": "s=abc"}),
            material.initial_provenance(),
        );

        watcher.send_event(&tx, event, "test_send", &material).await?;
        let _received = rx.recv().await.expect("event should reach the channel");

        let snapshot = watcher.health_snapshot();
        assert_eq!(snapshot.events_processed, 1);
        assert!(snapshot.last_event.is_some());
        Ok(())
    }
}
