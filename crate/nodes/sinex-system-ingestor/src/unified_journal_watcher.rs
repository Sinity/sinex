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
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use parking_lot::Mutex;
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
    SinexError,
    events::enums::{
        JournalSyncType, SystemdActiveState as CoreSystemdActiveState,
        SystemdUnitType as CoreSystemdUnitType,
    },
    units::{Microseconds, ProcessId, SyslogPriority, UnixGid, UnixUid},
};
use std::collections::{HashMap, HashSet};
use std::process::ExitStatus;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
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
const JOURNAL_STDERR_PREVIEW_LIMIT: usize = 512;
const GRACEFUL_CHILD_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Required keys in a systemd journal cursor string.
/// Format: `s=<hex>;i=<hex>;b=<boot_id>;m=<monotonic>;t=<realtime>;x=<xor_hash>`
const CURSOR_REQUIRED_KEYS: &[&str] = &["s", "i", "b", "m", "t", "x"];

#[derive(Debug, Clone, Copy)]
enum FollowExitReason {
    Shutdown,
    UnexpectedEof,
    ReadError,
}

struct StreamingActivityGuard {
    active: Arc<AtomicBool>,
}

impl Drop for StreamingActivityGuard {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Relaxed);
    }
}

fn stderr_preview(stderr: &str) -> Option<String> {
    let trimmed = stderr.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut preview = trimmed
        .chars()
        .take(JOURNAL_STDERR_PREVIEW_LIMIT)
        .collect::<String>();
    if trimmed.chars().count() > JOURNAL_STDERR_PREVIEW_LIMIT {
        preview.push('…');
    }
    Some(preview)
}

fn check_follow_exit_status(
    exit_reason: FollowExitReason,
    status: ExitStatus,
    stderr: Option<&str>,
) -> NodeResult<()> {
    let error = match exit_reason {
        FollowExitReason::Shutdown => return Ok(()),
        FollowExitReason::UnexpectedEof => {
            SinexError::processing("journalctl follow stream ended unexpectedly")
                .with_context("exit_status", status.to_string())
        }
        FollowExitReason::ReadError => {
            SinexError::processing("journalctl follow stream terminated after a read failure")
                .with_context("exit_status", status.to_string())
        }
    };

    Err(if let Some(stderr) = stderr.and_then(stderr_preview) {
        error.with_context("stderr", stderr)
    } else {
        error
    })
}

async fn read_child_stderr(child: &mut Child, process_name: &str) -> NodeResult<Option<String>> {
    let Some(mut stderr) = child.stderr.take() else {
        return Ok(None);
    };

    let mut bytes = Vec::new();
    stderr.read_to_end(&mut bytes).await.map_err(|error| {
        SinexError::io(format!("failed to read {process_name} stderr"))
            .with_std_error(&error)
    })?;

    Ok(Some(String::from_utf8_lossy(&bytes).into_owned()))
}

fn follow_exit_processing_error_message(exit_reason: FollowExitReason) -> &'static str {
    match exit_reason {
        FollowExitReason::Shutdown => "journalctl follow stream ended during shutdown",
        FollowExitReason::UnexpectedEof => "journalctl follow stream ended unexpectedly",
        FollowExitReason::ReadError => "journalctl follow stream terminated after a read failure",
    }
}

fn build_follow_wait_error(
    error: std::io::Error,
    exit_reason: FollowExitReason,
    stderr: Option<&str>,
    stderr_read_error: Option<&str>,
) -> SinexError {
    let mut wait_error = SinexError::io("Failed to wait for journal watcher process exit")
        .with_source(error)
        .with_context(
            "follow_exit_reason",
            follow_exit_processing_error_message(exit_reason),
        );
    if let Some(stderr) = stderr.and_then(stderr_preview) {
        wait_error = wait_error.with_context("stderr", stderr);
    }
    if let Some(stderr_read_error) = stderr_read_error {
        wait_error = wait_error.with_context("stderr_read_error", stderr_read_error.to_string());
    }
    wait_error
}

fn signal_child_terminate(child: &Child, process_name: &str) -> NodeResult<()> {
    let pid = child.id().ok_or_else(|| {
        SinexError::processing(format!(
            "{process_name} process has no PID for graceful shutdown"
        ))
    })?;
    let pid_raw = i32::try_from(pid).map_err(|error| {
        SinexError::processing(format!(
            "{process_name} process PID exceeds signalable range"
        ))
        .with_context("pid", pid.to_string())
        .with_std_error(&error)
    })?;
    signal::kill(Pid::from_raw(pid_raw), Signal::SIGTERM).map_err(|error| {
        SinexError::processing(format!(
            "failed to send SIGTERM to {process_name} process"
        ))
        .with_context("pid", pid.to_string())
        .with_std_error(&error)
    })
}

async fn shutdown_child_process(
    child: &mut Child,
    graceful: bool,
    graceful_timeout: Duration,
) -> NodeResult<ExitStatus> {
    if graceful {
        signal_child_terminate(child, "journal watcher")?;
        match tokio::time::timeout(graceful_timeout, child.wait()).await {
            Ok(Ok(status)) => {
                info!("Journal watcher process exited: {:?}", status);
                Ok(status)
            }
            Ok(Err(error)) => {
                Err(
                    SinexError::io("failed to wait for journal watcher process exit")
                        .with_std_error(&error),
                )
            }
            Err(_) => {
                warn!(
                    "Journal watcher process did not exit within {:?}, killing",
                    graceful_timeout
                );
                child.start_kill().map_err(|error| {
                    SinexError::io("failed to kill journal watcher process after shutdown timeout")
                        .with_std_error(&error)
                })?;
                child.wait().await.map_err(|error| {
                    SinexError::io("failed to reap journal watcher process after shutdown timeout")
                        .with_std_error(&error)
                })
            }
        }
    } else {
        child.start_kill().map_err(|error| {
            SinexError::io("failed to kill journal watcher process").with_std_error(&error)
        })?;
        child.wait().await.map_err(|error| {
            SinexError::io("failed to reap killed journal watcher process")
                .with_std_error(&error)
        })
    }
}

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

    let mut parts = HashSet::new();
    for segment in cursor.split(';') {
        let Some((key, value)) = segment.split_once('=') else {
            return false;
        };
        if key.is_empty() || value.is_empty() || !parts.insert(key) {
            return false;
        }
    }

    CURSOR_REQUIRED_KEYS
        .iter()
        .all(|key| parts.contains(key))
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
    streaming_active: Arc<AtomicBool>,
    child_process: Option<Child>,
    // Cursor batching state
    pending_cursor: Arc<StdMutex<Option<String>>>,
    cursor_save_count: Arc<AtomicU64>,
    last_cursor_save: Arc<StdMutex<Instant>>,
    // Backpressure metrics
    channel_drops: Arc<AtomicU64>,
    dlq_publisher: Option<Arc<NatsPublisher>>,
}

impl UnifiedJournalWatcher {
    fn begin_streaming(&self) -> StreamingActivityGuard {
        self.streaming_active.store(true, Ordering::Relaxed);
        StreamingActivityGuard {
            active: Arc::clone(&self.streaming_active),
        }
    }

    async fn load_last_cursor(cursor_file: &str) -> NodeResult<Option<String>> {
        match tokio::fs::read_to_string(cursor_file).await {
            Ok(contents) => {
                let trimmed = contents.trim().to_string();
                if validate_journal_cursor(&trimmed) {
                    Ok(Some(trimmed))
                } else {
                    Err(
                        SinexError::processing("Journal cursor file is invalid")
                            .with_context("cursor_file", cursor_file.to_string())
                            .with_context("cursor", trimmed),
                    )
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(
                SinexError::io("Failed to read journal cursor file")
                    .with_context("cursor_file", cursor_file.to_string())
                    .with_std_error(&error),
            ),
        }
    }

    fn resolve_max_line_bytes() -> NodeResult<usize> {
        match std::env::var("SINEX_JOURNAL_MAX_LINE_BYTES") {
            Ok(raw) => raw.parse::<usize>().map_err(|error| {
                sinex_node_sdk::SinexError::configuration(
                    "SINEX_JOURNAL_MAX_LINE_BYTES must be a positive integer".to_string(),
                )
                .with_context("env_var", "SINEX_JOURNAL_MAX_LINE_BYTES")
                .with_context("value", raw)
                .with_source(error)
            }),
            Err(std::env::VarError::NotPresent) => Ok(DEFAULT_MAX_JOURNAL_LINE_BYTES),
            Err(error) => Err(
                sinex_node_sdk::SinexError::configuration(
                    "Failed to read SINEX_JOURNAL_MAX_LINE_BYTES".to_string(),
                )
                .with_context("env_var", "SINEX_JOURNAL_MAX_LINE_BYTES")
                .with_source(error),
            ),
        }
    }

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
        let max_line_bytes = Self::resolve_max_line_bytes()?;

        info!("Journal max line size configured: {} bytes", max_line_bytes);

        // Load last cursor if cursor file exists, validating format before use.
        // A corrupt or unreadable cursor file would silently alter replay behavior,
        // so fail early instead of pretending a fresh start is equivalent.
        let last_cursor = if let Some(ref cursor_file) = journal_config.cursor_file {
            Self::load_last_cursor(cursor_file).await?
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
            streaming_active: Arc::new(AtomicBool::new(false)),
            child_process: None,
            pending_cursor: Arc::new(StdMutex::new(None)),
            cursor_save_count: Arc::new(AtomicU64::new(0)),
            last_cursor_save: Arc::new(StdMutex::new(Instant::now())),
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
    ) -> NodeResult<(String, Option<String>)> {
        let entry: serde_json::Value = serde_json::from_str(line).map_err(|error| {
            SinexError::processing("failed to parse oversized journal line metadata")
                .with_std_error(&error)
        })?;
        let obj = entry.as_object().ok_or_else(|| {
            SinexError::processing("oversized journal line metadata is not a JSON object")
        })?;
        let cursor = Self::require_nonempty_entry_string_field(
            obj,
            "__CURSOR",
            "Oversized journal line metadata",
        )?;
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

    fn record_invalid_oversized_line_metadata(&self, line: &str, error: &SinexError) {
        let (line_preview, line_preview_truncated) = Self::journal_line_preview(line);
        let message = format!("Failed to parse oversized journal line metadata: {error}");
        self.record_error(message);
        warn!(
            phase = "oversized",
            line_bytes = line.len(),
            line_preview = %line_preview,
            line_preview_truncated,
            error = %error,
            "Ignoring invalid oversized journal metadata"
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
        let _activity = self.begin_streaming();
        info!("Starting unified journal monitoring");

        // Import historical entries if configured
        if self.journal_config.import_on_startup {
            self.import_historical(&journal_tx, &systemd_tx, &material)
                .await
                .map_err(|error| {
                    self.record_error(format!(
                        "Failed to import historical journal entries: {error}"
                    ));
                    error.with_context("startup_phase", "historical_import".to_string())
                })?;
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
            let error = sinex_node_sdk::SinexError::processing(format!(
                "journalctl failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
            self.record_error(error.to_string());
            return Err(error);
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
            .map_err(|error| {
                SinexError::serialization(
                    "failed to serialize journal sync completion event",
                )
                .with_std_error(&error)
                .with_context("entries_count", entries_count.to_string())
            })?;
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
        let mut exit_reason = FollowExitReason::UnexpectedEof;
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
                    exit_reason = FollowExitReason::Shutdown;
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
                                    self.record_invalid_oversized_line_metadata(
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
                    self.record_error(format!("Error reading journal output: {e}"));
                    error!("Error reading journal output: {}", e);
                    exit_reason = FollowExitReason::ReadError;
                    break;
                }
            }
        }

        // Wait for child process
        if let Some(mut child) = self.child_process.take() {
            match child.wait().await {
                Ok(status) => {
                    let stderr = read_child_stderr(&mut child, "journal watcher").await?;
                    check_follow_exit_status(exit_reason, status, stderr.as_deref())?;
                }
                Err(error) => {
                    warn!(error = %error, "Failed to wait for journal watcher process exit");
                    if !matches!(exit_reason, FollowExitReason::Shutdown) {
                        let (stderr, stderr_read_error) =
                            match read_child_stderr(&mut child, "journal watcher").await {
                                Ok(stderr) => (stderr, None),
                                Err(stderr_error) => (None, Some(format!("{stderr_error:#}"))),
                            };
                        let message = if let Some(stderr_read_error) = &stderr_read_error {
                            format!(
                                "Failed to wait for journal watcher process exit: {error}; failed to read stderr: {stderr_read_error}"
                            )
                        } else {
                            format!("Failed to wait for journal watcher process exit: {error}")
                        };
                        self.record_error(message);
                        return Err(build_follow_wait_error(
                            error,
                            exit_reason,
                            stderr.as_deref(),
                            stderr_read_error.as_deref(),
                        ));
                    }
                }
            }
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
                let main_pid =
                    Self::parse_optional_field::<u32>(obj, "_PID", &cursor)?.map(ProcessId::from_raw);
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

    fn lock_pending_cursor(&self) -> NodeResult<std::sync::MutexGuard<'_, Option<String>>> {
        self.pending_cursor.lock().map_err(|error| {
            sinex_node_sdk::SinexError::processing(
                "Journal cursor state lock was poisoned".to_string(),
            )
            .with_context("lock", "pending_cursor")
            .with_source(error.to_string())
        })
    }

    fn lock_last_cursor_save(&self) -> NodeResult<std::sync::MutexGuard<'_, Instant>> {
        self.last_cursor_save.lock().map_err(|error| {
            sinex_node_sdk::SinexError::processing(
                "Journal cursor state lock was poisoned".to_string(),
            )
            .with_context("lock", "last_cursor_save")
            .with_source(error.to_string())
        })
    }

    /// Save cursor to file for position tracking (batched)
    /// Saves based on configured event threshold and interval (defaults: 100 events or 10 seconds)
    async fn save_cursor(&self, cursor: &str) -> NodeResult<()> {
        // Update pending cursor
        *self.lock_pending_cursor()? = Some(cursor.to_string());

        // Increment cursor save counter
        let count = self.cursor_save_count.fetch_add(1, Ordering::Relaxed) + 1;

        // Check if we should flush
        let should_flush = {
            let elapsed = self.lock_last_cursor_save()?.elapsed();

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
        let cursor_to_save = self.lock_pending_cursor()?.take();

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
            *self.lock_last_cursor_save()? = Instant::now();

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
            let error_message = format!("system journal event channel closed while sending {context}: {err}");
            self.record_error(error_message.clone());
            if drops == 1 || drops == 10 || drops == 100 || drops.is_multiple_of(1000) {
                warn!(
                    send_failures = drops,
                    context = context,
                    error = %err,
                    "System journal event channel closed"
                );
            }
            return Err(
                SinexError::processing("system journal event channel closed")
                    .with_context("context", context.to_string())
                    .with_context("detail", error_message)
                    .with_std_error(&err),
            );
        }
        self.record_event();
        Ok(())
    }

    /// Update event tracking.
    /// Reserved for metrics and diagnostics integration.
    fn record_event(&self) {
        *self.last_event_time.lock() = Some(Instant::now());
        self.event_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an error.
    /// Reserved for metrics and diagnostics integration.
    fn record_error(&self, error: String) {
        *self.last_error.lock() = Some(error);
    }
}

#[async_trait::async_trait]
impl WatcherLifecycle for UnifiedJournalWatcher {
    fn health_snapshot(&self) -> WatcherActivitySnapshot {
        let last_event = *self.last_event_time.lock();
        let last_error = self.last_error.lock().clone();

        WatcherActivitySnapshot {
            active: self.streaming_active.load(Ordering::Relaxed)
                && !self.cancel_token.is_cancelled(),
            last_event,
            last_error,
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
        if graceful {
            self.flush_cursor().await?;
        }

        // Kill the child process
        if let Some(ref mut child) = self.child_process {
            let status =
                shutdown_child_process(child, graceful, GRACEFUL_CHILD_SHUTDOWN_TIMEOUT).await?;
            debug!(?status, graceful, "Journal watcher child shutdown completed");
        }

        Ok(())
    }

    fn last_event_timestamp(&self) -> Option<Instant> {
        *self.last_event_time.lock()
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
    use std::os::unix::process::ExitStatusExt;
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
            streaming_active: Arc::new(AtomicBool::new(false)),
            child_process: None,
            pending_cursor: Arc::new(StdMutex::new(None)),
            cursor_save_count: Arc::new(AtomicU64::new(0)),
            last_cursor_save: Arc::new(StdMutex::new(Instant::now())),
            channel_drops: Arc::new(AtomicU64::new(0)),
            dlq_publisher: None,
        }
    }

    fn test_material() -> WatcherMaterialContext {
        Arc::new(TestMaterialContext)
    }

    struct EnvVarGuard {
        key: &'static str,
        original: Option<String>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let original = std::env::var(key).ok();
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, original }
        }

        fn unset(key: &'static str) -> Self {
            let original = std::env::var(key).ok();
            unsafe {
                std::env::remove_var(key);
            }
            Self { key, original }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match self.original.as_deref() {
                Some(value) => unsafe {
                    std::env::set_var(self.key, value);
                },
                None => unsafe {
                    std::env::remove_var(self.key);
                },
            }
        }
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
    async fn resolve_max_line_bytes_rejects_invalid_env(ctx: TestContext) -> TestResult<()> {
        let _ = ctx;
        let _guard = EnvVarGuard::set("SINEX_JOURNAL_MAX_LINE_BYTES", "not-a-number");

        let error = UnifiedJournalWatcher::resolve_max_line_bytes()
            .expect_err("invalid journal max line env must fail honestly");

        assert!(
            error
                .to_string()
                .contains("SINEX_JOURNAL_MAX_LINE_BYTES must be a positive integer")
        );
        assert!(error.to_string().contains("not-a-number"));
        Ok(())
    }

    #[sinex_test]
    async fn resolve_max_line_bytes_uses_default_when_env_is_absent(
        ctx: TestContext,
    ) -> TestResult<()> {
        let _ = ctx;
        let _guard = EnvVarGuard::unset("SINEX_JOURNAL_MAX_LINE_BYTES");

        assert_eq!(
            UnifiedJournalWatcher::resolve_max_line_bytes()?,
            DEFAULT_MAX_JOURNAL_LINE_BYTES
        );
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
    async fn parse_systemd_entry_rejects_invalid_pid(ctx: TestContext) -> TestResult<()> {
        let _ = ctx;
        let watcher = test_watcher();
        let material = test_material();
        let entry = json!({
            "MESSAGE": "Started test.service",
            "_SYSTEMD_UNIT": "test.service",
            "__CURSOR": "s=abc",
            "__REALTIME_TIMESTAMP": "1710000000000000",
            "_PID": "not-a-pid",
        });

        let error = watcher
            .parse_systemd_entry(&entry, &material)
            .expect_err("invalid systemd pid must fail honestly");

        assert!(error.to_string().contains("invalid _PID"));
        assert!(error.to_string().contains("not-a-pid"));
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
    async fn parse_oversized_line_metadata_rejects_missing_cursor(
        ctx: TestContext,
    ) -> TestResult<()> {
        let _ = ctx;
        let error = UnifiedJournalWatcher::parse_oversized_line_metadata(
            r#"{"_SYSTEMD_UNIT":"test.service"}"#,
        )
        .expect_err("oversized journal metadata must not fabricate a cursor");

        assert!(error.to_string().contains("missing required __CURSOR"));
        Ok(())
    }

    #[sinex_test]
    async fn graceful_shutdown_signals_journalctl_before_waiting(
        ctx: TestContext,
    ) -> TestResult<()> {
        let _ = ctx;
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("while :; do :; done")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()?;

        let status = shutdown_child_process(&mut child, true, Duration::from_millis(500)).await?;

        assert_eq!(status.signal(), Some(libc::SIGTERM));
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
    async fn load_last_cursor_rejects_invalid_cursor_file(ctx: TestContext) -> TestResult<()> {
        let _ = ctx;
        let runtime_dir = std::env::temp_dir().join(format!("sinex-journal-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&runtime_dir)?;
        let cursor_file = runtime_dir.join("cursor.state");
        std::fs::write(&cursor_file, b"not-a-cursor")?;

        let error = UnifiedJournalWatcher::load_last_cursor(
            cursor_file
                .to_str()
                .expect("cursor file should be valid UTF-8"),
        )
        .await
        .expect_err("invalid cursor files must not silently trigger a fresh replay");

        assert!(error.to_string().contains("cursor file is invalid"));
        let _ = std::fs::remove_dir_all(&runtime_dir);
        Ok(())
    }

    #[sinex_test]
    async fn validate_journal_cursor_rejects_malformed_segments(ctx: TestContext) -> TestResult<()> {
        let _ = ctx;

        assert!(!validate_journal_cursor("s=abc;i=1;b=boot;m=1;t=1;x=1;garbage"));
        assert!(!validate_journal_cursor("s=abc;i=1;b=boot;m=1;t=1"));
        assert!(!validate_journal_cursor("s=abc;i=1;b=boot;m=1;t=1;x="));
        Ok(())
    }

    #[sinex_test]
    async fn validate_journal_cursor_rejects_duplicate_keys(ctx: TestContext) -> TestResult<()> {
        let _ = ctx;

        assert!(!validate_journal_cursor(
            "s=abc;i=1;b=boot;m=1;t=1;x=1;s=duplicate"
        ));
        Ok(())
    }

    #[sinex_test]
    async fn load_last_cursor_rejects_unreadable_cursor_path(ctx: TestContext) -> TestResult<()> {
        let _ = ctx;
        let runtime_dir = std::env::temp_dir().join(format!("sinex-journal-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&runtime_dir)?;
        let cursor_dir = runtime_dir.join("cursor.state");
        std::fs::create_dir(&cursor_dir)?;

        let error = UnifiedJournalWatcher::load_last_cursor(
            cursor_dir
                .to_str()
                .expect("cursor directory should be valid UTF-8"),
        )
        .await
        .expect_err("unreadable cursor paths must not silently trigger a fresh replay");

        assert!(error.to_string().contains("Failed to read journal cursor file"));
        let _ = std::fs::remove_dir_all(&runtime_dir);
        Ok(())
    }

    #[sinex_test]
    async fn save_cursor_rejects_poisoned_pending_cursor_lock(
        ctx: TestContext,
    ) -> TestResult<()> {
        let _ = ctx;
        let watcher = test_watcher();
        poison_mutex(Arc::clone(&watcher.pending_cursor), None::<String>);

        let error = watcher
            .save_cursor("s=abc")
            .await
            .expect_err("poisoned pending cursor lock must surface as an error");

        assert!(error.to_string().contains("pending_cursor"));
        Ok(())
    }

    #[sinex_test]
    async fn save_cursor_rejects_poisoned_last_cursor_save_lock(
        ctx: TestContext,
    ) -> TestResult<()> {
        let _ = ctx;
        let watcher = test_watcher();
        poison_mutex(Arc::clone(&watcher.last_cursor_save), Instant::now());

        let error = watcher
            .save_cursor("s=abc")
            .await
            .expect_err("poisoned cursor timing lock must surface as an error");

        assert!(error.to_string().contains("last_cursor_save"));
        Ok(())
    }

    #[sinex_test]
    async fn flush_cursor_rejects_poisoned_pending_cursor_lock(
        ctx: TestContext,
    ) -> TestResult<()> {
        let _ = ctx;
        let watcher = test_watcher();
        poison_mutex(Arc::clone(&watcher.pending_cursor), Some("s=abc".to_string()));

        let error = watcher
            .flush_cursor()
            .await
            .expect_err("poisoned pending cursor lock must not be ignored during flush");

        assert!(error.to_string().contains("pending_cursor"));
        Ok(())
    }

    #[sinex_test]
    async fn flush_cursor_rejects_poisoned_last_cursor_save_lock(
        ctx: TestContext,
    ) -> TestResult<()> {
        let _ = ctx;
        let mut watcher = test_watcher();
        let runtime_dir = std::env::temp_dir().join(format!("sinex-journal-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&runtime_dir)?;
        watcher.journal_config.cursor_file =
            Some(runtime_dir.join("cursor.state").to_string_lossy().into_owned());
        watcher
            .pending_cursor
            .lock()
            .expect("pending cursor lock should not be poisoned")
            .replace("s=abc".to_string());
        poison_mutex(Arc::clone(&watcher.last_cursor_save), Instant::now());

        let error = watcher
            .flush_cursor()
            .await
            .expect_err("poisoned cursor timing lock must not be ignored during flush");

        assert!(error.to_string().contains("last_cursor_save"));
        let _ = std::fs::remove_dir_all(&runtime_dir);
        Ok(())
    }

    #[sinex_test]
    async fn graceful_shutdown_propagates_cursor_flush_failure(
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
            .shutdown(true)
            .await
            .expect_err("graceful shutdown must surface cursor flush failures");

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
        watcher.streaming_active.store(true, Ordering::Relaxed);
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
        assert!(snapshot.active);
        assert_eq!(snapshot.events_processed, 1);
        assert!(snapshot.last_event.is_some());
        Ok(())
    }

    #[sinex_test]
    async fn send_event_rejects_closed_channel(ctx: TestContext) -> TestResult<()> {
        let _ = ctx;
        let watcher = test_watcher();
        watcher.streaming_active.store(true, Ordering::Relaxed);
        let material = test_material();
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let event = Event::new_json(
            "system-watcher",
            "journal.entry.written",
            json!({"cursor": "s=closed"}),
            material.initial_provenance(),
        );

        let error = watcher
            .send_event(&tx, event, "test_closed_send", &material)
            .await
            .expect_err("closed journal event channels must fail honestly");

        assert!(error.to_string().contains("system journal event channel closed"));
        assert!(error.to_string().contains("test_closed_send"));

        let snapshot = watcher.health_snapshot();
        assert_eq!(snapshot.events_processed, 0);
        assert!(snapshot.last_error.is_some());
        Ok(())
    }

    #[sinex_test]
    async fn health_snapshot_requires_live_streaming_activity(ctx: TestContext) -> TestResult<()> {
        let _ = ctx;
        let watcher = test_watcher();

        watcher.streaming_active.store(true, Ordering::Relaxed);
        assert!(
            watcher.health_snapshot().active,
            "streaming watcher should report active"
        );

        watcher.streaming_active.store(false, Ordering::Relaxed);
        assert!(
            !watcher.health_snapshot().active,
            "stopped watcher must not stay active just because shutdown was not requested"
        );

        Ok(())
    }

    #[sinex_test]
    async fn start_streaming_surfaces_historical_import_failures(
        ctx: TestContext,
    ) -> TestResult<()> {
        let _ = ctx;
        let _path = EnvVarGuard::set("PATH", "");
        let mut watcher = test_watcher();
        watcher.journal_config.import_on_startup = true;
        watcher.journal_config.follow = false;
        let material = test_material();
        let (journal_tx, _journal_rx) = mpsc::channel(1);

        let error = watcher
            .start_streaming(journal_tx, None, material)
            .await
            .expect_err("historical import failure must abort startup");

        assert!(error.to_string().contains("Failed to run journalctl"));
        assert!(error.to_string().contains("historical_import"));
        let snapshot = watcher.health_snapshot();
        assert!(
            snapshot
                .last_error
                .as_deref()
                .is_some_and(|value| value.contains("Failed to import historical journal entries"))
        );
        assert!(
            !snapshot.active,
            "failed startup must not leave the watcher marked active"
        );

        Ok(())
    }

    fn poison_mutex<T: Send + 'static>(mutex: Arc<StdMutex<T>>, value: T) {
        let result = std::thread::spawn(move || {
            let mut guard = mutex.lock().expect("test mutex should lock before poisoning");
            *guard = value;
            panic!("poison mutex for regression coverage");
        })
        .join();
        assert!(result.is_err(), "poisoning thread should panic");
    }

    #[sinex_test]
    async fn check_follow_exit_status_rejects_unexpected_eof(ctx: TestContext) -> TestResult<()> {
        let _ = ctx;
        let status = std::process::Command::new("sh")
            .arg("-c")
            .arg("exit 0")
            .status()?;

        let error = check_follow_exit_status(FollowExitReason::UnexpectedEof, status, None)
            .expect_err("unexpected EOF must not be treated as healthy shutdown");
        assert!(error.to_string().contains("ended unexpectedly"));
        Ok(())
    }

    #[sinex_test]
    async fn check_follow_exit_status_rejects_read_failure(ctx: TestContext) -> TestResult<()> {
        let _ = ctx;
        let status = std::process::Command::new("sh")
            .arg("-c")
            .arg("exit 17")
            .status()?;

        let error = check_follow_exit_status(FollowExitReason::ReadError, status, None)
            .expect_err("read failures must surface even after the child exits");
        assert!(error.to_string().contains("read failure"));
        assert!(error.to_string().contains("17"));
        Ok(())
    }

    #[sinex_test]
    async fn check_follow_exit_status_preserves_stderr_excerpt(ctx: TestContext) -> TestResult<()> {
        let _ = ctx;
        let status = std::process::Command::new("sh")
            .arg("-c")
            .arg("exit 5")
            .status()?;

        let error = check_follow_exit_status(
            FollowExitReason::UnexpectedEof,
            status,
            Some("permission denied while opening journal"),
        )
        .expect_err("stderr should remain attached to follow failures");
        let rendered = format!("{error:#}");
        assert!(rendered.contains("permission denied while opening journal"));
        Ok(())
    }

    #[sinex_test]
    async fn build_follow_wait_error_preserves_stderr_read_failure_context(
        ctx: TestContext,
    ) -> TestResult<()> {
        let _ = ctx;

        let error = build_follow_wait_error(
            std::io::Error::other("broken wait"),
            FollowExitReason::UnexpectedEof,
            None,
            Some("failed to read journal watcher stderr: permission denied"),
        );
        let rendered = format!("{error:#}");

        assert!(rendered.contains("broken wait"));
        assert!(rendered.contains("journalctl follow stream ended unexpectedly"));
        assert!(rendered.contains("stderr_read_error"));
        assert!(rendered.contains("failed to read journal watcher stderr: permission denied"));
        Ok(())
    }

    #[sinex_test]
    async fn check_follow_exit_status_allows_shutdown_exit(ctx: TestContext) -> TestResult<()> {
        let _ = ctx;
        let status = std::process::Command::new("sh")
            .arg("-c")
            .arg("exit 143")
            .status()?;

        check_follow_exit_status(FollowExitReason::Shutdown, status, None)?;
        Ok(())
    }
}
