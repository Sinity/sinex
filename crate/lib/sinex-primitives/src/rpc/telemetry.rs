//! Telemetry RPC request/response types
//!
//! These types map to the `sinex_telemetry.*` read models exposed by the
//! gateway under the `telemetry.*` method namespace.

use serde::{Deserialize, Serialize};

// ─────────────────────────────────────────────────────────────
// Shared time-range parameters
// ─────────────────────────────────────────────────────────────

/// Optional time-range filter embedded in telemetry requests.
///
/// Both fields are RFC 3339 strings (e.g. `"2026-03-17T00:00:00Z"`).
/// When omitted, each handler applies its own default lookback window.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelemetryTimeRange {
    /// Start of the time range (inclusive).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// End of the time range (inclusive).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
}

// ─────────────────────────────────────────────────────────────
// telemetry.window_focus
// ─────────────────────────────────────────────────────────────

/// Request: `telemetry.window_focus`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelemetryWindowFocusRequest {
    #[serde(flatten)]
    pub time_range: TelemetryTimeRange,
    /// Maximum number of buckets to return (default: 50).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
}

/// A single 5-minute window-focus aggregate bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowFocusBucket {
    /// Bucket start timestamp (RFC 3339).
    pub bucket: String,
    /// Workspace associated with the focus bucket.
    pub workspace: Option<String>,
    /// Most recently focused window class in this bucket.
    pub window_class: Option<String>,
    /// Most recently focused window title in this bucket.
    pub window_title: Option<String>,
    /// Most recently focused compositor/window identifier.
    pub window_id: Option<String>,
    /// Timestamp of the latest focus event in this bucket.
    pub last_focus_time: Option<String>,
    /// Total number of focus events in this bucket.
    pub focus_event_count: i64,
}

/// Response: `telemetry.window_focus`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryWindowFocusResponse {
    pub buckets: Vec<WindowFocusBucket>,
}

// ─────────────────────────────────────────────────────────────
// telemetry.command_frequency
// ─────────────────────────────────────────────────────────────

/// Request: `telemetry.command_frequency`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelemetryCommandFrequencyRequest {
    #[serde(flatten)]
    pub time_range: TelemetryTimeRange,
    /// Maximum number of entries to return (default: 50).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
}

/// A single command-frequency aggregate entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandFrequencyEntry {
    /// The recorded shell command.
    pub command: String,
    /// Shell/runtime that emitted the command.
    pub shell: Option<String>,
    /// Total invocation count across the requested window.
    pub total_executions: i64,
    /// Successful invocation count (`exit_code = 0`) across the requested window.
    pub successful_executions: i64,
    /// Failed invocation count (`exit_code != 0`) across the requested window.
    pub failed_executions: i64,
    /// Average duration in milliseconds when present in the source events.
    pub avg_duration_ms: Option<f64>,
}

/// Response: `telemetry.command_frequency`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryCommandFrequencyResponse {
    pub entries: Vec<CommandFrequencyEntry>,
}

// ─────────────────────────────────────────────────────────────
// telemetry.file_activity
// ─────────────────────────────────────────────────────────────

/// Request: `telemetry.file_activity`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelemetryFileActivityRequest {
    #[serde(flatten)]
    pub time_range: TelemetryTimeRange,
    /// Maximum number of entries to return (default: 50).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
}

/// A single file-activity aggregate entry (per bucket + directory).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileActivityEntry {
    /// Bucket start timestamp (RFC 3339).
    pub bucket: String,
    /// Directory path that saw activity.
    pub directory: Option<String>,
    /// Filesystem event type aggregated into this bucket.
    pub event_type: String,
    /// Total filesystem event count in this bucket.
    pub total_events: i64,
    /// Distinct files observed in this bucket.
    pub unique_files: i64,
}

/// Response: `telemetry.file_activity`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryFileActivityResponse {
    pub entries: Vec<FileActivityEntry>,
}

// ─────────────────────────────────────────────────────────────
// telemetry.recent_activity
// ─────────────────────────────────────────────────────────────

/// Request: `telemetry.recent_activity` (no time params — view has hardcoded lookback).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelemetryRecentActivityRequest {
    /// Maximum number of entries to return (default: 50).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
}

/// A single recent-activity summary row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecentActivityEntry {
    /// Activity category (e.g. `"focus"`, `"command"`, `"system"`).
    pub activity_type: String,
    /// Secondary grouping or subsystem context.
    pub context: Option<String>,
    /// Human-readable activity detail.
    pub detail: Option<String>,
    /// When this activity was recorded (RFC 3339).
    pub timestamp: Option<String>,
}

/// Response: `telemetry.recent_activity`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryRecentActivityResponse {
    pub entries: Vec<RecentActivityEntry>,
}

// ─────────────────────────────────────────────────────────────
// telemetry.system_state
// ─────────────────────────────────────────────────────────────

/// Request: `telemetry.system_state`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelemetrySystemStateRequest {
    #[serde(flatten)]
    pub time_range: TelemetryTimeRange,
    /// Maximum number of buckets to return (default: 50).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
}

/// A single 5-minute system-state aggregate bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStateBucket {
    /// Bucket start timestamp (RFC 3339).
    pub bucket: String,
    /// Average CPU usage percentage across this bucket (0–100).
    pub avg_cpu_percent: Option<f64>,
    /// Maximum CPU usage percentage across this bucket (0–100).
    pub max_cpu_percent: Option<f64>,
    /// Average memory usage percentage across this bucket (0–100).
    pub avg_memory_percent: Option<f64>,
    /// Maximum memory usage percentage across this bucket (0–100).
    pub max_memory_percent: Option<f64>,
    /// Average disk usage percentage across this bucket (0–100).
    pub avg_disk_percent: Option<f64>,
    /// Latest active systemd unit count emitted in this bucket.
    pub current_active_units: Option<i64>,
    /// Number of source samples aggregated into the bucket.
    pub sample_count: i64,
}

/// Response: `telemetry.system_state`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetrySystemStateResponse {
    pub buckets: Vec<SystemStateBucket>,
}
