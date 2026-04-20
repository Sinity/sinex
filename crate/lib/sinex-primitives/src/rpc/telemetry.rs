//! Telemetry RPC request/response types
//!
//! These types map to the `sinex_telemetry.*` read models exposed by the
//! gateway under the `telemetry.*` method namespace.

use serde::{Deserialize, Serialize};

// ─────────────────────────────────────────────────────────────
// Shared parameters
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

/// Limit-only request used by current-state telemetry views.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelemetryLimitRequest {
    /// Maximum number of rows to return (default: 50).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
}

// ─────────────────────────────────────────────────────────────
// telemetry.current_health
// ─────────────────────────────────────────────────────────────

/// Request: `telemetry.current_health`
pub type TelemetryCurrentHealthRequest = TelemetryLimitRequest;

/// A single current-health row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentHealthEntry {
    pub source: String,
    pub event_type: String,
    pub component: Option<String>,
    pub status: Option<String>,
    pub reason: Option<String>,
    pub last_update: String,
}

/// Response: `telemetry.current_health`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryCurrentHealthResponse {
    pub entries: Vec<CurrentHealthEntry>,
}

// ─────────────────────────────────────────────────────────────
// telemetry.current_device_state
// ─────────────────────────────────────────────────────────────

/// Request: `telemetry.current_device_state`
pub type TelemetryCurrentDeviceStateRequest = TelemetryLimitRequest;

/// A single current-device-state row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentDeviceStateEntry {
    pub unit_name: Option<String>,
    pub unit_type: Option<String>,
    pub state: Option<String>,
    pub sub_state: Option<String>,
    pub last_update: String,
}

/// Response: `telemetry.current_device_state`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryCurrentDeviceStateResponse {
    pub entries: Vec<CurrentDeviceStateEntry>,
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
    pub bucket: String,
    pub workspace: Option<String>,
    pub window_class: Option<String>,
    pub window_title: Option<String>,
    pub window_id: Option<String>,
    pub last_focus_time: Option<String>,
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
    pub command: String,
    pub shell: Option<String>,
    pub total_executions: i64,
    pub successful_executions: i64,
    pub failed_executions: i64,
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
    pub bucket: String,
    pub directory: Option<String>,
    pub event_type: String,
    pub total_events: i64,
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

/// Request: `telemetry.recent_activity`
pub type TelemetryRecentActivityRequest = TelemetryLimitRequest;

/// A single recent-activity summary row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecentActivityEntry {
    pub activity_type: String,
    pub context: Option<String>,
    pub detail: Option<String>,
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
    pub bucket: String,
    pub avg_cpu_percent: Option<f64>,
    pub max_cpu_percent: Option<f64>,
    pub avg_memory_percent: Option<f64>,
    pub max_memory_percent: Option<f64>,
    pub avg_disk_percent: Option<f64>,
    pub current_active_units: Option<i64>,
    pub sample_count: i64,
}

/// Response: `telemetry.system_state`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetrySystemStateResponse {
    pub buckets: Vec<SystemStateBucket>,
}

// ─────────────────────────────────────────────────────────────
// telemetry.gateway_stats
// ─────────────────────────────────────────────────────────────

/// Request: `telemetry.gateway_stats`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelemetryGatewayStatsRequest {
    #[serde(flatten)]
    pub time_range: TelemetryTimeRange,
    /// Maximum number of buckets to return (default: 50).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
}

/// A single gateway-stat bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayStatsBucket {
    pub bucket: String,
    pub source: String,
    pub stat_events: i64,
    pub avg_total_requests: Option<f64>,
    pub total_rate_limited: Option<i64>,
    pub avg_latency_ms: Option<f64>,
    pub max_p99_latency_ms: Option<f64>,
}

/// Response: `telemetry.gateway_stats`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryGatewayStatsResponse {
    pub buckets: Vec<GatewayStatsBucket>,
}

// ─────────────────────────────────────────────────────────────
// telemetry.stream_stats
// ─────────────────────────────────────────────────────────────

/// Request: `telemetry.stream_stats`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelemetryStreamStatsRequest {
    #[serde(flatten)]
    pub time_range: TelemetryTimeRange,
    /// Maximum number of buckets to return (default: 50).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
}

/// A single stream-stat bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamStatsBucket {
    pub bucket: String,
    pub stream_name: Option<String>,
    pub avg_fill_pct: Option<f64>,
    pub max_fill_pct: Option<f64>,
    pub avg_messages: Option<f64>,
    pub max_messages: Option<i64>,
    pub sample_count: i64,
}

/// Response: `telemetry.stream_stats`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryStreamStatsResponse {
    pub buckets: Vec<StreamStatsBucket>,
}

// ─────────────────────────────────────────────────────────────
// telemetry.assembly_stats
// ─────────────────────────────────────────────────────────────

/// Request: `telemetry.assembly_stats`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelemetryAssemblyStatsRequest {
    #[serde(flatten)]
    pub time_range: TelemetryTimeRange,
    /// Maximum number of buckets to return (default: 50).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
}

/// A single assembly-stat bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssemblyStatsBucket {
    pub bucket: String,
    pub max_active_assemblies: Option<i64>,
    pub total_completed: Option<i64>,
    pub total_cancelled: Option<i64>,
    pub total_failed: Option<i64>,
    pub total_timed_out: Option<i64>,
    pub avg_duration_ms: Option<f64>,
    pub sample_count: i64,
}

/// Response: `telemetry.assembly_stats`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryAssemblyStatsResponse {
    pub buckets: Vec<AssemblyStatsBucket>,
}

// ─────────────────────────────────────────────────────────────
// telemetry.node_stats
// ─────────────────────────────────────────────────────────────

/// Request: `telemetry.node_stats`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelemetryNodeStatsRequest {
    #[serde(flatten)]
    pub time_range: TelemetryTimeRange,
    /// Maximum number of buckets to return (default: 50).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
}

/// A single node-stat bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatsBucket {
    pub bucket: String,
    pub node_type: Option<String>,
    pub total_events_processed: Option<i64>,
    pub total_events_dropped: Option<i64>,
    pub avg_latency_ms: Option<f64>,
    pub max_queue_depth: Option<i64>,
    pub total_errors: Option<i64>,
    pub sample_count: i64,
}

/// Response: `telemetry.node_stats`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryNodeStatsResponse {
    pub buckets: Vec<NodeStatsBucket>,
}

// ─────────────────────────────────────────────────────────────
// telemetry.metric_counters
// ─────────────────────────────────────────────────────────────

/// Request: `telemetry.metric_counters`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelemetryMetricCountersRequest {
    #[serde(flatten)]
    pub time_range: TelemetryTimeRange,
    /// Maximum number of buckets to return (default: 50).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
}

/// A single metric-counter bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricCounterBucket {
    pub bucket: String,
    pub component: Option<String>,
    pub metric_name: Option<String>,
    pub total_value: Option<i64>,
    pub max_value: Option<i64>,
    pub sample_count: i64,
}

/// Response: `telemetry.metric_counters`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryMetricCountersResponse {
    pub buckets: Vec<MetricCounterBucket>,
}

// ─────────────────────────────────────────────────────────────
// telemetry.ingestd_batch_stats
// ─────────────────────────────────────────────────────────────

/// Request: `telemetry.ingestd_batch_stats`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelemetryIngestdBatchStatsRequest {
    #[serde(flatten)]
    pub time_range: TelemetryTimeRange,
    /// Maximum number of buckets to return (default: 50).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
}

/// A single ingestd batch-stat bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestdBatchStatsBucket {
    pub bucket: String,
    pub avg_batch_size: Option<f64>,
    pub max_batch_size: Option<i64>,
    pub avg_latency_ms: Option<f64>,
    pub max_latency_ms: Option<f64>,
    pub total_deferred: Option<i64>,
    pub total_failed: Option<i64>,
    pub synthesis_batches: i64,
    pub batch_count: i64,
    pub validation_valid: Option<i64>,
    pub validation_skipped: Option<i64>,
    pub validation_no_schema: Option<i64>,
    pub validation_schema_not_found: Option<i64>,
    pub validation_invalid: Option<i64>,
    pub avg_validation_coverage_pct: Option<f64>,
}

/// Response: `telemetry.ingestd_batch_stats`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryIngestdBatchStatsResponse {
    pub buckets: Vec<IngestdBatchStatsBucket>,
}

// ─────────────────────────────────────────────────────────────
// telemetry.ingestd_validation
// ─────────────────────────────────────────────────────────────

/// Request: `telemetry.ingestd_validation`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelemetryIngestdValidationRequest {}

/// Latest ingestd validation / batch snapshot emitted via `sinex.ingestd batch.stats`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestdValidationSnapshot {
    pub observed_at: String,
    pub batch_size: i64,
    pub fetch_to_ack_ms: i64,
    pub events_deferred: i64,
    pub events_failed: i64,
    pub had_synthesis: bool,
    pub insert_path: String,
    pub validation_valid: i64,
    pub validation_skipped: i64,
    pub validation_no_schema: i64,
    pub validation_schema_not_found: i64,
    pub validation_invalid: i64,
    pub validation_coverage_pct: f64,
    pub suspicious_future_ts_orig: i64,
}

/// Response: `telemetry.ingestd_validation`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryIngestdValidationResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<IngestdValidationSnapshot>,
}
