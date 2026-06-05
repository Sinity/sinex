//! Telemetry RPC request/response types
//!
//! These types map to the `sinex_telemetry.*` read models exposed by the
//! gateway under the `telemetry.*` method namespace.

use crate::rpc::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};
use serde::{Deserialize, Serialize};

macro_rules! telemetry_method {
    ($name:ident, $method:ident, $req:ty, $resp:ty) => {
        pub const $name: RpcMethod<$req, $resp> = RpcMethod::new(
            methods::$method,
            RpcRole::ReadOnly,
            RpcDomain::Telemetry,
            RpcStability::Experimental,
            RpcMutability::ReadOnly,
        );
    };
}

telemetry_method!(
    TELEMETRY_CURRENT_HEALTH_METHOD,
    TELEMETRY_CURRENT_HEALTH,
    TelemetryCurrentHealthRequest,
    TelemetryCurrentHealthResponse
);
telemetry_method!(
    TELEMETRY_CURRENT_DEVICE_STATE_METHOD,
    TELEMETRY_CURRENT_DEVICE_STATE,
    TelemetryCurrentDeviceStateRequest,
    TelemetryCurrentDeviceStateResponse
);
telemetry_method!(
    TELEMETRY_WINDOW_FOCUS_METHOD,
    TELEMETRY_WINDOW_FOCUS,
    TelemetryWindowFocusRequest,
    TelemetryWindowFocusResponse
);
telemetry_method!(
    TELEMETRY_COMMAND_FREQUENCY_METHOD,
    TELEMETRY_COMMAND_FREQUENCY,
    TelemetryCommandFrequencyRequest,
    TelemetryCommandFrequencyResponse
);
telemetry_method!(
    TELEMETRY_FILE_ACTIVITY_METHOD,
    TELEMETRY_FILE_ACTIVITY,
    TelemetryFileActivityRequest,
    TelemetryFileActivityResponse
);
telemetry_method!(
    TELEMETRY_RECENT_ACTIVITY_METHOD,
    TELEMETRY_RECENT_ACTIVITY,
    TelemetryRecentActivityRequest,
    TelemetryRecentActivityResponse
);
telemetry_method!(
    TELEMETRY_SYSTEM_STATE_METHOD,
    TELEMETRY_SYSTEM_STATE,
    TelemetrySystemStateRequest,
    TelemetrySystemStateResponse
);
telemetry_method!(
    TELEMETRY_GATEWAY_STATS_METHOD,
    TELEMETRY_GATEWAY_STATS,
    TelemetryGatewayStatsRequest,
    TelemetryGatewayStatsResponse
);
telemetry_method!(
    TELEMETRY_STREAM_STATS_METHOD,
    TELEMETRY_STREAM_STATS,
    TelemetryStreamStatsRequest,
    TelemetryStreamStatsResponse
);
telemetry_method!(
    TELEMETRY_ASSEMBLY_STATS_METHOD,
    TELEMETRY_ASSEMBLY_STATS,
    TelemetryAssemblyStatsRequest,
    TelemetryAssemblyStatsResponse
);
telemetry_method!(
    TELEMETRY_SOURCE_STATS_METHOD,
    TELEMETRY_SOURCE_STATS,
    TelemetrySourceStatsRequest,
    TelemetrySourceStatsResponse
);
telemetry_method!(
    TELEMETRY_METRIC_COUNTERS_METHOD,
    TELEMETRY_METRIC_COUNTERS,
    TelemetryMetricCountersRequest,
    TelemetryMetricCountersResponse
);
telemetry_method!(
    TELEMETRY_EVENT_ENGINE_BATCH_STATS_METHOD,
    TELEMETRY_EVENT_ENGINE_BATCH_STATS,
    TelemetryEventEngineBatchStatsRequest,
    TelemetryEventEngineBatchStatsResponse
);
telemetry_method!(
    TELEMETRY_EVENT_ENGINE_VALIDATION_METHOD,
    TELEMETRY_EVENT_ENGINE_VALIDATION,
    TelemetryEventEngineValidationRequest,
    TelemetryEventEngineValidationResponse
);
telemetry_method!(
    TELEMETRY_THROUGHPUT_METHOD,
    TELEMETRY_THROUGHPUT,
    TelemetryThroughputRequest,
    TelemetryThroughputResponse
);

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
// telemetry.source_stats
// ─────────────────────────────────────────────────────────────

/// Request: `telemetry.source_stats`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelemetrySourceStatsRequest {
    #[serde(flatten)]
    pub time_range: TelemetryTimeRange,
    /// Maximum number of buckets to return (default: 50).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
}

/// A single source/runtime-stat bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceStatsBucket {
    pub bucket: String,
    pub module_kind: Option<String>,
    pub total_events_processed: Option<i64>,
    pub total_events_dropped: Option<i64>,
    pub avg_latency_ms: Option<f64>,
    pub max_queue_depth: Option<i64>,
    pub total_errors: Option<i64>,
    pub sample_count: i64,
}

/// Response: `telemetry.source_stats`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetrySourceStatsResponse {
    pub buckets: Vec<SourceStatsBucket>,
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
// telemetry.event_engine_batch_stats
// ─────────────────────────────────────────────────────────────

/// Request: `telemetry.event_engine_batch_stats`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelemetryEventEngineBatchStatsRequest {
    #[serde(flatten)]
    pub time_range: TelemetryTimeRange,
    /// Maximum number of buckets to return (default: 50).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
}

/// A single event_engine batch-stat bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEngineBatchStatsBucket {
    pub bucket: String,
    pub avg_batch_size: Option<f64>,
    pub max_batch_size: Option<i64>,
    pub avg_latency_ms: Option<f64>,
    pub max_latency_ms: Option<f64>,
    pub total_deferred: Option<i64>,
    pub total_failed: Option<i64>,
    pub derived_batches: i64,
    pub batch_count: i64,
    pub validation_valid: Option<i64>,
    pub validation_skipped: Option<i64>,
    pub validation_no_schema: Option<i64>,
    pub validation_schema_not_found: Option<i64>,
    pub validation_invalid: Option<i64>,
    pub avg_validation_coverage_pct: Option<f64>,
}

/// Response: `telemetry.event_engine_batch_stats`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryEventEngineBatchStatsResponse {
    pub buckets: Vec<EventEngineBatchStatsBucket>,
}

// ─────────────────────────────────────────────────────────────
// telemetry.event_engine_validation
// ─────────────────────────────────────────────────────────────

/// Request: `telemetry.event_engine_validation`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelemetryEventEngineValidationRequest {}

/// Latest event_engine validation / batch snapshot emitted via `sinex.event_engine batch.stats`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEngineValidationSnapshot {
    pub observed_at: String,
    pub batch_size: i64,
    pub fetch_to_ack_ms: i64,
    pub events_deferred: i64,
    pub events_failed: i64,
    pub had_derived: bool,
    pub insert_path: String,
    pub validation_valid: i64,
    pub validation_skipped: i64,
    pub validation_no_schema: i64,
    pub validation_schema_not_found: i64,
    pub validation_invalid: i64,
    pub validation_coverage_pct: f64,
    pub suspicious_future_ts_orig: i64,
}

/// Response: `telemetry.event_engine_validation`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryEventEngineValidationResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<EventEngineValidationSnapshot>,
}

// ─────────────────────────────────────────────────────────────
// telemetry.throughput  (#1172 AC-8)
// ─────────────────────────────────────────────────────────────

/// Request: `telemetry.throughput`. No parameters today; the handler returns
/// per-source EPS over fixed 1h and 24h windows plus a per-component
/// aggregate.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelemetryThroughputRequest {}

/// Per-source EPS over the fixed 1h and 24h windows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputSourceEntry {
    pub source: String,
    pub events_last_1h: i64,
    pub events_last_24h: i64,
    /// Events per second over the last 1h window.
    pub eps_1h: f64,
    /// Events per second over the last 24h window.
    pub eps_24h: f64,
}

/// Per-component aggregate: event_engine/gateway/automatons lumped into one
/// row each so an operator can see "is the gateway above its long-run rate?"
/// in a glance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputComponentEntry {
    pub component: String,
    /// Requests/events per second over the last 1h window.
    pub eps_1h: f64,
    /// Requests/events per second over the last 24h window.
    pub eps_24h: f64,
}

/// Response: `telemetry.throughput`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelemetryThroughputResponse {
    pub per_source: Vec<ThroughputSourceEntry>,
    pub per_component: Vec<ThroughputComponentEntry>,
}
