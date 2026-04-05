//! Self-observation metrics payloads for Sinex internal telemetry
//!
//! These payloads enable Sinex to observe itself without requiring external
//! observability infrastructure (OpenTelemetry, Prometheus). Metrics become
//! events in core.events, queryable via the same interfaces as all other data.
//!
//! # Design Philosophy
//!
//! Instead of exporting metrics to Prometheus or OpenTelemetry, Sinex emits
//! its own health and performance data as events. This enables:
//!
//! - **Unified query interface**: One query language for all data
//! - **Local-first**: No external dependencies for observability
//! - **Privacy-preserving**: Telemetry stays on the user's machine
//! - **Time-series native**: `TimescaleDB` + continuous aggregates
//!
//! # Event Types
//!
//! - `sinex.metric.counter` - Monotonically increasing values (requests, events)
//! - `sinex.metric.gauge` - Point-in-time values (connections, queue depth)
//! - `sinex.metric.histogram` - Distribution samples (latencies)
//! - `sinex.health.status` - Component health state changes
//! - `sinex.stream.stats` - NATS `JetStream` statistics

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;
use std::collections::HashMap;

/// Counter metric - monotonically increasing value
///
/// Use for: requests served, events processed, errors encountered
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "metric.counter")]
pub struct MetricCounterPayload {
    /// Metric name (e.g., "gateway.requests", "`ingestd.events_processed`")
    pub name: String,
    /// Current counter value
    pub value: u64,
    /// Delta since last emission (if tracked)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta: Option<u64>,
    /// Labels for dimensional filtering
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub labels: HashMap<String, String>,
    /// Component emitting this metric
    pub component: String,
}

/// Gauge metric - point-in-time value that can increase or decrease
///
/// Use for: connection count, queue depth, memory usage, fill percentage
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "metric.gauge")]
pub struct MetricGaugePayload {
    /// Metric name (e.g., "`stream.fill_pct`", "`pool.active_connections`")
    pub name: String,
    /// Current gauge value
    pub value: f64,
    /// Labels for dimensional filtering
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub labels: HashMap<String, String>,
    /// Component emitting this metric
    pub component: String,
}

/// Histogram metric - distribution of values with percentiles
///
/// Use for: latency distributions, size distributions
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "metric.histogram")]
pub struct MetricHistogramPayload {
    /// Metric name (e.g., "`gateway.request_latency_ms`")
    pub name: String,
    /// Sample count in this window
    pub count: u64,
    /// Sum of all values
    pub sum: f64,
    /// Minimum value
    pub min: f64,
    /// Maximum value
    pub max: f64,
    /// Percentiles: p50, p90, p95, p99
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p50: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p90: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p95: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p99: Option<f64>,
    /// Labels for dimensional filtering
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub labels: HashMap<String, String>,
    /// Component emitting this metric
    pub component: String,
}

/// NATS `JetStream` stream statistics
///
/// Addresses Issue 3: Stream Capacity Monitoring
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex.ingestd", event_type = "stream.stats")]
pub struct StreamStatsPayload {
    /// Stream name
    pub stream: String,
    /// Current message count
    pub messages: u64,
    /// Maximum message count (capacity)
    pub max_messages: u64,
    /// Current byte count
    pub bytes: u64,
    /// Maximum bytes
    pub max_bytes: u64,
    /// Consumer count
    pub consumer_count: u32,
    /// Fill percentage (0.0 - 100.0)
    pub fill_pct: f64,
    /// First sequence number
    pub first_seq: u64,
    /// Last sequence number
    pub last_seq: u64,
}

/// Material assembly progress
///
/// Addresses Issue 16: Assembly Metrics
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex.ingestd", event_type = "assembly.stats")]
pub struct AssemblyStatsPayload {
    /// Number of assemblies currently in progress
    pub active_assemblies: u32,
    /// Total assemblies started since service start
    pub total_started: u64,
    /// Total assemblies completed successfully
    pub total_completed: u64,
    /// Total assemblies cancelled intentionally after partial capture
    pub total_cancelled: u64,
    /// Total assemblies failed
    pub total_failed: u64,
    /// Total assemblies timed out
    pub total_timed_out: u64,
    /// Average assembly duration (ms)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_duration_ms: Option<f64>,
    /// Buffered slices waiting for completion
    pub buffered_slices: u32,
}

/// Gateway request statistics
///
/// Addresses Issue 133: Load Shedding Metrics
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex.gateway", event_type = "request.stats")]
pub struct GatewayRequestStatsPayload {
    /// Total requests received
    pub total_requests: u64,
    /// Requests successfully processed
    pub successful_requests: u64,
    /// Requests rejected (rate limited, auth failed, etc.)
    pub rejected_requests: u64,
    /// Requests rate-limited (subset of rejected)
    pub rate_limited_requests: u64,
    /// Average latency in milliseconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_latency_ms: Option<f64>,
    /// P99 latency in milliseconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p99_latency_ms: Option<f64>,
    /// Active connections
    pub active_connections: u32,
}

/// Rate limit event for specific token
///
/// Individual rate limit violations (for audit/debugging)
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex.gateway", event_type = "rate_limit.exceeded")]
pub struct RateLimitExceededPayload {
    /// Token prefix (first 8 chars for identification without full exposure)
    pub token_prefix: String,
    /// Number of requests in current window
    pub requests_in_window: u64,
    /// Configured limit
    pub limit: u64,
    /// Method being called
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
}

/// Component health status change
///
/// Emitted when a component's health status changes (healthy -> degraded, etc.)
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "health.status")]
pub struct HealthStatusPayload {
    /// Component name
    pub component: String,
    /// Previous status
    pub previous_status: String,
    /// Current status
    pub current_status: String,
    /// Reason for status change
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Additional context
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<serde_json::Value>,
}

/// Database connection pool statistics
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex.gateway", event_type = "pool.stats")]
pub struct PoolStatsPayload {
    /// Pool name/identifier
    pub pool: String,
    /// Total connections in pool
    pub size: u32,
    /// Idle connections available
    pub idle: u32,
    /// Active connections in use
    pub active: u32,
    /// Pending connection acquisitions
    pub pending: u32,
    /// Connection acquire timeout count
    pub timeout_count: u64,
}

/// Node event processing statistics
///
/// Addresses Issues 24, 29: Event Processing Metrics
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex.node", event_type = "processing.stats")]
pub struct NodeProcessingStatsPayload {
    /// Node type (fs-ingestor, terminal-ingestor, etc.)
    pub node_type: String,
    /// Events processed since last report
    pub events_processed: u64,
    /// Events dropped (channel full, errors)
    pub events_dropped: u64,
    /// Average processing latency (ms)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_latency_ms: Option<f64>,
    /// Current queue depth
    pub queue_depth: u32,
    /// Errors encountered
    pub error_count: u64,
}

/// Replay operation metrics
///
/// Addresses Issue 145: Replay Control Metrics
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex.gateway", event_type = "replay.stats")]
pub struct ReplayStatsPayload {
    /// Total replay requests
    pub total_requests: u64,
    /// Successful replays
    pub successful: u64,
    /// Failed replays
    pub failed: u64,
    /// Average replay duration (ms)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_duration_ms: Option<f64>,
    /// Events affected by replays
    pub events_affected: u64,
}

/// Ingestd batch processing statistics
///
/// Emitted after each batch is processed by the `JetStream` consumer.
/// Captures throughput, latency, and schema validation coverage data for batch processing.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "sinex.ingestd",
    event_type = "batch.stats",
    version = "2.0.0"
)]
pub struct IngestdBatchStatsPayload {
    /// Number of events in this batch
    pub batch_size: u32,
    /// Time from fetch to ack in milliseconds
    pub fetch_to_ack_ms: u64,
    /// Events deferred to retry
    pub events_deferred: u32,
    /// Events that failed processing
    pub events_failed: u32,
    /// Whether this batch contained synthesis events
    pub had_synthesis: bool,
    /// Insert path used: "copy" or "`query_builder`"
    pub insert_path: String,
    /// Cumulative count of events that passed schema validation
    pub validation_valid: u64,
    /// Cumulative count of events where validation was skipped (disabled)
    pub validation_skipped: u64,
    /// Cumulative count of events with no registered schema
    pub validation_no_schema: u64,
    /// Cumulative count of events whose schema was not found in the registry
    pub validation_schema_not_found: u64,
    /// Cumulative count of events that failed schema validation
    pub validation_invalid: u64,
    /// Schema coverage percentage: events with a schema / total validated (excluding skipped)
    pub validation_coverage_pct: f64,
    /// Cumulative count of events whose `ts_orig` is implausibly far in the future.
    pub suspicious_future_ts_orig: u64,
}

// Test helpers for external tests
#[cfg(any(test, feature = "testing"))]
impl MetricCounterPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            name: "test.counter".into(),
            value: 0,
            delta: None,
            labels: HashMap::new(),
            component: "test".into(),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl StreamStatsPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            stream: "test-stream".into(),
            messages: 0,
            max_messages: 0,
            bytes: 0,
            max_bytes: 0,
            consumer_count: 0,
            fill_pct: 0.0,
            first_seq: 0,
            last_seq: 0,
        }
    }
}
