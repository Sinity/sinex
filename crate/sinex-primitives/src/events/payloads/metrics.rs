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
    #[serde(deserialize_with = "crate::validation::reject_non_finite_f64")]
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
    #[serde(deserialize_with = "crate::validation::reject_non_finite_f64")]
    pub sum: f64,
    /// Minimum value
    #[serde(deserialize_with = "crate::validation::reject_non_finite_f64")]
    pub min: f64,
    /// Maximum value
    #[serde(deserialize_with = "crate::validation::reject_non_finite_f64")]
    pub max: f64,
    /// Percentiles: p50, p90, p95, p99
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::validation::reject_non_finite_optional_f64"
    )]
    pub p50: Option<f64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::validation::reject_non_finite_optional_f64"
    )]
    pub p90: Option<f64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::validation::reject_non_finite_optional_f64"
    )]
    pub p95: Option<f64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::validation::reject_non_finite_optional_f64"
    )]
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
    #[serde(deserialize_with = "crate::validation::reject_non_finite_f64")]
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
    /// Times the finalization COMMIT result was ambiguous (TCP timeout during COMMIT).
    /// Non-zero signals potential duplicate-material risk after restart.
    pub total_commit_outcome_unknown: u64,
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
    /// Minimum latency in milliseconds (over the snapshot window)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_latency_ms: Option<f64>,
    /// Maximum latency in milliseconds (over the snapshot window)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_latency_ms: Option<f64>,
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

/// Derived-node per-event latency snapshot
///
/// Single-event replacement for the six separate `metric.gauge` emissions that
/// `AutomatonRuntime::observe_processing_latency` previously produced
/// (`derived.event_lag_ms`, `derived.tick_runtime_ms`, the two
/// `event_lag_p{50,99}_ms` reservoir percentiles, `derived.tick_runtime_p99_ms`,
/// and `derived.throughput_eps`). Reducing six rows-per-snapshot to one row
/// cuts derived-node telemetry volume by ~6x without any information loss; see
/// issue #1556.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex.node", event_type = "derived.latency_snapshot")]
pub struct DerivedNodeLatencySnapshotPayload {
    /// Derived-node name (e.g., "session-detector")
    pub node_name: String,
    /// Last lag sample (ms) — wall time between upstream `ts_orig` and dispatch
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::validation::reject_non_finite_optional_f64"
    )]
    pub event_lag_ms: Option<f64>,
    /// Last per-event tick runtime (ms)
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::validation::reject_non_finite_optional_f64"
    )]
    pub tick_runtime_ms: Option<f64>,
    /// Sliding-reservoir p50 of `event_lag_ms` over the latency reservoir
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::validation::reject_non_finite_optional_f64"
    )]
    pub event_lag_p50_ms: Option<f64>,
    /// Sliding-reservoir p99 of `event_lag_ms`
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::validation::reject_non_finite_optional_f64"
    )]
    pub event_lag_p99_ms: Option<f64>,
    /// Sliding-reservoir p99 of `tick_runtime_ms`
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::validation::reject_non_finite_optional_f64"
    )]
    pub tick_runtime_p99_ms: Option<f64>,
    /// Events per second over the live throughput window
    #[serde(deserialize_with = "crate::validation::reject_non_finite_f64")]
    pub throughput_eps: f64,
    /// Dimensional labels (node_model, source_run_id, etc.)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub labels: HashMap<String, String>,
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
    version = "2.2.0"
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
    /// Whether this batch contained derived events
    pub had_derived: bool,
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
    #[serde(deserialize_with = "crate::validation::reject_non_finite_f64")]
    pub validation_coverage_pct: f64,
    /// Cumulative count of events whose `ts_orig` is implausibly far in the future.
    pub suspicious_future_ts_orig: u64,
    /// Cumulative count of failures emitting observer telemetry gauges.
    /// Non-zero indicates the self-observation NATS channel is degraded.
    pub telemetry_publish_failures: u64,
    /// Cumulative count of confirmation-durability gaps: events were persisted
    /// but the confirmation publish to the Confirmations stream failed
    /// non-transiently. Non-zero is a durable signal of pipeline split-brain.
    pub confirmation_durability_gaps: u64,
}

/// Startup snapshot for a `JetStream` pull consumer.
///
/// Emitted once per consumer before the ingestd pull loop begins (before READY/`sd_notify`).
/// Captures stream state and consumer configuration so operators can determine at a glance
/// whether this startup is a normal resume, a cold-start full replay, or a catch-up run.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex.ingestd", event_type = "consumer.startup_snapshot")]
pub struct ConsumerStartupSnapshotPayload {
    /// Name of the `JetStream` stream being consumed.
    pub stream_name: String,
    /// Durable consumer name.
    pub durable_name: String,
    /// Whether the durable consumer already existed in `JetStream` before this startup.
    pub consumer_existed: bool,
    /// Deliver policy as a string (e.g. `"All"`, `"New"`, `"ByStartSequence"`).
    pub deliver_policy: String,
    /// Number of messages currently retained in the stream.
    pub stream_messages: u64,
    /// Number of bytes currently retained in the stream.
    pub stream_bytes: u64,
    /// First sequence number retained in the stream.
    pub stream_first_sequence: u64,
    /// Last sequence number retained in the stream.
    pub stream_last_sequence: u64,
    /// Stream retention limit: maximum message count (0 = unlimited).
    pub stream_max_messages: u64,
    /// Stream retention limit: maximum bytes (0 = unlimited).
    pub stream_max_bytes: u64,
    /// Stream retention limit: maximum age in seconds (0 = unlimited).
    pub stream_max_age_secs: u64,
    /// Messages pending delivery to this consumer at startup.
    pub consumer_pending: u64,
    /// Messages acknowledged but not yet confirmed by the server.
    pub consumer_ack_pending: usize,
    /// Messages redelivered (NAK'd or timed out) at startup.
    pub consumer_redelivered: usize,
    /// Configured `max_ack_pending` for this consumer.
    pub consumer_max_ack_pending: i64,
    /// Configured `max_deliver` (redelivery budget) for this consumer.
    pub consumer_max_deliver: i64,
    /// True when this looks like a dangerous cold-start full replay
    /// (new consumer, `DeliverPolicy::All`, non-empty stream).
    pub initial_replay_risk: bool,
}

/// High-priority warning emitted when a dangerous replay is detected at ingestd startup.
///
/// A dangerous replay occurs when a durable consumer is missing (e.g. renamed, environment
/// drift, or first start after schema change) and the stream contains retained messages.
/// Without intervention, the consumer would replay the entire backlog from the beginning,
/// which can saturate I/O and destabilise the host.
///
/// The `reject_initial_replay` guard in `ensure_pull_consumer` already blocks this from
/// happening in production; this event is the pre-guard observability counterpart, emitted
/// even when the guard would have prevented the consumer from starting.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex.ingestd", event_type = "consumer.startup_replay_risk")]
pub struct DangerousReplayWarningPayload {
    /// Name of the `JetStream` stream.
    pub stream_name: String,
    /// Durable consumer name that was missing.
    pub durable_name: String,
    /// Number of messages that would be replayed from the beginning.
    pub stream_messages: u64,
    /// Byte volume that would be replayed.
    pub stream_bytes: u64,
    /// First sequence that would be delivered.
    pub stream_first_sequence: u64,
    /// Human-readable explanation of why this is dangerous.
    pub reason: String,
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

// ─────────────────────────────────────────────────────────────────────────────
// Source-unit descriptors for sinex.* self-observation metrics infra events.
//
// Every long-running sinex binary participates in self-observation:
// counters/gauges/histograms (`sinex.metric.*`), component health
// (`sinex.health.status`), and per-binary operational rollups
// (`sinex.ingestd.*`, `sinex.gateway.*`, `sinex.node.*`). These payloads have
// no dedicated systemd unit — they are produced from inside ingestd, gateway,
// and the node SDK as those processes run. We register infra source-unit
// descriptors so the (source, event_type) pairs declared by `#[event_payload]`
// are claimed by the source-unit inventory; bindings continue to live with the
// owning ingestor / runner-pack descriptors.
// ─────────────────────────────────────────────────────────────────────────────

use crate::proof::{
    CheckpointFamily as SuCheckpointFamily, Horizon as SuHorizon,
    OccurrenceIdentity as SuOccurrenceIdentity, PrivacyTier as SuPrivacyTier,
    RetentionPolicy as SuRetentionPolicy, RuntimeShape as SuRuntimeShape, SourceUnitBinding,
    SourceUnitBuildImpact, SourceUnitDescriptor, SubjectRef,
};
use crate::{register_source_unit, register_source_unit_binding};

register_source_unit! {
    SourceUnitDescriptor {
        id: "sinex-metrics",
        namespace: "infra",
        event_types: &[
            ("sinex", "metric.counter"),
            ("sinex", "metric.gauge"),
            ("sinex", "metric.histogram"),
            ("sinex", "health.status"),
        ],
        privacy_tier: SuPrivacyTier::Public,
        horizons: &[SuHorizon::Continuous],
        retention: SuRetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: SuOccurrenceIdentity::Natural,
        access_policy: "embedded_in_every_sinex_binary",
    }
}

register_source_unit! {
    SourceUnitDescriptor {
        id: "sinex-ingestd-telemetry",
        namespace: "infra",
        event_types: &[
            ("sinex.ingestd", "batch.stats"),
            ("sinex.ingestd", "stream.stats"),
            ("sinex.ingestd", "assembly.stats"),
            ("sinex.ingestd", "consumer.startup_snapshot"),
            ("sinex.ingestd", "consumer.startup_replay_risk"),
        ],
        privacy_tier: SuPrivacyTier::Public,
        horizons: &[SuHorizon::Continuous],
        retention: SuRetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: SuOccurrenceIdentity::Natural,
        access_policy: "embedded_in_ingestd",
    }
}

register_source_unit! {
    SourceUnitDescriptor {
        id: "sinex-gateway-telemetry",
        namespace: "infra",
        event_types: &[
            ("sinex.gateway", "request.stats"),
            ("sinex.gateway", "rate_limit.exceeded"),
            ("sinex.gateway", "pool.stats"),
            ("sinex.gateway", "replay.stats"),
            ("sinex.gateway", "gateway.rpc.call"),
        ],
        privacy_tier: SuPrivacyTier::Public,
        horizons: &[SuHorizon::Continuous],
        retention: SuRetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: SuOccurrenceIdentity::Natural,
        access_policy: "embedded_in_gateway",
    }
}

register_source_unit! {
    SourceUnitDescriptor {
        id: "sinex-node-telemetry",
        namespace: "infra",
        event_types: &[("sinex.node", "processing.stats")],
        privacy_tier: SuPrivacyTier::Public,
        horizons: &[SuHorizon::Continuous],
        retention: SuRetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: SuOccurrenceIdentity::Natural,
        access_policy: "embedded_in_node_sdk",
    }
}

// Infra source units: descriptor-only by design (events emitted from inside
// every sinex binary or specific runtime processes). Bindings are recorded
// with `proposed: true` so manifest renderers separate them from host-level
// adapter deployments.

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:sinex-metrics"),
        "sinex-metrics",
        "infra",
    )
    .implementation("sinex-primitives::metrics")
    .adapter("EmbeddedEmitter")
    .output_event_type("metric.counter")
    .privacy_context("none")
    .material_policy("none")
    .checkpoint_policy("live_observation")
    .resource_shape("embedded_emitter")
    .source_unit_id("sinex-metrics")
    .proposed(true)
    .runner_pack("infra")
    .checkpoint_family(SuCheckpointFamily::LiveObservation)
    .runtime_shape(SuRuntimeShape::Continuous)
    .package_impact("no_new_output")
    .implementation_mode("rust_in_every_sinex_binary")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:sinex-ingestd-telemetry"),
        "sinex-ingestd-telemetry",
        "infra",
    )
    .implementation("sinex-ingestd")
    .adapter("EmbeddedEmitter")
    .output_event_type("stream.stats")
    .privacy_context("none")
    .material_policy("none")
    .checkpoint_policy("live_observation")
    .resource_shape("embedded_emitter")
    .source_unit_id("sinex-ingestd-telemetry")
    .proposed(true)
    .runner_pack("infra")
    .checkpoint_family(SuCheckpointFamily::LiveObservation)
    .runtime_shape(SuRuntimeShape::Continuous)
    .package_impact("no_new_output")
    .implementation_mode("rust_in_ingestd")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:sinex-gateway-telemetry"),
        "sinex-gateway-telemetry",
        "infra",
    )
    .implementation("sinex-gateway")
    .adapter("EmbeddedEmitter")
    .output_event_type("request.stats")
    .privacy_context("none")
    .material_policy("none")
    .checkpoint_policy("live_observation")
    .resource_shape("embedded_emitter")
    .source_unit_id("sinex-gateway-telemetry")
    .proposed(true)
    .runner_pack("infra")
    .checkpoint_family(SuCheckpointFamily::LiveObservation)
    .runtime_shape(SuRuntimeShape::Continuous)
    .package_impact("no_new_output")
    .implementation_mode("rust_in_gateway")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:sinex-node-telemetry"),
        "sinex-node-telemetry",
        "infra",
    )
    .implementation("sinex-node-sdk")
    .adapter("EmbeddedEmitter")
    .output_event_type("processing.stats")
    .privacy_context("none")
    .material_policy("none")
    .checkpoint_policy("live_observation")
    .resource_shape("embedded_emitter")
    .source_unit_id("sinex-node-telemetry")
    .proposed(true)
    .runner_pack("infra")
    .checkpoint_family(SuCheckpointFamily::LiveObservation)
    .runtime_shape(SuRuntimeShape::Continuous)
    .package_impact("no_new_output")
    .implementation_mode("rust_in_node_sdk")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}
