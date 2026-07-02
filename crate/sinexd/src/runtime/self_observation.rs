//! Self-observation event emitter for Sinex internal telemetry
//!
//! This module enables Sinex components to observe themselves by emitting
//! metrics, health status, and operational data as events that flow through
//! the normal event pipeline and are stored in core.events.
//!
//! # Design Philosophy
//!
//! Instead of relying on external observability infrastructure (Prometheus,
//! OpenTelemetry), Sinex observes itself using its own event system:
//!
//! - Metrics become events with `source = "sinex.*"`
//! - All self-observation data queryable via the same interfaces
//! - No external dependencies for observability
//! - Privacy-preserving: telemetry stays local
//!
//! # Usage
//!
//! ```rust,no_run
//! use crate::runtime::self_observation::SelfObserver;
//!
//! // Create an observer for the gateway component
//! let observer = SelfObserver::new(
//!     nats_client,
//!     SelfObserverConfig::from_env("sinexd"),
//! );
//!
//! // Emit metrics periodically
//! observer.emit_counter("requests.total", 1000, None).await?;
//! observer.emit_gauge("connections.active", 42.0, None).await?;
//! ```

use crate::runtime::NatsPublisher;
use crate::runtime::acquisition_manager::{
    AcquisitionManager, BufferedAppendStreamWriter, BufferedAppendStreamWriterConfig,
    RotationPolicy,
};
use crate::runtime::error_helpers::env_nonempty_string_optional;
use async_nats::Client as NatsClient;
use sinex_primitives::domain::HealthStatus;
use sinex_primitives::env as shared_env;
use sinex_primitives::events::payloads::{
    ApiRequestStatsPayload, AssemblyStatsPayload, AutomatonLatencySnapshotPayload,
    ConsumerStartupSnapshotPayload, EventEngineBatchStatsPayload, GatewayRpcCallPayload,
    HealthStatusPayload, MetricCounterPayload, MetricGaugePayload, MetricHistogramPayload,
    PoolStatsPayload, RateLimitExceededPayload, ReplayStatsPayload, RpcStatus,
    SourceProcessingStatsPayload, StreamPressureSnapshot, StreamStatsPayload,
};
use sinex_primitives::events::{Event, Provenance, SourceMaterial};
use sinex_primitives::{Id, JsonValue, SinexError, Timestamp};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Self-observation event emitter
///
/// Provides methods for Sinex components to emit internal telemetry as events.
#[derive(Clone)]
pub struct SelfObserver {
    /// Shared publisher path for raw events (None when disabled)
    publisher: Option<NatsPublisher>,
    /// Source-material stream used to record the emitted observation bytes.
    ///
    /// A `BufferedAppendStreamWriter` owns the active source material behind a
    /// background task: it appends through an `&self` channel (interior
    /// mutability), creates the BEGIN frame lazily on the first append, and
    /// rotates streams without holding a mutex across NATS I/O. This is the same
    /// substrate the deleted `BufferedRecordMaterializer` wrapped; the wrapper's
    /// only added behavior was JSONL serialization, which now lives in
    /// `stable_json_line` below.
    materializer: Option<BufferedAppendStreamWriter>,
    /// Component name
    component: String,
    /// Whether self-observation is enabled
    enabled: bool,
    /// Per-metric emission tracking (`metric_key` -> `last_emission_time`)
    metric_emissions: Arc<RwLock<HashMap<String, Instant>>>,
    /// Minimum interval between emissions (rate limiting)
    min_interval: Duration,
    /// The `core.runs` row ID for this component instance, stamped on every emitted event.
    /// Shared across clones via `Arc<OnceLock>` so it can be set once after DB registration.
    module_run_id: Arc<OnceLock<sinex_primitives::Uuid>>,
}

impl std::fmt::Debug for SelfObserver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SelfObserver")
            .field("component", &self.component)
            .field("enabled", &self.enabled)
            .field("has_publisher", &self.publisher.is_some())
            .field("has_materializer", &self.materializer.is_some())
            .field("min_interval", &self.min_interval)
            .finish()
    }
}

/// Configuration for self-observation
#[derive(Debug, Clone)]
pub struct SelfObserverConfig {
    /// Component name (e.g., "sinexd", "sinexd")
    pub component: String,
    /// Optional NATS namespace used by test/runtime isolation.
    pub namespace: Option<String>,
    /// Enable self-observation
    pub enabled: bool,
    /// Minimum interval between emissions (default: 1s)
    pub min_emission_interval: Duration,
}

impl Default for SelfObserverConfig {
    fn default() -> Self {
        Self {
            component: "sinex-unknown".to_string(),
            namespace: None,
            enabled: true,
            min_emission_interval: Duration::from_secs(1),
        }
    }
}

impl SelfObserverConfig {
    /// Create configuration from environment variables
    #[must_use]
    pub fn from_env(component: &str) -> Self {
        let enabled =
            shared_env::bool_or("SINEX_SELF_OBSERVATION_ENABLED", true, "self-observation");
        let min_interval_secs = shared_env::parse_or(
            "SINEX_SELF_OBSERVATION_INTERVAL_SECS",
            1_u64,
            "self-observation",
        );
        let namespace =
            env_nonempty_string_optional("SINEX_NAMESPACE", "self-observation namespace");

        Self {
            component: component.to_string(),
            namespace,
            enabled,
            min_emission_interval: Duration::from_secs(min_interval_secs),
        }
    }
}

impl SelfObserver {
    /// Create a new self-observer for a component
    #[must_use]
    pub fn new(nats_client: NatsClient, config: SelfObserverConfig) -> Self {
        let SelfObserverConfig {
            component,
            namespace,
            enabled,
            min_emission_interval,
        } = config;
        let (publisher, materializer) = if enabled {
            let acquisition_manager = Arc::new(AcquisitionManager::new_with_namespace(
                nats_client.clone(),
                // Operator-tunable per-source granularity (#2184 prong B). Defaults
                // to the standard 100 MB / 1 h batching so reflection materials are
                // large by default, overridable via
                // SINEX_MATERIAL_ROTATION_SELF_OBSERVATION_MAX_{MB,AGE_SECS}.
                RotationPolicy::from_env("self_observation", RotationPolicy::default()),
                "self_observation".to_string(),
                namespace.clone(),
            ));
            (
                Some(NatsPublisher::with_namespace(nats_client, namespace)),
                Some(BufferedAppendStreamWriter::from_manager(
                    acquisition_manager,
                    self_observation_source_identifier(&component),
                    BufferedAppendStreamWriterConfig::default(),
                )),
            )
        } else {
            (None, None)
        };

        Self {
            publisher,
            materializer,
            component,
            enabled,
            metric_emissions: Arc::new(RwLock::new(HashMap::new())),
            min_interval: min_emission_interval,
            module_run_id: Arc::new(OnceLock::new()),
        }
    }

    /// Create a disabled observer (for testing or when NATS unavailable)
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            publisher: None,
            materializer: None,
            component: "disabled".to_string(),
            enabled: false,
            metric_emissions: Arc::new(RwLock::new(HashMap::new())),
            min_interval: Duration::from_secs(1),
            module_run_id: Arc::new(OnceLock::new()),
        }
    }

    /// Attach the `core.runs` row ID so every emitted event carries provenance
    /// back to the specific process instance that emitted it.
    ///
    /// Can be called after construction (even after clones are taken) because
    /// the backing `OnceLock` is shared across all clones. Subsequent calls are
    /// silently ignored — the first write wins.
    pub fn set_module_run_id(&self, run_id: sinex_primitives::Uuid) {
        let _ = self.module_run_id.set(run_id);
    }

    /// Check if self-observation is enabled
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.enabled && self.publisher.is_some() && self.materializer.is_some()
    }

    /// Eagerly open the underlying source material and commit its `BEGIN` frame
    /// to the NATS `SOURCE_MATERIAL` `JetStream` before any telemetry events are
    /// emitted.
    ///
    /// # Why this exists (#1241 prong 2)
    ///
    /// `BufferedAppendStreamWriter` creates its source material lazily on the
    /// first `append` call.  When the adapter emits a `metric.gauge` event,
    /// the event carries a `source_material_id` that was assigned by the
    /// materializer at append time.  If the BEGIN frame hasn't been committed to
    /// `JetStream` yet — or if event_engine's `MaterialAssembler` consumer hasn't
    /// processed it yet — event_engine's `MaterialReadySet` pre-check returns false
    /// and the event is NAK'd for retry.  Under a fast startup this retry window
    /// is often exhausted before the material lands, causing DLQ routing.
    ///
    /// Calling `prime()` before storing the observer (and therefore before any
    /// event is published) flushes the BEGIN frame synchronously, so the
    /// material is registered in `JetStream` before the first telemetry event can
    /// reference it.
    ///
    /// Returns `Ok(())` immediately when the observer is disabled.
    pub async fn prime(&self) -> Result<(), SelfObservationError> {
        let Some(materializer) = self.materializer.as_ref() else {
            return Ok(());
        };
        // Publish the BEGIN frame eagerly WITHOUT staging a content byte. The
        // earlier `append(vec![b'\n'])` minted a 1-byte source material per
        // observer instance — with the automata restart churn this produced the
        // ~30K degenerate 1-byte `self-observation.*` materials found in prod
        // (#2184 prong E). The first real telemetry record now anchors at offset 0
        // of a clean, size/age-batched material instead.
        materializer.prime().await.map_err(|e| {
            SelfObservationError::Materialization(format!(
                "failed to prime self-observation material stream: {e}"
            ))
        })?;
        debug!(
            component = %self.component,
            "Primed self-observation material stream (BEGIN frame committed, no content)"
        );
        Ok(())
    }

    fn metric_identity_key(event_type: &str, payload: &JsonValue) -> String {
        let mut parts = vec![event_type.to_string()];

        let Some(payload) = payload.as_object() else {
            return parts.join("|");
        };

        for key in [
            "component",
            "name",
            "stream",
            "pool",
            "module_kind",
            "method",
            "token_prefix",
            "previous_status",
            "current_status",
        ] {
            if let Some(value) = payload.get(key) {
                let value = value
                    .as_str()
                    .map_or_else(|| value.to_string(), ToString::to_string);
                parts.push(format!("{key}={value}"));
            }
        }

        if let Some(labels) = payload.get("labels").and_then(JsonValue::as_object)
            && !labels.is_empty()
        {
            let mut entries: Vec<_> = labels.iter().collect();
            entries.sort_by_key(|(left, _)| *left);
            let labels = entries
                .into_iter()
                .map(|(key, value)| {
                    let value = value
                        .as_str()
                        .map_or_else(|| value.to_string(), ToString::to_string);
                    format!("{key}={value}")
                })
                .collect::<Vec<_>>()
                .join(",");
            parts.push(format!("labels={labels}"));
        }

        parts.join("|")
    }

    async fn reserve_metric_slot(&self, metric_key: &str) -> bool {
        let now = Instant::now();
        let mut emissions = self.metric_emissions.write().await;

        emissions.retain(|_, last| now.duration_since(*last) < self.min_interval);

        if let Some(last) = emissions.get(metric_key)
            && now.duration_since(*last) < self.min_interval
        {
            debug!(
                metric_key = %metric_key,
                "Self-observation rate limited for this metric, skipping emission"
            );
            return false;
        }

        emissions.insert(metric_key.to_string(), now);
        true
    }

    async fn release_metric_slot(&self, metric_key: &str) {
        self.metric_emissions.write().await.remove(metric_key);
    }

    /// Publish a self-observation event to NATS (internal method)
    async fn publish<P: sinex_primitives::events::EventPayload>(
        &self,
        payload: P,
    ) -> Result<(), SelfObservationError> {
        if !self.enabled {
            return Ok(());
        }

        let payload_json = serde_json::to_value(&payload)
            .map_err(|e| SelfObservationError::Serialization(e.to_string()))?;
        let source = P::SOURCE.as_str().to_string();
        let event_type = P::EVENT_TYPE.as_str().to_string();
        let metric_key = Self::metric_identity_key(event_type.as_str(), &payload_json);
        if !self.reserve_metric_slot(&metric_key).await {
            return Ok(());
        }

        let (Some(materializer), Some(publisher)) =
            (self.materializer.as_ref(), self.publisher.as_ref())
        else {
            self.release_metric_slot(&metric_key).await;
            warn!(
                component = %self.component,
                event_type = %event_type,
                "Self-observation enabled but the runtime path is unavailable"
            );
            return Err(SelfObservationError::Unavailable);
        };

        let ts_orig = Timestamp::now();
        let host = sinex_primitives::events::builder::get_hostname();
        let record = SelfObservationRecord {
            component: &self.component,
            source: source.as_str(),
            event_type: event_type.as_str(),
            ts_orig: ts_orig.format_rfc3339(),
            host: host.as_str(),
            payload: &payload_json,
        };

        let anchor = match async {
            let line = stable_json_line(&record)?;
            materializer.append(line).await
        }
        .await
        {
            Ok(anchor) => anchor,
            Err(error) => {
                self.release_metric_slot(&metric_key).await;
                warn!(
                    component = %self.component,
                    event_type = %event_type,
                    error = %error,
                    "Failed to materialize self-observation record"
                );
                return Err(SelfObservationError::Materialization(error.to_string()));
            }
        };

        let mut event = match Event::builder(payload)
            .with_provenance(Provenance::from_material(
                Id::<SourceMaterial>::from_uuid(anchor.material_id),
                anchor.offset_start,
                Some(anchor.offset_start),
                Some(anchor.offset_end),
            ))
            .build()
        {
            Ok(mut event) => {
                event.module_run_id = self.module_run_id.get().copied();
                event.with_timestamp(ts_orig).with_host(host)
            }
            Err(error) => {
                self.release_metric_slot(&metric_key).await;
                return Err(SelfObservationError::Build(error));
            }
        };
        // Mint a fresh random UUIDv7: event ID is interpretation identity, not occurrence
        // identity. ON CONFLICT (id) DO NOTHING dedup works because the id is minted once
        // at publish time and carried unchanged through NATS redelivery.
        let new_id = Id::new();
        event.id = Some(new_id);
        let event_id = new_id.to_uuid();

        let event = match event.to_json_event() {
            Ok(event) => event,
            Err(error) => {
                self.release_metric_slot(&metric_key).await;
                return Err(SelfObservationError::Serialization(error.to_string()));
            }
        };

        if let Err(e) = publisher
            .publish_telemetry(&event, sinex_primitives::transport::Class::Telemetry)
            .await
        {
            self.release_metric_slot(&metric_key).await;
            warn!(
                component = %self.component,
                error = %e,
                "Failed to publish self-observation event"
            );
            return Err(SelfObservationError::Publish(e.to_string()));
        }

        debug!(
            component = %self.component,
            event_type = %event.event_type,
            event_id = %event_id,
            material_id = %anchor.material_id,
            offset_start = anchor.offset_start,
            offset_end = anchor.offset_end,
            "Published self-observation event"
        );

        Ok(())
    }

    // =========================================================================
    // Generic Metric Emission
    // =========================================================================

    /// Emit a counter metric
    pub async fn emit_counter(
        &self,
        name: &str,
        value: u64,
        labels: Option<HashMap<String, String>>,
    ) -> Result<(), SelfObservationError> {
        self.publish(MetricCounterPayload {
            name: name.to_string(),
            value,
            delta: None,
            labels: labels.unwrap_or_default(),
            component: self.component.clone(),
        })
        .await
    }

    /// Emit a counter metric with delta
    pub async fn emit_counter_with_delta(
        &self,
        name: &str,
        value: u64,
        delta: u64,
        labels: Option<HashMap<String, String>>,
    ) -> Result<(), SelfObservationError> {
        self.publish(MetricCounterPayload {
            name: name.to_string(),
            value,
            delta: Some(delta),
            labels: labels.unwrap_or_default(),
            component: self.component.clone(),
        })
        .await
    }

    /// Emit a gauge metric
    pub async fn emit_gauge(
        &self,
        name: &str,
        value: f64,
        labels: Option<HashMap<String, String>>,
    ) -> Result<(), SelfObservationError> {
        self.publish(MetricGaugePayload {
            name: name.to_string(),
            value,
            labels: labels.unwrap_or_default(),
            component: self.component.clone(),
        })
        .await
    }

    /// Emit a histogram metric with percentiles
    pub async fn emit_histogram(
        &self,
        name: &str,
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
        percentiles: Option<(f64, f64, f64, f64)>, // p50, p90, p95, p99
        labels: Option<HashMap<String, String>>,
    ) -> Result<(), SelfObservationError> {
        let (p50, p90, p95, p99) = match percentiles {
            Some((a, b, c, d)) => (Some(a), Some(b), Some(c), Some(d)),
            None => (None, None, None, None),
        };
        self.publish(MetricHistogramPayload {
            name: name.to_string(),
            count,
            sum,
            min,
            max,
            p50,
            p90,
            p95,
            p99,
            labels: labels.unwrap_or_default(),
            component: self.component.clone(),
        })
        .await
    }

    // =========================================================================
    // Specialized Metrics
    // =========================================================================

    /// Emit NATS stream statistics.
    pub async fn emit_stream_stats(
        &self,
        stream: &str,
        messages: u64,
        max_messages: u64,
        bytes: u64,
        max_bytes: u64,
        consumer_count: u32,
        first_seq: u64,
        last_seq: u64,
    ) -> Result<(), SelfObservationError> {
        let pressure =
            StreamPressureSnapshot::from_limits(messages, max_messages, bytes, max_bytes);

        self.publish(StreamStatsPayload {
            stream: stream.to_string(),
            messages,
            max_messages,
            bytes,
            max_bytes,
            consumer_count,
            fill_pct: pressure.fill_pct,
            message_fill_pct: pressure.message_fill_pct,
            byte_fill_pct: pressure.byte_fill_pct,
            pressure_level: pressure.pressure_level,
            limiting_dimension: pressure.limiting_dimension,
            first_seq,
            last_seq,
        })
        .await
    }

    /// Emit material assembly statistics.
    pub async fn emit_assembly_stats(
        &self,
        active: u32,
        started: u64,
        completed: u64,
        cancelled: u64,
        failed: u64,
        timed_out: u64,
        commit_outcome_unknown: u64,
        avg_duration_ms: Option<f64>,
        buffered_slices: u32,
    ) -> Result<(), SelfObservationError> {
        self.publish(AssemblyStatsPayload {
            active_assemblies: active,
            total_started: started,
            total_completed: completed,
            total_cancelled: cancelled,
            total_failed: failed,
            total_timed_out: timed_out,
            total_commit_outcome_unknown: commit_outcome_unknown,
            avg_duration_ms,
            buffered_slices,
        })
        .await
    }

    /// Emit gateway request statistics.
    #[allow(clippy::too_many_arguments)]
    pub async fn emit_gateway_stats(
        &self,
        total: u64,
        successful: u64,
        rejected: u64,
        rate_limited: u64,
        avg_latency_ms: Option<f64>,
        p99_latency_ms: Option<f64>,
        min_latency_ms: Option<f64>,
        max_latency_ms: Option<f64>,
        active_connections: u32,
    ) -> Result<(), SelfObservationError> {
        self.publish(ApiRequestStatsPayload {
            total_requests: total,
            successful_requests: successful,
            rejected_requests: rejected,
            rate_limited_requests: rate_limited,
            avg_latency_ms,
            p99_latency_ms,
            min_latency_ms,
            max_latency_ms,
            active_connections,
        })
        .await
    }

    /// Emit a single completed RPC dispatch as a `gateway.rpc.call` audit
    /// event (#1172 AC-7). The token prefix is recorded for correlation;
    /// the full token is never persisted.
    pub async fn emit_rpc_call(
        &self,
        method: &str,
        role: &str,
        latency_ms: u64,
        status: RpcStatus,
        token_prefix: &str,
    ) -> Result<(), SelfObservationError> {
        // Cap the recorded token prefix at 8 chars defensively even if a
        // caller passed a longer string.
        let prefix = token_prefix.chars().take(8).collect::<String>();
        self.publish(GatewayRpcCallPayload {
            method: method.to_string(),
            role: role.to_string(),
            latency_ms,
            status,
            token_prefix: prefix,
        })
        .await
    }

    /// Emit individual rate limit exceeded event
    pub async fn emit_rate_limit_exceeded(
        &self,
        token_prefix: &str,
        requests_in_window: u64,
        limit: u64,
        method: Option<&str>,
    ) -> Result<(), SelfObservationError> {
        self.publish(RateLimitExceededPayload {
            token_prefix: token_prefix.to_string(),
            requests_in_window,
            limit,
            method: method.map(String::from),
        })
        .await
    }

    /// Emit a health status observation
    pub async fn emit_health_status(
        &self,
        component: &str,
        previous: HealthStatus,
        current: HealthStatus,
        reason: Option<&str>,
    ) -> Result<(), SelfObservationError> {
        self.publish(HealthStatusPayload {
            component: component.to_string(),
            previous_status: previous,
            current_status: current,
            reason: reason.map(String::from),
            context: None,
        })
        .await
    }

    /// Emit connection pool statistics
    pub async fn emit_pool_stats(
        &self,
        pool: &str,
        size: u32,
        idle: u32,
        active: u32,
        pending: u32,
        timeout_count: u64,
    ) -> Result<(), SelfObservationError> {
        self.publish(PoolStatsPayload {
            pool: pool.to_string(),
            size,
            idle,
            active,
            pending,
            timeout_count,
        })
        .await
    }

    /// Emit runtime module processing statistics.
    pub async fn emit_source_processing_stats(
        &self,
        module_kind: &str,
        events_processed: u64,
        events_dropped: u64,
        avg_latency_ms: Option<f64>,
        queue_depth: u32,
        error_count: u64,
    ) -> Result<(), SelfObservationError> {
        self.publish(SourceProcessingStatsPayload {
            module_kind: module_kind.to_string(),
            events_processed,
            events_dropped,
            avg_latency_ms,
            queue_depth,
            error_count,
        })
        .await
    }

    /// Emit a per-event latency snapshot for a automaton.
    ///
    /// One event collapses what was previously six separate `metric.gauge`
    /// emissions (`derived.event_lag_ms`, `derived.tick_runtime_ms`, the two
    /// `event_lag_p{50,99}_ms` reservoir percentiles, `derived.tick_runtime_p99_ms`,
    /// and `derived.throughput_eps`). See issue #1556.
    pub async fn emit_automaton_latency_snapshot(
        &self,
        module_name: &str,
        event_lag_ms: Option<f64>,
        tick_runtime_ms: Option<f64>,
        event_lag_p50_ms: Option<f64>,
        event_lag_p99_ms: Option<f64>,
        tick_runtime_p99_ms: Option<f64>,
        throughput_eps: f64,
        labels: HashMap<String, String>,
    ) -> Result<(), SelfObservationError> {
        self.publish(AutomatonLatencySnapshotPayload {
            module_name: module_name.to_string(),
            event_lag_ms,
            tick_runtime_ms,
            event_lag_p50_ms,
            event_lag_p99_ms,
            tick_runtime_p99_ms,
            throughput_eps,
            labels,
        })
        .await
    }

    /// Emit replay statistics.
    pub async fn emit_replay_stats(
        &self,
        total: u64,
        successful: u64,
        failed: u64,
        avg_duration_ms: Option<f64>,
        events_affected: u64,
    ) -> Result<(), SelfObservationError> {
        self.publish(ReplayStatsPayload {
            total_requests: total,
            successful,
            failed,
            avg_duration_ms,
            events_affected,
        })
        .await
    }

    /// Emit event_engine batch processing statistics.
    #[allow(clippy::too_many_arguments)]
    pub async fn emit_event_engine_batch_stats(
        &self,
        batch_size: u32,
        fetch_to_ack_ms: u64,
        events_deferred: u32,
        events_failed: u32,
        had_derived: bool,
        insert_path: &str,
        validation_valid: u64,
        validation_skipped: u64,
        validation_no_schema: u64,
        validation_schema_not_found: u64,
        validation_invalid: u64,
        validation_coverage_pct: f64,
        suspicious_future_ts_orig: u64,
        telemetry_publish_failures: u64,
        confirmation_durability_gaps: u64,
    ) -> Result<(), SelfObservationError> {
        self.publish(EventEngineBatchStatsPayload {
            batch_size,
            fetch_to_ack_ms,
            events_deferred,
            events_failed,
            had_derived,
            insert_path: insert_path.to_string(),
            validation_valid,
            validation_skipped,
            validation_no_schema,
            validation_schema_not_found,
            validation_invalid,
            validation_coverage_pct,
            suspicious_future_ts_orig,
            telemetry_publish_failures,
            confirmation_durability_gaps,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn emit_consumer_startup_snapshot(
        &self,
        stream_name: String,
        durable_name: String,
        consumer_existed: bool,
        deliver_policy: String,
        stream_messages: u64,
        stream_bytes: u64,
        stream_first_sequence: u64,
        stream_last_sequence: u64,
        stream_max_messages: u64,
        stream_max_bytes: u64,
        stream_max_age_secs: u64,
        consumer_pending: u64,
        consumer_ack_pending: usize,
        consumer_redelivered: usize,
        consumer_max_ack_pending: i64,
        consumer_max_deliver: i64,
        initial_replay_risk: bool,
    ) -> Result<(), SelfObservationError> {
        self.publish(ConsumerStartupSnapshotPayload {
            stream_name,
            durable_name,
            consumer_existed,
            deliver_policy,
            stream_messages,
            stream_bytes,
            stream_first_sequence,
            stream_last_sequence,
            stream_max_messages,
            stream_max_bytes,
            stream_max_age_secs,
            consumer_pending,
            consumer_ack_pending,
            consumer_redelivered,
            consumer_max_ack_pending,
            consumer_max_deliver,
            initial_replay_risk,
        })
        .await
    }
}

/// Errors from self-observation emission
#[derive(Debug)]
pub enum SelfObservationError {
    Build(SinexError),
    Serialization(String),
    Materialization(String),
    Unavailable,
    Publish(String),
}

impl std::fmt::Display for SelfObservationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Build(_) => write!(f, "Failed to build self-observation event"),
            Self::Serialization(message) => write!(f, "Failed to serialize event: {message}"),
            Self::Materialization(message) => {
                write!(f, "Self-observation materialization failed: {message}")
            }
            Self::Unavailable => write!(
                f,
                "Self-observation is enabled but the runtime path is unavailable"
            ),
            Self::Publish(message) => write!(f, "Failed to publish event: {message}"),
        }
    }
}

impl std::error::Error for SelfObservationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Build(error) => Some(error),
            _ => None,
        }
    }
}

impl From<SelfObservationError> for SinexError {
    fn from(err: SelfObservationError) -> Self {
        match err {
            SelfObservationError::Build(inner) => {
                SinexError::processing("failed to build self-observation event").with_source(&inner)
            }
            SelfObservationError::Serialization(ref msg) => {
                SinexError::serialization("failed to serialize self-observation event")
                    .with_context("detail", msg)
            }
            SelfObservationError::Materialization(ref msg) => {
                SinexError::processing("self-observation materialization failed")
                    .with_context("detail", msg)
            }
            SelfObservationError::Unavailable => SinexError::invalid_state(
                "self-observation is enabled but the runtime path is unavailable",
            ),
            SelfObservationError::Publish(ref msg) => {
                SinexError::processing("failed to publish self-observation event")
                    .with_context("detail", msg)
            }
        }
    }
}

#[derive(serde::Serialize)]
struct SelfObservationRecord<'a> {
    component: &'a str,
    source: &'a str,
    event_type: &'a str,
    ts_orig: String,
    host: &'a str,
    payload: &'a JsonValue,
}

fn self_observation_source_identifier(component: &str) -> String {
    format!("sinex.self-observation.{component}")
}

/// Serialize a record as a newline-terminated JSON line for stable, byte-anchored
/// appends to the source-material stream.
fn stable_json_line<T>(record: &T) -> Result<Vec<u8>, SinexError>
where
    T: serde::Serialize + ?Sized,
{
    let mut data = serde_json::to_vec(record).map_err(|error| {
        SinexError::serialization("failed to serialize self-observation record")
            .with_std_error(&error)
    })?;
    data.push(b'\n');
    Ok(data)
}

/// Background task for periodic metric emission
pub struct SelfObservationTask {
    observer: SelfObserver,
    interval: Duration,
    cancel: tokio::sync::watch::Receiver<bool>,
}

impl SelfObservationTask {
    /// Create a new background observation task
    #[must_use]
    pub fn new(
        observer: SelfObserver,
        interval: Duration,
        cancel: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        Self {
            observer,
            interval,
            cancel,
        }
    }

    /// Run with a custom metrics collector function
    pub async fn run<F, Fut>(mut self, collect_metrics: F)
    where
        F: Fn(&SelfObserver) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let mut interval = tokio::time::interval(self.interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if self.observer.is_enabled() {
                        collect_metrics(&self.observer).await;
                    }
                }
                _ = self.cancel.changed() => {
                    if *self.cancel.borrow() {
                        debug!("Self-observation task cancelled");
                        break;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "self_observation_test.rs"]
mod tests;
