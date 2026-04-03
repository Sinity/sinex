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
//! use sinex_node_sdk::self_observation::SelfObserver;
//!
//! // Create an observer for the gateway component
//! let observer = SelfObserver::new(
//!     nats_client,
//!     SelfObserverConfig::from_env("sinex-gateway"),
//! );
//!
//! // Emit metrics periodically
//! observer.emit_counter("requests.total", 1000, None).await?;
//! observer.emit_gauge("connections.active", 42.0, None).await?;
//! ```

use crate::error_helpers::{env_bool_with_default, env_parse_with_default};
use async_nats::Client as NatsClient;
use sinex_primitives::JsonValue;
use sinex_primitives::events::payloads::{
    AssemblyStatsPayload, GatewayRequestStatsPayload, HealthStatusPayload,
    IngestdBatchStatsPayload, MetricCounterPayload, MetricGaugePayload, MetricHistogramPayload,
    NodeProcessingStatsPayload, PoolStatsPayload, RateLimitExceededPayload, ReplayStatsPayload,
    StreamStatsPayload,
};
use sinex_primitives::events::{Event, EventId, Provenance};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, warn};
use uuid::Uuid;

/// Self-observation event emitter
///
/// Provides methods for Sinex components to emit internal telemetry as events.
#[derive(Clone, Debug)]
pub struct SelfObserver {
    /// NATS client for publishing (None when disabled)
    nats_client: Option<NatsClient>,
    /// Component name
    component: String,
    /// Subject prefix for publishing
    subject_prefix: String,
    /// Whether self-observation is enabled
    enabled: bool,
    /// Per-metric emission tracking (`metric_key` -> `last_emission_time`)
    metric_emissions: Arc<RwLock<HashMap<String, Instant>>>,
    /// Minimum interval between emissions (rate limiting)
    min_interval: Duration,
}

/// Configuration for self-observation
#[derive(Debug, Clone)]
pub struct SelfObserverConfig {
    /// Component name (e.g., "sinex-gateway", "sinex-ingestd")
    pub component: String,
    /// NATS subject prefix (default: "sinex.telemetry")
    pub subject_prefix: String,
    /// Enable self-observation
    pub enabled: bool,
    /// Minimum interval between emissions (default: 1s)
    pub min_emission_interval: Duration,
}

impl Default for SelfObserverConfig {
    fn default() -> Self {
        Self {
            component: "sinex-unknown".to_string(),
            subject_prefix: "sinex.telemetry".to_string(),
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
            env_bool_with_default("SINEX_SELF_OBSERVATION_ENABLED", true, "self-observation");
        let min_interval_secs = env_parse_with_default(
            "SINEX_SELF_OBSERVATION_INTERVAL_SECS",
            1_u64,
            "self-observation",
        );

        Self {
            component: component.to_string(),
            subject_prefix: "sinex.telemetry".to_string(),
            enabled,
            min_emission_interval: Duration::from_secs(min_interval_secs),
        }
    }
}

impl SelfObserver {
    /// Create a new self-observer for a component
    #[must_use]
    pub fn new(nats_client: NatsClient, config: SelfObserverConfig) -> Self {
        Self {
            nats_client: Some(nats_client),
            component: config.component,
            subject_prefix: config.subject_prefix,
            enabled: config.enabled,
            metric_emissions: Arc::new(RwLock::new(HashMap::new())),
            min_interval: config.min_emission_interval,
        }
    }

    /// Create a disabled observer (for testing or when NATS unavailable)
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            nats_client: None,
            component: "disabled".to_string(),
            subject_prefix: "sinex.telemetry".to_string(),
            enabled: false,
            metric_emissions: Arc::new(RwLock::new(HashMap::new())),
            min_interval: Duration::from_secs(1),
        }
    }

    /// Check if self-observation is enabled
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.enabled && self.nats_client.is_some()
    }

    /// Create provenance for self-observation events
    ///
    /// Self-observation events are synthetic with a self-referential source.
    /// We use a new `UUIDv7` as the "source" event ID, following the pattern
    /// used elsewhere in the codebase for internally-generated events.
    fn self_provenance(&self) -> Provenance {
        Provenance::from_synthesis_safe(EventId::from_uuid(Uuid::now_v7()), Vec::new())
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
            "node_type",
            "method",
            "token_prefix",
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
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
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

        let event = Event::new(payload, self.self_provenance())
            .to_json_event()
            .map_err(|e| SelfObservationError::Serialization(e.to_string()))?;

        let metric_key = Self::metric_identity_key(&event.event_type.to_string(), &event.payload);
        if !self.reserve_metric_slot(&metric_key).await {
            return Ok(());
        }

        let subject = format!("{}.{}", self.subject_prefix, self.component);
        let data = serde_json::to_vec(&event)
            .map_err(|e| SelfObservationError::Serialization(e.to_string()))?;

        // Publish to NATS (without waiting for ack for telemetry)
        let Some(ref nats_client) = self.nats_client else {
            self.release_metric_slot(&metric_key).await;
            warn!(
                component = %self.component,
                event_type = %event.event_type,
                "Self-observation enabled but no NATS client is available"
            );
            return Err(SelfObservationError::Unavailable);
        };

        if let Err(e) = nats_client.publish(subject.clone(), data.into()).await {
            self.release_metric_slot(&metric_key).await;
            warn!(
                component = %self.component,
                subject = %subject,
                error = %e,
                "Failed to publish self-observation event"
            );
            return Err(SelfObservationError::Publish(e.to_string()));
        }

        debug!(
            component = %self.component,
            event_type = %event.event_type,
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
        let fill_pct = if max_messages > 0 {
            (messages as f64 / max_messages as f64) * 100.0
        } else if max_bytes > 0 {
            (bytes as f64 / max_bytes as f64) * 100.0
        } else {
            0.0
        };

        self.publish(StreamStatsPayload {
            stream: stream.to_string(),
            messages,
            max_messages,
            bytes,
            max_bytes,
            consumer_count,
            fill_pct,
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
            avg_duration_ms,
            buffered_slices,
        })
        .await
    }

    /// Emit gateway request statistics.
    pub async fn emit_gateway_stats(
        &self,
        total: u64,
        successful: u64,
        rejected: u64,
        rate_limited: u64,
        avg_latency_ms: Option<f64>,
        p99_latency_ms: Option<f64>,
        active_connections: u32,
    ) -> Result<(), SelfObservationError> {
        self.publish(GatewayRequestStatsPayload {
            total_requests: total,
            successful_requests: successful,
            rejected_requests: rejected,
            rate_limited_requests: rate_limited,
            avg_latency_ms,
            p99_latency_ms,
            active_connections,
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

    /// Emit health status change
    pub async fn emit_health_status(
        &self,
        component: &str,
        previous: &str,
        current: &str,
        reason: Option<&str>,
    ) -> Result<(), SelfObservationError> {
        self.publish(HealthStatusPayload {
            component: component.to_string(),
            previous_status: previous.to_string(),
            current_status: current.to_string(),
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

    /// Emit node processing statistics.
    pub async fn emit_node_processing_stats(
        &self,
        node_type: &str,
        events_processed: u64,
        events_dropped: u64,
        avg_latency_ms: Option<f64>,
        queue_depth: u32,
        error_count: u64,
    ) -> Result<(), SelfObservationError> {
        self.publish(NodeProcessingStatsPayload {
            node_type: node_type.to_string(),
            events_processed,
            events_dropped,
            avg_latency_ms,
            queue_depth,
            error_count,
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

    /// Emit ingestd batch processing statistics.
    #[allow(clippy::too_many_arguments)]
    pub async fn emit_ingestd_batch_stats(
        &self,
        batch_size: u32,
        fetch_to_ack_ms: u64,
        events_deferred: u32,
        events_failed: u32,
        had_synthesis: bool,
        insert_path: &str,
        validation_valid: u64,
        validation_skipped: u64,
        validation_no_schema: u64,
        validation_schema_not_found: u64,
        validation_invalid: u64,
        validation_coverage_pct: f64,
        suspicious_future_ts_orig: u64,
    ) -> Result<(), SelfObservationError> {
        self.publish(IngestdBatchStatsPayload {
            batch_size,
            fetch_to_ack_ms,
            events_deferred,
            events_failed,
            had_synthesis,
            insert_path: insert_path.to_string(),
            validation_valid,
            validation_skipped,
            validation_no_schema,
            validation_schema_not_found,
            validation_invalid,
            validation_coverage_pct,
            suspicious_future_ts_orig,
        })
        .await
    }
}

/// Errors from self-observation emission
#[derive(Debug, thiserror::Error)]
pub enum SelfObservationError {
    #[error("Failed to serialize event: {0}")]
    Serialization(String),
    #[error("Self-observation is enabled but no NATS client is available")]
    Unavailable,
    #[error("Failed to publish event: {0}")]
    Publish(String),
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
mod tests {
    use super::*;
    use xtask::sandbox::prelude::*;

    fn test_observer() -> SelfObserver {
        SelfObserver {
            nats_client: None,
            component: "test-component".to_string(),
            subject_prefix: "sinex.telemetry".to_string(),
            enabled: true,
            metric_emissions: Arc::new(RwLock::new(HashMap::new())),
            min_interval: Duration::from_secs(1),
        }
    }

    #[sinex_test]
    async fn test_metric_identity_key_distinguishes_name_and_labels() -> TestResult<()> {
        let first = JsonValue::Object(
            serde_json::json!({
                "component": "ingestd",
                "name": "ingestd.consumer.lag.pending",
                "labels": { "consumer": "alpha" },
                "value": 1.0
            })
            .as_object()
            .cloned()
            .expect("json object"),
        );
        let second = JsonValue::Object(
            serde_json::json!({
                "component": "ingestd",
                "name": "ingestd.consumer.lag.ack_pending",
                "labels": { "consumer": "alpha" },
                "value": 1.0
            })
            .as_object()
            .cloned()
            .expect("json object"),
        );

        let first_key = SelfObserver::metric_identity_key("metric.gauge", &first);
        let second_key = SelfObserver::metric_identity_key("metric.gauge", &second);

        assert_ne!(first_key, second_key);
        Ok(())
    }

    #[sinex_test]
    async fn test_metric_reservations_are_per_metric_identity() -> TestResult<()> {
        let observer = test_observer();

        assert!(
            observer
                .reserve_metric_slot("metric.counter|name=assembly_started")
                .await
        );
        assert!(
            observer
                .reserve_metric_slot("metric.counter|name=assembly_completed")
                .await
        );
        assert!(
            !observer
                .reserve_metric_slot("metric.counter|name=assembly_started")
                .await
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_release_metric_slot_clears_failed_publish_reservation() -> TestResult<()> {
        let observer = test_observer();
        let key = "metric.counter|name=assembly_completed";

        assert!(observer.reserve_metric_slot(key).await);
        observer.release_metric_slot(key).await;
        assert!(observer.reserve_metric_slot(key).await);
        Ok(())
    }

    #[sinex_test]
    async fn test_publish_fails_honestly_without_nats_client() -> TestResult<()> {
        let observer = test_observer();

        let first_error = observer
            .emit_counter("requests.total", 1, None)
            .await
            .expect_err("expected missing NATS client to fail");
        assert!(matches!(first_error, SelfObservationError::Unavailable));

        let second_error = observer
            .emit_counter("requests.total", 1, None)
            .await
            .expect_err("expected reservation to be released after missing client");
        assert!(matches!(second_error, SelfObservationError::Unavailable));
        Ok(())
    }
}
