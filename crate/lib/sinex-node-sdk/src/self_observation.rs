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
//! let observer = SelfObserver::new(nats_client, "sinex-gateway".to_string());
//!
//! // Emit metrics periodically
//! observer.emit_counter("requests.total", 1000, None).await?;
//! observer.emit_gauge("connections.active", 42.0, None).await?;
//! ```

use async_nats::Client as NatsClient;
use sinex_primitives::events::payloads::{
    AssemblyStatsPayload, GatewayRequestStatsPayload, HealthStatusPayload, MetricCounterPayload,
    MetricGaugePayload, MetricHistogramPayload, NodeProcessingStatsPayload, PoolStatsPayload,
    RateLimitExceededPayload, ReplayStatsPayload, StreamStatsPayload,
};
use sinex_primitives::events::{Event, EventId, Provenance};
use sinex_primitives::Ulid;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, warn};

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
    /// Per-metric emission tracking (metric_key -> last_emission_time)
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
    pub fn from_env(component: &str) -> Self {
        let enabled = std::env::var("SINEX_SELF_OBSERVATION_ENABLED")
            .map_or(true, |v| v.to_lowercase() != "false" && v != "0");

        let min_interval_secs = std::env::var("SINEX_SELF_OBSERVATION_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1);

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
    pub fn is_enabled(&self) -> bool {
        self.enabled && self.nats_client.is_some()
    }

    /// Create provenance for self-observation events
    ///
    /// Self-observation events are synthetic with a self-referential source.
    /// We use a new ULID as the "source" event ID, following the pattern
    /// used elsewhere in the codebase for internally-generated events.
    fn self_provenance(&self) -> Provenance {
        Provenance::from_synthesis_safe(EventId::from_ulid(Ulid::new()), Vec::new())
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

        let metric_key = event.event_type.to_string();

        // Per-metric rate limiting check
        {
            let emissions = self.metric_emissions.read().await;
            if let Some(last) = emissions.get(&metric_key) {
                if last.elapsed() < self.min_interval {
                    debug!(
                        event_type = %metric_key,
                        "Self-observation rate limited for this metric, skipping emission"
                    );
                    return Ok(());
                }
            }
        }

        // Update last emission time for this metric
        {
            let mut emissions = self.metric_emissions.write().await;
            emissions.insert(metric_key.clone(), Instant::now());
        }

        let subject = format!("{}.{}", self.subject_prefix, self.component);
        let data = serde_json::to_vec(&event)
            .map_err(|e| SelfObservationError::Serialization(e.to_string()))?;

        // Publish to NATS (without waiting for ack for telemetry)
        let Some(ref nats_client) = self.nats_client else {
            return Ok(()); // No client, silently skip
        };

        if let Err(e) = nats_client.publish(subject.clone(), data.into()).await {
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
    // Specialized Metrics (Issue-specific)
    // =========================================================================

    /// Emit NATS stream statistics (Issue 3)
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

    /// Emit material assembly statistics (Issue 16)
    pub async fn emit_assembly_stats(
        &self,
        active: u32,
        started: u64,
        completed: u64,
        failed: u64,
        timed_out: u64,
        avg_duration_ms: Option<f64>,
        buffered_slices: u32,
    ) -> Result<(), SelfObservationError> {
        self.publish(AssemblyStatsPayload {
            active_assemblies: active,
            total_started: started,
            total_completed: completed,
            total_failed: failed,
            total_timed_out: timed_out,
            avg_duration_ms,
            buffered_slices,
        })
        .await
    }

    /// Emit gateway request statistics (Issue 133)
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

    /// Emit node processing statistics (Issues 24, 29)
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

    /// Emit replay statistics (Issue 145)
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
}

/// Errors from self-observation emission
#[derive(Debug, thiserror::Error)]
pub enum SelfObservationError {
    #[error("Failed to serialize event: {0}")]
    Serialization(String),
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

    #[sinex_test]
    async fn test_config_defaults() -> TestResult<()> {
        let config = SelfObserverConfig::default();
        assert!(config.enabled);
        assert_eq!(config.min_emission_interval, Duration::from_secs(1));
        Ok(())
    }
}
