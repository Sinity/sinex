//! Gateway self-observation metrics
//!
//! Collects and emits internal metrics for the gateway using Sinex's
//! self-observation architecture. Metrics are emitted as events to NATS
//! and stored in core.events for querying.
//!
//! # Design
//!
//! Request handling stays lightweight:
//! - Atomic increments on hot path (nanoseconds)
//! - Background task emits aggregated metrics every 10 seconds
//! - No allocations or locks on request path
//!
//! # Metrics Collected
//!
//! - `requests.total` - Total requests received
//! - `requests.successful` - Requests that returned 200 OK
//! - `requests.rejected` - Requests rejected (auth, rate limit, errors)
//! - `requests.rate_limited` - Subset of rejected that were rate limited
//! - `latency` - Request processing latency histogram

use sinex_node_sdk::{SelfObservationError, SelfObserver, SelfObserverConfig};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// Gateway metrics collector
///
/// Thread-safe counters for request tracking. All operations on the hot path
/// use relaxed atomic ordering for maximum performance.
#[derive(Debug)]
pub struct GatewayMetrics {
    /// Total requests received
    total_requests: AtomicU64,
    /// Requests that completed successfully (200 OK)
    successful_requests: AtomicU64,
    /// Requests rejected (any reason)
    rejected_requests: AtomicU64,
    /// Requests rejected due to rate limiting
    rate_limited_requests: AtomicU64,
    /// Sum of latencies in microseconds (for computing average)
    latency_sum_us: AtomicU64,
    /// Count of latency samples
    latency_count: AtomicU64,
    /// Minimum latency in microseconds
    latency_min_us: AtomicU64,
    /// Maximum latency in microseconds
    latency_max_us: AtomicU64,
    /// Current active connections (gauge)
    active_connections: AtomicU32,
    /// Self-observer for emitting events
    observer: SelfObserver,
}

impl GatewayMetrics {
    /// Create new gateway metrics collector
    #[must_use]
    pub fn new(nats_client: async_nats::Client) -> Self {
        let config = SelfObserverConfig::from_env("sinex-gateway");
        Self {
            total_requests: AtomicU64::new(0),
            successful_requests: AtomicU64::new(0),
            rejected_requests: AtomicU64::new(0),
            rate_limited_requests: AtomicU64::new(0),
            latency_sum_us: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            latency_min_us: AtomicU64::new(u64::MAX),
            latency_max_us: AtomicU64::new(0),
            active_connections: AtomicU32::new(0),
            observer: SelfObserver::new(nats_client, config),
        }
    }

    /// Create disabled metrics (no NATS connection)
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            successful_requests: AtomicU64::new(0),
            rejected_requests: AtomicU64::new(0),
            rate_limited_requests: AtomicU64::new(0),
            latency_sum_us: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            latency_min_us: AtomicU64::new(u64::MAX),
            latency_max_us: AtomicU64::new(0),
            active_connections: AtomicU32::new(0),
            observer: SelfObserver::disabled(),
        }
    }

    /// Check if metrics are enabled
    pub fn is_enabled(&self) -> bool {
        self.observer.is_enabled()
    }

    /// Record a request started
    #[inline]
    pub fn record_request_start(&self) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful request
    #[inline]
    pub fn record_request_success(&self, latency_us: u64) {
        self.successful_requests.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
        self.record_latency(latency_us);
    }

    /// Record a rejected request
    #[inline]
    pub fn record_request_rejected(&self) {
        self.rejected_requests.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a rate-limited request
    #[inline]
    pub fn record_rate_limited(&self) {
        self.rate_limited_requests.fetch_add(1, Ordering::Relaxed);
        self.rejected_requests.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_sub(1, Ordering::Relaxed);

        // Optimisation: Do NOT spawn task for every rate limit.
        // We rely on the atomic counter `rate_limited_requests` which is aggregated
        // and emitted by the background task every 10s.
        // This prevents an event storm DoS during high load.
        /*
        if self.observer.is_enabled() {
            let token = token_prefix.to_string();
            let observer = self.observer.clone();
            tokio::spawn(async move { ... });
        }
        */
    }

    /// Record latency sample
    #[inline]
    fn record_latency(&self, latency_us: u64) {
        self.latency_sum_us.fetch_add(latency_us, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);

        // Update min/max using compare-and-swap loops
        let mut current_min = self.latency_min_us.load(Ordering::Relaxed);
        while latency_us < current_min {
            match self.latency_min_us.compare_exchange_weak(
                current_min,
                latency_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_min = actual,
            }
        }

        let mut current_max = self.latency_max_us.load(Ordering::Relaxed);
        while latency_us > current_max {
            match self.latency_max_us.compare_exchange_weak(
                current_max,
                latency_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }
    }

    /// Get current metrics snapshot and reset counters
    fn snapshot_and_reset(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            total_requests: self.total_requests.swap(0, Ordering::Relaxed),
            successful_requests: self.successful_requests.swap(0, Ordering::Relaxed),
            rejected_requests: self.rejected_requests.swap(0, Ordering::Relaxed),
            rate_limited_requests: self.rate_limited_requests.swap(0, Ordering::Relaxed),
            latency_sum_us: self.latency_sum_us.swap(0, Ordering::Relaxed),
            latency_count: self.latency_count.swap(0, Ordering::Relaxed),
            latency_min_us: self.latency_min_us.swap(u64::MAX, Ordering::Relaxed),
            latency_max_us: self.latency_max_us.swap(0, Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
        }
    }

    fn restore_from_snapshot(&self, snapshot: &MetricsSnapshot) {
        self.total_requests
            .fetch_add(snapshot.total_requests, Ordering::Relaxed);
        self.successful_requests
            .fetch_add(snapshot.successful_requests, Ordering::Relaxed);
        self.rejected_requests
            .fetch_add(snapshot.rejected_requests, Ordering::Relaxed);
        self.rate_limited_requests
            .fetch_add(snapshot.rate_limited_requests, Ordering::Relaxed);
        self.latency_sum_us
            .fetch_add(snapshot.latency_sum_us, Ordering::Relaxed);
        self.latency_count
            .fetch_add(snapshot.latency_count, Ordering::Relaxed);

        if snapshot.latency_count > 0 {
            self.restore_latency_min(snapshot.latency_min_us);
            self.restore_latency_max(snapshot.latency_max_us);
        }
    }

    fn restore_latency_min(&self, latency_us: u64) {
        let mut current_min = self.latency_min_us.load(Ordering::Relaxed);
        while latency_us < current_min {
            match self.latency_min_us.compare_exchange_weak(
                current_min,
                latency_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_min = actual,
            }
        }
    }

    fn restore_latency_max(&self, latency_us: u64) {
        let mut current_max = self.latency_max_us.load(Ordering::Relaxed);
        while latency_us > current_max {
            match self.latency_max_us.compare_exchange_weak(
                current_max,
                latency_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }
    }

    /// Emit current metrics to self-observation system
    async fn emit_metrics(&self) -> Result<(), SelfObservationError> {
        let snapshot = self.snapshot_and_reset();

        // Skip if no requests in this interval
        if snapshot.total_requests == 0 {
            return Ok(());
        }

        let avg_latency_ms = if snapshot.latency_count > 0 {
            Some((snapshot.latency_sum_us as f64 / snapshot.latency_count as f64) / 1000.0)
        } else {
            None
        };

        // We don't have p99 without a histogram, so skip it
        if let Err(error) = self
            .observer
            .emit_gateway_stats(
                snapshot.total_requests,
                snapshot.successful_requests,
                snapshot.rejected_requests,
                snapshot.rate_limited_requests,
                avg_latency_ms,
                None, // p99 requires histogram
                snapshot.active_connections,
            )
            .await
        {
            self.restore_from_snapshot(&snapshot);
            return Err(error);
        }

        Ok(())
    }

    /// Spawn background metrics emission task
    ///
    /// Emits aggregated metrics every 10 seconds.
    pub fn spawn_emission_task(
        self: Arc<Self>,
        cancel: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            let mut cancel = cancel;

            info!("Gateway metrics emission task started");

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if self.observer.is_enabled()
                            && let Err(e) = self.emit_metrics().await {
                                warn!("Failed to emit gateway metrics: {}", e);
                            }
                    }
                    cancel_result = cancel.changed() => {
                        if cancel_result.is_err() {
                            warn!("Gateway metrics shutdown channel dropped before explicit shutdown");
                        }
                        if cancel_result.is_err() || *cancel.borrow() {
                            debug!("Gateway metrics emission task cancelled");
                            // Final emission before shutdown
                            if self.observer.is_enabled()
                                && let Err(error) = self.emit_metrics().await {
                                    warn!("Failed to emit gateway metrics during shutdown: {}", error);
                            }
                            break;
                        }
                    }
                }
            }
        })
    }
}

/// Snapshot of metrics for emission
#[derive(Debug)]
struct MetricsSnapshot {
    total_requests: u64,
    successful_requests: u64,
    rejected_requests: u64,
    rate_limited_requests: u64,
    latency_sum_us: u64,
    latency_count: u64,
    /// Min latency - collected for future histogram support
    #[allow(dead_code)]
    latency_min_us: u64,
    /// Max latency - collected for future histogram support
    #[allow(dead_code)]
    latency_max_us: u64,
    active_connections: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn test_metrics_collection() -> TestResult<()> {
        let metrics = GatewayMetrics::disabled();

        metrics.record_request_start();
        metrics.record_request_success(1000); // 1ms

        let snapshot = metrics.snapshot_and_reset();
        assert_eq!(snapshot.total_requests, 1);
        assert_eq!(snapshot.successful_requests, 1);
        assert_eq!(snapshot.latency_count, 1);
        assert_eq!(snapshot.latency_sum_us, 1000);
        Ok(())
    }

    #[sinex_test]
    async fn test_latency_min_max() -> TestResult<()> {
        let metrics = GatewayMetrics::disabled();

        metrics.record_request_start();
        metrics.record_request_success(500); // 0.5ms

        metrics.record_request_start();
        metrics.record_request_success(2000); // 2ms

        metrics.record_request_start();
        metrics.record_request_success(1000); // 1ms

        let snapshot = metrics.snapshot_and_reset();
        assert_eq!(snapshot.latency_min_us, 500);
        assert_eq!(snapshot.latency_max_us, 2000);
        assert_eq!(snapshot.latency_count, 3);
        Ok(())
    }

    #[sinex_test]
    async fn test_restore_snapshot_preserves_failed_interval_and_new_traffic() -> TestResult<()> {
        let metrics = GatewayMetrics::disabled();

        metrics.record_request_start();
        metrics.record_request_success(500);

        let failed_snapshot = metrics.snapshot_and_reset();

        metrics.record_request_start();
        metrics.record_request_success(2000);

        metrics.restore_from_snapshot(&failed_snapshot);

        let snapshot = metrics.snapshot_and_reset();
        assert_eq!(snapshot.total_requests, 2);
        assert_eq!(snapshot.successful_requests, 2);
        assert_eq!(snapshot.latency_count, 2);
        assert_eq!(snapshot.latency_sum_us, 2500);
        assert_eq!(snapshot.latency_min_us, 500);
        assert_eq!(snapshot.latency_max_us, 2000);
        Ok(())
    }

    #[sinex_test]
    async fn emission_task_exits_when_shutdown_sender_is_dropped() -> TestResult<()> {
        let metrics = Arc::new(GatewayMetrics::disabled());
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let handle = metrics.spawn_emission_task(cancel_rx);

        drop(cancel_tx);

        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .map_err(|_| {
                color_eyre::eyre::eyre!("metrics task should exit after shutdown sender drops")
            })??;
        Ok(())
    }
}
