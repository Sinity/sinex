//! Standardized health reporting for all nodes
//!
//! Provides uniform health tracking that automatically monitors success/error rates
//! and emits health.status events via SelfObserver when status changes.

use crate::self_observation::SelfObserver;
use sinex_core::types::{error::SinexError, events::payloads::process::ProcessStatus, Result};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
};

/// Atomic counters for health metrics
#[derive(Debug, Default)]
pub struct HealthMetrics {
    pub events_processed: AtomicU64,
    pub errors: AtomicU64,
    pub warnings: AtomicU64,
    pub last_error_time: AtomicU64, // Unix timestamp in seconds
}

impl HealthMetrics {
    /// Calculate error rate over the sliding window
    pub fn error_rate(&self, window_seconds: u64) -> f64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let last_error = self.last_error_time.load(Ordering::Relaxed);

        // If last error is outside the window, error rate is 0
        if last_error > 0 && now.saturating_sub(last_error) > window_seconds {
            return 0.0;
        }

        let total = self.events_processed.load(Ordering::Relaxed);
        let errors = self.errors.load(Ordering::Relaxed);

        if total == 0 {
            0.0
        } else {
            errors as f64 / total as f64
        }
    }
}

/// Configuration thresholds for health status determination
#[derive(Debug, Clone)]
pub struct HealthThresholds {
    /// Error rate threshold for degraded status (e.g., 0.05 = 5%)
    pub error_rate_degraded: f64,
    /// Error rate threshold for failed status (e.g., 0.20 = 20%)
    pub error_rate_failed: f64,
    /// Sliding window for error rate calculation (in seconds)
    pub window_seconds: u64,
}

impl Default for HealthThresholds {
    fn default() -> Self {
        Self {
            error_rate_degraded: 0.05, // 5%
            error_rate_failed: 0.20,   // 20%
            window_seconds: 300,       // 5 minutes
        }
    }
}

impl HealthThresholds {
    /// Load thresholds from environment variables
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            error_rate_degraded: std::env::var("SINEX_HEALTH_ERROR_RATE_DEGRADED")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.05),
            error_rate_failed: std::env::var("SINEX_HEALTH_ERROR_RATE_FAILED")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.20),
            window_seconds: std::env::var("SINEX_HEALTH_WINDOW_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300),
        })
    }
}

/// Standardized health reporter for nodes
///
/// Tracks events/errors and automatically emits health.status events
/// when the component's health status changes.
#[derive(Debug)]
pub struct HealthReporter {
    component_name: String,
    observer: Arc<SelfObserver>,
    metrics: Arc<HealthMetrics>,
    last_status: Arc<RwLock<ProcessStatus>>,
    thresholds: HealthThresholds,
}

impl HealthReporter {
    /// Create a new health reporter
    pub fn new(
        component_name: String,
        observer: Arc<SelfObserver>,
        thresholds: HealthThresholds,
    ) -> Self {
        Self {
            component_name,
            observer,
            metrics: Arc::new(HealthMetrics::default()),
            last_status: Arc::new(RwLock::new(ProcessStatus::Healthy)),
            thresholds,
        }
    }

    /// Record a successful event processing
    pub fn record_success(&self) {
        self.metrics
            .events_processed
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record an error with context
    pub fn record_error(&self, _error: &SinexError) {
        self.metrics
            .events_processed
            .fetch_add(1, Ordering::Relaxed);
        self.metrics.errors.fetch_add(1, Ordering::Relaxed);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        self.metrics.last_error_time.store(now, Ordering::Relaxed);
    }

    /// Record a warning (non-fatal issue)
    pub fn record_warning(&self, _message: &str) {
        self.metrics.warnings.fetch_add(1, Ordering::Relaxed);
    }

    /// Calculate current health status based on error rate
    fn calculate_status(&self) -> ProcessStatus {
        let error_rate = self.metrics.error_rate(self.thresholds.window_seconds);

        if error_rate >= self.thresholds.error_rate_failed {
            ProcessStatus::Failed
        } else if error_rate >= self.thresholds.error_rate_degraded {
            ProcessStatus::Degraded
        } else {
            ProcessStatus::Healthy
        }
    }

    /// Get current health status without emitting
    pub fn current_status(&self) -> ProcessStatus {
        self.calculate_status()
    }

    /// Check current health and emit status event if changed
    ///
    /// Returns the current status after checking.
    pub async fn check_and_emit(&self) -> Result<ProcessStatus> {
        let new_status = self.calculate_status();
        let mut last_status_guard = self.last_status.write().unwrap();
        let old_status = *last_status_guard;

        // Only emit if status changed
        if new_status != old_status {
            let error_rate = self.metrics.error_rate(self.thresholds.window_seconds);
            let reason = format!(
                "Status changed from {} to {} (error rate: {:.2}%, events: {}, errors: {})",
                old_status,
                new_status,
                error_rate * 100.0,
                self.metrics.events_processed.load(Ordering::Relaxed),
                self.metrics.errors.load(Ordering::Relaxed),
            );

            self.observer
                .emit_health_status(
                    &self.component_name,
                    &old_status.to_string(),
                    &new_status.to_string(),
                    Some(&reason),
                )
                .await
                .map_err(|e| SinexError::service(format!("Failed to emit health status: {}", e)))?;

            *last_status_guard = new_status;
        }

        Ok(new_status)
    }

    /// Get access to the metrics for external monitoring
    pub fn metrics(&self) -> &Arc<HealthMetrics> {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_metrics_error_rate() {
        let metrics = HealthMetrics::default();

        // No events processed
        assert_eq!(metrics.error_rate(300), 0.0);

        // Process some successful events
        metrics.events_processed.store(100, Ordering::Relaxed);
        metrics.errors.store(5, Ordering::Relaxed);

        // Error rate should be 5%
        assert!((metrics.error_rate(300) - 0.05).abs() < 0.001);

        // Higher error rate
        metrics.errors.store(20, Ordering::Relaxed);
        assert!((metrics.error_rate(300) - 0.20).abs() < 0.001);
    }

    #[test]
    fn test_health_thresholds_defaults() {
        let thresholds = HealthThresholds::default();
        assert_eq!(thresholds.error_rate_degraded, 0.05);
        assert_eq!(thresholds.error_rate_failed, 0.20);
        assert_eq!(thresholds.window_seconds, 300);
    }

    #[test]
    fn test_process_status_calculation() {
        let thresholds = HealthThresholds::default();
        let metrics = HealthMetrics::default();

        // Healthy: 0% errors
        metrics.events_processed.store(100, Ordering::Relaxed);
        metrics.errors.store(0, Ordering::Relaxed);
        assert_eq!(metrics.error_rate(300), 0.0);

        // Degraded: 5% errors
        metrics.errors.store(5, Ordering::Relaxed);
        let rate = metrics.error_rate(300);
        assert!(rate >= thresholds.error_rate_degraded);
        assert!(rate < thresholds.error_rate_failed);

        // Failed: 20% errors
        metrics.errors.store(20, Ordering::Relaxed);
        let rate = metrics.error_rate(300);
        assert!(rate >= thresholds.error_rate_failed);
    }
}
