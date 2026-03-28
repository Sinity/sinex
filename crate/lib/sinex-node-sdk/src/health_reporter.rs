//! Standardized health reporting for all nodes
//!
//! Provides uniform health tracking that automatically monitors success/error rates
//! and emits health.status events via `SelfObserver` when status changes.

use crate::self_observation::SelfObserver;
use sinex_primitives::{Result, SinexError, events::payloads::process::ProcessStatus};
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;
use tracing::warn;

static PROCESS_START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

fn get_process_start() -> Instant {
    *PROCESS_START.get_or_init(Instant::now)
}

/// Atomic counters for health metrics
#[derive(Debug, Default)]
pub struct HealthMetrics {
    pub events_processed: AtomicU64,
    pub errors: AtomicU64,
    pub warnings: AtomicU64,
    pub last_error_time: AtomicU64, // Unix timestamp in seconds (wall clock)
    pub last_error_monotonic: AtomicU64, // Seconds since process start (monotonic)
}

impl HealthMetrics {
    /// Calculate error rate over the sliding window.
    ///
    /// Returns 0.0 when no errors have been recorded, or when the most recent
    /// error is older than `window_seconds`. Otherwise returns the cumulative
    /// error rate (errors / total events). This is a conservative approximation:
    /// once errors leave the window the rate drops to zero, but while any error
    /// is inside the window the all-time rate is reported.
    pub fn error_rate(&self, window_seconds: u64) -> f64 {
        let errors = self.errors.load(Ordering::Relaxed);
        if errors == 0 {
            return 0.0;
        }

        let last_error = self.last_error_monotonic.load(Ordering::Relaxed);
        let now_monotonic = Instant::now().duration_since(get_process_start()).as_secs();

        // If the most recent error is at or beyond the window boundary, rate is 0.
        // Uses >= to avoid flakiness from as_secs() truncation at the boundary.
        if now_monotonic.saturating_sub(last_error) >= window_seconds {
            return 0.0;
        }

        let total = self.events_processed.load(Ordering::Relaxed);
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
            error_rate_degraded: env_parsed("SINEX_HEALTH_ERROR_RATE_DEGRADED")?.unwrap_or(0.05),
            error_rate_failed: env_parsed("SINEX_HEALTH_ERROR_RATE_FAILED")?.unwrap_or(0.20),
            window_seconds: env_parsed("SINEX_HEALTH_WINDOW_SECONDS")?.unwrap_or(300),
        })
    }
}

fn env_parsed<T>(name: &str) -> Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Ok(value) => value.parse::<T>().map(Some).map_err(|error| {
            SinexError::configuration(format!(
                "Environment variable {name} has invalid value `{value}`: {error}"
            ))
        }),
        Err(std::env::VarError::NotUnicode(_)) => Err(SinexError::configuration(format!(
            "Environment variable {name} is not valid UTF-8"
        ))),
        Err(std::env::VarError::NotPresent) => Ok(None),
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
    #[must_use]
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

        // Update wall clock time (for display/observability)
        let now_wall = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.metrics
            .last_error_time
            .store(now_wall, Ordering::Relaxed);

        // Update monotonic time (for accurate rate calculation)
        let now_monotonic = Instant::now().duration_since(get_process_start()).as_secs();
        self.metrics
            .last_error_monotonic
            .store(now_monotonic, Ordering::Relaxed);
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
    #[must_use]
    pub fn current_status(&self) -> ProcessStatus {
        self.calculate_status()
    }

    fn read_last_status(&self) -> ProcessStatus {
        let guard = self.last_status.read().unwrap_or_else(|poisoned| {
            warn!("Health reporter status lock poisoned during read; recovering");
            poisoned.into_inner()
        });
        *guard
    }

    fn write_last_status(&self, status: ProcessStatus) {
        let mut guard = self.last_status.write().unwrap_or_else(|poisoned| {
            warn!("Health reporter status lock poisoned during write; recovering");
            poisoned.into_inner()
        });
        *guard = status;
    }

    /// Check current health and emit status event if changed
    ///
    /// Returns the current status after checking.
    pub async fn check_and_emit(&self) -> Result<ProcessStatus> {
        let new_status = self.calculate_status();

        // Read current status and determine if emission is needed.
        // Guard must be dropped before the await to keep the future Send.
        let (should_emit, old_status, reason) = {
            let old_status = self.read_last_status();

            if new_status == old_status {
                (false, old_status, String::new())
            } else {
                let error_rate = self.metrics.error_rate(self.thresholds.window_seconds);
                let reason = format!(
                    "Status changed from {} to {} (error rate: {:.2}%, events: {}, errors: {})",
                    old_status,
                    new_status,
                    error_rate * 100.0,
                    self.metrics.events_processed.load(Ordering::Relaxed),
                    self.metrics.errors.load(Ordering::Relaxed),
                );
                (true, old_status, reason)
            }
            // guard dropped here
        };

        if should_emit {
            self.observer
                .emit_health_status(
                    &self.component_name,
                    &old_status.to_string(),
                    &new_status.to_string(),
                    Some(&reason),
                )
                .await
                .map_err(|e| SinexError::service(format!("Failed to emit health status: {e}")))?;

            // Update stored status after successful emission
            self.write_last_status(new_status);
        }

        Ok(new_status)
    }

    /// Get access to the metrics for external monitoring
    #[must_use]
    pub fn metrics(&self) -> &Arc<HealthMetrics> {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::sinex_test;

    fn poison_lock<T: Send + Sync + 'static>(lock: Arc<RwLock<T>>, value: T) {
        let result = std::thread::spawn(move || {
            let mut guard = lock.write().expect("test lock should poison cleanly");
            *guard = value;
            panic!("poison lock for regression coverage");
        })
        .join();
        assert!(result.is_err(), "poisoning thread should panic");
    }

    #[sinex_test]
    async fn check_and_emit_recovers_from_poisoned_last_status_read() -> xtask::sandbox::TestResult<()> {
        let reporter = HealthReporter::new(
            "test-component".to_string(),
            Arc::new(SelfObserver::disabled()),
            HealthThresholds::default(),
        );
        poison_lock(Arc::clone(&reporter.last_status), ProcessStatus::Healthy);

        reporter.record_error(&SinexError::processing("boom"));
        let status = reporter.check_and_emit().await?;

        assert_eq!(status, ProcessStatus::Failed);
        assert_eq!(reporter.read_last_status(), ProcessStatus::Failed);
        Ok(())
    }

    #[sinex_test]
    async fn check_and_emit_recovers_from_poisoned_last_status_write() -> xtask::sandbox::TestResult<()> {
        let reporter = HealthReporter::new(
            "test-component".to_string(),
            Arc::new(SelfObserver::disabled()),
            HealthThresholds::default(),
        );

        reporter.record_error(&SinexError::processing("boom"));
        poison_lock(Arc::clone(&reporter.last_status), ProcessStatus::Healthy);

        let status = reporter.check_and_emit().await?;

        assert_eq!(status, ProcessStatus::Failed);
        assert_eq!(reporter.read_last_status(), ProcessStatus::Failed);
        Ok(())
    }
}
