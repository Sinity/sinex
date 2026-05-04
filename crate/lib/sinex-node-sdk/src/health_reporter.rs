//! Standardized health reporting for all nodes
//!
//! Provides uniform health tracking that automatically monitors success/error rates
//! and emits health.status events via `SelfObserver` when status changes.

use crate::error_helpers::unix_timestamp_secs_with_warning;
use crate::self_observation::SelfObserver;
use parking_lot::Mutex;
use parking_lot::RwLock;
use sinex_primitives::env as shared_env;
use sinex_primitives::{Result, SinexError, events::payloads::process::ProcessStatus};
use std::collections::VecDeque;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

static PROCESS_START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

fn get_process_start() -> Instant {
    *PROCESS_START.get_or_init(Instant::now)
}

/// Monotonic clock source used for health-window calculations.
pub trait HealthClock: std::fmt::Debug + Send + Sync {
    /// Monotonic time since an arbitrary epoch.
    fn now(&self) -> Duration;
}

#[derive(Debug)]
struct SystemHealthClock {
    started_at: Instant,
}

impl Default for SystemHealthClock {
    fn default() -> Self {
        Self {
            started_at: Instant::now(),
        }
    }
}

impl HealthClock for SystemHealthClock {
    fn now(&self) -> Duration {
        self.started_at.elapsed()
    }
}

#[derive(Debug)]
struct OutcomeSample {
    recorded_at: Duration,
    is_error: bool,
}

/// Atomic counters for health metrics
#[derive(Debug)]
pub struct HealthMetrics {
    pub events_processed: AtomicU64,
    pub errors: AtomicU64,
    pub warnings: AtomicU64,
    pub last_error_time: AtomicU64, // Unix timestamp in seconds (wall clock)
    pub last_error_monotonic: AtomicU64, // Seconds since process start (monotonic)
    recent_outcomes: Mutex<VecDeque<OutcomeSample>>,
    clock: Arc<dyn HealthClock>,
}

impl Default for HealthMetrics {
    fn default() -> Self {
        Self::with_clock(Arc::new(SystemHealthClock::default()))
    }
}

impl HealthMetrics {
    fn with_clock(clock: Arc<dyn HealthClock>) -> Self {
        Self {
            events_processed: AtomicU64::default(),
            errors: AtomicU64::default(),
            warnings: AtomicU64::default(),
            last_error_time: AtomicU64::default(),
            last_error_monotonic: AtomicU64::default(),
            recent_outcomes: Mutex::new(VecDeque::new()),
            clock,
        }
    }

    fn prune_recent_outcomes(
        outcomes: &mut VecDeque<OutcomeSample>,
        window_seconds: u64,
        now: Duration,
    ) {
        let window = Duration::from_secs(window_seconds);
        while outcomes
            .front()
            .is_some_and(|sample| now.saturating_sub(sample.recorded_at) >= window)
        {
            outcomes.pop_front();
        }
    }

    fn push_recent_outcome(&self, is_error: bool, window_seconds: u64) {
        let now = self.clock.now();
        let mut outcomes = self.recent_outcomes.lock();
        Self::prune_recent_outcomes(&mut outcomes, window_seconds, now);
        outcomes.push_back(OutcomeSample {
            recorded_at: now,
            is_error,
        });
    }

    /// Calculate error rate over the sliding window.
    ///
    /// Returns the share of recorded outcomes inside the active window that were
    /// errors. This stays faithful to the advertised sliding-window semantics
    /// instead of diluting recent failures with long-expired lifetime totals.
    pub fn error_rate(&self, window_seconds: u64) -> f64 {
        let now = self.clock.now();
        let mut outcomes = self.recent_outcomes.lock();
        Self::prune_recent_outcomes(&mut outcomes, window_seconds, now);
        let total = outcomes.len();
        if total == 0 {
            0.0
        } else {
            let errors = outcomes.iter().filter(|sample| sample.is_error).count();
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
    fn validate_rate(name: &str, value: f64) -> Result<()> {
        if !value.is_finite() {
            return Err(
                SinexError::configuration(format!("{name} must be a finite number"))
                    .with_context("value", value.to_string()),
            );
        }

        if !(0.0..=1.0).contains(&value) {
            return Err(
                SinexError::configuration(format!("{name} must be between 0.0 and 1.0"))
                    .with_context("value", value.to_string()),
            );
        }

        Ok(())
    }

    fn validate(self) -> Result<Self> {
        Self::validate_rate("health degraded threshold", self.error_rate_degraded)?;
        Self::validate_rate("health failed threshold", self.error_rate_failed)?;

        if self.error_rate_degraded > self.error_rate_failed {
            return Err(SinexError::configuration(
                "health degraded threshold must not exceed the failed threshold".to_string(),
            )
            .with_context("error_rate_degraded", self.error_rate_degraded.to_string())
            .with_context("error_rate_failed", self.error_rate_failed.to_string()));
        }

        if self.window_seconds == 0 {
            return Err(SinexError::configuration(
                "health window must be greater than zero".to_string(),
            )
            .with_context("window_seconds", self.window_seconds.to_string()));
        }

        Ok(self)
    }

    /// Load thresholds from environment variables
    pub fn from_env() -> Result<Self> {
        Self {
            error_rate_degraded: shared_env::strict_parsed("SINEX_HEALTH_ERROR_RATE_DEGRADED")?.unwrap_or(0.05),
            error_rate_failed: shared_env::strict_parsed("SINEX_HEALTH_ERROR_RATE_FAILED")?.unwrap_or(0.20),
            window_seconds: shared_env::strict_parsed("SINEX_HEALTH_WINDOW_SECONDS")?.unwrap_or(300),
        }
        .validate()
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
        Self::new_with_clock(
            component_name,
            observer,
            thresholds,
            Arc::new(SystemHealthClock::default()),
        )
    }

    /// Create a health reporter with an explicit monotonic clock source.
    #[must_use]
    pub fn new_with_clock(
        component_name: String,
        observer: Arc<SelfObserver>,
        thresholds: HealthThresholds,
        clock: Arc<dyn HealthClock>,
    ) -> Self {
        Self {
            component_name,
            observer,
            metrics: Arc::new(HealthMetrics::with_clock(clock)),
            last_status: Arc::new(RwLock::new(ProcessStatus::Healthy)),
            thresholds,
        }
    }

    /// Record a successful event processing
    pub fn record_success(&self) {
        self.metrics
            .events_processed
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .push_recent_outcome(false, self.thresholds.window_seconds);
    }

    /// Record an error with context
    pub fn record_error(&self, _error: &SinexError) {
        self.metrics
            .events_processed
            .fetch_add(1, Ordering::Relaxed);
        self.metrics.errors.fetch_add(1, Ordering::Relaxed);

        // Update wall clock time (for display/observability)
        let now_wall = unix_timestamp_secs_with_warning(
            std::time::SystemTime::now(),
            "health reporter error timestamp",
        );
        self.metrics
            .last_error_time
            .store(now_wall, Ordering::Relaxed);

        // Update monotonic time (for accurate rate calculation)
        let now_monotonic = Instant::now().duration_since(get_process_start()).as_secs();
        self.metrics
            .last_error_monotonic
            .store(now_monotonic, Ordering::Relaxed);
        self.metrics
            .push_recent_outcome(true, self.thresholds.window_seconds);
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
        *self.last_status.read()
    }

    fn write_last_status(&self, status: ProcessStatus) {
        let mut guard = self.last_status.write();
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
