//! Standardized health reporting for runtime modules.
//!
//! Provides uniform health tracking that automatically monitors success/error rates
//! and emits health.status events via `SelfObserver` when status changes or when
//! the event-derived liveness view needs a freshness refresh.

use crate::runtime::error_helpers::unix_timestamp_secs_with_warning;
use crate::runtime::self_observation::SelfObserver;
use futures::future::BoxFuture;
use parking_lot::Mutex;
use parking_lot::RwLock;
use sinex_macros::SinexConfig;
use sinex_primitives::{Result, SinexError, domain::HealthStatus};
use std::collections::VecDeque;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
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

/// Emission tracker shared with `EventEmitter` to record real upstream emissions.
///
/// Unlike `HealthMetrics::events_processed` (bumped on every adapter tick, including
/// idle keepalives), this tracker is incremented **only when a module actually pushes
/// an event** through the runtime's `EventEmitter::emit`. The monotonic last-emit
/// seconds drives the emit-stall detector in `HealthReporter::calculate_status`.
#[derive(Debug)]
pub struct EmitTracker {
    /// Monotonic seconds since `clock` epoch when the most recent emission occurred.
    /// `0` indicates "no emission observed yet".
    last_emit_monotonic_secs: AtomicU64,
    /// Lifetime count of emissions observed through this tracker.
    total_emits: AtomicU64,
    clock: Arc<dyn HealthClock>,
}

impl EmitTracker {
    fn new(clock: Arc<dyn HealthClock>) -> Self {
        Self {
            last_emit_monotonic_secs: AtomicU64::new(0),
            total_emits: AtomicU64::new(0),
            clock,
        }
    }

    /// Record `count` real emissions. Called by `EventEmitter::emit` (or any other
    /// publish chokepoint) on successful delivery.
    pub fn notify_emit(&self, count: u64) {
        if count == 0 {
            return;
        }
        let now_secs = self.clock.now().as_secs().max(1); // avoid 0 (= "never")
        self.last_emit_monotonic_secs
            .store(now_secs, Ordering::Relaxed);
        self.total_emits.fetch_add(count, Ordering::Relaxed);
    }

    /// Read the monotonic seconds of the last observed emission, or `None`.
    #[must_use]
    pub fn last_emit_monotonic(&self) -> Option<u64> {
        match self.last_emit_monotonic_secs.load(Ordering::Relaxed) {
            0 => None,
            secs => Some(secs),
        }
    }

    /// Lifetime emission count observed by this tracker.
    #[must_use]
    pub fn total_emits(&self) -> u64 {
        self.total_emits.load(Ordering::Relaxed)
    }
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
    /// Monotonic time the reporter was created — anchor for uptime in emit-stall checks.
    started_at_secs: u64,
    /// Optional shared emit tracker. When `Some`, the reporter consults this for emit-stall
    /// detection; when `None`, emit-stall detection is disabled.
    emit_tracker: RwLock<Option<Arc<EmitTracker>>>,
}

impl Default for HealthMetrics {
    fn default() -> Self {
        Self::with_clock(Arc::new(SystemHealthClock::default()))
    }
}

impl HealthMetrics {
    fn with_clock(clock: Arc<dyn HealthClock>) -> Self {
        let started_at_secs = clock.now().as_secs();
        Self {
            events_processed: AtomicU64::default(),
            errors: AtomicU64::default(),
            warnings: AtomicU64::default(),
            last_error_time: AtomicU64::default(),
            last_error_monotonic: AtomicU64::default(),
            recent_outcomes: Mutex::new(VecDeque::new()),
            clock,
            started_at_secs,
            emit_tracker: RwLock::new(None),
        }
    }

    fn install_emit_tracker(&self, tracker: Arc<EmitTracker>) {
        *self.emit_tracker.write() = Some(tracker);
    }

    /// Seconds since the metrics were created (uptime relative to the configured clock).
    pub fn uptime_secs(&self) -> u64 {
        self.clock
            .now()
            .as_secs()
            .saturating_sub(self.started_at_secs)
    }

    /// Seconds since the most recent observed emission, or `None` if no emission seen yet
    /// or no tracker has been installed.
    pub fn seconds_since_last_emit(&self) -> Option<u64> {
        let tracker_guard = self.emit_tracker.read();
        let tracker = tracker_guard.as_ref()?;
        let last = tracker.last_emit_monotonic()?;
        let now = self.clock.now().as_secs();
        Some(now.saturating_sub(last))
    }

    /// Whether an emit tracker has been installed.
    #[must_use]
    pub fn has_emit_tracker(&self) -> bool {
        self.emit_tracker.read().is_some()
    }

    /// Clone the installed emit tracker, if any.
    #[must_use]
    pub fn emit_tracker(&self) -> Option<Arc<EmitTracker>> {
        self.emit_tracker.read().clone()
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

/// Configuration thresholds for health status determination.
///
/// The `from_env()` method is generated by `#[derive(SinexConfig)]`.
#[derive(Debug, Clone, SinexConfig)]
#[sinex_config(
    prefix = "SINEX_HEALTH",
    context = "health reporter",
    fallible,
    normalize_fn = "validate"
)]
pub struct HealthThresholds {
    /// Error rate threshold for degraded status (e.g., 0.05 = 5%)
    #[sinex_config(env = "SINEX_HEALTH_ERROR_RATE_DEGRADED", default_expr = "0.05_f64")]
    pub error_rate_degraded: f64,
    /// Error rate threshold for failed status (e.g., 0.20 = 20%)
    #[sinex_config(env = "SINEX_HEALTH_ERROR_RATE_FAILED", default_expr = "0.20_f64")]
    pub error_rate_failed: f64,
    /// Sliding window for error rate calculation (in seconds)
    #[sinex_config(env = "SINEX_HEALTH_WINDOW_SECONDS", default_expr = "300_u64")]
    pub window_seconds: u64,
    /// Seconds without any real emission, *after* the module has been up at least this
    /// long, before degrading to `Degraded`. Defaults to 600s (10 min). Set to `0`
    /// to disable emit-stall detection entirely. Only meaningful when an
    /// `EmitTracker` has been wired into the reporter (otherwise the check is a
    /// no-op).
    #[sinex_config(env = "SINEX_HEALTH_EMIT_STALL_SECS", default_expr = "600_u64")]
    pub emit_stall_seconds: u64,
    /// Maximum seconds between emitted `health.status` observations for an unchanged
    /// component. Runtime liveness views derive freshness from these append-only
    /// events, so steady-state healthy modules must still refresh before
    /// `sinex_telemetry.current_health` ages out.
    #[sinex_config(env = "SINEX_HEALTH_REFRESH_SECONDS", default_expr = "900_u64")]
    pub refresh_seconds: u64,
}

impl Default for HealthThresholds {
    fn default() -> Self {
        Self {
            error_rate_degraded: 0.05, // 5%
            error_rate_failed: 0.20,   // 20%
            window_seconds: 300,       // 5 minutes
            emit_stall_seconds: 600,   // 10 minutes — conservative; some sources legitimately quiet
            refresh_seconds: 900,      // 15 minutes — comfortably inside current_health's 1h window
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

        if self.refresh_seconds == 0 {
            return Err(SinexError::configuration(
                "health refresh interval must be greater than zero".to_string(),
            )
            .with_context("refresh_seconds", self.refresh_seconds.to_string()));
        }

        Ok(self)
    }

    /// Whether emit-stall detection is enabled (i.e. `emit_stall_seconds > 0`).
    #[must_use]
    pub fn emit_stall_enabled(&self) -> bool {
        self.emit_stall_seconds > 0
    }
}

/// Liveness probe: an async function that returns `true` when the module's
/// dependencies (NATS, DB, external socket) are reachable.
pub type LivenessProbe = Arc<dyn Fn() -> BoxFuture<'static, bool> + Send + Sync>;

/// Standardized health reporter for runtime modules.
///
/// Tracks events/errors and automatically emits health.status observations
/// when the component's health status changes or needs a freshness refresh.
pub struct HealthReporter {
    component_name: String,
    observer: Arc<SelfObserver>,
    metrics: Arc<HealthMetrics>,
    last_status: Arc<RwLock<HealthStatus>>,
    has_emitted_status: Arc<AtomicBool>,
    last_status_emit_secs: Arc<AtomicU64>,
    thresholds: HealthThresholds,
    clock: Arc<dyn HealthClock>,
    /// Optional async probe that verifies module dependencies are reachable.
    /// When set, `check_and_emit()` calls the probe and caches the result in
    /// `liveness_ok`. `calculate_status()` demotes `Healthy` → `Degraded` when
    /// `liveness_ok = false`.
    #[allow(clippy::type_complexity)]
    liveness_probe: Option<LivenessProbe>,
    liveness_ok: Arc<AtomicBool>,
}

impl std::fmt::Debug for HealthReporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealthReporter")
            .field("component_name", &self.component_name)
            .field("has_liveness_probe", &self.liveness_probe.is_some())
            .field("liveness_ok", &self.liveness_ok.load(Ordering::Relaxed))
            .field(
                "has_emitted_status",
                &self.has_emitted_status.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
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
            metrics: Arc::new(HealthMetrics::with_clock(Arc::clone(&clock))),
            last_status: Arc::new(RwLock::new(HealthStatus::Healthy)),
            has_emitted_status: Arc::new(AtomicBool::new(false)),
            last_status_emit_secs: Arc::new(AtomicU64::new(0)),
            thresholds,
            clock,
            liveness_probe: None,
            liveness_ok: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Attach a liveness probe that verifies module dependencies are reachable.
    ///
    /// The probe is called once per `check_and_emit()` invocation. If it returns
    /// `false`, `calculate_status()` downgrades `Healthy` → `Degraded` so the
    /// health surface reflects connectivity failures immediately — before errors
    /// accumulate in the error-rate window.
    #[must_use]
    pub fn with_liveness_probe(mut self, probe: LivenessProbe) -> Self {
        self.liveness_probe = Some(probe);
        self
    }

    /// Enable emit-stall detection on this reporter, returning a shared
    /// `EmitTracker` handle. Callers should install the returned handle into
    /// the `EventEmitter` (or any other publish chokepoint) so that
    /// `notify_emit` is invoked on every real emission.
    ///
    /// Idempotent semantics: if a tracker was already installed, this returns
    /// a clone of the existing handle so all emitters share the same counters.
    #[must_use]
    pub fn enable_emit_stall_detection(&self) -> Arc<EmitTracker> {
        if let Some(existing) = self.metrics.emit_tracker() {
            return existing;
        }
        let tracker = Arc::new(EmitTracker::new(Arc::clone(&self.clock)));
        self.metrics.install_emit_tracker(Arc::clone(&tracker));
        tracker
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

    /// Notify the reporter that `count` real events were emitted upstream.
    ///
    /// Distinct from `record_success`, which is bumped by adapter-level keepalive
    /// ticks (e.g. the 30s "alive but idle" pulse in `SourceDriverRuntime`).
    /// `notify_emit` is the signal that the module is *actually doing work*, and
    /// is what feeds emit-stall detection.
    pub fn notify_emit(&self, count: u64) {
        if count == 0 {
            return;
        }
        if let Some(tracker) = self.metrics.emit_tracker() {
            tracker.notify_emit(count);
        }
    }

    /// Whether the module's emit rate has stalled past the configured threshold.
    ///
    /// Returns `false` (i.e. healthy) when stall detection is disabled, when no
    /// tracker has been installed, or when the module has not yet been up long
    /// enough to be considered overdue. Once both `uptime` and "seconds since
    /// last emit" cross the threshold, this returns `true`. If no emit has been
    /// observed yet, uptime alone gates the verdict — a module that runs for
    /// > `emit_stall_seconds` without ever emitting is degraded.
    fn emit_stalled(&self) -> bool {
        if !self.thresholds.emit_stall_enabled() {
            return false;
        }
        let threshold = self.thresholds.emit_stall_seconds;
        let uptime = self.metrics.uptime_secs();
        if uptime < threshold {
            return false;
        }
        match self.metrics.seconds_since_last_emit() {
            Some(elapsed) => elapsed >= threshold,
            None => {
                // Tracker may be absent (stall detection not wired) — in which case
                // we cannot reason about emit rate; treat as healthy.
                // Or tracker exists but has never recorded an emit — uptime gate
                // already passed, so we are stalled.
                self.metrics.has_emit_tracker()
            }
        }
    }

    /// Calculate current health status based on error rate and emit-stall signal.
    fn calculate_status(&self) -> HealthStatus {
        let error_rate = self.metrics.error_rate(self.thresholds.window_seconds);

        let base = if error_rate >= self.thresholds.error_rate_failed {
            HealthStatus::Unhealthy
        } else if error_rate >= self.thresholds.error_rate_degraded {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        };

        if matches!(base, HealthStatus::Healthy) && self.emit_stalled() {
            return HealthStatus::Degraded;
        }

        // Liveness probe result is cached by check_and_emit(). Demote Healthy →
        // Degraded when connectivity failed on the last probe tick.
        if matches!(base, HealthStatus::Healthy)
            && self.liveness_probe.is_some()
            && !self.liveness_ok.load(Ordering::Relaxed)
        {
            return HealthStatus::Degraded;
        }

        base
    }

    /// Get current health status without emitting
    #[must_use]
    pub fn current_status(&self) -> HealthStatus {
        self.calculate_status()
    }

    fn read_last_status(&self) -> HealthStatus {
        *self.last_status.read()
    }

    fn write_last_status(&self, status: HealthStatus) {
        let mut guard = self.last_status.write();
        *guard = status;
    }

    fn status_emit_reason(
        &self,
        old_status: HealthStatus,
        new_status: HealthStatus,
        now_secs: u64,
    ) -> Option<String> {
        if !self.has_emitted_status.load(Ordering::Relaxed) {
            return Some(format!(
                "Initial health.status observation for {}: status {}",
                self.component_name, new_status
            ));
        }

        if new_status != old_status {
            let error_rate = self.metrics.error_rate(self.thresholds.window_seconds);
            let stall_note = match self.metrics.seconds_since_last_emit() {
                Some(elapsed) if self.emit_stalled() => {
                    format!(", emit-stalled: last emit {elapsed}s ago")
                }
                None if self.emit_stalled() => {
                    format!(
                        ", emit-stalled: never emitted (uptime {}s)",
                        self.metrics.uptime_secs()
                    )
                }
                _ => String::new(),
            };
            return Some(format!(
                "Status changed from {} to {} (error rate: {:.2}%, events: {}, errors: {}{})",
                old_status,
                new_status,
                error_rate * 100.0,
                self.metrics.events_processed.load(Ordering::Relaxed),
                self.metrics.errors.load(Ordering::Relaxed),
                stall_note,
            ));
        }

        let last_emit = self.last_status_emit_secs.load(Ordering::Relaxed);
        if now_secs.saturating_sub(last_emit) >= self.thresholds.refresh_seconds {
            return Some(format!(
                "Refreshed unchanged health.status for {} after {}s",
                self.component_name,
                now_secs.saturating_sub(last_emit)
            ));
        }

        None
    }

    /// Check current health and emit status evidence when changed or stale.
    ///
    /// Returns the current status after checking.
    pub async fn check_and_emit(&self) -> Result<HealthStatus> {
        // Run the liveness probe (if configured) and cache the result so
        // calculate_status() — which is sync — can read it atomically.
        if let Some(ref probe) = self.liveness_probe {
            let alive = probe().await;
            self.liveness_ok.store(alive, Ordering::Relaxed);
        }

        let new_status = self.calculate_status();
        let now_secs = self.clock.now().as_secs();

        // Read current status and determine if emission is needed.
        // Guard must be dropped before the await to keep the future Send.
        let (should_emit, old_status, reason) = {
            let old_status = self.read_last_status();
            if let Some(reason) = self.status_emit_reason(old_status, new_status, now_secs) {
                (true, old_status, reason)
            } else {
                (false, old_status, String::new())
            }
            // guard dropped here
        };

        if should_emit {
            self.observer
                .emit_health_status(&self.component_name, old_status, new_status, Some(&reason))
                .await
                .map_err(|e| SinexError::service(format!("Failed to emit health status: {e}")))?;

            // Update stored status after successful emission
            self.write_last_status(new_status);
            self.has_emitted_status.store(true, Ordering::Relaxed);
            self.last_status_emit_secs
                .store(now_secs, Ordering::Relaxed);
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
    use std::sync::atomic::AtomicU64;

    #[derive(Debug)]
    struct ManualHealthClock {
        now_secs: AtomicU64,
    }

    impl ManualHealthClock {
        fn new(now_secs: u64) -> Self {
            Self {
                now_secs: AtomicU64::new(now_secs),
            }
        }

        fn set(&self, now_secs: u64) {
            self.now_secs.store(now_secs, Ordering::Relaxed);
        }
    }

    impl HealthClock for ManualHealthClock {
        fn now(&self) -> Duration {
            Duration::from_secs(self.now_secs.load(Ordering::Relaxed))
        }
    }

    fn reporter_with_clock(clock: Arc<ManualHealthClock>) -> HealthReporter {
        HealthReporter::new_with_clock(
            "runtime-health-test".to_string(),
            Arc::new(SelfObserver::disabled()),
            HealthThresholds {
                error_rate_degraded: 0.05,
                error_rate_failed: 0.20,
                window_seconds: 60,
                emit_stall_seconds: 0,
                refresh_seconds: 10,
            },
            clock,
        )
    }

    #[tokio::test]
    async fn first_health_check_emits_initial_status_evidence() -> Result<()> {
        let clock = Arc::new(ManualHealthClock::new(1));
        let reporter = reporter_with_clock(clock);

        assert!(!reporter.has_emitted_status.load(Ordering::Relaxed));
        assert_eq!(reporter.check_and_emit().await?, HealthStatus::Healthy);

        assert!(reporter.has_emitted_status.load(Ordering::Relaxed));
        assert_eq!(reporter.last_status_emit_secs.load(Ordering::Relaxed), 1);
        Ok(())
    }

    #[tokio::test]
    async fn unchanged_health_refreshes_after_configured_interval() -> Result<()> {
        let clock = Arc::new(ManualHealthClock::new(1));
        let reporter = reporter_with_clock(Arc::clone(&clock));

        reporter.check_and_emit().await?;
        clock.set(5);
        reporter.check_and_emit().await?;
        assert_eq!(
            reporter.last_status_emit_secs.load(Ordering::Relaxed),
            1,
            "unchanged health should not emit before the refresh interval"
        );

        clock.set(11);
        reporter.check_and_emit().await?;
        assert_eq!(
            reporter.last_status_emit_secs.load(Ordering::Relaxed),
            11,
            "unchanged health must refresh before event-derived liveness ages out"
        );
        Ok(())
    }
}
