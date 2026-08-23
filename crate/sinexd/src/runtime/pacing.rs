//! Historical-import pacing (sinex-2n9).
//!
//! # Why this exists
//!
//! An unthrottled 14-month journald gap-fill produced a 1.24M-event raw
//! backlog that flapped the daemon on boot (the original incident behind
//! this bead). The event-engine's publish-side backpressure gate
//! (`nats_publisher::wait_for_raw_events_stream_capacity`) reacts to that
//! backlog *after* it has already piled up in NATS — throttling only the
//! consumer just moves the backlog into the raw stream, which is the exact
//! incident shape. This module paces the SOURCE SCAN LOOP itself (the
//! producer), so a large historical import never generates the backlog in
//! the first place.
//!
//! # Design
//!
//! - [`RateBudget`] (re-exported from `sinex_primitives::pacing`, where it
//!   lives because it is also a wire type on `ReplayGateOverrides`) is the
//!   operator-facing config: events/sec, bytes/sec, and the raw-stream
//!   backlog pause/resume thresholds. It is set via source binding config
//!   (operator default) and overridable per replay/import operation
//!   (`ScanArgs::rate_budget`). `RateBudget::default_paced` is a real,
//!   non-zero default — historical scans are paced *by default*.
//!   `RateBudget::unlimited` is the explicit opt-out (`--unlimited`).
//! - [`PacingController`] enforces the events/sec and bytes/sec budget
//!   between batches with capped token buckets, so idle time cannot create an
//!   unbounded post-pause burst.
//! - [`BacklogGate`] pauses the scan loop (with hysteresis) when the raw
//!   stream's event-engine consumer backlog exceeds the configured
//!   threshold, pulling the threshold from config instead of a hardcoded
//!   constant (per sinex-2n9 design notes; formalizing against sinex-n23.3's
//!   capacity model is separate follow-up work).
//!
//! This is the ONE pacing mechanism for all three catch-up entry points that
//! flow through `AdapterBackedSource::scan_historical` → `drain_adapter`:
//! historical gap-fill scans, replay re-ingest (`SourceScanCommand`), and
//! staged/batch imports. Continuous (live-tail) capture is deliberately never
//! gated by this budget — see `drain_adapter`'s callers.

use std::future::Future;
use std::time::{Duration, Instant};

pub use sinex_primitives::pacing::RateBudget;

use crate::runtime::RuntimeResult;
use crate::runtime::work_control::WorkCancellation;

const BACKLOG_POLL_INTERVAL: Duration = Duration::from_millis(250);
const BURST_WINDOW: Duration = Duration::from_secs(1);

/// Enforces a [`RateBudget`]'s events/sec and bytes/sec limits between scan
/// batches with capped token buckets. Each bucket can hold at most one second
/// of credit, so a long pause cannot accumulate an unbounded burst. The
/// counters remain lifetime totals for progress reporting, while enforcement
/// uses only the bounded token state.
#[derive(Debug)]
pub struct PacingController {
    budget: RateBudget,
    started: Instant,
    tokens_refilled_at: Instant,
    event_tokens: Option<f64>,
    byte_tokens: Option<f64>,
    events_total: u64,
    bytes_total: u64,
}

impl PacingController {
    #[must_use]
    pub fn new(budget: RateBudget) -> Self {
        let now = Instant::now();
        Self {
            budget,
            started: now,
            tokens_refilled_at: now,
            event_tokens: Self::bucket_capacity(budget.events_per_sec),
            byte_tokens: Self::bucket_capacity(budget.bytes_per_sec),
            events_total: 0,
            bytes_total: 0,
        }
    }

    #[must_use]
    pub fn budget(&self) -> RateBudget {
        self.budget
    }

    #[must_use]
    pub fn events_total(&self) -> u64 {
        self.events_total
    }

    #[must_use]
    pub fn bytes_total(&self) -> u64 {
        self.bytes_total
    }

    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    #[must_use]
    pub fn rate_events_per_sec(&self) -> f64 {
        let secs = self.elapsed().as_secs_f64();
        if secs <= 0.0 {
            0.0
        } else {
            self.events_total as f64 / secs
        }
    }

    #[must_use]
    pub fn rate_bytes_per_sec(&self) -> f64 {
        let secs = self.elapsed().as_secs_f64();
        if secs <= 0.0 {
            0.0
        } else {
            self.bytes_total as f64 / secs
        }
    }

    fn bucket_capacity(rate: Option<f64>) -> Option<f64> {
        rate.filter(|rate| rate.is_finite() && *rate > 0.0)
            .map(|rate| rate * BURST_WINDOW.as_secs_f64())
    }

    fn refill_bucket(tokens: Option<f64>, rate: Option<f64>, elapsed: Duration) -> Option<f64> {
        let capacity = Self::bucket_capacity(rate)?;
        Some((tokens.unwrap_or(capacity) + rate.unwrap() * elapsed.as_secs_f64()).min(capacity))
    }

    fn refill_tokens(&mut self) {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.tokens_refilled_at);
        self.event_tokens =
            Self::refill_bucket(self.event_tokens, self.budget.events_per_sec, elapsed);
        self.byte_tokens =
            Self::refill_bucket(self.byte_tokens, self.budget.bytes_per_sec, elapsed);
        self.tokens_refilled_at = now;
    }

    fn consume(tokens: &mut Option<f64>, amount: u64) {
        if let Some(tokens) = tokens {
            *tokens -= amount as f64;
        }
    }

    fn wait_for_bucket(tokens: Option<f64>, rate: Option<f64>, elapsed: Duration) -> Duration {
        let Some(rate) = rate.filter(|rate| rate.is_finite() && *rate > 0.0) else {
            return Duration::ZERO;
        };
        let available =
            tokens.unwrap_or(rate * BURST_WINDOW.as_secs_f64()) + rate * elapsed.as_secs_f64();
        if available >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-available / rate)
        }
    }

    /// Duration to sleep (if any) until the current token debt clears. Pure
    /// function of the bounded bucket state, so it remains unit-testable.
    fn required_sleep(&self, elapsed: Duration) -> Duration {
        Self::wait_for_bucket(self.event_tokens, self.budget.events_per_sec, elapsed).max(
            Self::wait_for_bucket(self.byte_tokens, self.budget.bytes_per_sec, elapsed),
        )
    }

    /// Record a processed batch and sleep until its bounded token debt clears.
    /// No-op when the budget is unlimited on both rate dimensions.
    pub async fn record_and_throttle(&mut self, events: u64, bytes: u64) {
        let _ = self.record_and_throttle_inner(events, bytes, None).await;
    }

    /// Record a processed batch with a cooperative cancellation source. This
    /// is the control-plane entry point for callers that own a
    /// [`WorkCancellation`]; cancellation interrupts rate waits without
    /// discarding the already-accounted batch.
    pub async fn record_and_throttle_with_cancellation(
        &mut self,
        events: u64,
        bytes: u64,
        cancellation: &WorkCancellation,
    ) -> RuntimeResult<()> {
        self.record_and_throttle_inner(events, bytes, Some(cancellation))
            .await
    }

    async fn record_and_throttle_inner(
        &mut self,
        events: u64,
        bytes: u64,
        cancellation: Option<&WorkCancellation>,
    ) -> RuntimeResult<()> {
        if let Some(cancellation) = cancellation {
            if cancellation.is_cancelled() {
                return Err(sinex_primitives::SinexError::cancelled(
                    "Pacing cancelled before rate wait",
                ));
            }
        }

        self.events_total = self.events_total.saturating_add(events);
        self.bytes_total = self.bytes_total.saturating_add(bytes);

        if self.budget.events_per_sec.is_none() && self.budget.bytes_per_sec.is_none() {
            return Ok(());
        }

        self.refill_tokens();
        Self::consume(&mut self.event_tokens, events);
        Self::consume(&mut self.byte_tokens, bytes);
        // `refill_tokens` has already advanced the bucket clock to now. The
        // debt calculation must therefore start from the current balance,
        // rather than adding lifetime elapsed time a second time.
        let sleep_for = self.required_sleep(Duration::ZERO);
        if sleep_for.is_zero() {
            return Ok(());
        }

        match cancellation {
            Some(cancellation) => {
                let cancellation_wait = cancellation.wait();
                if cancellation.is_cancelled() {
                    return Err(sinex_primitives::SinexError::cancelled(
                        "Pacing cancelled while rate limited",
                    ));
                }
                tokio::select! {
                    () = tokio::time::sleep(sleep_for) => {
                        self.refill_tokens();
                        Ok(())
                    }
                    () = cancellation_wait => Err(sinex_primitives::SinexError::cancelled(
                        "Pacing cancelled while rate limited",
                    )),
                }
            }
            None => {
                tokio::time::sleep(sleep_for).await;
                self.refill_tokens();
                Ok(())
            }
        }
    }
}

/// Pauses (with hysteresis) when a polled backlog depth exceeds
/// `pause_threshold`, resuming once it drains back to `resume_threshold`.
///
/// Generic over the pending-depth fetcher so this is testable without a live
/// NATS connection: production callers pass a closure backed by
/// [`crate::runtime::backlog::raw_events_consumer_pending`]; tests pass a
/// closure over a fixed/synthetic sequence.
#[derive(Debug, Clone, Copy)]
pub struct BacklogGate {
    pause_threshold: u64,
    resume_threshold: u64,
    poll_interval: Duration,
}

impl BacklogGate {
    #[must_use]
    pub fn new(pause_threshold: u64, resume_threshold: u64) -> Self {
        Self {
            pause_threshold,
            resume_threshold: resume_threshold.min(pause_threshold),
            poll_interval: BACKLOG_POLL_INTERVAL,
        }
    }

    /// Build a gate from a [`RateBudget`], if it configures backlog
    /// thresholds. Returns `None` when the budget has no backlog threshold
    /// (including [`RateBudget::unlimited`]).
    #[must_use]
    pub fn from_budget(budget: &RateBudget) -> Option<Self> {
        let pause = budget.backlog_pause_threshold?;
        let resume = budget.backlog_resume_threshold.unwrap_or(0);
        Some(Self::new(pause, resume))
    }

    #[must_use]
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Poll `fetch_pending` until the backlog is at or below
    /// `pause_threshold`. On the first poll above threshold, keeps polling
    /// until it drops to `resume_threshold` (hysteresis) rather than
    /// resuming the instant it dips just under `pause_threshold`, matching
    /// the publish-side gate's behavior. `fetch_pending` returning `Ok(None)`
    /// (no pressure signal available, e.g. consumer not created yet) is
    /// treated as "no pressure" and returns immediately.
    /// Returns the last observed pending depth on success (`None` if
    /// `fetch_pending` never reported a signal, e.g. the consumer doesn't
    /// exist yet) — callers that want to surface backlog depth (progress
    /// reporting) get it for free instead of needing a second query.
    pub async fn wait_for_capacity<F, Fut>(
        &self,
        mut fetch_pending: F,
    ) -> RuntimeResult<Option<u64>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = RuntimeResult<Option<u64>>>,
    {
        let mut paused = false;
        loop {
            let pending = fetch_pending().await?;
            let Some(pending) = pending else {
                return Ok(None);
            };

            let target = if paused {
                self.resume_threshold
            } else {
                self.pause_threshold
            };

            if pending <= target {
                return Ok(Some(pending));
            }

            paused = true;
            tracing::warn!(
                target: "sinex_metrics",
                metric = "runtime.historical_import_backlog_pause_total",
                pending,
                pause_threshold = self.pause_threshold,
                resume_threshold = self.resume_threshold,
                "Historical import paced by raw-stream backlog; waiting for event engine to drain"
            );
            tokio::time::sleep(self.poll_interval).await;
        }
    }
}

/// Orchestrates a [`PacingController`] plus an optional [`BacklogGate`] for
/// one historical/catch-up scan call. Built once per `scan_historical` (or
/// equivalent) invocation; [`ScanPacer::after_batch`] is the single call a
/// scan loop needs to make after each materialized/durably-emitted batch —
/// this is the "enforcement function other code can call" the pacing
/// mechanism exists to provide (sinex-2n9).
pub struct ScanPacer {
    controller: PacingController,
    backlog_gate: Option<BacklogGate>,
    nats_client: Option<async_nats::Client>,
    env: sinex_primitives::environment::SinexEnvironment,
    namespace: Option<String>,
    module_name: String,
    started_at: sinex_primitives::temporal::Timestamp,
    tracker: crate::runtime::scan_progress::ScanProgressTracker,
    /// Lazily opened on first `after_batch` call. `None` means either "not
    /// attempted yet" or "attempted and unavailable" — either way, progress
    /// publishing degrades to a no-op rather than failing the scan; pacing
    /// enforcement never depends on progress observability succeeding.
    progress_store: Option<crate::runtime::scan_progress::ScanProgressStore>,
    progress_store_attempted: bool,
    last_progress_publish: Option<Instant>,
    last_backlog_pending: Option<u64>,
}

impl ScanPacer {
    #[must_use]
    pub fn new(
        budget: RateBudget,
        nats_client: Option<async_nats::Client>,
        namespace: Option<String>,
        module_name: impl Into<String>,
        horizon: Option<sinex_primitives::temporal::Timestamp>,
    ) -> Self {
        Self {
            backlog_gate: BacklogGate::from_budget(&budget),
            controller: PacingController::new(budget),
            nats_client,
            env: sinex_primitives::environment::environment(),
            namespace,
            module_name: module_name.into(),
            started_at: sinex_primitives::temporal::Timestamp::now(),
            tracker: crate::runtime::scan_progress::ScanProgressTracker::new(horizon),
            progress_store: None,
            progress_store_attempted: false,
            last_progress_publish: None,
            last_backlog_pending: None,
        }
    }

    #[must_use]
    pub fn budget(&self) -> RateBudget {
        self.controller.budget()
    }

    #[must_use]
    pub fn is_paced(&self) -> bool {
        !self.controller.budget().is_unlimited()
    }

    #[must_use]
    pub fn controller(&self) -> &PacingController {
        &self.controller
    }

    async fn ensure_progress_store(&mut self) {
        if self.progress_store.is_some() || self.progress_store_attempted {
            return;
        }
        self.progress_store_attempted = true;
        let Some(client) = &self.nats_client else {
            return;
        };
        match crate::runtime::scan_progress::ScanProgressStore::open(
            client,
            &self.env,
            self.namespace.as_deref(),
        )
        .await
        {
            Ok(store) => self.progress_store = Some(store),
            Err(error) => {
                tracing::debug!(
                    module = %self.module_name,
                    error = %error,
                    "sinex-2n9 scan progress KV unavailable; live progress reporting disabled for this scan"
                );
            }
        }
    }

    async fn publish_progress_if_due(&mut self, force: bool) {
        let due = force
            || self.last_progress_publish.is_none_or(|last| {
                last.elapsed() >= crate::runtime::scan_progress::PUBLISH_INTERVAL
            });
        if !due {
            return;
        }
        self.ensure_progress_store().await;
        let Some(store) = &self.progress_store else {
            return;
        };
        let snapshot = crate::runtime::scan_progress::ScanProgressSnapshot::from_controller(
            &self.module_name,
            self.started_at,
            &self.controller,
            &self.tracker,
            self.last_backlog_pending,
        );
        if let Err(error) = store.publish(&snapshot).await {
            tracing::debug!(
                module = %self.module_name,
                error = %error,
                "sinex-2n9 scan progress publish failed"
            );
        }
        self.last_progress_publish = Some(Instant::now());
    }

    /// Clear this scan's live-progress entry. Call on scan completion
    /// (success or error) so `sinexctl ops import list` only ever shows
    /// genuinely in-flight scans. Best-effort.
    pub async fn finish(&mut self) {
        self.ensure_progress_store().await;
        if let Some(store) = &self.progress_store
            && let Err(error) = store.clear(&self.module_name).await
        {
            tracing::debug!(
                module = %self.module_name,
                error = %error,
                "sinex-2n9 scan progress clear-on-finish failed"
            );
        }
    }

    /// Record a processed batch: throttle to the events/sec and bytes/sec
    /// budget, observe `position` for ETA estimation, publish a live
    /// progress snapshot (throttled to `scan_progress::PUBLISH_INTERVAL`),
    /// then (if a backlog threshold is configured and a NATS client is
    /// available) wait for the raw-events consumer backlog to drain back
    /// under threshold before returning. Rate/backlog enforcement still
    /// applies even when progress publishing is unavailable.
    pub async fn after_batch(
        &mut self,
        events: u64,
        bytes: u64,
        position: Option<sinex_primitives::temporal::Timestamp>,
    ) -> RuntimeResult<()> {
        self.controller.record_and_throttle(events, bytes).await;
        self.tracker.observe(position);

        let Some(gate) = &self.backlog_gate else {
            self.publish_progress_if_due(false).await;
            return Ok(());
        };
        let Some(client) = &self.nats_client else {
            self.publish_progress_if_due(false).await;
            return Ok(());
        };
        let js = async_nats::jetstream::new(client.clone());
        let env = &self.env;
        let namespace = self.namespace.as_deref();

        let result = gate
            .wait_for_capacity(|| async {
                Ok(
                    crate::runtime::backlog::raw_events_consumer_pending(&js, env, namespace)
                        .await?
                        .map(|info| info.num_pending),
                )
            })
            .await;
        match result {
            Ok(pending) => {
                self.last_backlog_pending = pending;
                self.publish_progress_if_due(false).await;
                Ok(())
            }
            Err(error) => {
                self.publish_progress_if_due(false).await;
                Err(error)
            }
        }
    }
}

#[cfg(test)]
#[path = "pacing_test.rs"]
mod tests;
