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
//! - [`RateBudget`] is the operator-facing config: events/sec, bytes/sec, and
//!   the raw-stream backlog pause/resume thresholds. It is set via source
//!   binding config (operator default) and overridable per replay/import
//!   operation (`ScanArgs::rate_budget`). [`RateBudget::default_paced`] is a
//!   real, non-zero default — historical scans are paced *by default*.
//!   [`RateBudget::unlimited`] is the explicit opt-out (`--unlimited`).
//! - [`PacingController`] enforces the events/sec and bytes/sec budget
//!   between batches with a simple "average rate since start" throttle.
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

use serde::{Deserialize, Serialize};
use sinex_primitives::env as shared_env;

use crate::runtime::RuntimeResult;

/// Default target event rate for historical/catch-up scans, in events/sec.
///
/// Chosen so a large historical import stays comfortably under the
/// publish-side hard backpressure gate
/// (`RAW_STREAM_BACKPRESSURE_HIGH_PENDING` = 10,000 pending in
/// `nats_publisher.rs`): at 500 events/sec the raw stream would need >20s of
/// total consumer stall before it even approaches that ceiling, which gives
/// operators visible, gradual pressure (via [`BacklogGate`]) instead of a
/// silent hard stop. This is a starting point, not a value tuned from
/// incident anecdotes alone (sinex-2n9 notes) — operators should override it
/// per source/operation once they have real throughput data.
pub const DEFAULT_EVENTS_PER_SEC: f64 = 500.0;

/// Default target byte rate for historical/catch-up scans, in bytes/sec (5 MB/s).
pub const DEFAULT_BYTES_PER_SEC: f64 = 5.0 * 1024.0 * 1024.0;

/// Default raw-stream backlog depth above which the scan loop pauses.
///
/// Deliberately below the publish-side hard gate (10,000) so this proactive,
/// visible pause acts first; the publish-side gate remains a backstop.
pub const DEFAULT_BACKLOG_PAUSE_THRESHOLD: u64 = 8_000;

/// Backlog depth the scan loop waits to drain back down to before resuming,
/// once paused (hysteresis, mirrors the publish-side gate's low watermark).
pub const DEFAULT_BACKLOG_RESUME_THRESHOLD: u64 = 2_000;

const BACKLOG_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Operator-configurable rate budget for a historical/catch-up scan.
///
/// `None` fields mean "unbounded on this dimension". [`RateBudget::default`]
/// (and [`RateBudget::default_paced`]) are paced; [`RateBudget::unlimited`]
/// is the only way to get fully unpaced behavior, and callers must request
/// it explicitly (e.g. `--unlimited` on `sinexctl replay execute/submit`).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RateBudget {
    /// Maximum sustained events/sec. `None` = unbounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub events_per_sec: Option<f64>,

    /// Maximum sustained bytes/sec. `None` = unbounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_per_sec: Option<f64>,

    /// Raw-stream consumer backlog depth above which the scan loop pauses.
    /// `None` = no backlog-based pausing (rate budget still applies).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backlog_pause_threshold: Option<u64>,

    /// Backlog depth to wait for before resuming after a pause.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backlog_resume_threshold: Option<u64>,
}

impl Default for RateBudget {
    fn default() -> Self {
        Self::default_paced()
    }
}

impl RateBudget {
    /// The default paced budget applied whenever an operator has not set an
    /// explicit override. This is what makes unpaced historical scans
    /// impossible without an explicit `--unlimited`.
    #[must_use]
    pub const fn default_paced() -> Self {
        Self {
            events_per_sec: Some(DEFAULT_EVENTS_PER_SEC),
            bytes_per_sec: Some(DEFAULT_BYTES_PER_SEC),
            backlog_pause_threshold: Some(DEFAULT_BACKLOG_PAUSE_THRESHOLD),
            backlog_resume_threshold: Some(DEFAULT_BACKLOG_RESUME_THRESHOLD),
        }
    }

    /// Fully unpaced: no rate limit, no backlog-based pausing. Only reachable
    /// via an explicit operator override (`--unlimited`), never a default.
    #[must_use]
    pub const fn unlimited() -> Self {
        Self {
            events_per_sec: None,
            bytes_per_sec: None,
            backlog_pause_threshold: None,
            backlog_resume_threshold: None,
        }
    }

    #[must_use]
    pub fn is_unlimited(&self) -> bool {
        self.events_per_sec.is_none()
            && self.bytes_per_sec.is_none()
            && self.backlog_pause_threshold.is_none()
    }

    /// Load operator overrides from environment variables, falling back to
    /// [`RateBudget::default_paced`] for any unset field. `prefix` is the
    /// service env prefix (e.g. `"SINEXD"`), matching the
    /// `service_or_global_env_*` convention used elsewhere in `runtime::config`.
    #[must_use]
    pub fn from_env() -> Self {
        let defaults = Self::default_paced();
        Self {
            events_per_sec: shared_env::parse_optional(
                "SINEX_HISTORICAL_IMPORT_RATE_EVENTS_PER_SEC",
                "historical import pacing",
            )
            .or(defaults.events_per_sec),
            bytes_per_sec: shared_env::parse_optional(
                "SINEX_HISTORICAL_IMPORT_RATE_BYTES_PER_SEC",
                "historical import pacing",
            )
            .or(defaults.bytes_per_sec),
            backlog_pause_threshold: shared_env::parse_optional(
                "SINEX_HISTORICAL_IMPORT_BACKLOG_PAUSE_THRESHOLD",
                "historical import pacing",
            )
            .or(defaults.backlog_pause_threshold),
            backlog_resume_threshold: shared_env::parse_optional(
                "SINEX_HISTORICAL_IMPORT_BACKLOG_RESUME_THRESHOLD",
                "historical import pacing",
            )
            .or(defaults.backlog_resume_threshold),
        }
    }

    /// Merge a per-operation override on top of this budget: any field the
    /// override sets wins; unset fields fall through to `self`. Used to
    /// implement "operator-set via binding config, overridable per
    /// operation" — `self` is the binding-config/env default,
    /// `override_budget` is what a replay/import operation explicitly asked
    /// for (e.g. `RateBudget::unlimited()` for `--unlimited`).
    #[must_use]
    pub fn merged_with_override(self, override_budget: Option<RateBudget>) -> Self {
        override_budget.unwrap_or(self)
    }
}

/// Enforces a [`RateBudget`]'s events/sec and bytes/sec limits between scan
/// batches using a simple "average rate since start" throttle: after each
/// recorded batch, sleep just long enough that the observed average rate
/// never exceeds the configured budget.
#[derive(Debug)]
pub struct PacingController {
    budget: RateBudget,
    started: Instant,
    events_total: u64,
    bytes_total: u64,
}

impl PacingController {
    #[must_use]
    pub fn new(budget: RateBudget) -> Self {
        Self {
            budget,
            started: Instant::now(),
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

    /// Duration to sleep (if any) so that the average rate since `started`
    /// stays within budget, given `events`/`bytes` just processed. Pure
    /// function of internal counters — no I/O, fully unit-testable.
    fn required_sleep(&self, elapsed: Duration) -> Duration {
        let elapsed_secs = elapsed.as_secs_f64();
        let mut required_secs: f64 = 0.0;

        if let Some(rate) = self.budget.events_per_sec
            && rate > 0.0
        {
            required_secs = required_secs.max(self.events_total as f64 / rate);
        }
        if let Some(rate) = self.budget.bytes_per_sec
            && rate > 0.0
        {
            required_secs = required_secs.max(self.bytes_total as f64 / rate);
        }

        if required_secs > elapsed_secs {
            Duration::from_secs_f64(required_secs - elapsed_secs)
        } else {
            Duration::ZERO
        }
    }

    /// Record a processed batch and sleep as needed to hold the average rate
    /// within budget. No-op sleep when the budget is unlimited on both
    /// dimensions.
    pub async fn record_and_throttle(&mut self, events: u64, bytes: u64) {
        self.events_total = self.events_total.saturating_add(events);
        self.bytes_total = self.bytes_total.saturating_add(bytes);

        if self.budget.events_per_sec.is_none() && self.budget.bytes_per_sec.is_none() {
            return;
        }

        let sleep_for = self.required_sleep(self.elapsed());
        if sleep_for > Duration::ZERO {
            tokio::time::sleep(sleep_for).await;
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
    pub async fn wait_for_capacity<F, Fut>(&self, mut fetch_pending: F) -> RuntimeResult<()>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = RuntimeResult<Option<u64>>>,
    {
        let mut paused = false;
        loop {
            let pending = fetch_pending().await?;
            let Some(pending) = pending else {
                return Ok(());
            };

            let target = if paused {
                self.resume_threshold
            } else {
                self.pause_threshold
            };

            if pending <= target {
                return Ok(());
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

#[cfg(test)]
#[path = "pacing_test.rs"]
mod tests;
