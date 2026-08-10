//! Historical-import rate budget (sinex-2n9).
//!
//! [`RateBudget`] is the operator-facing config for pacing catch-up work
//! (historical scans, replay re-ingest, staged imports): a target events/sec
//! and bytes/sec, plus raw-stream backlog pause/resume thresholds. It lives
//! here (not in `sinexd`) because it is a wire type: it travels both inside
//! `sinexd`'s internal `ScanArgs` and over the RPC boundary as a per-operation
//! override on `replay.execute_operation` / `replay.submit_operation`
//! (`ReplayGateOverrides::rate_budget`), so `sinexctl` and `sinexd` must
//! agree on one shape.
//!
//! The enforcement side (`PacingController`, `BacklogGate`) lives in
//! `sinexd::runtime::pacing` since it needs `tokio::time` and NATS access,
//! which this primitives crate does not depend on.

use serde::{Deserialize, Serialize};

use crate::env as shared_env;

/// Default target event rate for historical/catch-up scans, in events/sec.
///
/// Chosen so a large historical import stays comfortably under the
/// publish-side hard backpressure gate (`RAW_STREAM_BACKPRESSURE_HIGH_PENDING`
/// = 10,000 pending in `sinexd::runtime::nats_publisher`): at 500 events/sec
/// the raw stream would need >20s of total consumer stall before it even
/// approaches that ceiling, which gives operators visible, gradual pressure
/// (via `BacklogGate`) instead of a silent hard stop. This is a starting
/// point, not a value tuned from incident anecdotes alone (sinex-2n9 notes)
/// — operators should override it per source/operation once they have real
/// throughput data.
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
    /// [`RateBudget::default_paced`] for any unset field.
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

    /// Merge a per-operation override on top of this budget: the override
    /// wins outright when present; falls through to `self` when absent. Used
    /// to implement "operator-set via binding config, overridable per
    /// operation" — `self` is the binding-config/env default,
    /// `override_budget` is what a replay/import operation explicitly asked
    /// for (e.g. `RateBudget::unlimited()` for `--unlimited`).
    #[must_use]
    pub fn merged_with_override(self, override_budget: Option<RateBudget>) -> Self {
        override_budget.unwrap_or(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_budget_is_paced_not_unlimited() {
        let budget = RateBudget::default();
        assert!(!budget.is_unlimited());
        assert_eq!(budget.events_per_sec, Some(DEFAULT_EVENTS_PER_SEC));
        assert_eq!(budget.bytes_per_sec, Some(DEFAULT_BYTES_PER_SEC));
    }

    #[test]
    fn unlimited_budget_is_unlimited() {
        assert!(RateBudget::unlimited().is_unlimited());
    }

    #[test]
    fn merged_override_wins_when_present() {
        let base = RateBudget::default_paced();
        let merged = base.merged_with_override(Some(RateBudget::unlimited()));
        assert!(merged.is_unlimited());
    }

    #[test]
    fn merged_override_falls_through_when_absent() {
        let base = RateBudget::default_paced();
        let merged = base.merged_with_override(None);
        assert_eq!(merged.events_per_sec, base.events_per_sec);
    }

    #[test]
    fn round_trips_through_json_without_none_noise() {
        let unlimited = RateBudget::unlimited();
        let json = serde_json::to_value(unlimited).unwrap();
        assert_eq!(json, serde_json::json!({}));
        let round_tripped: RateBudget = serde_json::from_value(json).unwrap();
        assert_eq!(round_tripped, unlimited);
    }
}
