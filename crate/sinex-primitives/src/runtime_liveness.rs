//! Canonical runtime liveness evaluation.
//!
//! Every operator-facing runtime surface should evaluate the same evidence:
//! run lifecycle, health, heartbeat, and output recency.  This module is
//! deliberately pure so API handlers, tests, and future non-DB consumers
//! cannot silently grow different definitions of "live".

use crate::Timestamp;
use crate::domain::HealthStatus;

/// Shared default used by runtime, source, and automaton status requests.
pub const DEFAULT_RUNTIME_LIVENESS_STALE_AFTER_SECS: u64 = 300;

/// Explicit policy input for a runtime liveness evaluation.
///
/// Callers may choose a different threshold for a deliberately narrower
/// observation window, but they must pass that choice as policy rather than
/// embedding another freshness rule in their own status calculation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeLivenessPolicy {
    pub stale_after_secs: u64,
}

impl RuntimeLivenessPolicy {
    #[must_use]
    pub const fn new(stale_after_secs: u64) -> Self {
        Self { stale_after_secs }
    }

    #[must_use]
    pub const fn considers_stale(self, age_secs: i64) -> bool {
        age_secs >= self.stale_after_secs as i64
    }
}

impl Default for RuntimeLivenessPolicy {
    fn default() -> Self {
        Self::new(DEFAULT_RUNTIME_LIVENESS_STALE_AFTER_SECS)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeLivenessStatus {
    Healthy,
    Degraded,
    Unhealthy,
    Stale,
    Stopped,
    Unknown,
}

impl RuntimeLivenessStatus {
    #[must_use]
    pub const fn is_live(self) -> bool {
        matches!(self, Self::Healthy | Self::Degraded)
    }

    #[must_use]
    pub const fn is_failure(self) -> bool {
        matches!(self, Self::Unhealthy | Self::Stale | Self::Stopped)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RuntimeLiveness {
    pub status: RuntimeLivenessStatus,
    pub last_observed_at: Option<Timestamp>,
    pub age_secs: Option<i64>,
    /// Human-readable evidence labels, retained with the result so callers
    /// can explain the verdict without reimplementing the precedence rules.
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct RuntimeLivenessSignals<'a> {
    pub run_status: Option<&'a str>,
    pub health_status: Option<HealthStatus>,
    pub last_heartbeat_at: Option<Timestamp>,
    pub last_output_at: Option<Timestamp>,
}

#[must_use]
pub fn evaluate_runtime_liveness(
    signals: RuntimeLivenessSignals<'_>,
    policy: RuntimeLivenessPolicy,
    now: Timestamp,
) -> RuntimeLiveness {
    let last_observed_at = [signals.last_heartbeat_at, signals.last_output_at]
        .into_iter()
        .flatten()
        .max();
    let age_secs = last_observed_at.map(|observed| (now - observed).whole_seconds().max(0));

    let mut evidence = Vec::new();
    if let Some(run_status) = signals.run_status {
        evidence.push(format!("run_status={run_status}"));
    }
    if let Some(health_status) = signals.health_status {
        evidence.push(format!("health_status={health_status}"));
    }
    if let Some(heartbeat) = signals.last_heartbeat_at {
        evidence.push(format!("heartbeat_at={heartbeat}"));
    }
    if let Some(output) = signals.last_output_at {
        evidence.push(format!("output_at={output}"));
    }
    if let Some(age) = age_secs {
        evidence.push(format!("observed_age_secs={age}"));
    }
    evidence.push(format!("stale_after_secs={}", policy.stale_after_secs));

    let run_status = signals.run_status.map(str::to_ascii_lowercase);
    let status = if matches!(
        run_status.as_deref(),
        Some("failed" | "error" | "cancelled" | "canceled")
    ) {
        RuntimeLivenessStatus::Unhealthy
    } else if matches!(run_status.as_deref(), Some("stopped" | "terminated")) {
        RuntimeLivenessStatus::Stopped
    } else if matches!(signals.health_status, Some(HealthStatus::Unhealthy)) {
        RuntimeLivenessStatus::Unhealthy
    } else if last_observed_at.is_none() {
        RuntimeLivenessStatus::Unknown
    } else if age_secs.is_some_and(|age| age >= policy.stale_after_secs as i64) {
        RuntimeLivenessStatus::Stale
    } else if matches!(run_status.as_deref(), Some("draining" | "paused"))
        || matches!(signals.health_status, Some(HealthStatus::Degraded))
    {
        RuntimeLivenessStatus::Degraded
    } else {
        RuntimeLivenessStatus::Healthy
    };

    evidence.push(format!("status={status:?}"));
    RuntimeLiveness {
        status,
        last_observed_at,
        age_secs,
        evidence,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    fn now() -> Timestamp {
        datetime!(2026-08-12 12:00 UTC).into()
    }

    #[test]
    fn stale_timestamp_is_not_live() {
        let result = evaluate_runtime_liveness(
            RuntimeLivenessSignals {
                run_status: Some("running"),
                health_status: Some(HealthStatus::Healthy),
                last_heartbeat_at: Some((datetime!(2026-08-12 11:54 UTC)).into()),
                last_output_at: None,
            },
            RuntimeLivenessPolicy::new(300),
            now(),
        );
        assert_eq!(result.status, RuntimeLivenessStatus::Stale);
        assert_eq!(result.age_secs, Some(360));
    }

    #[test]
    fn historical_output_is_stale_when_no_newer_observation_exists() {
        let result = evaluate_runtime_liveness(
            RuntimeLivenessSignals {
                run_status: Some("running"),
                health_status: None,
                last_heartbeat_at: None,
                last_output_at: Some((datetime!(2026-08-12 11:50 UTC)).into()),
            },
            RuntimeLivenessPolicy::new(300),
            now(),
        );
        assert_eq!(result.status, RuntimeLivenessStatus::Stale);
        assert_eq!(
            result.last_observed_at,
            Some(datetime!(2026-08-12 11:50 UTC).into())
        );
        assert!(
            result
                .evidence
                .iter()
                .any(|item| item == "stale_after_secs=300")
        );
    }

    #[test]
    fn never_observed_is_unknown_even_when_runtime_is_registered() {
        let result = evaluate_runtime_liveness(
            RuntimeLivenessSignals {
                run_status: Some("running"),
                health_status: None,
                last_heartbeat_at: None,
                last_output_at: None,
            },
            RuntimeLivenessPolicy::new(300),
            now(),
        );
        assert_eq!(result.status, RuntimeLivenessStatus::Unknown);
        assert_eq!(result.last_observed_at, None);
        assert!(!result.status.is_live());
    }

    #[test]
    fn runtime_info_failed_status_wins_over_fresh_heartbeat() {
        let result = evaluate_runtime_liveness(
            RuntimeLivenessSignals {
                run_status: Some("failed"),
                health_status: Some(HealthStatus::Healthy),
                last_heartbeat_at: Some((datetime!(2026-08-12 11:59:30 UTC)).into()),
                last_output_at: None,
            },
            RuntimeLivenessPolicy::new(300),
            now(),
        );
        assert_eq!(result.status, RuntimeLivenessStatus::Unhealthy);
        assert!(result.status.is_failure());
    }

    #[test]
    fn failed_run_wins_over_fresh_output() {
        let result = evaluate_runtime_liveness(
            RuntimeLivenessSignals {
                run_status: Some("failed"),
                health_status: Some(HealthStatus::Healthy),
                last_heartbeat_at: None,
                last_output_at: Some((datetime!(2026-08-12 11:59 UTC)).into()),
            },
            RuntimeLivenessPolicy::new(300),
            now(),
        );
        assert_eq!(result.status, RuntimeLivenessStatus::Unhealthy);
    }

    #[test]
    fn fresh_degraded_health_is_degraded_not_stale() {
        let result = evaluate_runtime_liveness(
            RuntimeLivenessSignals {
                run_status: Some("running"),
                health_status: Some(HealthStatus::Degraded),
                last_heartbeat_at: Some((datetime!(2026-08-12 11:59:30 UTC)).into()),
                last_output_at: None,
            },
            RuntimeLivenessPolicy::new(300),
            now(),
        );
        assert_eq!(result.status, RuntimeLivenessStatus::Degraded);
    }

    #[test]
    fn paused_run_is_degraded_even_with_fresh_heartbeat() {
        let result = evaluate_runtime_liveness(
            RuntimeLivenessSignals {
                run_status: Some("paused"),
                health_status: Some(HealthStatus::Healthy),
                last_heartbeat_at: Some((datetime!(2026-08-12 11:59:30 UTC)).into()),
                last_output_at: None,
            },
            RuntimeLivenessPolicy::new(300),
            now(),
        );
        assert_eq!(result.status, RuntimeLivenessStatus::Degraded);
    }

    #[test]
    fn latest_of_heartbeat_and_output_is_used() {
        let result = evaluate_runtime_liveness(
            RuntimeLivenessSignals {
                run_status: Some("running"),
                health_status: Some(HealthStatus::Healthy),
                last_heartbeat_at: Some((datetime!(2026-08-12 11:50 UTC)).into()),
                last_output_at: Some((datetime!(2026-08-12 11:59 UTC)).into()),
            },
            RuntimeLivenessPolicy::new(300),
            now(),
        );
        assert_eq!(result.status, RuntimeLivenessStatus::Healthy);
        assert_eq!(
            result.last_observed_at,
            Some((datetime!(2026-08-12 11:59 UTC)).into())
        );
    }

    #[test]
    fn policy_is_the_single_staleness_boundary() {
        let policy = RuntimeLivenessPolicy::new(30);
        assert!(!policy.considers_stale(29));
        assert!(policy.considers_stale(30));
        assert_eq!(RuntimeLivenessPolicy::default().stale_after_secs, 300);
    }
}
