//! Canonical runtime liveness evaluation.
//!
//! Every operator-facing runtime surface should evaluate the same evidence:
//! run lifecycle, health, heartbeat, and output recency.  This module is
//! deliberately pure so API handlers, tests, and future non-DB consumers
//! cannot silently grow different definitions of "live".

use crate::Timestamp;
use crate::domain::{HealthStatus, ModuleKind, ModuleName};

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

impl std::fmt::Display for RuntimeLivenessStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Unhealthy => "unhealthy",
            Self::Stale => "stale",
            Self::Stopped => "stopped",
            Self::Unknown => "unknown",
        };
        formatter.write_str(value)
    }
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

/// Whether a persisted runtime row participates in system health.
///
/// A manifest without a concrete run is excluded because runtime state cannot
/// distinguish disabled, profile-gated, and never-started modules. A latest
/// stopped run is historical evidence and is excluded. Every other concrete
/// run is assessed, including failed and stale evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeLivenessMembership {
    Assessed,
    DisabledOrProfileGated,
    HistoricalStopped,
}

impl RuntimeLivenessMembership {
    #[must_use]
    pub const fn is_assessed(self) -> bool {
        matches!(self, Self::Assessed)
    }
}

/// Raw persisted evidence for one runtime module.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RuntimeLivenessEvidence {
    pub module_name: ModuleName,
    pub module_kind: ModuleKind,
    pub membership: RuntimeLivenessMembership,
    pub run_status: Option<String>,
    pub health_status: Option<HealthStatus>,
    pub last_heartbeat_at: Option<Timestamp>,
    pub last_output_at: Option<Timestamp>,
}

/// Canonical liveness verdict for one persisted runtime module.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RuntimeLivenessAssessment {
    pub module_name: ModuleName,
    pub module_kind: ModuleKind,
    pub membership: RuntimeLivenessMembership,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub liveness: Option<RuntimeLiveness>,
}

/// Aggregate runtime liveness used by system health surfaces.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RuntimeLivenessAggregate {
    pub status: HealthStatus,
    pub healthy: bool,
    pub assessed_count: usize,
    pub healthy_count: usize,
    pub degraded_count: usize,
    pub unhealthy_count: usize,
    pub stale_count: usize,
    pub unknown_count: usize,
    pub excluded_disabled_or_profile_gated_count: usize,
    pub excluded_historical_stopped_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation_error: Option<String>,
    pub runtimes: Vec<RuntimeLivenessAssessment>,
}

impl RuntimeLivenessAggregate {
    #[must_use]
    pub fn evaluate(
        evidence: impl IntoIterator<Item = RuntimeLivenessEvidence>,
        policy: RuntimeLivenessPolicy,
        now: Timestamp,
    ) -> Self {
        let mut aggregate = Self {
            status: HealthStatus::Healthy,
            healthy: true,
            assessed_count: 0,
            healthy_count: 0,
            degraded_count: 0,
            unhealthy_count: 0,
            stale_count: 0,
            unknown_count: 0,
            excluded_disabled_or_profile_gated_count: 0,
            excluded_historical_stopped_count: 0,
            observation_error: None,
            runtimes: Vec::new(),
        };

        for evidence in evidence {
            let liveness = evidence.membership.is_assessed().then(|| {
                evaluate_runtime_liveness(
                    RuntimeLivenessSignals {
                        run_status: evidence.run_status.as_deref(),
                        health_status: evidence.health_status,
                        last_heartbeat_at: evidence.last_heartbeat_at,
                        last_output_at: evidence.last_output_at,
                    },
                    policy,
                    now,
                )
            });

            match liveness.as_ref().map(|value| value.status) {
                Some(RuntimeLivenessStatus::Healthy) => {
                    aggregate.assessed_count += 1;
                    aggregate.healthy_count += 1;
                }
                Some(RuntimeLivenessStatus::Degraded) => {
                    aggregate.assessed_count += 1;
                    aggregate.degraded_count += 1;
                }
                Some(RuntimeLivenessStatus::Unhealthy) => {
                    aggregate.assessed_count += 1;
                    aggregate.unhealthy_count += 1;
                }
                Some(RuntimeLivenessStatus::Stale) => {
                    aggregate.assessed_count += 1;
                    aggregate.stale_count += 1;
                }
                Some(RuntimeLivenessStatus::Unknown) => {
                    aggregate.assessed_count += 1;
                    aggregate.unknown_count += 1;
                }
                Some(RuntimeLivenessStatus::Stopped) => {
                    aggregate.assessed_count += 1;
                    aggregate.unhealthy_count += 1;
                }
                None => match evidence.membership {
                    RuntimeLivenessMembership::DisabledOrProfileGated => {
                        aggregate.excluded_disabled_or_profile_gated_count += 1;
                    }
                    RuntimeLivenessMembership::HistoricalStopped => {
                        aggregate.excluded_historical_stopped_count += 1;
                    }
                    RuntimeLivenessMembership::Assessed => {
                        unreachable!("assessed evidence has a verdict")
                    }
                },
            }

            aggregate.runtimes.push(RuntimeLivenessAssessment {
                module_name: evidence.module_name,
                module_kind: evidence.module_kind,
                membership: evidence.membership,
                liveness,
            });
        }

        aggregate.runtimes.sort_by(|left, right| {
            left.module_name
                .as_ref()
                .cmp(right.module_name.as_ref())
                .then_with(|| {
                    left.module_kind
                        .to_string()
                        .cmp(&right.module_kind.to_string())
                })
        });
        aggregate.status = if aggregate.unhealthy_count > 0 || aggregate.stale_count > 0 {
            HealthStatus::Unhealthy
        } else if aggregate.degraded_count > 0 || aggregate.unknown_count > 0 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        };
        aggregate.healthy = aggregate.status == HealthStatus::Healthy;
        aggregate
    }

    #[must_use]
    pub fn unavailable(error: impl Into<String>) -> Self {
        Self {
            status: HealthStatus::Unhealthy,
            healthy: false,
            assessed_count: 0,
            healthy_count: 0,
            degraded_count: 0,
            unhealthy_count: 0,
            stale_count: 0,
            unknown_count: 0,
            excluded_disabled_or_profile_gated_count: 0,
            excluded_historical_stopped_count: 0,
            observation_error: Some(error.into()),
            runtimes: Vec::new(),
        }
    }
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

    #[test]
    fn aggregate_keeps_failed_and_stale_evidence_out_of_all_clear() {
        let aggregate = RuntimeLivenessAggregate::evaluate(
            [
                RuntimeLivenessEvidence {
                    module_name: ModuleName::new("failed-source"),
                    module_kind: ModuleKind::Source,
                    membership: RuntimeLivenessMembership::Assessed,
                    run_status: Some("failed".to_string()),
                    health_status: Some(HealthStatus::Healthy),
                    last_heartbeat_at: Some((datetime!(2026-08-12 11:59 UTC)).into()),
                    last_output_at: None,
                },
                RuntimeLivenessEvidence {
                    module_name: ModuleName::new("stale-automaton"),
                    module_kind: ModuleKind::Automaton,
                    membership: RuntimeLivenessMembership::Assessed,
                    run_status: Some("running".to_string()),
                    health_status: Some(HealthStatus::Healthy),
                    last_heartbeat_at: Some((datetime!(2026-08-12 11:54 UTC)).into()),
                    last_output_at: None,
                },
            ],
            RuntimeLivenessPolicy::new(300),
            now(),
        );

        assert_eq!(aggregate.status, HealthStatus::Unhealthy);
        assert!(!aggregate.healthy);
        assert_eq!(aggregate.unhealthy_count, 1);
        assert_eq!(aggregate.stale_count, 1);
        assert_eq!(aggregate.assessed_count, 2);
    }

    #[test]
    fn aggregate_excludes_disabled_profile_gated_and_historical_stopped_members() {
        let aggregate = RuntimeLivenessAggregate::evaluate(
            [
                RuntimeLivenessEvidence {
                    module_name: ModuleName::new("profile-gated-source"),
                    module_kind: ModuleKind::Source,
                    membership: RuntimeLivenessMembership::DisabledOrProfileGated,
                    run_status: None,
                    health_status: None,
                    last_heartbeat_at: None,
                    last_output_at: None,
                },
                RuntimeLivenessEvidence {
                    module_name: ModuleName::new("old-stopped-automaton"),
                    module_kind: ModuleKind::Automaton,
                    membership: RuntimeLivenessMembership::HistoricalStopped,
                    run_status: Some("stopped".to_string()),
                    health_status: Some(HealthStatus::Healthy),
                    last_heartbeat_at: Some((datetime!(2026-08-12 11:00 UTC)).into()),
                    last_output_at: None,
                },
            ],
            RuntimeLivenessPolicy::default(),
            now(),
        );

        assert_eq!(aggregate.status, HealthStatus::Healthy);
        assert!(aggregate.healthy);
        assert_eq!(aggregate.assessed_count, 0);
        assert_eq!(aggregate.excluded_disabled_or_profile_gated_count, 1);
        assert_eq!(aggregate.excluded_historical_stopped_count, 1);
        assert!(
            aggregate
                .runtimes
                .iter()
                .all(|runtime| runtime.liveness.is_none())
        );
    }
}
