//! Actor and preview-state validation helpers shared by the replay control
//! client and server.
//!
//! The functions here are intentionally free of any networking or database
//! dependencies so they can be exercised in isolation by inline unit tests
//! and reused as defense-in-depth checks on both sides of the NATS bus.

use crate::api::cascade_analyzer::{CascadeAnalyzerConfig, Severity, StreamingCascadeAnalyzer};
use serde_json::Value as JsonValue;
use sinex_db::replay::state_machine::{ReplayOperation, ReplayState};
use sinex_primitives::env as shared_env;
use sinex_primitives::rpc::replay::ReplayGateOverrides;
use sinex_primitives::{Result, SinexError, Uuid};
use std::collections::HashSet;
use tracing::warn;

/// Valid actor roles for replay operations.
pub(super) const VALID_ACTOR_ROLES: &[&str] = &[
    "system",   // Internal system operations
    "service",  // Service accounts
    "user",     // Authenticated users
    "admin",    // Administrative operations
    "operator", // Operations team
    "test",     // Test actors (testing-only)
];

#[derive(Debug, Clone, Copy)]
pub(super) enum ReplayAction {
    Plan,
    Approve,
    Execute,
    Cancel,
}

pub(super) const ANCHOR_CHURN_THRESHOLD_PERCENT: f64 = 5.0;
pub(super) const TIME_QUALITY_FLIP_THRESHOLD_PERCENT: f64 = 2.0;
pub(super) const MAX_CASCADE_DEPTH_WARN: u64 = 5;

#[derive(Debug, Clone)]
struct ReplayGate {
    name: &'static str,
    tripped: bool,
    advisory: bool,
    override_allowed: bool,
    override_flag: &'static str,
    observed: String,
    threshold: String,
}

fn preview_f64(preview: &JsonValue, key: &str) -> Option<f64> {
    preview.get(key).and_then(JsonValue::as_f64)
}

fn preview_u64(preview: &JsonValue, key: &str) -> u64 {
    preview.get(key).and_then(JsonValue::as_u64).unwrap_or(0)
}

fn preview_bool(preview: &JsonValue, key: &str) -> bool {
    preview
        .get(key)
        .and_then(JsonValue::as_bool)
        .unwrap_or(false)
}

fn replay_gates(preview: &JsonValue, overrides: &ReplayGateOverrides) -> [ReplayGate; 4] {
    let anchor_churn_pct = preview_f64(preview, "anchor_churn_pct");
    let time_quality_flip_pct = preview_f64(preview, "time_quality_flip_pct");
    let max_observed_depth = preview_u64(preview, "max_observed_depth");
    let schema_boundary_crossed = preview_bool(preview, "schema_boundary_crossed");

    [
        ReplayGate {
            name: "anchor_churn_threshold_percent",
            tripped: anchor_churn_pct.is_some_and(|pct| pct > ANCHOR_CHURN_THRESHOLD_PERCENT),
            advisory: anchor_churn_pct.is_none(),
            override_allowed: overrides.allow_anchor_churn,
            override_flag: "--allow-anchor-churn",
            observed: anchor_churn_pct.map_or_else(
                || "not measured (advisory)".to_string(),
                |pct| format!("{pct:.2}%"),
            ),
            threshold: format!("{ANCHOR_CHURN_THRESHOLD_PERCENT:.2}%"),
        },
        ReplayGate {
            name: "time_quality_flip_threshold_percent",
            tripped: time_quality_flip_pct
                .is_some_and(|pct| pct > TIME_QUALITY_FLIP_THRESHOLD_PERCENT),
            advisory: time_quality_flip_pct.is_none(),
            override_allowed: overrides.allow_time_quality_flips,
            override_flag: "--allow-time-quality-flips",
            observed: time_quality_flip_pct.map_or_else(
                || "not measured (advisory)".to_string(),
                |pct| format!("{pct:.2}%"),
            ),
            threshold: format!("{TIME_QUALITY_FLIP_THRESHOLD_PERCENT:.2}%"),
        },
        ReplayGate {
            name: "max_cascade_depth_warn",
            tripped: max_observed_depth > MAX_CASCADE_DEPTH_WARN,
            advisory: false,
            override_allowed: overrides.allow_deep_cascade,
            override_flag: "--allow-deep-cascade",
            observed: max_observed_depth.to_string(),
            threshold: MAX_CASCADE_DEPTH_WARN.to_string(),
        },
        ReplayGate {
            name: "require_force_on_schema_mismatch",
            tripped: schema_boundary_crossed,
            advisory: false,
            override_allowed: overrides.force_schema_mismatch,
            override_flag: "--force-schema-mismatch",
            observed: schema_boundary_crossed.to_string(),
            threshold: "false".to_string(),
        },
    ]
}

pub(super) fn replay_gate_report(preview: &JsonValue) -> JsonValue {
    let overrides = ReplayGateOverrides::default();
    let gates = replay_gates(preview, &overrides)
        .into_iter()
        .map(|gate| {
            serde_json::json!({
                "name": gate.name,
                "tripped": gate.tripped,
                "advisory": gate.advisory,
                "override_flag": gate.override_flag,
                "observed": gate.observed,
                "threshold": gate.threshold,
            })
        })
        .collect::<Vec<_>>();

    serde_json::json!({
        "defaults": {
            "anchor_churn_threshold_percent": ANCHOR_CHURN_THRESHOLD_PERCENT,
            "time_quality_flip_threshold_percent": TIME_QUALITY_FLIP_THRESHOLD_PERCENT,
            "max_cascade_depth_warn": MAX_CASCADE_DEPTH_WARN,
            "require_force_on_schema_mismatch": true,
        },
        "tripped": gates
            .iter()
            .filter(|gate| gate.get("tripped").and_then(JsonValue::as_bool).unwrap_or(false))
            .count(),
        "gates": gates,
    })
}

pub(super) fn ensure_replay_gates_pass(
    operation_id: Uuid,
    preview: &JsonValue,
    overrides: &ReplayGateOverrides,
) -> Result<()> {
    let blocked = replay_gates(preview, overrides)
        .into_iter()
        .filter(|gate| gate.tripped && !gate.override_allowed)
        .collect::<Vec<_>>();
    if blocked.is_empty() {
        return Ok(());
    }

    let gate_names = blocked
        .iter()
        .map(|gate| gate.name)
        .collect::<Vec<_>>()
        .join(", ");
    let override_flags = blocked
        .iter()
        .map(|gate| gate.override_flag)
        .collect::<Vec<_>>()
        .join(", ");
    let observed = blocked
        .iter()
        .map(|gate| {
            format!(
                "{}={} threshold={}",
                gate.name, gate.observed, gate.threshold
            )
        })
        .collect::<Vec<_>>()
        .join("; ");

    Err(
        SinexError::invalid_state("Replay preview trips TARGET_CANONICAL gate defaults")
            .with_context("operation_id", operation_id.to_string())
            .with_context("tripped_gates", gate_names)
            .with_context("override_flags", override_flags)
            .with_context("observed", observed)
            .with_context(
                "hint",
                "refresh preview or pass the explicit override flag(s)",
            ),
    )
}

pub(super) fn ensure_preview_allowed(operation: &ReplayOperation) -> Result<()> {
    match operation.state {
        ReplayState::Planning | ReplayState::Previewed => Ok(()),
        ReplayState::Approved => Err(SinexError::invalid_state(
            "Replay operation is already approved; create a new plan to refresh the preview",
        )
        .with_context("operation_id", operation.operation_id.to_string())
        .with_context("state", format!("{:?}", operation.state))),
        ReplayState::Executing | ReplayState::Committing | ReplayState::Cancelling => {
            Err(SinexError::invalid_state(
                "Replay operation is already executing; preview is no longer available",
            )
            .with_context("operation_id", operation.operation_id.to_string())
            .with_context("state", format!("{:?}", operation.state)))
        }
        ReplayState::Completed | ReplayState::Failed | ReplayState::Cancelled => {
            Err(SinexError::invalid_state(
                "Replay operation is in a terminal state; preview is no longer available",
            )
            .with_context("operation_id", operation.operation_id.to_string())
            .with_context("state", format!("{:?}", operation.state)))
        }
    }
}

pub(super) fn allow_test_actors_in_runtime(is_test_runtime: bool) -> Result<bool> {
    if is_test_runtime {
        return Ok(true);
    }

    Ok(shared_env::strict_flag("SINEX_ALLOW_TEST_ACTORS")?.unwrap_or(false))
}

pub(super) fn allow_test_actors() -> Result<bool> {
    allow_test_actors_in_runtime(cfg!(test))
}

pub(super) fn validate_actor_for_action(actor: &str, action: ReplayAction) -> Result<()> {
    if actor.is_empty() {
        return Err(SinexError::validation("Actor cannot be empty"));
    }
    if actor.trim() != actor {
        return Err(SinexError::validation(
            "Actor cannot contain leading or trailing whitespace",
        ));
    }
    if actor.chars().any(char::is_control) {
        return Err(SinexError::validation(
            "Actor contains invalid control characters",
        ));
    }

    let (role, identifier) = actor.split_once(':').ok_or_else(|| {
        SinexError::validation("Invalid actor format")
            .with_context("expected", "<role>:<identifier>")
    })?;

    if !VALID_ACTOR_ROLES.contains(&role) {
        return Err(SinexError::validation("Invalid actor role")
            .with_context("role", role)
            .with_context("allowed_roles", VALID_ACTOR_ROLES.join(", ")));
    }

    if identifier.is_empty() || identifier.trim().is_empty() {
        return Err(SinexError::validation("Actor identifier cannot be empty"));
    }
    if identifier.trim() != identifier {
        return Err(SinexError::validation(
            "Actor identifier cannot contain leading or trailing whitespace",
        ));
    }
    if identifier.chars().any(char::is_control) {
        return Err(SinexError::validation(
            "Actor identifier contains invalid control characters",
        ));
    }

    if role == "test" && !allow_test_actors()? {
        return Err(
            SinexError::permission_denied("Test actors are disabled in this environment")
                .with_context("hint", "set SINEX_ALLOW_TEST_ACTORS=1 to enable"),
        );
    }

    let requires_privileged_role = matches!(
        action,
        ReplayAction::Approve | ReplayAction::Execute | ReplayAction::Cancel
    );
    if requires_privileged_role && !matches!(role, "admin" | "operator" | "service" | "system") {
        return Err(
            SinexError::permission_denied("Actor role cannot perform this replay action")
                .with_context("role", role)
                .with_context("action", format!("{action:?}")),
        );
    }

    Ok(())
}

/// Run the `StreamingCascadeAnalyzer` against a set of root event IDs and return the
/// results as a JSON blob suitable for embedding in a preview response under
/// `"safety_analysis"`.
///
/// This is best-effort: on error the result becomes a structured failure object so that
/// the preview remains useful even when the analyzer cannot complete (e.g., timeout,
/// memory limit exceeded).
pub(super) async fn run_safety_analysis(
    pool: &sqlx::PgPool,
    root_ids: &[Uuid],
) -> serde_json::Value {
    if root_ids.is_empty() {
        return serde_json::json!({
            "integrity_violations": [],
            "circular_dependencies": [],
            "warnings": [],
        });
    }

    let config = CascadeAnalyzerConfig::from_env();
    let analyzer = StreamingCascadeAnalyzer::with_config(pool.clone(), config);

    match analyzer.analyze_cascades(root_ids).await {
        Ok(analysis) => {
            let critical_violation_count = analysis
                .integrity_violations
                .iter()
                .filter(|v| matches!(v.severity, Severity::Critical))
                .count();

            let mut warnings: Vec<serde_json::Value> = Vec::new();
            if critical_violation_count > 0 {
                warnings.push(serde_json::json!({
                    "level": "critical",
                    "message": format!(
                        "{} integrity violation(s) detected: live events reference events that \
                        are already outside the live cascade. Refresh the preview before execution.",
                        critical_violation_count
                    ),
                }));
            }
            if !analysis.circular_dependencies.is_empty() {
                warnings.push(serde_json::json!({
                    "level": "warning",
                    "message": format!(
                        "{} circular dependency cycle(s) detected in the cascade graph.",
                        analysis.circular_dependencies.len()
                    ),
                }));
            }

            serde_json::json!({
                "integrity_violations": analysis.integrity_violations,
                "circular_dependencies": analysis.circular_dependencies,
                "max_depth": analysis.max_depth,
                "total_affected": analysis.total_affected,
                "warnings": warnings,
            })
        }
        Err(e) => {
            warn!(error = %e, "Cascade safety analysis failed");
            serde_json::json!({
                "status": "failed",
                "error": e.to_string(),
                "warning": "Cascade impact could not be determined. Approve with caution."
            })
        }
    }
}

pub(super) fn summarize_uuid_set(ids: &HashSet<Uuid>) -> String {
    let mut sorted: Vec<_> = ids.iter().copied().collect();
    sorted.sort_unstable();

    let total = sorted.len();
    let sample = sorted
        .into_iter()
        .take(3)
        .map(|id| id.to_string())
        .collect::<Vec<_>>();

    match sample.len() {
        0 => "none".to_string(),
        count if total > count => format!("{} ...", sample.join(", ")),
        _ => sample.join(", "),
    }
}

pub(super) fn stale_preview_missing_root_ids_error(
    operation_id: Uuid,
    expected_total_events: u64,
) -> SinexError {
    SinexError::invalid_state(
        "Replay preview is stale: root_event_ids is absent, so ID-level staleness detection is not possible",
    )
    .with_context("operation_id", operation_id.to_string())
    .with_context("expected_total_events", expected_total_events.to_string())
    .with_context("hint", "refresh preview before execution")
}

pub(super) fn replay_scope_drift_error(
    operation_id: Uuid,
    expected_total_events: u64,
    expected_root_ids: &[Uuid],
    actual_root_ids: &[Uuid],
) -> SinexError {
    if expected_root_ids.is_empty() {
        return SinexError::invalid_state("Replay preview is stale")
            .with_context("operation_id", operation_id.to_string())
            .with_context("expected_total_events", expected_total_events.to_string())
            .with_context("actual_root_events", actual_root_ids.len().to_string())
            .with_context("hint", "refresh preview before execution");
    }

    let expected: HashSet<_> = expected_root_ids.iter().copied().collect();
    let actual: HashSet<_> = actual_root_ids.iter().copied().collect();
    let missing: HashSet<_> = expected.difference(&actual).copied().collect();
    let unexpected: HashSet<_> = actual.difference(&expected).copied().collect();

    SinexError::invalid_state(
        "Replay preview is stale: live scope no longer matches approved preview",
    )
    .with_context("operation_id", operation_id.to_string())
    .with_context("expected_total_events", expected_total_events.to_string())
    .with_context("actual_root_events", actual_root_ids.len().to_string())
    .with_context("missing_previewed_roots", missing.len().to_string())
    .with_context(
        "missing_previewed_root_sample",
        summarize_uuid_set(&missing),
    )
    .with_context("unexpected_live_roots", unexpected.len().to_string())
    .with_context(
        "unexpected_live_root_sample",
        summarize_uuid_set(&unexpected),
    )
    .with_context("hint", "refresh preview before execution")
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::{EnvGuard, sinex_test};

    #[sinex_test]
    async fn actor_validation_rejects_empty_actor() -> Result<()> {
        let result = validate_actor_for_action("", ReplayAction::Plan);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));
        Ok(())
    }

    #[sinex_test]
    async fn actor_validation_rejects_invalid_role() -> Result<()> {
        let result = validate_actor_for_action("invalid:user", ReplayAction::Plan);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid actor role"));
        Ok(())
    }

    #[sinex_test]
    async fn actor_validation_rejects_empty_identifier() -> Result<()> {
        let result = validate_actor_for_action("user:", ReplayAction::Plan);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("identifier cannot be empty")
        );
        Ok(())
    }

    #[sinex_test]
    async fn actor_validation_accepts_valid_actors() -> Result<()> {
        assert!(validate_actor_for_action("user:alice", ReplayAction::Plan).is_ok());
        assert!(validate_actor_for_action("admin:root", ReplayAction::Plan).is_ok());
        assert!(validate_actor_for_action("service:replay-worker", ReplayAction::Plan).is_ok());
        assert!(validate_actor_for_action("system:internal", ReplayAction::Plan).is_ok());
        assert!(validate_actor_for_action("operator:ops-team", ReplayAction::Plan).is_ok());
        assert!(validate_actor_for_action("test:unit-test", ReplayAction::Plan).is_ok());
        Ok(())
    }

    #[sinex_test]
    async fn replay_test_actor_flag_rejects_invalid_boolean() -> Result<()> {
        let mut env = EnvGuard::new();
        env.set("SINEX_ALLOW_TEST_ACTORS", "certainly");

        let error = allow_test_actors_in_runtime(false)
            .expect_err("invalid replay actor toggle should be rejected");
        assert!(error.to_string().contains("SINEX_ALLOW_TEST_ACTORS"));
        Ok(())
    }

    #[sinex_test]
    async fn privileged_actions_reject_user_role() -> Result<()> {
        let result = validate_actor_for_action("user:alice", ReplayAction::Execute);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("cannot perform this replay action")
        );
        Ok(())
    }

    #[sinex_test]
    async fn replay_gates_report_tripped_defaults() -> Result<()> {
        let preview = serde_json::json!({
            "anchor_churn_pct": 6.0,
            "time_quality_flip_pct": 3.0,
            "max_observed_depth": 6,
            "schema_boundary_crossed": true,
        });

        let report = replay_gate_report(&preview);
        assert_eq!(report["tripped"].as_u64(), Some(4));
        assert!(format!("{report}").contains("--force-schema-mismatch"));
        Ok(())
    }

    #[sinex_test]
    async fn replay_gates_report_unmeasured_metrics_as_advisory() -> Result<()> {
        let preview = serde_json::json!({
            "anchor_churn_pct": null,
            "time_quality_flip_pct": null,
            "max_observed_depth": 0,
            "schema_boundary_crossed": false,
        });

        let report = replay_gate_report(&preview);
        assert_eq!(report["tripped"].as_u64(), Some(0));
        let gates = report["gates"]
            .as_array()
            .expect("gate report should include gates");
        assert!(
            gates
                .iter()
                .filter(|gate| gate["advisory"].as_bool() == Some(true))
                .all(|gate| gate["observed"].as_str() == Some("not measured (advisory)")),
            "unmeasured metrics should be advisory, not rendered as zero: {report}"
        );
        ensure_replay_gates_pass(Uuid::now_v7(), &preview, &ReplayGateOverrides::default())?;
        Ok(())
    }

    #[sinex_test]
    async fn replay_gates_reject_without_required_overrides() -> Result<()> {
        let cases = [
            (
                "anchor_churn_threshold_percent",
                serde_json::json!({
                    "anchor_churn_pct": 5.01,
                    "time_quality_flip_pct": 0.0,
                    "max_observed_depth": 0,
                    "schema_boundary_crossed": false,
                }),
            ),
            (
                "time_quality_flip_threshold_percent",
                serde_json::json!({
                    "anchor_churn_pct": 0.0,
                    "time_quality_flip_pct": 2.01,
                    "max_observed_depth": 0,
                    "schema_boundary_crossed": false,
                }),
            ),
            (
                "max_cascade_depth_warn",
                serde_json::json!({
                    "anchor_churn_pct": 0.0,
                    "time_quality_flip_pct": 0.0,
                    "max_observed_depth": 6,
                    "schema_boundary_crossed": false,
                }),
            ),
            (
                "require_force_on_schema_mismatch",
                serde_json::json!({
                    "anchor_churn_pct": 0.0,
                    "time_quality_flip_pct": 0.0,
                    "max_observed_depth": 0,
                    "schema_boundary_crossed": true,
                }),
            ),
        ];

        for (gate_name, preview) in cases {
            let error =
                ensure_replay_gates_pass(Uuid::now_v7(), &preview, &ReplayGateOverrides::default())
                    .expect_err("tripped gate must require an explicit override");
            assert!(error.to_string().contains("TARGET_CANONICAL"));
            assert!(
                format!("{error:#}").contains(gate_name),
                "expected error for {gate_name}, got {error:#}"
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn replay_gates_accept_matching_overrides() -> Result<()> {
        let preview = serde_json::json!({
            "anchor_churn_pct": 6.0,
            "time_quality_flip_pct": 3.0,
            "max_observed_depth": 6,
            "schema_boundary_crossed": true,
        });
        let overrides = ReplayGateOverrides {
            allow_anchor_churn: true,
            allow_time_quality_flips: true,
            allow_deep_cascade: true,
            force_schema_mismatch: true,
        };

        ensure_replay_gates_pass(Uuid::now_v7(), &preview, &overrides)?;
        Ok(())
    }
}
