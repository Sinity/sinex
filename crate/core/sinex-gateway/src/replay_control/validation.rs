use crate::cascade_analyzer::{CascadeAnalyzerConfig, Severity, StreamingCascadeAnalyzer};
use crate::config::env_bool_optional;
use color_eyre::eyre::{Result, eyre};
use sinex_db::replay::state_machine::{ReplayOperation, ReplayState};
use sinex_primitives::Uuid;
use std::collections::HashSet;
use std::time::Duration;
use tracing::warn;

pub(super) const REPLAY_CONTROL_SUBSCRIBE_ATTEMPTS: usize = 5;
pub(super) const REPLAY_CONTROL_SUBSCRIBE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
pub(super) const REPLAY_CONTROL_SUBSCRIBE_BACKOFF_BASE: Duration = Duration::from_millis(200);
pub(super) const REPLAY_CONTROL_SUBSCRIBE_BACKOFF_MAX: Duration = Duration::from_secs(2);
pub(super) const REPLAY_OUTPUT_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(30);

/// Valid actor roles for replay operations.
const VALID_ACTOR_ROLES: &[&str] = &[
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

pub(super) fn ensure_preview_allowed(operation: &ReplayOperation) -> Result<()> {
    match operation.state {
        ReplayState::Planning | ReplayState::Previewed => Ok(()),
        ReplayState::Approved => Err(eyre!(
            "Operation {} is already approved; create a new plan to refresh the preview",
            operation.operation_id
        )),
        ReplayState::Executing | ReplayState::Committing | ReplayState::Cancelling => Err(eyre!(
            "Operation {} is already executing; preview is no longer available",
            operation.operation_id
        )),
        ReplayState::Completed | ReplayState::Failed | ReplayState::Cancelled => Err(eyre!(
            "Operation {} is in terminal state {:?}; preview is no longer available",
            operation.operation_id,
            operation.state
        )),
    }
}

pub(super) fn allow_test_actors_in_runtime(is_test_runtime: bool) -> Result<bool> {
    if is_test_runtime {
        return Ok(true);
    }

    Ok(env_bool_optional("SINEX_ALLOW_TEST_ACTORS")?.unwrap_or(false))
}

pub(super) fn allow_test_actors() -> Result<bool> {
    allow_test_actors_in_runtime(cfg!(test))
}

pub(super) fn validate_actor_for_action(actor: &str, action: ReplayAction) -> Result<()> {
    if actor.is_empty() {
        return Err(eyre!("Actor cannot be empty"));
    }
    if actor.trim() != actor {
        return Err(eyre!("Actor cannot contain leading or trailing whitespace"));
    }
    if actor.chars().any(char::is_control) {
        return Err(eyre!("Actor contains invalid control characters"));
    }

    let (role, identifier) = actor
        .split_once(':')
        .ok_or_else(|| eyre!("Invalid actor format. Expected '<role>:<identifier>'"))?;

    if !VALID_ACTOR_ROLES.contains(&role) {
        return Err(eyre!(
            "Invalid actor role '{role}'. Allowed roles: {}",
            VALID_ACTOR_ROLES.join(", ")
        ));
    }

    if identifier.is_empty() || identifier.trim().is_empty() {
        return Err(eyre!("Actor identifier cannot be empty"));
    }
    if identifier.trim() != identifier {
        return Err(eyre!(
            "Actor identifier cannot contain leading or trailing whitespace"
        ));
    }
    if identifier.chars().any(char::is_control) {
        return Err(eyre!(
            "Actor identifier contains invalid control characters"
        ));
    }

    if role == "test" && !allow_test_actors()? {
        return Err(eyre!(
            "Test actors are disabled in this environment (set SINEX_ALLOW_TEST_ACTORS=1 to enable)"
        ));
    }

    let requires_privileged_role = matches!(
        action,
        ReplayAction::Approve | ReplayAction::Execute | ReplayAction::Cancel
    );
    if requires_privileged_role && !matches!(role, "admin" | "operator" | "service" | "system") {
        return Err(eyre!(
            "Actor role '{role}' cannot perform this replay action"
        ));
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
pub(super) async fn run_safety_analysis(pool: &sqlx::PgPool, root_ids: &[Uuid]) -> serde_json::Value {
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
                        would be archived. Execution may leave dangling references.",
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
) -> color_eyre::eyre::Report {
    eyre!(
        "Operation {} preview is stale: preview covered {} material-root events but \
         root_event_ids is absent. ID-level staleness detection is not possible; \
         refresh preview before execution",
        operation_id,
        expected_total_events,
    )
}

pub(super) fn replay_scope_drift_error(
    operation_id: Uuid,
    expected_total_events: u64,
    expected_root_ids: &[Uuid],
    actual_root_ids: &[Uuid],
) -> color_eyre::eyre::Report {
    if expected_root_ids.is_empty() {
        return eyre!(
            "Operation {} preview is stale: approved preview covered {} material-root events, \
             but execution matched {}. Refresh preview before execution",
            operation_id,
            expected_total_events,
            actual_root_ids.len()
        );
    }

    let expected: HashSet<_> = expected_root_ids.iter().copied().collect();
    let actual: HashSet<_> = actual_root_ids.iter().copied().collect();
    let missing: HashSet<_> = expected.difference(&actual).copied().collect();
    let unexpected: HashSet<_> = actual.difference(&expected).copied().collect();

    eyre!(
        "Operation {} preview is stale: approved preview covered {} material-root events, \
         but execution matched {}. Missing previewed roots: {} ({}). Unexpected live roots: {} ({}). \
         Refresh preview before execution",
        operation_id,
        expected_total_events,
        actual_root_ids.len(),
        missing.len(),
        summarize_uuid_set(&missing),
        unexpected.len(),
        summarize_uuid_set(&unexpected),
    )
}
