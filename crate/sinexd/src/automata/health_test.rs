use super::*;
use crate::runtime::automaton::AutomatonContext;
use sinex_primitives::JsonValue;
use xtask::sandbox::sinex_test;

fn status_event(component: &str, previous: &str, current: &str) -> JsonValue {
    serde_json::json!({
        "component": component,
        "previous_status": previous,
        "current_status": current,
    })
}

/// sinex-audit-outoforder-pattern: an out-of-order `health.status` event must
/// never revert `current_status` to a stale healthier value -- that would
/// silently suppress a real, currently-ongoing outage. This is the most
/// operationally dangerous instance of the six-site audit finding.
///
/// Reverting the monotonic guard in `health.rs` (comparing `now` against
/// `component_health.last_seen` before applying the transition) makes this
/// test fail: the late "healthy" event would overwrite `current_status` back
/// to `Healthy` even though the real, later state is `Unhealthy`.
#[sinex_test]
async fn health_out_of_order_status_does_not_revert_current_status()
-> xtask::sandbox::TestResult<()> {
    let mut aggregator = HealthAggregator::default();
    let mut state = HealthState::default();

    let t_outage = Timestamp::from_unix_timestamp(1_700_001_000).expect("valid ts");
    let t_stale = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid ts"); // before t_outage

    // The real, later observation: the component went unhealthy.
    let outage_ctx = AutomatonContext::timer_flush(t_outage)?;
    aggregator
        .reconcile(
            &mut state,
            "worker-a",
            status_event("worker-a", "healthy", "unhealthy"),
            &outage_ctx,
        )
        .await?;
    assert_eq!(
        state.component_health["worker-a"].current_status,
        HealthStatus::Unhealthy,
        "the real outage must be reflected"
    );

    // A late-arriving, out-of-order event claiming the component was healthy
    // BEFORE the outage. It must not revert the current status.
    let stale_ctx = AutomatonContext::timer_flush(t_stale)?;
    aggregator
        .reconcile(
            &mut state,
            "worker-a",
            status_event("worker-a", "unknown", "healthy"),
            &stale_ctx,
        )
        .await?;

    assert_eq!(
        state.component_health["worker-a"].current_status,
        HealthStatus::Unhealthy,
        "an out-of-order status event must never revert a real outage to a stale healthier status"
    );
    assert_eq!(
        state.component_health["worker-a"].status_since, t_outage,
        "status_since must still reflect the real (later) transition, not the stale event"
    );
    assert_eq!(
        state.component_health["worker-a"].last_seen, t_outage,
        "last_seen watermark must not move backward on out-of-order arrival"
    );
    Ok(())
}

/// A tie (an event at exactly the same `ts_orig` as the tracked watermark)
/// follows the same deterministic tiebreak as interval_lift (sinex-uzc): the
/// later-processed observation supersedes in place.
#[sinex_test]
async fn health_tied_timestamp_supersedes_deterministically() -> xtask::sandbox::TestResult<()> {
    let mut aggregator = HealthAggregator::default();
    let mut state = HealthState::default();
    let t = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid ts");

    let ctx = AutomatonContext::timer_flush(t)?;
    aggregator
        .reconcile(
            &mut state,
            "worker-b",
            status_event("worker-b", "unknown", "healthy"),
            &ctx,
        )
        .await?;
    aggregator
        .reconcile(
            &mut state,
            "worker-b",
            status_event("worker-b", "healthy", "degraded"),
            &ctx,
        )
        .await?;

    assert_eq!(
        state.component_health["worker-b"].current_status,
        HealthStatus::Degraded,
        "the later-processed observation at the same ts supersedes in place"
    );
    Ok(())
}
