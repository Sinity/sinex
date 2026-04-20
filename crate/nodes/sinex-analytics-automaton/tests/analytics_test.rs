//! Tests for the analytics automaton's bounded activity windows.

use sinex_analytics_automaton::{AnalyticsAutomaton, AnalyticsState};
use sinex_node_sdk::derived_node::{DerivedAggregationMeta, DerivedOutput, DerivedTriggerContext};
use sinex_node_sdk::{NodeLogicError, WindowedNode};
use sinex_primitives::activity::ActivitySourceKind;
use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::Event;
use sinex_primitives::events::payloads::{ActivityWindowCloseReason, ActivityWindowSummaryPayload};
use sinex_primitives::temporal::{Duration, Timestamp};
use sinex_primitives::{Id, JsonValue};
use xtask::sandbox::prelude::*;

fn make_context_with_optional_ts(
    source: &str,
    event_type: &str,
    ts_orig: Option<Timestamp>,
) -> DerivedTriggerContext {
    let event_id: Id<Event<JsonValue>> = Id::new();
    DerivedTriggerContext {
        trigger_event_id: event_id,
        source: source.into(),
        event_type: event_type.into(),
        ts_orig,
        ts_coided: event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
    }
}

fn make_terminal_context(ts_orig: Timestamp) -> DerivedTriggerContext {
    make_context_with_optional_ts("shell.kitty", "command.executed", Some(ts_orig))
}

async fn process(
    automaton: &mut AnalyticsAutomaton,
    state: &mut AnalyticsState,
    ctx: &DerivedTriggerContext,
) -> Result<Option<DerivedOutput<ActivityWindowSummaryPayload>>, NodeLogicError> {
    automaton
        .accumulate(state, serde_json::json!({}), ctx)
        .await?;
    if automaton.window_complete(state) {
        automaton.emit(state, ctx).await
    } else {
        Ok(None)
    }
}

#[sinex_test]
async fn missing_ts_orig_is_rejected() -> TestResult<()> {
    let mut automaton = AnalyticsAutomaton::default();
    let mut state = AnalyticsState::default();
    let ctx = make_context_with_optional_ts("shell.kitty", "command.executed", None);

    let error = process(&mut automaton, &mut state, &ctx)
        .await
        .expect_err("missing ts_orig must be rejected");

    assert!(error.to_string().contains("missing ts_orig"));
    Ok(())
}

#[sinex_test]
async fn non_activity_events_do_not_seed_windows() -> TestResult<()> {
    let mut automaton = AnalyticsAutomaton::default();
    let mut state = AnalyticsState::default();
    let ts = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let ctx = make_context_with_optional_ts("systemd", "unit.started", Some(ts));

    let result = process(&mut automaton, &mut state, &ctx).await?;
    assert!(result.is_none());
    assert_eq!(state.event_count, 0);
    assert!(state.window_start.is_none());
    Ok(())
}

#[sinex_test]
async fn gap_closes_window_and_seeds_next_one() -> TestResult<()> {
    let mut automaton = AnalyticsAutomaton::default();
    let mut state = AnalyticsState::default();

    let first = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let second = first + Duration::seconds(301);
    let first_ctx = make_terminal_context(first);
    let second_ctx = make_terminal_context(second);

    assert!(
        process(&mut automaton, &mut state, &first_ctx)
            .await?
            .is_none()
    );
    let output = process(&mut automaton, &mut state, &second_ctx)
        .await?
        .expect("gap must close the current window");

    assert_eq!(output.payload.close_reason, ActivityWindowCloseReason::Gap);
    assert_eq!(output.payload.event_count, 1);
    assert_eq!(output.payload.primary_source, ActivitySourceKind::Terminal);
    assert_eq!(output.payload.window_start, first);
    assert_eq!(output.payload.window_end, first);
    assert_eq!(
        output.source_event_ids,
        vec![first_ctx.trigger_event_id.as_uuid().to_owned()]
    );
    assert_eq!(
        output.aggregation,
        Some(DerivedAggregationMeta::new("activity.window", 0, 1))
    );

    assert_eq!(state.window_start, Some(second));
    assert_eq!(state.last_event_time, Some(second));
    assert_eq!(state.event_count, 1);
    assert_eq!(
        state.event_ids,
        vec![*second_ctx.trigger_event_id.as_uuid()]
    );
    Ok(())
}

#[sinex_test]
async fn max_duration_closes_window_before_gap() -> TestResult<()> {
    let mut guard = EnvGuard::new();
    guard.set("SINEX_ACTIVITY_WINDOW_MAX_DURATION_SECS", "60");

    let mut automaton = AnalyticsAutomaton::default();
    let mut state = AnalyticsState::default();

    let first = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let second = first + Duration::seconds(61);
    let first_ctx = make_terminal_context(first);
    let second_ctx = make_terminal_context(second);

    assert!(
        process(&mut automaton, &mut state, &first_ctx)
            .await?
            .is_none()
    );
    let output = process(&mut automaton, &mut state, &second_ctx)
        .await?
        .expect("duration bound must close the current window");

    assert_eq!(
        output.payload.close_reason,
        ActivityWindowCloseReason::MaxDuration
    );
    assert_eq!(output.payload.duration_secs, 0);
    assert_eq!(state.window_start, Some(second));
    Ok(())
}

#[sinex_test]
async fn max_event_budget_closes_window_without_truncating_provenance() -> TestResult<()> {
    let mut guard = EnvGuard::new();
    guard.set("SINEX_ACTIVITY_WINDOW_MAX_EVENTS", "2");

    let mut automaton = AnalyticsAutomaton::default();
    let mut state = AnalyticsState::default();

    let base = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let first_ctx = make_terminal_context(base);
    let second_ctx = make_terminal_context(base + Duration::seconds(10));
    let third_ctx = make_terminal_context(base + Duration::seconds(20));

    assert!(
        process(&mut automaton, &mut state, &first_ctx)
            .await?
            .is_none()
    );
    assert!(
        process(&mut automaton, &mut state, &second_ctx)
            .await?
            .is_none()
    );
    let output = process(&mut automaton, &mut state, &third_ctx)
        .await?
        .expect("parent budget must close the current window");

    assert_eq!(
        output.payload.close_reason,
        ActivityWindowCloseReason::MaxEventCount
    );
    assert_eq!(output.payload.event_count, 2);
    assert_eq!(output.source_event_ids.len(), 2);
    assert_eq!(state.event_count, 1);
    assert_eq!(state.event_ids, vec![*third_ctx.trigger_event_id.as_uuid()]);
    Ok(())
}
