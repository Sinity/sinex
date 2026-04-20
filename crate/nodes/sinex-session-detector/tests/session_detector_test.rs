use sinex_node_sdk::WindowedNode;
use sinex_node_sdk::derived_node::{DerivedAggregationMeta, DerivedTriggerContext};
use sinex_primitives::activity::ActivitySourceKind;
use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::Event;
use sinex_primitives::events::payloads::{
    ActivitySessionBoundaryPayload, ActivityWindowCloseReason, ActivityWindowSummaryPayload,
};
use sinex_primitives::temporal::{Duration, Timestamp};
use sinex_primitives::{Id, JsonValue};
use sinex_session_detector::{SessionDetector, SessionState};
use std::collections::BTreeMap;
use xtask::sandbox::prelude::*;

fn make_context(ts_orig: Timestamp) -> DerivedTriggerContext {
    let event_id: Id<Event<JsonValue>> = Id::new();
    DerivedTriggerContext {
        trigger_event_id: event_id,
        source: ActivityWindowSummaryPayload::SOURCE,
        event_type: ActivityWindowSummaryPayload::EVENT_TYPE,
        ts_orig: Some(ts_orig),
        ts_coided: event_id.timestamp(),
        processing_mode: ProcessingMode::Replay,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
    }
}

fn make_window(
    index: u64,
    start: Timestamp,
    end: Timestamp,
    event_count: u64,
    close_reason: ActivityWindowCloseReason,
    primary_source: ActivitySourceKind,
) -> ActivityWindowSummaryPayload {
    ActivityWindowSummaryPayload {
        window_id: format!("window-{index}"),
        window_start: start,
        window_end: end,
        duration_secs: (end - start).whole_seconds().max(0) as u64,
        event_count,
        source_count: 1,
        sources: vec!["shell.kitty".to_string()],
        activity_sources: vec![primary_source],
        activity_source_counts: BTreeMap::from([(primary_source, event_count)]),
        primary_source,
        close_reason,
    }
}

#[sinex_test]
async fn budget_windows_accumulate_without_emitting_session() -> TestResult<()> {
    let mut detector = SessionDetector;
    let mut state = SessionState::default();
    let start = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let payload = make_window(
        1,
        start,
        start + Duration::seconds(30),
        12,
        ActivityWindowCloseReason::MaxEventCount,
        ActivitySourceKind::Terminal,
    );
    let ctx = make_context(payload.window_end);

    detector.accumulate(&mut state, payload, &ctx).await?;

    assert!(!detector.window_complete(&state));
    assert_eq!(state.event_count, 12);
    assert_eq!(state.window_count, 1);
    assert_eq!(
        state.window_event_ids,
        vec![*ctx.trigger_event_id.as_uuid()]
    );
    Ok(())
}

#[sinex_test]
async fn gap_closed_window_emits_completed_session() -> TestResult<()> {
    let mut detector = SessionDetector;
    let mut state = SessionState::default();

    let first_start = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let first_payload = make_window(
        1,
        first_start,
        first_start + Duration::seconds(120),
        20,
        ActivityWindowCloseReason::MaxDuration,
        ActivitySourceKind::Terminal,
    );
    let second_payload = make_window(
        2,
        first_start + Duration::seconds(120),
        first_start + Duration::seconds(240),
        10,
        ActivityWindowCloseReason::Gap,
        ActivitySourceKind::Window,
    );

    let first_ctx = make_context(first_payload.window_end);
    let second_ctx = make_context(second_payload.window_end);

    detector
        .accumulate(&mut state, first_payload, &first_ctx)
        .await?;
    detector
        .accumulate(&mut state, second_payload, &second_ctx)
        .await?;

    assert!(detector.window_complete(&state));
    let output = detector
        .emit(&mut state, &second_ctx)
        .await?
        .expect("gap-closed window should emit a completed session");

    let payload = output.payload;
    assert_eq!(payload.event_count, 30);
    assert_eq!(payload.window_count, 2);
    assert_eq!(payload.start_time, first_start);
    assert_eq!(payload.end_time, first_start + Duration::seconds(240));
    assert_eq!(payload.primary_source, ActivitySourceKind::Terminal);
    assert_eq!(
        payload.activity_sources,
        vec![ActivitySourceKind::Terminal, ActivitySourceKind::Window]
    );
    assert_eq!(
        payload
            .activity_source_counts
            .get(&ActivitySourceKind::Terminal),
        Some(&20)
    );
    assert_eq!(
        payload
            .activity_source_counts
            .get(&ActivitySourceKind::Window),
        Some(&10)
    );
    assert_eq!(
        output.source_event_ids,
        vec![
            first_ctx.trigger_event_id.as_uuid().to_owned(),
            second_ctx.trigger_event_id.as_uuid().to_owned(),
        ]
    );
    assert_eq!(
        output.aggregation,
        Some(DerivedAggregationMeta::new("activity.session", 1, 30))
    );
    assert_eq!(state.window_count, 0);
    assert!(state.window_event_ids.is_empty());

    Ok(())
}

#[sinex_test]
async fn parse_session_event_shape_roundtrips() -> TestResult<()> {
    let start = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let end = start + Duration::minutes(42);
    let payload = ActivitySessionBoundaryPayload {
        session_id: "session-7".to_string(),
        start_time: start,
        end_time: end,
        duration_secs: 2520,
        event_count: 4,
        window_count: 2,
        source_count: 2,
        sources: vec!["shell.kitty".to_string(), "wm.hyprland".to_string()],
        activity_sources: vec![ActivitySourceKind::Terminal, ActivitySourceKind::Window],
        activity_source_counts: BTreeMap::from([
            (ActivitySourceKind::Terminal, 3),
            (ActivitySourceKind::Window, 1),
        ]),
        primary_source: ActivitySourceKind::Terminal,
    };

    let encoded = serde_json::to_value(&payload)?;
    let decoded: ActivitySessionBoundaryPayload = serde_json::from_value(encoded)?;

    assert_eq!(decoded.window_count, 2);
    assert_eq!(decoded.primary_source, ActivitySourceKind::Terminal);
    Ok(())
}
