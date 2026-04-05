use sinex_node_sdk::WindowedNode;
use sinex_node_sdk::derived_node::DerivedTriggerContext;
use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::Event;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{Id, JsonValue};
use sinex_session_detector::{SessionDetector, SessionState};
use xtask::sandbox::prelude::*;

fn make_context_with_optional_ts(ts_orig: Option<Timestamp>) -> DerivedTriggerContext {
    let event_id: Id<Event<JsonValue>> = Id::new();
    DerivedTriggerContext {
        trigger_event_id: event_id,
        source: "test".into(),
        event_type: "desktop.window.focused".into(),
        ts_orig,
        ts_coided: event_id.timestamp(),
        processing_mode: ProcessingMode::Replay,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
    }
}

fn make_context(ts_orig: Timestamp) -> DerivedTriggerContext {
    make_context_with_optional_ts(Some(ts_orig))
}

#[sinex_test]
async fn replay_events_do_not_trigger_gap_from_wall_clock() -> TestResult<()> {
    let mut detector = SessionDetector::default();
    let mut state = SessionState::default();

    let first = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let second = Timestamp::from_unix_timestamp(1_700_000_001).expect("valid timestamp");

    detector
        .accumulate(&mut state, serde_json::json!({}), &make_context(first))
        .await?;
    detector
        .accumulate(&mut state, serde_json::json!({}), &make_context(second))
        .await?;

    assert!(
        !detector.window_complete(&state),
        "replay of closely spaced historical events must not trigger a session boundary from wall-clock drift"
    );
    Ok(())
}

#[sinex_test]
async fn missing_ts_orig_is_rejected() -> TestResult<()> {
    let mut detector = SessionDetector::default();
    let mut state = SessionState::default();

    let error = detector
        .accumulate(
            &mut state,
            serde_json::json!({}),
            &make_context_with_optional_ts(None),
        )
        .await
        .expect_err("missing ts_orig must be rejected");

    assert!(error.to_string().contains("missing ts_orig"));
    Ok(())
}

#[sinex_test]
async fn event_time_gap_triggers_session_boundary() -> TestResult<()> {
    let mut detector = SessionDetector::default();
    let mut state = SessionState::default();

    let first = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let second = Timestamp::from_unix_timestamp(1_700_000_301).expect("valid timestamp");

    detector
        .accumulate(&mut state, serde_json::json!({}), &make_context(first))
        .await?;
    detector
        .accumulate(&mut state, serde_json::json!({}), &make_context(second))
        .await?;

    assert!(
        detector.window_complete(&state),
        "a five-minute event-time gap must trigger a session boundary"
    );
    Ok(())
}

#[sinex_test]
async fn invalid_gap_override_falls_back_to_default_threshold() -> TestResult<()> {
    let mut _guard = EnvGuard::new();
    _guard.set("SINEX_SESSION_GAP_SECS", "not-a-number");
    let mut detector = SessionDetector::default();
    let mut state = SessionState::default();

    let first = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let second = Timestamp::from_unix_timestamp(1_700_000_301).expect("valid timestamp");

    detector
        .accumulate(&mut state, serde_json::json!({}), &make_context(first))
        .await?;
    detector
        .accumulate(&mut state, serde_json::json!({}), &make_context(second))
        .await?;

    assert!(
        detector.window_complete(&state),
        "invalid overrides should fall back to the five-minute default"
    );
    Ok(())
}

#[sinex_test]
async fn valid_gap_override_changes_boundary_detection() -> TestResult<()> {
    let mut _guard = EnvGuard::new();
    _guard.set("SINEX_SESSION_GAP_SECS", "600");
    let mut detector = SessionDetector::default();
    let mut state = SessionState::default();

    let first = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let second = Timestamp::from_unix_timestamp(1_700_000_301).expect("valid timestamp");

    detector
        .accumulate(&mut state, serde_json::json!({}), &make_context(first))
        .await?;
    detector
        .accumulate(&mut state, serde_json::json!({}), &make_context(second))
        .await?;

    assert!(
        !detector.window_complete(&state),
        "a wider configured threshold should suppress the default five-minute boundary"
    );
    Ok(())
}

#[sinex_test]
async fn non_positive_gap_override_falls_back_to_default_threshold() -> TestResult<()> {
    let mut _guard = EnvGuard::new();
    _guard.set("SINEX_SESSION_GAP_SECS", "0");
    let mut detector = SessionDetector::default();
    let mut state = SessionState::default();

    let first = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let second = Timestamp::from_unix_timestamp(1_700_000_301).expect("valid timestamp");

    detector
        .accumulate(&mut state, serde_json::json!({}), &make_context(first))
        .await?;
    detector
        .accumulate(&mut state, serde_json::json!({}), &make_context(second))
        .await?;

    assert!(
        detector.window_complete(&state),
        "non-positive overrides should be rejected and fall back to default behavior"
    );
    Ok(())
}

#[sinex_test]
async fn gap_boundary_emits_previous_session_and_seeds_next_one() -> TestResult<()> {
    let mut detector = SessionDetector::default();
    let mut state = SessionState::default();

    let first = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
    let second = Timestamp::from_unix_timestamp(1_700_000_301).expect("valid timestamp");

    let first_context = make_context(first);
    let second_context = make_context(second);

    detector
        .accumulate(&mut state, serde_json::json!({}), &first_context)
        .await?;
    detector
        .accumulate(&mut state, serde_json::json!({}), &second_context)
        .await?;

    assert!(detector.window_complete(&state));
    assert_eq!(
        state.event_count, 1,
        "the post-gap event must not be absorbed into the emitted session"
    );

    let output = detector
        .emit(&mut state, &second_context)
        .await?
        .expect("gap boundary should emit a completed session");

    let payload = output
        .payload
        .as_object()
        .expect("session output should be an object");
    assert_eq!(payload.get("event_count"), Some(&serde_json::json!(1)));
    assert_eq!(
        payload.get("start_time"),
        Some(&serde_json::json!(first.format_rfc3339()))
    );
    assert_eq!(
        payload.get("end_time"),
        Some(&serde_json::json!(first.format_rfc3339()))
    );
    assert_eq!(
        output.source_event_ids,
        vec![first_context.trigger_event_id.as_uuid().to_owned()],
        "emitted provenance must stay attached to the completed session only"
    );

    assert_eq!(state.session_start, Some(second));
    assert_eq!(state.last_event_time, Some(second));
    assert_eq!(state.event_count, 1);
    assert!(
        !state.gap_detected,
        "next session should be ready to accumulate"
    );
    assert_eq!(
        state.event_ids,
        vec![*second_context.trigger_event_id.as_uuid()]
    );

    Ok(())
}
