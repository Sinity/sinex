use sinex_node_sdk::derived_node::DerivedTriggerContext;
use sinex_node_sdk::WindowedNode;
use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::Event;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{Id, JsonValue};
use sinex_session_detector::{SessionDetector, SessionState};
use xtask::sandbox::prelude::*;

fn make_context(ts_orig: Timestamp) -> DerivedTriggerContext {
    let event_id: Id<Event<JsonValue>> = Id::new();
    DerivedTriggerContext {
        trigger_event_id: event_id,
        source: "test".into(),
        event_type: "desktop.window.focused".into(),
        ts_orig: Some(ts_orig),
        ts_coided: event_id.timestamp(),
        processing_mode: ProcessingMode::Replay,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
    }
}

#[sinex_test]
async fn replay_events_do_not_trigger_gap_from_wall_clock() -> TestResult<()> {
    let mut detector = SessionDetector;
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
async fn event_time_gap_triggers_session_boundary() -> TestResult<()> {
    let mut detector = SessionDetector;
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
