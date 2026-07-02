use super::*;
use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::Event;
use sinex_primitives::{EventSource, EventType, Id};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn analytics_filters_to_trusted_activity_event_types() -> xtask::sandbox::TestResult<()> {
    let automaton = AnalyticsAutomaton::default();

    assert_eq!(automaton.input_event_type(), "*");
    assert_eq!(
        automaton.input_event_types(),
        vec![
            HyprlandWindowFocusedPayload::EVENT_TYPE.as_static_str(),
            ActivityWatchWindowActivePayload::EVENT_TYPE.as_static_str(),
            ActivityWatchBrowserTabActivePayload::EVENT_TYPE.as_static_str(),
            PageVisitedPayload::EVENT_TYPE.as_static_str(),
            KittyCommandExecutedPayload::EVENT_TYPE.as_static_str(),
        ]
    );
    assert_eq!(
        automaton.input_provenance_filter(),
        InputProvenanceFilter::MaterialOnly
    );
    Ok(())
}

#[sinex_test]
async fn analytics_default_window_budget_bounds_parent_fan_in(
) -> xtask::sandbox::TestResult<()> {
    let mut automaton = AnalyticsAutomaton::default();
    let mut state = AnalyticsState::default();
    let event_time = Timestamp::now();

    for _ in 0..DEFAULT_WINDOW_MAX_EVENTS {
        let context = trusted_window_context(event_time);
        automaton.accumulate(&mut state, JsonValue::Null, &context).await?;
        assert!(
            !automaton.window_complete(&state),
            "window should accept exactly the default budget before closing"
        );
    }

    let overflow_context = trusted_window_context(event_time);
    automaton
        .accumulate(&mut state, JsonValue::Null, &overflow_context)
        .await?;
    assert!(
        automaton.window_complete(&state),
        "the event after the budget should close the current window"
    );

    let flush_context = AutomatonContext::timer_flush(event_time)?;
    let output = automaton
        .emit(&mut state, &flush_context)
        .await?
        .expect("closed window should emit a summary");

    assert_eq!(
        output.source_event_ids.len(),
        DEFAULT_WINDOW_MAX_EVENTS,
        "default analytics windows should not emit more parents than the warning budget"
    );
    assert_eq!(
        output
            .aggregation
            .as_ref()
            .expect("analytics output should carry aggregation metadata")
            .total_input_count,
        DEFAULT_WINDOW_MAX_EVENTS as u64
    );
    assert_eq!(
        state.event_count, 1,
        "overflow event should seed the next window instead of joining the emitted window"
    );
    assert_eq!(
        state
            .event_ids
            .first()
            .copied()
            .expect("next window seed should keep the overflow event id"),
        overflow_context.trigger_uuid()
    );
    Ok(())
}

fn trusted_window_context(event_time: Timestamp) -> AutomatonContext {
    let trigger_event_id: Id<Event<JsonValue>> = Id::new();
    AutomatonContext {
        trigger_event_id,
        source: EventSource::from_static("wm.hyprland"),
        event_type: EventType::from_static("window.focused"),
        ts_orig: Some(event_time),
        ts_coided: trigger_event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
    }
}
