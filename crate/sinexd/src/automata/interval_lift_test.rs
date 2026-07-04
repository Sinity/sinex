use super::*;
use crate::runtime::Transducer;
use crate::runtime::automaton::AutomatonContext;
use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::Event;
use sinex_primitives::{EventSource, EventType, Id, JsonValue, Timestamp};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn interval_lift_consumes_focus_transitions() -> xtask::sandbox::TestResult<()> {
    let automaton = IntervalLift;

    assert_eq!(automaton.name(), "interval-lift");
    assert_eq!(automaton.input_event_type(), "window.focused");
    assert_eq!(automaton.output_event_type(), "state.interval");
    assert_eq!(automaton.output_event_source(), "derived.interval-lift");
    assert_eq!(
        automaton.input_provenance_filter(),
        InputProvenanceFilter::MaterialOnly
    );
    Ok(())
}

#[sinex_test]
async fn interval_lift_closes_previous_focus_on_next_transition(
) -> xtask::sandbox::TestResult<()> {
    let start = Timestamp::from_unix_timestamp(1_700_000_000)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let end = Timestamp::from_unix_timestamp(1_700_000_045)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;

    let mut automaton = IntervalLift;
    let mut state = IntervalLiftState::default();
    let first_context = focus_context(start);
    let first_id = first_context.trigger_uuid();
    let first = HyprlandWindowFocusedPayload {
        window_id: Some("0xabc".to_string()),
        window_class: Some("kitty".to_string()),
        window_title: Some("codex".to_string()),
        workspace_id: Some(2),
        previous_window_id: None,
    };

    let first_output = automaton.process(&mut state, first, &first_context).await?;
    assert!(first_output.is_none(), "first transition seeds the open interval");

    let second_context = focus_context(end);
    let second_id = second_context.trigger_uuid();
    let second = HyprlandWindowFocusedPayload {
        window_id: Some("0xdef".to_string()),
        window_class: Some("qutebrowser".to_string()),
        window_title: Some("Sinex".to_string()),
        workspace_id: Some(2),
        previous_window_id: Some("0xabc".to_string()),
    };

    let output = automaton
        .process(&mut state, second, &second_context)
        .await?
        .expect("second transition closes the previous focus interval");

    assert_eq!(output.ts_orig, end);
    assert_eq!(output.source_event_ids, vec![first_id, second_id]);
    assert_eq!(output.semantics_version.as_deref(), Some("1.0.0"));
    assert_eq!(output.payload.state_kind, "desktop.focus");
    assert_eq!(output.payload.subject_id.as_deref(), Some("0xabc"));
    assert_eq!(output.payload.label.as_deref(), Some("kitty: codex"));
    assert_eq!(output.payload.start_time, start);
    assert_eq!(output.payload.end_time, end);
    assert_eq!(output.payload.duration_secs, 45);
    assert_eq!(output.payload.start_event_type, "window.focused");
    assert_eq!(output.payload.end_event_type, "window.focused");
    assert_eq!(
        output.payload.attributes.get("window_class").map(String::as_str),
        Some("kitty")
    );
    assert_eq!(
        output.payload.attributes.get("workspace_id").map(String::as_str),
        Some("2")
    );
    let expected_key = format!("interval:desktop.focus:0xabc:{first_id}:{second_id}");
    assert_eq!(output.equivalence_key.as_deref(), Some(expected_key.as_str()));
    Ok(())
}

#[sinex_test]
async fn interval_lift_equivalence_key_uses_parent_ids_not_local_sequence(
) -> xtask::sandbox::TestResult<()> {
    let start = Timestamp::from_unix_timestamp(1_700_000_000)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let end = Timestamp::from_unix_timestamp(1_700_000_010)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;

    let mut automaton = IntervalLift;
    let mut state = IntervalLiftState::default();
    let first_context = focus_context(start);
    let first_id = first_context.trigger_uuid();
    let second_context = focus_context(end);
    let second_id = second_context.trigger_uuid();

    automaton
        .process(
            &mut state,
            HyprlandWindowFocusedPayload {
                window_id: Some("0xabc".to_string()),
                window_class: Some("kitty".to_string()),
                window_title: Some("codex".to_string()),
                workspace_id: Some(1),
                previous_window_id: None,
            },
            &first_context,
        )
        .await?;
    let output = automaton
        .process(
            &mut state,
            HyprlandWindowFocusedPayload {
                window_id: Some("0xdef".to_string()),
                window_class: Some("qutebrowser".to_string()),
                window_title: Some("Sinex".to_string()),
                workspace_id: Some(1),
                previous_window_id: Some("0xabc".to_string()),
            },
            &second_context,
        )
        .await?
        .expect("subject transition closes one interval");

    assert_eq!(
        output.payload.interval_id,
        format!("interval:desktop.focus:0xabc:{first_id}:{second_id}")
    );
    assert_eq!(
        output.equivalence_key.as_deref(),
        Some(output.payload.interval_id.as_str())
    );
    Ok(())
}

#[sinex_test]
async fn interval_lift_updates_same_focus_without_closing_interval(
) -> xtask::sandbox::TestResult<()> {
    let start = Timestamp::from_unix_timestamp(1_700_000_000)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let refresh = Timestamp::from_unix_timestamp(1_700_000_005)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let end = Timestamp::from_unix_timestamp(1_700_000_045)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;

    let mut automaton = IntervalLift;
    let mut state = IntervalLiftState::default();
    let first_context = focus_context(start);
    let first_id = first_context.trigger_uuid();

    let first = HyprlandWindowFocusedPayload {
        window_id: Some("0xabc".to_string()),
        window_class: Some("kitty".to_string()),
        window_title: Some("spinner 1".to_string()),
        workspace_id: Some(2),
        previous_window_id: None,
    };
    let refresh_payload = HyprlandWindowFocusedPayload {
        window_id: Some("0xabc".to_string()),
        window_class: Some("kitty".to_string()),
        window_title: Some("spinner 2".to_string()),
        workspace_id: Some(2),
        previous_window_id: Some("0xabc".to_string()),
    };
    let second_context = focus_context(end);
    let second_id = second_context.trigger_uuid();
    let second = HyprlandWindowFocusedPayload {
        window_id: Some("0xdef".to_string()),
        window_class: Some("qutebrowser".to_string()),
        window_title: Some("Sinex".to_string()),
        workspace_id: Some(2),
        previous_window_id: Some("0xabc".to_string()),
    };

    automaton.process(&mut state, first, &first_context).await?;
    let same_focus_output = automaton
        .process(&mut state, refresh_payload, &focus_context(refresh))
        .await?;
    assert!(
        same_focus_output.is_none(),
        "same-window focus refreshes must not close zero-duration intervals"
    );

    let output = automaton
        .process(&mut state, second, &second_context)
        .await?
        .expect("different subject closes the original focus interval");

    assert_eq!(output.source_event_ids, vec![first_id, second_id]);
    assert_eq!(output.payload.start_time, start);
    assert_eq!(output.payload.end_time, end);
    assert_eq!(output.payload.duration_secs, 45);
    assert_eq!(output.payload.label.as_deref(), Some("kitty: spinner 2"));
    Ok(())
}

#[sinex_test]
async fn interval_lift_ignores_non_monotonic_transition() -> xtask::sandbox::TestResult<()> {
    let later = Timestamp::from_unix_timestamp(1_700_000_100)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let earlier = Timestamp::from_unix_timestamp(1_700_000_090)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;

    let mut automaton = IntervalLift;
    let mut state = IntervalLiftState::default();
    let first = HyprlandWindowFocusedPayload {
        window_id: Some("0xabc".to_string()),
        window_class: Some("kitty".to_string()),
        window_title: Some("codex".to_string()),
        workspace_id: Some(2),
        previous_window_id: None,
    };
    let second = HyprlandWindowFocusedPayload {
        window_id: Some("0xdef".to_string()),
        window_class: Some("qutebrowser".to_string()),
        window_title: Some("Sinex".to_string()),
        workspace_id: Some(2),
        previous_window_id: Some("0xabc".to_string()),
    };

    automaton
        .process(&mut state, first, &focus_context(later))
        .await?;
    let output = automaton
        .process(&mut state, second, &focus_context(earlier))
        .await?;

    assert!(output.is_none());
    Ok(())
}

fn focus_context(ts_orig: Timestamp) -> AutomatonContext {
    let trigger_event_id: Id<Event<JsonValue>> = Id::new();
    AutomatonContext {
        trigger_event_id,
        source: EventSource::from_static("wm.hyprland"),
        event_type: EventType::from_static("window.focused"),
        ts_orig: Some(ts_orig),
        ts_coided: trigger_event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
    }
}
