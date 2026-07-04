use super::*;
use crate::runtime::Transducer;
use crate::runtime::automaton::AutomatonContext;
use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::Event;
use sinex_primitives::{EventSource, EventType, Id, JsonValue, Timestamp, Uuid};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn interval_lift_consumes_focus_transitions() -> xtask::sandbox::TestResult<()> {
    let automaton = IntervalLift;

    assert_eq!(automaton.name(), "interval-lift");
    assert_eq!(automaton.input_event_type(), "*");
    assert_eq!(
        automaton.input_event_types(),
        vec!["window.focused", "window.active"]
    );
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

    let first_output = automaton
        .process(&mut state, serde_json::to_value(first)?, &first_context)
        .await?;
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
        .process(&mut state, serde_json::to_value(second)?, &second_context)
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
            serde_json::to_value(HyprlandWindowFocusedPayload {
                window_id: Some("0xabc".to_string()),
                window_class: Some("kitty".to_string()),
                window_title: Some("codex".to_string()),
                workspace_id: Some(1),
                previous_window_id: None,
            })?,
            &first_context,
        )
        .await?;
    let output = automaton
        .process(
            &mut state,
            serde_json::to_value(HyprlandWindowFocusedPayload {
                window_id: Some("0xdef".to_string()),
                window_class: Some("qutebrowser".to_string()),
                window_title: Some("Sinex".to_string()),
                workspace_id: Some(1),
                previous_window_id: Some("0xabc".to_string()),
            })?,
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

    automaton
        .process(&mut state, serde_json::to_value(first)?, &first_context)
        .await?;
    let same_focus_output = automaton
        .process(
            &mut state,
            serde_json::to_value(refresh_payload)?,
            &focus_context(refresh),
        )
        .await?;
    assert!(
        same_focus_output.is_none(),
        "same-window focus refreshes must not close zero-duration intervals"
    );

    let output = automaton
        .process(&mut state, serde_json::to_value(second)?, &second_context)
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
        .process(&mut state, serde_json::to_value(first)?, &focus_context(later))
        .await?;
    let output = automaton
        .process(
            &mut state,
            serde_json::to_value(second)?,
            &focus_context(earlier),
        )
        .await?;

    assert!(output.is_none());
    Ok(())
}

#[sinex_test]
async fn interval_lift_lifts_activitywatch_window_observed_duration(
) -> xtask::sandbox::TestResult<()> {
    let start = Timestamp::from_unix_timestamp(1_700_000_000)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let end = start + sinex_primitives::temporal::Duration::seconds(30);

    let mut automaton = IntervalLift;
    let mut state = IntervalLiftState::default();
    let first_context = activitywatch_context(start);
    let first_id = first_context.trigger_uuid();

    let output = automaton
        .process(
            &mut state,
            serde_json::to_value(ActivityWatchWindowActivePayload {
                app: "kitty".to_string(),
                title: "codex".to_string(),
                duration_ms: 30_000,
                bucket_id: "aw-watcher-window_sinnix-prime".to_string(),
            })?,
            &first_context,
        )
        .await?
        .expect("AW window rows carry observed duration and emit immediately");

    assert_eq!(output.ts_orig, end);
    assert_eq!(output.source_event_ids, vec![first_id]);
    assert_eq!(output.payload.state_kind, "desktop.activitywatch.window");
    assert_eq!(
        output.payload.subject_id.as_deref(),
        Some("app:kitty|title:codex")
    );
    assert_eq!(output.payload.label.as_deref(), Some("kitty: codex"));
    assert_eq!(output.payload.start_time, start);
    assert_eq!(output.payload.end_time, end);
    assert_eq!(output.payload.duration_secs, 30);
    assert_eq!(output.payload.start_event_type, "window.active");
    assert_eq!(output.payload.end_event_type, "window.active");
    assert_eq!(
        output.payload.attributes.get("bucket_id").map(String::as_str),
        Some("aw-watcher-window_sinnix-prime")
    );
    assert_eq!(
        output.payload.attributes.get("duration_ms").map(String::as_str),
        Some("30000")
    );
    let expected_key =
        format!("interval:desktop.activitywatch.window:app:kitty|title:codex:{first_id}:{first_id}");
    assert_eq!(output.equivalence_key.as_deref(), Some(expected_key.as_str()));
    Ok(())
}

#[sinex_test]
async fn interval_lift_emits_each_activitywatch_window_row_independently(
) -> xtask::sandbox::TestResult<()> {
    let start = Timestamp::from_unix_timestamp(1_700_000_000)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let end = Timestamp::from_unix_timestamp(1_700_000_030)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;

    let mut automaton = IntervalLift;
    let mut state = IntervalLiftState::default();
    let first = automaton
        .process(
            &mut state,
            serde_json::to_value(ActivityWatchWindowActivePayload {
                app: "kitty".to_string(),
                title: "codex".to_string(),
                duration_ms: 10_000,
                bucket_id: "aw-watcher-window_sinnix-prime".to_string(),
            })?,
            &activitywatch_context(start),
        )
        .await?
        .expect("first AW row emits a span");

    let second = automaton
        .process(
            &mut state,
            serde_json::to_value(ActivityWatchWindowActivePayload {
                app: "qutebrowser".to_string(),
                title: "Sinex".to_string(),
                duration_ms: 10_000,
                bucket_id: "aw-watcher-window_sinnix-prime".to_string(),
            })?,
            &activitywatch_context(end),
        )
        .await?
        .expect("second AW row emits its own span");

    assert_eq!(first.payload.start_time, start);
    assert_eq!(first.payload.duration_secs, 10);
    assert_eq!(second.payload.start_time, end);
    assert_eq!(second.payload.duration_secs, 10);
    assert_eq!(
        second.payload.attributes.get("duration_ms").map(String::as_str),
        Some("10000")
    );
    Ok(())
}

#[sinex_test]
async fn interval_lift_decodes_legacy_focus_checkpoint_state(
) -> xtask::sandbox::TestResult<()> {
    let event_id = Uuid::now_v7();
    let state: IntervalLiftState = serde_json::from_value(serde_json::json!({
        "active_focus": {
            "event_id": event_id,
            "ts_orig": "2026-07-04T07:00:00Z",
            "window_id": "0xabc",
            "window_class": "kitty",
            "window_title": "codex",
            "workspace_id": 2
        }
    }))?;

    let active_focus = state
        .active_focus
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("legacy focus state should decode"))?;
    assert_eq!(active_focus.state_kind, "desktop.focus");
    assert_eq!(active_focus.event_id, event_id);
    assert_eq!(active_focus.subject_id.as_deref(), Some("0xabc"));
    assert_eq!(active_focus.label.as_deref(), Some("kitty: codex"));
    assert_eq!(active_focus.event_type, "window.focused");
    assert_eq!(
        active_focus
            .attributes
            .get("workspace_id")
            .map(String::as_str),
        Some("2")
    );
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

fn activitywatch_context(ts_orig: Timestamp) -> AutomatonContext {
    let trigger_event_id: Id<Event<JsonValue>> = Id::new();
    AutomatonContext {
        trigger_event_id,
        source: EventSource::from_static("activitywatch"),
        event_type: EventType::from_static("window.active"),
        ts_orig: Some(ts_orig),
        ts_coided: trigger_event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
    }
}
