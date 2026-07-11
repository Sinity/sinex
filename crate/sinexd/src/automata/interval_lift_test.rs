use super::*;
use crate::runtime::MultiOutputTransducer;
use crate::runtime::automaton::AutomatonContext;
use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::enums::{SystemdActiveState, SystemdUnitType};
use sinex_primitives::events::Event;
use sinex_primitives::{EventSource, EventType, Id, JsonValue, Timestamp, Uuid};
use std::collections::{BTreeMap, BTreeSet};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn interval_lift_consumes_focus_transitions() -> xtask::sandbox::TestResult<()> {
    let automaton = IntervalLift;

    assert_eq!(automaton.name(), "interval-lift");
    assert_eq!(automaton.input_event_type(), "*");
    assert_eq!(
        automaton.input_event_types(),
        vec![
            "window.focused",
            "workspace.switched",
            "window.active",
            "afk.changed",
            "unit.started",
            "unit.stopped"
        ]
    );
    assert_eq!(automaton.output_event_types(), &["state.interval"]);
    assert_eq!(automaton.output_event_source(), "derived.interval-lift");
    assert_eq!(
        automaton.input_provenance_filter(),
        InputProvenanceFilter::MaterialOnly
    );
    Ok(())
}

#[sinex_test]
async fn interval_lift_rule_catalog_is_the_input_contract() -> xtask::sandbox::TestResult<()> {
    let automaton = IntervalLift;
    let catalog = IntervalLift::rule_catalog();
    let advertised_inputs: BTreeSet<_> = automaton.input_event_types().into_iter().collect();
    let catalog_inputs: BTreeSet<_> = catalog
        .iter()
        .flat_map(|rule| rule.event_types.iter().copied())
        .collect();
    let state_kinds: BTreeSet<_> = catalog.iter().map(|rule| rule.state_kind).collect();
    let sources: BTreeSet<_> = catalog.iter().map(|rule| rule.source).collect();

    assert_eq!(
        advertised_inputs, catalog_inputs,
        "input_event_types must be derived from the interval-lift rule catalog"
    );
    assert_eq!(
        catalog_inputs,
        BTreeSet::from([
            "afk.changed",
            "unit.started",
            "unit.stopped",
            "window.active",
            "window.focused",
            "workspace.switched",
        ])
    );
    assert_eq!(
        state_kinds,
        BTreeSet::from([
            "desktop.activitywatch.afk",
            "desktop.activitywatch.window",
            "desktop.focus",
            "desktop.workspace",
            "system.systemd.unit",
        ])
    );
    assert_eq!(
        sources,
        BTreeSet::from(["activitywatch", "systemd", "wm.hyprland"])
    );
    assert_eq!(
        WINDOW_FOCUSED_EVENT_TYPE,
        HyprlandWindowFocusedPayload::EVENT_TYPE.as_static_str()
    );
    assert_eq!(
        WORKSPACE_SWITCHED_EVENT_TYPE,
        HyprlandWorkspaceSwitchedPayload::EVENT_TYPE.as_static_str()
    );
    assert_eq!(
        WINDOW_ACTIVE_EVENT_TYPE,
        ActivityWatchWindowActivePayload::EVENT_TYPE.as_static_str()
    );
    assert_eq!(
        AFK_CHANGED_EVENT_TYPE,
        ActivityWatchAfkChangedPayload::EVENT_TYPE.as_static_str()
    );
    assert_eq!(
        UNIT_STARTED_EVENT_TYPE,
        SystemdUnitStartedPayload::EVENT_TYPE.as_static_str()
    );
    assert_eq!(
        UNIT_STOPPED_EVENT_TYPE,
        SystemdUnitStoppedPayload::EVENT_TYPE.as_static_str()
    );
    assert!(
        catalog
            .iter()
            .any(|rule| rule.shape == IntervalLiftRuleShape::AdjacentTransitions),
        "catalog should include adjacent transition lifters"
    );
    assert!(
        catalog
            .iter()
            .any(|rule| rule.shape == IntervalLiftRuleShape::ObservedDuration),
        "catalog should include observed-duration lifters"
    );
    assert!(
        catalog
            .iter()
            .any(|rule| rule.shape == IntervalLiftRuleShape::StartStopPair),
        "catalog should include start/stop pair lifters"
    );
    assert!(
        catalog.iter().all(|rule| !rule.consumer_hint.is_empty()),
        "every rule should name the composite/read surfaces it exists to feed"
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
        .process_single(&mut state, serde_json::to_value(first)?, &first_context)
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
        .process_single(&mut state, serde_json::to_value(second)?, &second_context)
        .await?
        .expect("second transition closes the previous focus interval");

    assert_eq!(output.ts_orig, end);
    assert_eq!(output.source_event_ids, vec![first_id, second_id]);
    assert_eq!(output.semantics_version.as_deref(), Some("2.0.0"));
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
    let expected_key = format!("interval:desktop.focus:0xabc:ts:{start}");
    assert_eq!(output.equivalence_key.as_deref(), Some(expected_key.as_str()));
    Ok(())
}

#[sinex_test]
async fn interval_lift_equivalence_key_is_start_occurrence_not_parent_ids(
) -> xtask::sandbox::TestResult<()> {
    // sinex-ecy / y8v: the interval key is the material occurrence of the START
    // evidence (start-anchored — ends move, starts do not), never the parent event
    // interpretation ids (which re-mint every replay and collide -> silent suppression).
    let start = Timestamp::from_unix_timestamp(1_700_000_000)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let end = Timestamp::from_unix_timestamp(1_700_000_010)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let material = Uuid::now_v7();

    let mut automaton = IntervalLift;
    let mut state = IntervalLiftState::default();
    let first_context = focus_context_with_material(start, material, 100);
    let first_id = first_context.trigger_uuid();
    let second_context = focus_context_with_material(end, material, 200);
    let second_id = second_context.trigger_uuid();

    automaton
        .process_single(
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
        .process_single(
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

    // Start-anchored on the FIRST evidence's material occurrence (anchor 100), not
    // the end's (200), and not either parent event interpretation id.
    let expected = format!("interval:desktop.focus:0xabc:{material}:100");
    assert_eq!(output.payload.interval_id, expected);
    assert_eq!(output.equivalence_key.as_deref(), Some(expected.as_str()));
    let key = output.equivalence_key.expect("interval carries an equivalence key");
    assert!(
        !key.contains(&first_id.to_string()) && !key.contains(&second_id.to_string()),
        "occurrence key must not embed parent event interpretation ids: {key}"
    );
    assert!(
        !key.contains(":200"),
        "key must be start-anchored (anchor 100), not the moved end (200): {key}"
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
        .process_single(&mut state, serde_json::to_value(first)?, &first_context)
        .await?;
    let same_focus_output = automaton
        .process_single(
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
        .process_single(&mut state, serde_json::to_value(second)?, &second_context)
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
        .process_single(&mut state, serde_json::to_value(first)?, &focus_context(later))
        .await?;
    let output = automaton
        .process_single(
            &mut state,
            serde_json::to_value(second)?,
            &focus_context(earlier),
        )
        .await?;

    assert!(output.is_none());
    Ok(())
}

#[sinex_test]
async fn interval_lift_closes_previous_workspace_on_next_switch(
) -> xtask::sandbox::TestResult<()> {
    let start = Timestamp::from_unix_timestamp(1_700_000_000)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let end = Timestamp::from_unix_timestamp(1_700_000_090)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;

    let mut automaton = IntervalLift;
    let mut state = IntervalLiftState::default();
    let first_context = workspace_context(start);
    let first_id = first_context.trigger_uuid();
    let second_context = workspace_context(end);
    let second_id = second_context.trigger_uuid();

    let first_output = automaton
        .process_single(
            &mut state,
            serde_json::to_value(HyprlandWorkspaceSwitchedPayload {
                to_workspace_id: 2,
                workspace_name: Some("dev".to_string()),
                from_workspace_id: None,
                monitor_id: Some(1),
                active_window_id: Some("0xabc".to_string()),
            })?,
            &first_context,
        )
        .await?;
    assert!(
        first_output.is_none(),
        "first workspace switch seeds the open workspace interval"
    );

    let output = automaton
        .process_single(
            &mut state,
            serde_json::to_value(HyprlandWorkspaceSwitchedPayload {
                to_workspace_id: 3,
                workspace_name: Some("browser".to_string()),
                from_workspace_id: Some(2),
                monitor_id: Some(1),
                active_window_id: Some("0xdef".to_string()),
            })?,
            &second_context,
        )
        .await?
        .expect("second workspace switch closes the previous workspace interval");

    assert_eq!(output.ts_orig, end);
    assert_eq!(output.source_event_ids, vec![first_id, second_id]);
    assert_eq!(output.payload.state_kind, "desktop.workspace");
    assert_eq!(output.payload.subject_id.as_deref(), Some("workspace:2"));
    assert_eq!(output.payload.label.as_deref(), Some("dev"));
    assert_eq!(output.payload.start_time, start);
    assert_eq!(output.payload.end_time, end);
    assert_eq!(output.payload.duration_secs, 90);
    assert_eq!(output.payload.start_event_type, "workspace.switched");
    assert_eq!(output.payload.end_event_type, "workspace.switched");
    assert_eq!(
        output
            .payload
            .attributes
            .get("to_workspace_id")
            .map(String::as_str),
        Some("2")
    );
    assert_eq!(
        output
            .payload
            .attributes
            .get("workspace_name")
            .map(String::as_str),
        Some("dev")
    );
    assert_eq!(
        output
            .payload
            .attributes
            .get("active_window_id")
            .map(String::as_str),
        Some("0xabc")
    );
    let expected_key = format!("interval:desktop.workspace:workspace:2:ts:{start}");
    assert_eq!(output.equivalence_key.as_deref(), Some(expected_key.as_str()));
    Ok(())
}

#[sinex_test]
async fn interval_lift_updates_same_workspace_without_closing_interval(
) -> xtask::sandbox::TestResult<()> {
    let start = Timestamp::from_unix_timestamp(1_700_000_000)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let refresh = Timestamp::from_unix_timestamp(1_700_000_005)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;

    let mut automaton = IntervalLift;
    let mut state = IntervalLiftState::default();
    automaton
        .process_single(
            &mut state,
            serde_json::to_value(HyprlandWorkspaceSwitchedPayload {
                to_workspace_id: 2,
                workspace_name: Some("dev".to_string()),
                from_workspace_id: None,
                monitor_id: Some(1),
                active_window_id: Some("0xabc".to_string()),
            })?,
            &workspace_context(start),
        )
        .await?;

    let output = automaton
        .process_single(
            &mut state,
            serde_json::to_value(HyprlandWorkspaceSwitchedPayload {
                to_workspace_id: 2,
                workspace_name: Some("dev".to_string()),
                from_workspace_id: Some(2),
                monitor_id: Some(1),
                active_window_id: Some("0xdef".to_string()),
            })?,
            &workspace_context(refresh),
        )
        .await?;

    assert!(
        output.is_none(),
        "same-workspace refreshes must not close zero-duration intervals"
    );
    assert_eq!(
        state
            .active_workspace
            .as_ref()
            .and_then(|workspace| workspace.attributes.get("active_window_id"))
            .map(String::as_str),
        Some("0xdef")
    );
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
        .process_single(
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
        format!("interval:desktop.activitywatch.window:app:kitty|title:codex:ts:{start}");
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
        .process_single(
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
        .process_single(
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
async fn interval_lift_merges_activitywatch_zero_duration_heartbeats(
) -> xtask::sandbox::TestResult<()> {
    let first_ts = Timestamp::from_unix_timestamp(1_700_000_000)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let refresh_ts = first_ts + sinex_primitives::temporal::Duration::seconds(2);
    let switch_ts = first_ts + sinex_primitives::temporal::Duration::seconds(4);

    let mut automaton = IntervalLift;
    let mut state = IntervalLiftState::default();
    let first_context = activitywatch_context(first_ts);
    let first_id = first_context.trigger_uuid();
    let refresh_context = activitywatch_context(refresh_ts);
    let switch_context = activitywatch_context(switch_ts);
    let switch_id = switch_context.trigger_uuid();

    let first = automaton
        .process_single(
            &mut state,
            serde_json::to_value(ActivityWatchWindowActivePayload {
                app: "kitty".to_string(),
                title: "codex".to_string(),
                duration_ms: 0,
                bucket_id: "aw-watcher-window_sinnix-prime".to_string(),
            })?,
            &first_context,
        )
        .await?;
    assert!(
        first.is_none(),
        "first zero-duration heartbeat opens the derived interval"
    );

    let refresh = automaton
        .process_single(
            &mut state,
            serde_json::to_value(ActivityWatchWindowActivePayload {
                app: "kitty".to_string(),
                title: "codex".to_string(),
                duration_ms: 0,
                bucket_id: "aw-watcher-window_sinnix-prime".to_string(),
            })?,
            &refresh_context,
        )
        .await?;
    assert!(
        refresh.is_none(),
        "same-subject heartbeat inside the merge window extends the open interval"
    );

    let output = automaton
        .process_single(
            &mut state,
            serde_json::to_value(ActivityWatchWindowActivePayload {
                app: "qutebrowser".to_string(),
                title: "Sinex".to_string(),
                duration_ms: 0,
                bucket_id: "aw-watcher-window_sinnix-prime".to_string(),
            })?,
            &switch_context,
        )
        .await?
        .expect("changed subject closes the previous heartbeat interval");

    assert_eq!(output.source_event_ids, vec![first_id, switch_id]);
    assert_eq!(output.payload.state_kind, "desktop.activitywatch.window");
    assert_eq!(
        output.payload.subject_id.as_deref(),
        Some("app:kitty|title:codex")
    );
    assert_eq!(output.payload.start_time, first_ts);
    assert_eq!(output.payload.end_time, switch_ts);
    assert_eq!(output.payload.duration_secs, 4);
    assert_eq!(output.payload.start_event_type, "window.active");
    assert_eq!(output.payload.end_event_type, "window.active");
    Ok(())
}

#[sinex_test]
async fn interval_lift_splits_activitywatch_heartbeat_after_large_gap(
) -> xtask::sandbox::TestResult<()> {
    let first_ts = Timestamp::from_unix_timestamp(1_700_000_000)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let late_ts = first_ts + sinex_primitives::temporal::Duration::seconds(45);

    let mut automaton = IntervalLift;
    let mut state = IntervalLiftState::default();
    let first_context = activitywatch_context(first_ts);
    let late_context = activitywatch_context(late_ts);

    let first = automaton
        .process_single(
            &mut state,
            serde_json::to_value(ActivityWatchWindowActivePayload {
                app: "kitty".to_string(),
                title: "codex".to_string(),
                duration_ms: 0,
                bucket_id: "aw-watcher-window_sinnix-prime".to_string(),
            })?,
            &first_context,
        )
        .await?;
    assert!(first.is_none());

    let output = automaton
        .process_single(
            &mut state,
            serde_json::to_value(ActivityWatchWindowActivePayload {
                app: "kitty".to_string(),
                title: "codex".to_string(),
                duration_ms: 0,
                bucket_id: "aw-watcher-window_sinnix-prime".to_string(),
            })?,
            &late_context,
        )
        .await?
        .expect("large same-subject gap closes the previous heartbeat interval");

    assert_eq!(output.payload.start_time, first_ts);
    // sinex-zs6: the bout ends at last_seen (== first_ts, the only heartbeat) plus
    // the slack, NOT at the next post-gap event (+45s) — the idle 30..45s window is
    // absence of evidence and must not be attributed to the bout.
    let expected_end = first_ts + sinex_primitives::temporal::Duration::seconds(30);
    assert_eq!(output.payload.end_time, expected_end);
    assert_eq!(output.payload.duration_secs, 30);
    Ok(())
}

#[sinex_test]
async fn interval_lift_continuous_heartbeat_stream_is_one_bout(
) -> xtask::sandbox::TestResult<()> {
    // sinex-zs6 regression: a continuous same-subject heartbeat stream must be ONE
    // interval, not chopped into fixed ~30s pieces. Pre-fix the gap was measured
    // from the FIRST heartbeat, so the merge window degenerated into a max interval
    // length. Now gap is measured from last_seen, so 5s beats merge indefinitely.
    let start = Timestamp::from_unix_timestamp(1_700_000_000)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let mut automaton = IntervalLift;
    let mut state = IntervalLiftState::default();

    let payload = || {
        serde_json::to_value(ActivityWatchWindowActivePayload {
            app: "kitty".to_string(),
            title: "codex".to_string(),
            duration_ms: 0,
            bucket_id: "aw-watcher-window_sinnix-prime".to_string(),
        })
    };

    // 13 heartbeats, 5s apart (0..60s). Every 5s gap is within the 30s window, so
    // they all merge — zero intervals emitted, last_seen advances to +60s.
    for i in 0..=12 {
        let ts = start + sinex_primitives::temporal::Duration::seconds(i * 5);
        let out = automaton
            .process_single(&mut state, payload()?, &activitywatch_context(ts))
            .await?;
        assert!(out.is_none(), "continuous 5s heartbeats must merge, not chop (beat {i})");
    }

    // A beat after a 2-minute silence (>30s) ends the bout at last_seen(+60) + slack.
    let after_silence = start + sinex_primitives::temporal::Duration::seconds(180);
    let output = automaton
        .process_single(&mut state, payload()?, &activitywatch_context(after_silence))
        .await?
        .expect("the post-silence beat closes the single continuous bout");

    assert_eq!(output.payload.start_time, start);
    let expected_end = start + sinex_primitives::temporal::Duration::seconds(60 + 30);
    assert_eq!(
        output.payload.end_time, expected_end,
        "one bout spanning the whole stream, ending at last_seen + slack"
    );
    assert_eq!(output.payload.duration_secs, 90);
    Ok(())
}

#[sinex_test]
async fn interval_lift_lifts_activitywatch_afk_observed_duration(
) -> xtask::sandbox::TestResult<()> {
    let start = Timestamp::from_unix_timestamp(1_700_000_000)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let end = start + sinex_primitives::temporal::Duration::milliseconds(45_714);

    let mut automaton = IntervalLift;
    let mut state = IntervalLiftState::default();
    let context = activitywatch_afk_context(start);
    let parent_id = context.trigger_uuid();

    let output = automaton
        .process_single(
            &mut state,
            serde_json::to_value(ActivityWatchAfkChangedPayload {
                status: "afk".to_string(),
                duration_ms: 45_714,
                bucket_id: "aw-watcher-afk_sinnix-prime".to_string(),
            })?,
            &context,
        )
        .await?
        .expect("AW AFK rows carry observed duration and emit immediately");

    assert_eq!(output.ts_orig, end);
    assert_eq!(output.source_event_ids, vec![parent_id]);
    assert_eq!(output.payload.state_kind, "desktop.activitywatch.afk");
    assert_eq!(output.payload.subject_id.as_deref(), Some("status:afk"));
    assert_eq!(output.payload.label.as_deref(), Some("afk"));
    assert_eq!(output.payload.start_time, start);
    assert_eq!(output.payload.end_time, end);
    assert_eq!(output.payload.duration_secs, 45);
    assert_eq!(output.payload.start_event_type, "afk.changed");
    assert_eq!(output.payload.end_event_type, "afk.changed");
    assert_eq!(
        output.payload.attributes.get("bucket_id").map(String::as_str),
        Some("aw-watcher-afk_sinnix-prime")
    );
    assert_eq!(
        output.payload.attributes.get("duration_ms").map(String::as_str),
        Some("45714")
    );
    assert_eq!(
        output.payload.attributes.get("status").map(String::as_str),
        Some("afk")
    );
    let expected_key =
        format!("interval:desktop.activitywatch.afk:status:afk:ts:{start}");
    assert_eq!(output.equivalence_key.as_deref(), Some(expected_key.as_str()));
    Ok(())
}

#[sinex_test]
async fn interval_lift_emits_not_afk_as_distinct_status_interval(
) -> xtask::sandbox::TestResult<()> {
    let start = Timestamp::from_unix_timestamp(1_700_000_100)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;

    let output = IntervalLift
        .process_single(
            &mut IntervalLiftState::default(),
            serde_json::to_value(ActivityWatchAfkChangedPayload {
                status: "not-afk".to_string(),
                duration_ms: 1_000,
                bucket_id: "aw-watcher-afk_sinnix-prime".to_string(),
            })?,
            &activitywatch_afk_context(start),
        )
        .await?
        .expect("not-afk rows are state intervals too");

    assert_eq!(output.payload.state_kind, "desktop.activitywatch.afk");
    assert_eq!(output.payload.subject_id.as_deref(), Some("status:not-afk"));
    assert_eq!(output.payload.label.as_deref(), Some("not-afk"));
    assert_eq!(output.payload.duration_secs, 1);
    Ok(())
}

#[sinex_test]
async fn interval_lift_clamps_open_activitywatch_afk_duration_at_creation_time(
) -> xtask::sandbox::TestResult<()> {
    let start = Timestamp::from_unix_timestamp(1_700_000_200)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let bound = start + sinex_primitives::temporal::Duration::seconds(61);
    let event_id = Uuid::now_v7();
    let observation = StateObservation {
        state_kind: "desktop.activitywatch.afk".to_string(),
        event_id,
        material_id: Some(Uuid::now_v7()),
        anchor_byte: Some(4096),
        last_seen: None,
        ts_orig: start,
        subject_id: Some("status:afk".to_string()),
        label: Some("afk".to_string()),
        event_type: "afk.changed".to_string(),
        attributes: BTreeMap::from([
            (
                "bucket_id".to_string(),
                "aw-watcher-afk_sinnix-prime".to_string(),
            ),
            ("duration_ms".to_string(), "919451".to_string()),
            ("status".to_string(), "afk".to_string()),
        ]),
    };

    let output = observation.observed_duration_interval(919_451, bound);

    assert_eq!(output.ts_orig, bound);
    assert_eq!(output.payload.start_time, start);
    assert_eq!(output.payload.end_time, bound);
    assert_eq!(output.payload.duration_secs, 61);
    assert_eq!(output.source_event_ids, vec![event_id]);
    assert_eq!(
        output.payload.attributes.get("duration_ms").map(String::as_str),
        Some("919451")
    );
    Ok(())
}

#[sinex_test]
async fn interval_lift_closes_systemd_unit_on_stop() -> xtask::sandbox::TestResult<()> {
    let start = Timestamp::from_unix_timestamp(1_700_000_000)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let end = Timestamp::from_unix_timestamp(1_700_000_125)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;

    let mut automaton = IntervalLift;
    let mut state = IntervalLiftState::default();
    let start_context = systemd_context("unit.started", start);
    let start_id = start_context.trigger_uuid();
    let stop_context = systemd_context("unit.stopped", end);
    let stop_id = stop_context.trigger_uuid();

    let start_output = automaton
        .process_single(
            &mut state,
            serde_json::to_value(SystemdUnitStartedPayload {
                unit_name: "sinexd.service".to_string(),
                unit_type: SystemdUnitType::Service,
                main_pid: None,
                active_state: SystemdActiveState::Active,
                sub_state: "running".to_string(),
            })?,
            &start_context,
        )
        .await?;
    assert!(start_output.is_none(), "start opens the unit interval");

    let output = automaton
        .process_single(
            &mut state,
            serde_json::to_value(SystemdUnitStoppedPayload {
                unit_name: "sinexd.service".to_string(),
                unit_type: SystemdUnitType::Service,
                exit_code: None,
                active_state: SystemdActiveState::Inactive,
                sub_state: "dead".to_string(),
            })?,
            &stop_context,
        )
        .await?
        .expect("stop closes the matching unit interval");

    assert_eq!(output.ts_orig, end);
    assert_eq!(output.source_event_ids, vec![start_id, stop_id]);
    assert_eq!(output.payload.state_kind, "system.systemd.unit");
    assert_eq!(output.payload.subject_id.as_deref(), Some("sinexd.service"));
    assert_eq!(output.payload.label.as_deref(), Some("sinexd.service"));
    assert_eq!(output.payload.start_time, start);
    assert_eq!(output.payload.end_time, end);
    assert_eq!(output.payload.duration_secs, 125);
    assert_eq!(output.payload.start_event_type, "unit.started");
    assert_eq!(output.payload.end_event_type, "unit.stopped");
    assert_eq!(
        output.payload.attributes.get("unit_type").map(String::as_str),
        Some("service")
    );
    assert_eq!(
        output.payload.attributes.get("active_state").map(String::as_str),
        Some("active")
    );
    assert_eq!(
        output.payload.attributes.get("sub_state").map(String::as_str),
        Some("running")
    );
    let expected_key =
        format!("interval:system.systemd.unit:sinexd.service:ts:{start}");
    assert_eq!(output.equivalence_key.as_deref(), Some(expected_key.as_str()));
    Ok(())
}

fn uzc_focus(window_id: &str) -> serde_json::Result<serde_json::Value> {
    serde_json::to_value(HyprlandWindowFocusedPayload {
        window_id: Some(window_id.to_string()),
        window_class: Some("kitty".to_string()),
        window_title: Some("x".to_string()),
        workspace_id: Some(1),
        previous_window_id: None,
    })
}

fn uzc_unit_started() -> serde_json::Result<serde_json::Value> {
    serde_json::to_value(SystemdUnitStartedPayload {
        unit_name: "u.service".to_string(),
        unit_type: SystemdUnitType::Service,
        main_pid: None,
        active_state: SystemdActiveState::Active,
        sub_state: "running".to_string(),
    })
}

#[sinex_test]
async fn interval_lift_uzc_tie_supersedes_open_state_in_place() -> xtask::sandbox::TestResult<()> {
    // sinex-uzc(a): two transitions at the SAME ts — the later supersedes in place
    // (deterministic tiebreak), no zero-duration interval; the next transition closes
    // from the superseding observation.
    let t = Timestamp::from_unix_timestamp(1_700_000_000).ok_or_else(|| color_eyre::eyre::eyre!("ts"))?;
    let later = t + sinex_primitives::temporal::Duration::seconds(10);
    let mut automaton = IntervalLift;
    let mut state = IntervalLiftState::default();
    assert!(automaton.process_single(&mut state, uzc_focus("0xA")?, &focus_context(t)).await?.is_none());
    assert!(automaton.process_single(&mut state, uzc_focus("0xB")?, &focus_context(t)).await?.is_none());
    let out = automaton.process_single(&mut state, uzc_focus("0xC")?, &focus_context(later)).await?
        .expect("next transition closes the superseding state");
    assert_eq!(out.payload.subject_id.as_deref(), Some("0xB"));
    assert_eq!(out.payload.start_time, t);
    assert_eq!(out.payload.end_time, later);
    Ok(())
}

#[sinex_test]
async fn interval_lift_uzc_out_of_order_transition_is_skipped() -> xtask::sandbox::TestResult<()> {
    // sinex-uzc(a): a transition older than the open state is skipped (durable debt),
    // never silently folded — the open state survives and closes on the next in-order
    // transition.
    let t10 = Timestamp::from_unix_timestamp(1_700_000_010).ok_or_else(|| color_eyre::eyre::eyre!("ts"))?;
    let t0 = Timestamp::from_unix_timestamp(1_700_000_000).ok_or_else(|| color_eyre::eyre::eyre!("ts"))?;
    let t20 = Timestamp::from_unix_timestamp(1_700_000_020).ok_or_else(|| color_eyre::eyre::eyre!("ts"))?;
    let mut automaton = IntervalLift;
    let mut state = IntervalLiftState::default();
    assert!(automaton.process_single(&mut state, uzc_focus("0xA")?, &focus_context(t10)).await?.is_none());
    assert!(automaton.process_single(&mut state, uzc_focus("0xB")?, &focus_context(t0)).await?.is_none());
    let out = automaton.process_single(&mut state, uzc_focus("0xC")?, &focus_context(t20)).await?
        .expect("in-order transition closes the still-open 0xA");
    assert_eq!(out.payload.subject_id.as_deref(), Some("0xA"));
    assert_eq!(out.payload.start_time, t10);
    assert_eq!(out.payload.end_time, t20);
    Ok(())
}

#[sinex_test]
async fn interval_lift_uzc_start_after_start_emits_restart_fence() -> xtask::sandbox::TestResult<()> {
    // sinex-uzc(c): a second start for the same unit emits an implied restart-fence
    // close of the first, instead of silently discarding it.
    let t = Timestamp::from_unix_timestamp(1_700_000_000).ok_or_else(|| color_eyre::eyre::eyre!("ts"))?;
    let restart = t + sinex_primitives::temporal::Duration::seconds(10);
    let mut automaton = IntervalLift;
    let mut state = IntervalLiftState::default();
    assert!(automaton.process_single(&mut state, uzc_unit_started()?, &systemd_context("unit.started", t)).await?.is_none());
    let out = automaton.process_single(&mut state, uzc_unit_started()?, &systemd_context("unit.started", restart)).await?
        .expect("start-after-start emits the implied restart-fence close");
    assert_eq!(out.payload.subject_id.as_deref(), Some("u.service"));
    assert_eq!(out.payload.start_time, t);
    assert_eq!(out.payload.end_time, restart);
    assert_eq!(out.payload.duration_secs, 10);
    Ok(())
}

#[sinex_test]
async fn interval_lift_uzc_stop_before_start_is_zero_duration() -> xtask::sandbox::TestResult<()> {
    // sinex-uzc(b): a stop with ts <= start closes a zero-duration interval (end
    // clamped to start) rather than discarding the matched start+stop.
    let start = Timestamp::from_unix_timestamp(1_700_000_010).ok_or_else(|| color_eyre::eyre::eyre!("ts"))?;
    let early_stop = Timestamp::from_unix_timestamp(1_700_000_005).ok_or_else(|| color_eyre::eyre::eyre!("ts"))?;
    let mut automaton = IntervalLift;
    let mut state = IntervalLiftState::default();
    automaton.process_single(&mut state, uzc_unit_started()?, &systemd_context("unit.started", start)).await?;
    let out = automaton.process_single(&mut state, serde_json::to_value(SystemdUnitStoppedPayload {
        unit_name: "u.service".to_string(),
        unit_type: SystemdUnitType::Service,
        exit_code: None,
        active_state: SystemdActiveState::Inactive,
        sub_state: "dead".to_string(),
    })?, &systemd_context("unit.stopped", early_stop)).await?
        .expect("stop-before-start still closes (zero duration), never dropped");
    assert_eq!(out.payload.start_time, start);
    assert_eq!(out.payload.end_time, start);
    assert_eq!(out.payload.duration_secs, 0);
    Ok(())
}

#[sinex_test]
async fn interval_lift_ignores_systemd_stop_without_matching_start(
) -> xtask::sandbox::TestResult<()> {
    let end = Timestamp::from_unix_timestamp(1_700_000_125)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;

    let output = IntervalLift
        .process_single(
            &mut IntervalLiftState::default(),
            serde_json::to_value(SystemdUnitStoppedPayload {
                unit_name: "sinexd.service".to_string(),
                unit_type: SystemdUnitType::Service,
                exit_code: None,
                active_state: SystemdActiveState::Inactive,
                sub_state: "dead".to_string(),
            })?,
            &systemd_context("unit.stopped", end),
        )
        .await?;

    assert!(output.is_none());
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

/// sinex-5s6: a machine suspend (systemd sleep.target start) fences the open
/// focus interval — it ends at the suspend, not at the next-morning focus event
/// (which would otherwise attribute the whole overnight gap to focus).
#[sinex_test]
async fn suspend_fence_closes_open_focus_interval() -> xtask::sandbox::TestResult<()> {
    let mut automaton = IntervalLift::default();
    let mut state = IntervalLiftState::default();
    let focus_start = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid ts");
    let suspend_ts = Timestamp::from_unix_timestamp(1_700_000_300).expect("valid ts");

    let focus_ctx = focus_context(focus_start);
    let focus_id = focus_ctx.trigger_uuid();
    let opened = automaton
        .process(
            &mut state,
            serde_json::to_value(HyprlandWindowFocusedPayload {
                window_id: Some("0xabc".to_string()),
                window_class: Some("kitty".to_string()),
                window_title: Some("codex".to_string()),
                workspace_id: Some(1),
                previous_window_id: None,
            })?,
            &focus_ctx,
        )
        .await?;
    assert!(opened.is_empty(), "focus start only opens the interval");

    let suspend_ctx = systemd_context("unit.started", suspend_ts);
    let suspend_id = suspend_ctx.trigger_uuid();
    let closes = automaton
        .process(
            &mut state,
            serde_json::to_value(SystemdUnitStartedPayload {
                unit_name: "sleep.target".to_string(),
                unit_type: SystemdUnitType::Target,
                main_pid: None,
                active_state: SystemdActiveState::Active,
                sub_state: "active".to_string(),
            })?,
            &suspend_ctx,
        )
        .await?;

    assert_eq!(closes.len(), 1, "suspend closes the one open focus interval");
    let closed = &closes[0];
    assert_eq!(
        closed.payload.end_time, suspend_ts,
        "focus interval ends at the suspend, not the next focus event"
    );
    assert_eq!(closed.payload.start_time, focus_start);
    assert_eq!(closed.payload.end_event_type, "fence.suspend");
    assert_eq!(closed.source_event_ids, vec![focus_id, suspend_id]);
    assert!(
        state.active_focus.is_none(),
        "the fence cleared the open focus slot"
    );
    // The sleep unit is a fence, not a subject interval — no unit state opened.
    assert!(
        state.active_subject_states.is_empty(),
        "the suspend unit is a fence, not lifted as a subject interval"
    );
    Ok(())
}

/// sinex-5s6: an AFK transition fences the open focus interval (the user stopped
/// attending) AND still lifts the AFK period as its own interval.
#[sinex_test]
async fn afk_fence_closes_focus_and_lifts_afk_interval() -> xtask::sandbox::TestResult<()> {
    let mut automaton = IntervalLift::default();
    let mut state = IntervalLiftState::default();
    let focus_start = Timestamp::from_unix_timestamp(1_700_000_000).expect("valid ts");
    let afk_ts = Timestamp::from_unix_timestamp(1_700_000_600).expect("valid ts");

    let focus_ctx = focus_context(focus_start);
    let focus_id = focus_ctx.trigger_uuid();
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
            &focus_ctx,
        )
        .await?;

    let afk_ctx = activitywatch_afk_context(afk_ts);
    let outputs = automaton
        .process(
            &mut state,
            serde_json::to_value(ActivityWatchAfkChangedPayload {
                status: "afk".to_string(),
                duration_ms: 60_000,
                bucket_id: "aw-watcher-afk".to_string(),
            })?,
            &afk_ctx,
        )
        .await?;

    let focus_close = outputs
        .iter()
        .find(|o| o.payload.end_event_type == "fence.afk")
        .expect("afk fences the open focus interval");
    assert_eq!(focus_close.payload.end_time, afk_ts);
    assert!(focus_close.source_event_ids.contains(&focus_id));
    assert!(
        state.active_focus.is_none(),
        "the afk fence cleared the open focus slot"
    );
    // The AFK period itself is still lifted as its own interval.
    assert!(
        outputs.len() >= 2,
        "afk fence emits the focus close AND the afk interval (got {})",
        outputs.len()
    );
    Ok(())
}

fn focus_context_with_material(
    ts_orig: Timestamp,
    material_id: Uuid,
    anchor_byte: i64,
) -> AutomatonContext {
    AutomatonContext {
        trigger_material_id: Some(material_id),
        trigger_anchor_byte: Some(anchor_byte),
        ..focus_context(ts_orig)
    }
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
        trigger_material_id: None,
        trigger_anchor_byte: None,
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
        trigger_material_id: None,
        trigger_anchor_byte: None,
    }
}

fn workspace_context(ts_orig: Timestamp) -> AutomatonContext {
    let trigger_event_id: Id<Event<JsonValue>> = Id::new();
    AutomatonContext {
        trigger_event_id,
        source: EventSource::from_static("wm.hyprland"),
        event_type: EventType::from_static("workspace.switched"),
        ts_orig: Some(ts_orig),
        ts_coided: trigger_event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
        trigger_material_id: None,
        trigger_anchor_byte: None,
    }
}

fn activitywatch_afk_context(ts_orig: Timestamp) -> AutomatonContext {
    let trigger_event_id: Id<Event<JsonValue>> = Id::new();
    AutomatonContext {
        trigger_event_id,
        source: EventSource::from_static("activitywatch"),
        event_type: EventType::from_static("afk.changed"),
        ts_orig: Some(ts_orig),
        ts_coided: trigger_event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
        trigger_material_id: None,
        trigger_anchor_byte: None,
    }
}

fn systemd_context(event_type: &'static str, ts_orig: Timestamp) -> AutomatonContext {
    let trigger_event_id: Id<Event<JsonValue>> = Id::new();
    AutomatonContext {
        trigger_event_id,
        source: EventSource::from_static("systemd"),
        event_type: EventType::from_static(event_type),
        ts_orig: Some(ts_orig),
        ts_coided: trigger_event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
        trigger_material_id: None,
        trigger_anchor_byte: None,
    }
}
