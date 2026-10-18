//! Scenarios ported from the predecessor's `interval_lift_test.rs`, on the
//! recompute model: inputs are folded in time order, so arrival order never
//! matters and there is no carried state to decode. ActivityWatch heartbeats
//! are joined into spans by the ActivityWatch source, so those scenarios are
//! span scenarios here.
use super::*;
use sinex_model::{
    Class, EquivalenceKey, EventId, MaterialId, ReadingOrder, RecordLocation, RecordState,
    ReplaceRule, TimeQuality, TsOrig,
};

const T: i64 = 1_718_438_400; // 2024-06-15 10:00 CEST

fn at(secs: i64) -> Timestamp {
    Timestamp::from_unix_secs(T + secs).unwrap()
}

fn input(event_type: &str, secs: i64, payload: Value) -> Event {
    let event_type: &'static str = INPUT_TYPES
        .iter()
        .copied()
        .find(|t| *t == event_type)
        .expect("a lift input type");
    Event {
        id: EventId::new(),
        event_type,
        source: "test".into(),
        origin: Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: u64::try_from(secs.max(0)).unwrap(),
            },
            ordinal: 0,
        },
        ts_orig: TsOrig::known(at(secs), TimeQuality::Captured),
        payload,
        equivalence_key: Some(EquivalenceKey("k".into())),
        replace: ReplaceRule::SuppressRepeat,
        reading_order: ReadingOrder(0),
        record_hash: [0; 32],
        record_state: RecordState::Present,
        class: Class::Open,
        support: None,
    }
}

fn focus(secs: i64, window: &str, title: &str) -> Event {
    input(
        "window.focused",
        secs,
        json!({"received_at": at(secs), "window": window, "class": "kitty", "title": title}),
    )
}

fn workspace(secs: i64, id: i64) -> Event {
    input(
        "workspace.switched",
        secs,
        json!({"received_at": at(secs), "workspace_id": id, "workspace_name": format!("ws{id}")}),
    )
}

fn window_span(secs: i64, end: i64, app: &str, title: &str) -> Event {
    input(
        "window.span",
        secs,
        json!({"bucket": "aw-watcher-window_box", "span_id": secs, "started_at": at(secs),
               "ended_at": at(end), "app": app, "title": title, "merged": 3}),
    )
}

fn presence(secs: i64, end: i64, status: &str) -> Event {
    input(
        "presence.span",
        secs,
        json!({"bucket": "aw-watcher-afk_box", "row": secs, "started_at": at(secs),
               "ended_at": at(end), "status": status}),
    )
}

fn unit(kind: &str, secs: i64, name: &str) -> Event {
    input(
        kind,
        secs,
        json!({"unit_name": name, "unit_type": "service", "active_state": "active", "sub_state": "running"}),
    )
}

/// Fold `events` (in the given arrival order) for one rule, as intervals.
fn lift(state: &str, events: &[Event]) -> Vec<StateInterval> {
    let r = rule(state).unwrap();
    let mut sorted: Vec<&Event> = events.iter().collect();
    sorted.sort_by_key(|e| e.ts_orig.at);
    let mut out: Vec<StateInterval> = fold(r, &sorted)
        .into_iter()
        .map(|l| to_event(r, l).payload_as::<StateInterval>().unwrap())
        .collect();
    out.sort_by(|a, b| {
        a.started_at
            .cmp(&b.started_at)
            .then(a.subject_id.cmp(&b.subject_id))
    });
    out
}

#[test]
fn next_focus_closes_the_previous_one() {
    let out = lift(
        "desktop.focus",
        &[focus(0, "0xa", "vim"), focus(60, "0xb", "web")],
    );
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].subject_id.as_deref(), Some("0xa"));
    assert_eq!(out[0].label.as_deref(), Some("kitty: vim"));
    assert_eq!(out[0].ended_at, Some(at(60)));
    assert_eq!(out[0].end_event_type.as_deref(), Some("window.focused"));
    assert_eq!(out[1].ended_at, None, "the last focus is still open");
}

#[test]
fn a_focus_without_an_address_is_its_class() {
    let e = input(
        "window.focused",
        0,
        json!({"received_at": at(0), "window": null, "class": "firefox", "title": null}),
    );
    let out = lift("desktop.focus", &[e]);
    assert_eq!(out[0].subject_id.as_deref(), Some("class:firefox"));
    assert_eq!(out[0].label.as_deref(), Some("firefox"));
}

#[test]
fn identity_is_the_start_record_not_the_end() {
    let a = focus(0, "0xa", "vim");
    let first = lift("desktop.focus", &[a.clone(), focus(60, "0xb", "web")]);
    let later = lift("desktop.focus", &[a, focus(90, "0xc", "mail")]);
    assert_eq!(first[0].interval_id, later[0].interval_id);
    assert_ne!(first[0].ended_at, later[0].ended_at);
}

#[test]
fn same_focus_again_updates_without_closing() {
    let out = lift(
        "desktop.focus",
        &[
            focus(0, "0xa", "vim a.rs"),
            focus(30, "0xa", "vim b.rs"),
            focus(60, "0xb", "web"),
        ],
    );
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].label.as_deref(), Some("kitty: vim b.rs"));
    assert_eq!(out[0].ended_at, Some(at(60)));
}

#[test]
fn arrival_order_does_not_matter() {
    let events = vec![
        focus(0, "0xa", "a"),
        focus(10, "0xb", "b"),
        focus(20, "0xc", "c"),
    ];
    let mut reversed = events.clone();
    reversed.reverse();
    assert_eq!(
        lift("desktop.focus", &events),
        lift("desktop.focus", &reversed)
    );
}

#[test]
fn a_tie_takes_the_open_state_in_place() {
    let out = lift(
        "desktop.focus",
        &[
            focus(0, "0xa", "a"),
            focus(0, "0xb", "b"),
            focus(10, "0xc", "c"),
        ],
    );
    assert_eq!(out.len(), 2, "no zero-length interval for the tie: {out:?}");
    assert_eq!(out[0].subject_id.as_deref(), Some("0xb"));
}

#[test]
fn next_workspace_closes_the_previous_one_and_same_one_does_not() {
    let out = lift(
        "desktop.workspace",
        &[workspace(0, 1), workspace(20, 1), workspace(50, 2)],
    );
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].subject_id.as_deref(), Some("workspace:1"));
    assert_eq!(out[0].label.as_deref(), Some("ws1"));
    assert_eq!(out[0].ended_at, Some(at(50)));
}

#[test]
fn each_activitywatch_span_is_its_own_interval() {
    let out = lift(
        "desktop.activitywatch.window",
        &[
            window_span(0, 5, "firefox", "docs"),
            window_span(10, 12, "firefox", "docs"),
        ],
    );
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].ended_at, Some(at(5)));
    assert_eq!(out[1].ended_at, Some(at(12)));
    assert_eq!(out[0].label.as_deref(), Some("firefox: docs"));
    assert_eq!(out[0].subject_id.as_deref(), Some("app:firefox|title:docs"));
}

#[test]
fn presence_spans_lift_and_not_afk_is_its_own_status() {
    let out = lift(
        "desktop.activitywatch.afk",
        &[presence(0, 60, "afk"), presence(60, 90, "not-afk")],
    );
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].subject_id.as_deref(), Some("status:afk"));
    assert_eq!(out[1].subject_id.as_deref(), Some("status:not-afk"));
    assert_eq!(out[1].ended_at, Some(at(90)));
}

#[test]
fn a_span_ending_before_its_start_is_zero_length() {
    let out = lift(
        "desktop.activitywatch.window",
        &[window_span(10, 5, "kitty", "zsh")],
    );
    assert_eq!(out[0].ended_at, Some(at(10)));
}

#[test]
fn a_unit_stop_closes_its_start() {
    let out = lift(
        "system.systemd.unit",
        &[
            unit("unit.started", 0, "a.service"),
            unit("unit.started", 5, "b.service"),
            unit("unit.stopped", 30, "a.service"),
        ],
    );
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].subject_id.as_deref(), Some("a.service"));
    assert_eq!(out[0].ended_at, Some(at(30)));
    assert_eq!(out[0].end_event_type.as_deref(), Some("unit.stopped"));
    assert_eq!(out[1].ended_at, None);
}

#[test]
fn a_second_start_closes_the_first() {
    let out = lift(
        "system.systemd.unit",
        &[
            unit("unit.started", 0, "a.service"),
            unit("unit.started", 40, "a.service"),
        ],
    );
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].ended_at, Some(at(40)));
    assert_eq!(out[0].end_event_type.as_deref(), Some("unit.started"));
}

#[test]
fn a_stop_with_no_start_closes_nothing() {
    assert!(
        lift(
            "system.systemd.unit",
            &[unit("unit.stopped", 0, "a.service")]
        )
        .is_empty()
    );
}

#[test]
fn a_stop_at_its_start_is_zero_length() {
    let out = lift(
        "system.systemd.unit",
        &[
            unit("unit.started", 10, "a.service"),
            unit("unit.stopped", 10, "a.service"),
        ],
    );
    assert_eq!(out[0].started_at, out[0].ended_at.unwrap());
}

#[test]
fn a_suspend_fences_focus() {
    let out = lift(
        "desktop.focus",
        &[
            focus(0, "0xa", "vim"),
            unit("unit.started", 100, "sleep.target"),
            focus(9000, "0xb", "web"),
        ],
    );
    assert_eq!(
        out[0].ended_at,
        Some(at(100)),
        "not the 9000 s overnight span"
    );
    assert_eq!(out[0].end_event_type.as_deref(), Some("unit.started"));
    // An ordinary unit start is not a fence.
    let out = lift(
        "desktop.focus",
        &[
            focus(0, "0xa", "vim"),
            unit("unit.started", 100, "cron.service"),
            focus(200, "0xb", "web"),
        ],
    );
    assert_eq!(out[0].ended_at, Some(at(200)));
}

#[test]
fn going_afk_fences_focus_and_is_lifted_itself() {
    let events = [focus(0, "0xa", "vim"), presence(50, 650, "afk")];
    let f = lift("desktop.focus", &events);
    assert_eq!(f[0].ended_at, Some(at(50)));
    let a = lift("desktop.activitywatch.afk", &events);
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].ended_at, Some(at(650)));
}

#[test]
fn a_suspend_cuts_a_window_span_but_afk_does_not() {
    let out = lift(
        "desktop.activitywatch.window",
        &[
            window_span(0, 100, "kitty", "zsh"),
            unit("unit.started", 15, "suspend.target"),
        ],
    );
    assert_eq!(out[0].ended_at, Some(at(15)));
    let out = lift(
        "desktop.activitywatch.window",
        &[window_span(0, 100, "kitty", "zsh"), presence(15, 50, "afk")],
    );
    assert_eq!(out[0].ended_at, Some(at(100)));
}

fn key(state: &str) -> Value {
    json!({"state": state, "day": "2024-06-15"})
}

#[test]
fn a_key_emits_what_starts_in_its_day_and_waits_on_open_states() {
    let env = Env::at(at(100_000));
    // Logical day 2024-06-15 runs 04:00 to 04:00 CEST: T-6h .. T+18h.
    let day_end = 18 * 3600;
    let inputs = [
        focus(-7 * 3600, "0xz", "yesterday"),
        focus(0, "0xa", "vim"),
        focus(day_end - 10, "0xb", "late"),
        focus(day_end + 50, "0xc", "tomorrow"),
    ];
    let out = IntervalLift.compute(&key("desktop.focus"), &inputs, &env);
    let lifted: Vec<StateInterval> = out
        .outputs
        .iter()
        .map(|e| e.payload_as::<StateInterval>().unwrap())
        .collect();
    assert_eq!(lifted.len(), 2, "{lifted:?}");
    assert_eq!(
        lifted[1].ended_at,
        Some(at(day_end + 50)),
        "ended by the next day's first transition"
    );
    assert!(!out.waiting);

    let open = IntervalLift.compute(&key("desktop.focus"), &inputs[..3], &env);
    assert!(open.waiting, "the late focus has no end yet");
}

#[test]
fn inputs_ask_for_the_transitions_after_the_day() {
    let env = Env::at(at(0));
    let q = IntervalLift.inputs(&key("desktop.focus"), &env).unwrap();
    assert_eq!(q.window.0, at(-6 * 3600));
    assert_eq!(q.window.1, at(18 * 3600));
    assert!(q.event_types.contains(&"window.focused"));
    assert!(q.event_types.contains(&"unit.started"), "fences are inputs");
    assert_eq!(q.after.len(), 3, "next transitions, next suspend, next afk");
    let q = IntervalLift
        .inputs(&key("system.systemd.unit"), &env)
        .unwrap();
    assert_eq!(q.after[0].per, Some("unit_name"));
}

#[test]
fn keys_cover_the_rules_a_fence_ends() {
    let env = Env::at(at(0));
    let view = |e: &Event| {
        IntervalLift.keys_for(
            &KeyView {
                event_type: e.event_type,
                ts_orig: e.ts_orig.at,
                payload: &e.payload,
            },
            &env,
        )
    };
    let suspend = unit("unit.started", 0, "sleep.target");
    let keys = view(&suspend);
    for r in RULES {
        assert!(
            keys.iter().any(|k| k["state"] == r.state_kind),
            "{} in {keys:?}",
            r.state_kind
        );
    }
    // An ordinary unit start is a transition for units only.
    let cron = unit("unit.started", 0, "cron.service");
    assert_eq!(
        view(&cron),
        vec![json!({"state": "system.systemd.unit", "day": "2024-06-15"})]
    );
    // Going AFK ends focus and workspace, and is itself a presence span.
    let afk = presence(0, 10, "afk");
    assert_eq!(view(&afk).len(), 3);
}
