use super::*;
use sinex_model::event_types::activity::{ActivitySession, ActivitySpan, ActivityWindow, Agency};
use sinex_model::event_types::desktop::PresenceSpan;
use sinex_model::event_types::fused::FocusFused;
use sinex_model::event_types::input::KeyPressed;
use sinex_model::{Class, EventPayload, MaterialId, Origin, ReadingOrder, RecordLocation};

const T: i64 = 1_718_438_400;
fn at(seconds: i64) -> Timestamp {
    Timestamp::from_unix_secs(T + seconds).unwrap()
}
fn env() -> Env {
    Env::at(at(100 * 86_400))
}
fn captured<P: EventPayload>(payload: &P, anchor: u64) -> Event {
    Event::build(
        payload,
        "synthetic.activity",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [0; 32],
        Class::Open,
    )
    .unwrap()
}
fn key(ns: i128, host: &str, physical: bool) -> Event {
    captured(&KeyPressed { session: host.into(), logged_at: timestamp(i128::from(T) * SECOND + ns), kernel_at: None,
        key: "KEY_A".into(), code: Some(30), value: Some(1), changed: Some(true), clipboard: None, window: None,
        n: u32::try_from(ns.rem_euclid(1_000_000_000)).unwrap(),
        extra: serde_json::from_value(json!({"host": host, "seat": "seat0", "layer": if physical { "evdev" } else { "uinput" }, "virtual": !physical})).unwrap() }, u64::try_from(ns).unwrap())
}
fn physical(seconds: i64) -> Event {
    key(i128::from(seconds) * SECOND, "box", true)
}
fn hour(start: i64) -> Value {
    json!({"hour": at(start)})
}
fn windows(events: &[Event], start: i64) -> Vec<ActivityWindow> {
    Keyed::compute(&ActivityWindows, &hour(start), events, &env())
        .outputs
        .iter()
        .map(|event| event.payload_as().unwrap())
        .collect()
}
fn spans(events: &[Event], start: i64) -> Vec<ActivitySpan> {
    Keyed::compute(&ActivitySpans, &hour(start), events, &env())
        .outputs
        .iter()
        .map(|event| event.payload_as().unwrap())
        .collect()
}

#[devkit::test]
fn window_gaps_close_at_three_hundred_seconds_without_adding_quiet_time() {
    for (gap, expected) in [
        (299_999_000_000, 1),
        (300_000_000_000, 2),
        (300_001_000_000, 2),
    ] {
        let events = vec![physical(0), key(gap, "box", true)];
        let out = windows(&events, 0);
        assert_eq!(out.len(), expected, "gap={gap}");
        assert_eq!(
            out.last().unwrap().ended_at,
            timestamp(i128::from(T) * SECOND + gap)
        );
        assert!(out.iter().all(|window| window.operator_present_s == 0.0));
    }
}

#[devkit::test]
fn agent_focus_and_commits_while_the_operator_is_away_do_not_extend_windows() {
    let mut events = vec![physical(0), physical(20)];
    let focus = captured(
        &FocusFused {
            record: "agent-window".into(),
            started_at: at(100),
            ended_at: Some(at(3600)),
            app: Some("kitty".into()),
            title: Some("agent build".into()),
            observed_by: vec!["desktop.hyprland".into()],
        },
        3,
    );
    // Fused context lacking a captured host stays explicitly unassigned.
    events.push(focus);
    events.push(captured(
        &sinex_model::event_types::code::GitCommit {
            repository: "/synthetic/repo".into(),
            commit: "synthetic-commit".into(),
            parents: vec![],
            tree: "tree".into(),
            author_name: "Agent".into(),
            author_email: "agent@example.org".into(),
            authored_at: at(1800),
            committer_name: "Agent".into(),
            committer_email: "agent@example.org".into(),
            committed_at: at(1800),
            message: "Build output".into(),
            refs: vec![],
        },
        4,
    ));
    events.push(key(200 * SECOND, "box", false));
    let out = windows(&events, 0);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].ended_at, at(20));
    let spans = spans(&events, 0);
    assert!(
        spans
            .iter()
            .filter(|span| span.started_at >= at(100))
            .all(|span| span.agency != Agency::Operator)
    );
    assert!(
        spans
            .iter()
            .any(|span| !span.apps.is_empty() && span.agency == Agency::Unknown)
    );
}

#[devkit::test]
fn hour_partitions_keep_continuation_and_count_each_input_start_once() {
    let events: Vec<_> = (0..=240)
        .step_by(20)
        .map(|seconds| physical(3480 + seconds))
        .collect();
    let before = windows(&events, 0);
    let after = windows(&events, 3600);
    assert_eq!((before.len(), after.len()), (1, 1));
    assert!(before[0].continuation_after && after[0].continuation_before);
    assert_eq!(before[0].ended_at, at(3600));
    assert_eq!(after[0].started_at, at(3600));
    assert_eq!(
        before[0].counts.records["key.pressed"] + after[0].counts.records["key.pressed"],
        13
    );
    assert_eq!(
        before[0].operator_present_s + after[0].operator_present_s,
        240.0
    );
}

fn session_events(events: &[Event], hours: &[i64]) -> Vec<Event> {
    hours
        .iter()
        .flat_map(|hour_start| {
            Keyed::compute(&ActivityWindows, &hour(*hour_start), events, &env()).outputs
        })
        .collect()
}
fn sessions(events: &[Event], profile: ActivityProfile) -> Vec<ActivitySession> {
    let scope = events[0].payload["scope"].clone();
    Keyed::compute(
        &ActivitySessions(profile),
        &json!({"scope": scope}),
        events,
        &env(),
    )
    .outputs
    .iter()
    .map(|event| event.payload_as().unwrap())
    .collect()
}

#[devkit::test]
fn sessions_cross_logical_days_and_report_short_breaks_separately() {
    // 04:00 CEST is the day boundary, 18 hours after T.
    let events: Vec<_> = [64700, 64720, 65100, 65120]
        .into_iter()
        .map(physical)
        .collect();
    let windows = session_events(&events, &[61200, 64800]);
    let out = sessions(&windows, ActivityProfile::default());
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].windows, 2);
    assert_eq!(out[0].short_breaks, 1);
    assert_eq!(out[0].operator_present_s, 40.0);
    assert_eq!(out[0].started_at, at(64700));
    assert_eq!(out[0].ended_at, at(65120));
    let split = sessions(&windows, ActivityProfile { break_seconds: 300 });
    assert_eq!(split.len(), 2);
}

#[devkit::test]
fn elementary_spans_preserve_afk_claims_and_physical_contradictions_without_overlap() {
    let mut events = vec![physical(0), physical(20), physical(40)];
    let claim = captured(
        &PresenceSpan {
            bucket: "afk-box".into(),
            row: 1,
            started_at: at(0),
            ended_at: at(40),
            status: "afk".into(),
        },
        10,
    );
    let shared = scope(&claim.payload, claim.event_type);
    for input in &mut events {
        input.payload["extra"]["scope"] = json!(shared);
    }
    events.push(claim);
    let out = spans(&events, 0);
    assert_eq!(
        out.iter().map(|span| span.operator_present_s).sum::<f64>(),
        40.0
    );
    assert!(
        out.iter()
            .all(|span| span.agency == Agency::Operator && span.presence_claims == ["afk"])
    );
    assert!(
        out.windows(2)
            .all(|pair| pair[0].ended_at <= pair[1].started_at)
    );
    assert!(out.iter().all(|span| !span.agency_evidence.is_empty()));
}

#[devkit::test]
fn two_hosts_and_unknown_input_provenance_never_merge() {
    let events = vec![
        physical(0),
        key(20 * SECOND, "other", true),
        key(40 * SECOND, "box", false),
    ];
    let out = windows(&events, 0);
    assert_eq!(out.len(), 2);
    assert_ne!(out[0].scope, out[1].scope);
    assert!(spans(&events, 0).is_empty());
}

#[devkit::test]
fn arrival_order_and_new_reading_ids_do_not_change_activity_payloads() {
    let mut events: Vec<_> = (0..=400).step_by(20).map(physical).collect();
    let before = windows(&events, 0);
    events.reverse();
    for event in &mut events {
        event.id = sinex_model::EventId::new();
    }
    assert_eq!(windows(&events, 0), before);
}

#[devkit::test]
fn repeated_dst_hour_has_distinct_windows_and_one_session() {
    let first: Timestamp = serde_json::from_value(json!("2026-10-25T00:50:00Z")).unwrap();
    let second: Timestamp = serde_json::from_value(json!("2026-10-25T01:00:00Z")).unwrap();
    let events: Vec<_> = [first, second]
        .into_iter()
        .enumerate()
        .map(|(index, at)| {
            let mut event = physical(index as i64);
            event.ts_orig.at = Some(at);
            event.payload["logged_at"] = json!(at);
            event
        })
        .collect();
    let mut outputs = Vec::new();
    for event in &events {
        let start = env()
            .time_settings
            .hour_start(event.ts_orig.at.unwrap())
            .unwrap();
        outputs.extend(
            Keyed::compute(&ActivityWindows, &json!({"hour": start}), &events, &env()).outputs,
        );
    }
    assert_ne!(
        outputs[0].payload["partition_start"],
        outputs[1].payload["partition_start"]
    );
    assert_eq!(sessions(&outputs, ActivityProfile::default()).len(), 1);
}

#[devkit::test]
fn lock_suspend_and_sleep_close_sessions_even_when_the_break_is_short() {
    for (kind, label, host, reason) in [
        ("desktop.lock", "locked", Some("box"), "lock"),
        ("machine.suspend", "suspended", Some("box"), "suspend"),
    ] {
        let mut boundary = captured(
            &sinex_model::event_types::StateInterval {
                interval_id: kind.into(),
                state_kind: kind.into(),
                subject_id: None,
                label: Some(label.into()),
                started_at: at(100),
                ended_at: Some(at(200)),
                start_event_type: "synthetic".into(),
                end_event_type: None,
                attributes: Default::default(),
            },
            1,
        );
        boundary.payload["attributes"]["host"] = json!(host);
        if reason == "lock" {
            boundary.payload["attributes"]["seat"] = json!("seat0");
        }
        let evidence = vec![
            physical(0),
            physical(20),
            physical(400),
            physical(420),
            boundary.clone(),
        ];
        let mut inputs = session_events(&evidence, &[0]);
        inputs.push(boundary);
        let out = sessions(&inputs, ActivityProfile::default());
        assert_eq!(out.len(), 2, "{reason}");
        assert_eq!(out[0].close_reason.as_deref(), Some(reason));
    }
    let sleep = captured(
        &sinex_model::event_types::fused::RestFused {
            record: "sleep".into(),
            started_at: at(100),
            ended_at: Some(at(200)),
            kind: Some("sleep".into()),
            observed_by: vec![],
        },
        2,
    );
    let evidence = vec![
        physical(0),
        physical(20),
        physical(400),
        physical(420),
        sleep.clone(),
    ];
    let mut inputs = session_events(&evidence, &[0]);
    inputs.push(sleep);
    let out = sessions(&inputs, ActivityProfile::default());
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].close_reason.as_deref(), Some("sleep"));
}

#[devkit::test]
fn exact_absence_conflicting_with_physical_support_is_mixed_and_not_counted() {
    let sleep = captured(
        &sinex_model::event_types::fused::RestFused {
            record: "sleep".into(),
            started_at: at(10),
            ended_at: Some(at(30)),
            kind: Some("sleep".into()),
            observed_by: vec![],
        },
        2,
    );
    let events = vec![physical(0), physical(20), physical(40), sleep];
    let out = spans(&events, 0);
    assert!(out.iter().any(|span| span.agency == Agency::Mixed));
    assert_eq!(
        out.iter().map(|span| span.operator_present_s).sum::<f64>(),
        20.0
    );
    assert!(
        out.iter()
            .filter(|span| span.agency == Agency::Mixed)
            .all(|span| span.operator_present_s == 0.0)
    );
}

#[devkit::test]
fn trailing_windows_close_on_quiet_without_a_future_event() {
    let events = vec![physical(0), physical(20)];
    let compute =
        |seconds| Keyed::compute(&ActivityWindows, &hour(0), &events, &Env::at(at(seconds)));
    assert!(compute(319).waiting);
    let closed = compute(320);
    assert!(!closed.waiting);
    assert_eq!(closed.outputs[0].payload["close_reason"], "gap");
    assert_eq!(closed.outputs[0].payload["ended_at"], json!(at(20)));
}

#[devkit::test]
fn more_than_ten_thousand_inputs_keep_all_support_and_counts() {
    let events: Vec<_> = (0..10_001)
        .map(|index| key(index * 10_000_000, "box", true))
        .collect();
    let windows = windows(&events, 0);
    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0].counts.records["key.pressed"], 10_001);
    assert_eq!(windows[0].operator_present_s, 100.0);
    let spans = spans(&events, 0);
    assert_eq!(spans.len(), 10_000);
    assert!(
        (spans
            .iter()
            .map(|span| span.operator_present_s)
            .sum::<f64>()
            - 100.0)
            .abs()
            < 1e-6
    );
    assert!(
        spans
            .windows(2)
            .all(|pair| pair[0].ended_at <= pair[1].started_at)
    );
}

#[devkit::test]
fn only_sleep_journal_records_are_read_or_mark_activity_keys() {
    let payload = json!({"identifier":"unrelated", "message":"Performing sleep operation"});
    let input = KeyView {
        event_type: "journal.entry",
        ts_orig: Some(at(0)),
        payload: &payload,
    };
    assert!(Keyed::keys_for(&ActivityWindows, &input, &env()).is_empty());
    assert!(
        Keyed::keys_for(
            &ActivitySessions(ActivityProfile::default()),
            &input,
            &env()
        )
        .is_empty()
    );
    let query = Keyed::inputs(
        &ActivitySessions(ActivityProfile::default()),
        &json!({"at":at(0),"scope":"synthetic"}),
        &env(),
    )
    .unwrap();
    let journal = query
        .kinds
        .iter()
        .find(|kind| kind.event_type == "journal.entry")
        .unwrap();
    assert!(!journal.admits("journal.entry", &payload));
    assert!(journal.admits("journal.entry", &json!({"identifier":"systemd-sleep"})));
}

#[devkit::test]
fn short_breaks_inside_a_window_are_members_without_inflating_operator_time() {
    let events: Vec<_> = [0, 20, 200, 220].into_iter().map(physical).collect();
    let windows = session_events(&events, &[0]);
    assert_eq!(windows.len(), 1);
    let out = sessions(&windows, ActivityProfile::default());
    assert_eq!(out[0].short_breaks, 1);
    assert_eq!(out[0].operator_present_s, 40.0);
    let pause = out[0]
        .members
        .iter()
        .find(|member| member.kind == "short_break")
        .unwrap();
    assert_eq!((pause.started_at, pause.ended_at), (at(20), at(200)));
}

#[devkit::test]
fn profile_versions_fit_the_registry_and_change_with_the_break_threshold() {
    let ordinary = ActivitySessions(ActivityProfile::default());
    let changed = ActivitySessions(ActivityProfile { break_seconds: 600 });
    let first = Keyed::version(&ordinary);
    let second = Keyed::version(&changed);
    assert_ne!(first, second);
    assert!(i32::try_from(first).is_ok() && i32::try_from(second).is_ok());
}

#[devkit::test]
fn absence_boundaries_are_parents_of_the_windows_and_spans_they_cut() {
    let sleep = captured(
        &sinex_model::event_types::fused::RestFused {
            record: "sleep".into(),
            started_at: at(10),
            ended_at: Some(at(30)),
            kind: Some("sleep".into()),
            observed_by: vec![],
        },
        2,
    );
    let sleep_id = sleep.id;
    let events = vec![physical(0), physical(20), physical(40), sleep];
    for derivation in [
        Box::new(ActivityWindows) as Box<dyn crate::AnyKeyed>,
        Box::new(ActivitySpans),
    ] {
        let output = derivation.compute(&hour(0), &events, &env());
        let after = output
            .outputs
            .iter()
            .find(|event| {
                event.payload["started_at"] == json!(at(30))
                    && event.payload["scope"].as_str().unwrap().contains("box")
            })
            .unwrap();
        let Origin::Derived { parents } = &after.origin else {
            panic!("activity is derived")
        };
        assert!(parents.contains(&sleep_id));
    }
}

#[devkit::test]
fn human_prompt_activity_cites_the_session_that_establishes_its_agency() {
    use sinex_model::event_types::polylogue::{Message, Session, Usage};
    let session = captured(
        &Session {
            session: "session".into(),
            origin: "codex".into(),
            kind: "main".into(),
            title: None,
            cwd: vec![],
            repo: None,
            branch: None,
            tool_version: None,
            mode: None,
            machine: Some("box".into()),
            started: Some(at(0)),
            last_activity: Some(at(20)),
            links: vec![],
        },
        1,
    );
    let prompt = captured(
        &Message {
            message: "message".into(),
            session: "session".into(),
            parent: None,
            role: "user".into(),
            written_by: "human".into(),
            prompt_kind: Some("prose".into()),
            at: Some(at(10)),
            model: None,
            text: "A synthetic prompt".into(),
            usage: Usage::default(),
            cost_nusd: None,
            stop_reason: None,
            duration_ms: None,
            attachments: vec![],
            copies: vec![],
        },
        2,
    );
    assert!(windows(std::slice::from_ref(&prompt), 0).is_empty());
    let session_id = session.id;
    let prompt_id = prompt.id;
    let events = [session, prompt];
    let acts = Keyed::compute(
        &crate::work::WorkActs,
        &json!({"session":"session"}),
        &events,
        &env(),
    )
    .outputs;
    assert_eq!(acts.len(), 1);
    let Origin::Derived { parents } = &acts[0].origin else {
        panic!("an act is derived")
    };
    assert!(parents.contains(&session_id) && parents.contains(&prompt_id));
    let out = ActivityWindows.compute(&hour(0), &acts, &env());
    assert_eq!(out.outputs.len(), 1);
    let Origin::Derived { parents } = &out.outputs[0].origin else {
        panic!("activity is derived")
    };
    assert_eq!(parents, &[acts[0].id]);
    assert_eq!(out.outputs[0].payload["operator_present_s"], 0.0);
}

#[devkit::test]
fn point_inputs_never_become_open_intervals_in_later_hour_queries() {
    let query = ActivitySpans.inputs(&hour(3600), &env()).unwrap();
    assert!(!query.admits(&physical(0)));
    let sleep = captured(
        &sinex_model::event_types::fused::RestFused {
            record: "sleep".into(),
            started_at: at(0),
            ended_at: Some(at(4000)),
            kind: Some("sleep".into()),
            observed_by: vec![],
        },
        2,
    );
    assert!(query.admits(&sleep));
    for kind in [
        "key.pressed",
        "pointer.button",
        "pointer.moved",
        "pointer.scrolled",
        "visit.fused",
        "clipboard.fused",
        "journal.entry",
        "polylogue.message",
        "polylogue.tool_call",
        "operator_log.entry",
    ] {
        assert!(
            !query
                .intervals
                .iter()
                .any(|(interval_kind, _)| *interval_kind == kind),
            "{kind} is a point"
        );
    }
}

#[devkit::test]
fn bracketed_quiet_requires_the_same_recorded_foreground_window() {
    let mut first = physical(0);
    let mut next = physical(200);
    first.payload["window"] = json!("recorded-window");
    next.payload["window"] = json!("recorded-window");
    let output = spans(&[first.clone(), next.clone()], 0);
    assert_eq!(
        output
            .iter()
            .map(|span| span.operator_present_s)
            .sum::<f64>(),
        200.0
    );
    assert!(
        output
            .iter()
            .all(|span| span.presence_method == "bracketed_quiet")
    );
    next.payload["window"] = json!("another-window");
    assert!(spans(&[first, next], 0).is_empty());
}

#[devkit::test]
fn capture_lifetimes_require_explicit_physical_provenance_and_a_stop() {
    use sinex_model::event_types::input::InputLogger;
    let make = |seconds, event: &str, fields| {
        captured(
            &InputLogger {
                session: "physical-recorder".into(),
                logged_at: at(seconds),
                event: event.into(),
                n: 0,
                fields,
            },
            seconds as u64,
        )
    };
    let start = make(
        0,
        "start",
        serde_json::from_value(
            json!({"layer":"evdev","virtual":false,"host":"box","seat":"seat0"}),
        )
        .unwrap(),
    );
    let stop = make(
        3600,
        "stop",
        serde_json::from_value(json!({"host":"box","seat":"seat0"})).unwrap(),
    );
    let key =
        json!({"session":"physical-recorder", "scope": json!(["host","box","seat0"]).to_string()});
    assert!(
        Keyed::compute(&InputCoverage, &key, std::slice::from_ref(&start), &env())
            .outputs
            .is_empty()
    );
    let output = Keyed::compute(&InputCoverage, &key, &[stop.clone(), start.clone()], &env());
    assert_eq!(output.outputs.len(), 1);
    assert_eq!(output.outputs[0].payload["ended_at"], json!(at(3600)));
    let mut virtual_start = start;
    virtual_start.payload["fields"]["virtual"] = json!(true);
    assert!(
        Keyed::compute(&InputCoverage, &key, &[virtual_start, stop], &env())
            .outputs
            .is_empty()
    );
}

#[devkit::test]
fn recorder_lifetimes_keep_hosts_separate_when_session_ids_repeat() {
    use sinex_model::event_types::input::InputLogger;
    let make = |host: Option<&str>, seconds, event: &str| {
        let fields = if let Some(host) = host {
            json!({"host":host,"seat":"seat0","layer":"evdev","virtual":false})
        } else {
            json!({})
        };
        captured(
            &InputLogger {
                session: "reused-session".into(),
                logged_at: at(seconds),
                event: event.into(),
                n: 0,
                fields: serde_json::from_value(fields).unwrap(),
            },
            seconds as u64,
        )
    };
    let inputs = vec![
        make(Some("first"), 0, "start"),
        make(Some("second"), 10, "start"),
        make(Some("second"), 20, "stop"),
        make(None, 30, "stop"),
        make(Some("first"), 40, "stop"),
    ];
    fn view(event: &Event) -> KeyView<'_> {
        KeyView {
            event_type: event.event_type,
            ts_orig: event.ts_orig.at,
            payload: &event.payload,
        }
    }
    let first_key = Keyed::keys_for(&InputCoverage, &view(&inputs[0]), &env()).remove(0);
    let second_key = Keyed::keys_for(&InputCoverage, &view(&inputs[1]), &env()).remove(0);
    assert_ne!(first_key, second_key);
    assert!(Keyed::keys_for(&InputCoverage, &view(&inputs[3]), &env()).is_empty());
    for (key, start, end) in [(first_key, 0, 40), (second_key, 10, 20)] {
        let query = Keyed::inputs(&InputCoverage, &key, &env()).unwrap();
        let snapshot = make(Some("first"), 15, "focus");
        assert!(!query.admits(&snapshot));
        assert!(Keyed::keys_for(&InputCoverage, &view(&snapshot), &env()).is_empty());
        let output = Keyed::compute(&InputCoverage, &key, &inputs, &env());
        assert_eq!(output.outputs.len(), 1);
        assert_eq!(output.outputs[0].payload["started_at"], json!(at(start)));
        assert_eq!(output.outputs[0].payload["ended_at"], json!(at(end)));
    }
}

#[devkit::test]
fn delegated_time_is_retained_when_it_overlaps_operator_presence() {
    let make = |kind: &str, payload: Value| {
        (sinex_model::event_types::find(kind).unwrap().build)(
            &payload,
            "polylogue",
            Origin::Record {
                location: RecordLocation {
                    material: MaterialId::new(),
                    anchor: 0,
                },
                output_index: 0,
            },
            ReadingOrder(0),
            [0; 32],
            Class::Open,
        )
        .unwrap()
    };
    let metadata = make(
        "polylogue.session",
        json!({"session":"a","origin":"synthetic","kind":"main","cwd":[],"machine":"box","links":[]}),
    );
    let turn = make(
        "polylogue.turn",
        json!({"turn":"a/0","session":"a","start":at(0),"end":at(100),"prompts":[],"slash_command":false,"tool_calls":0,"errors":0,"abandoned":false}),
    );
    let acts = Keyed::compute(
        &crate::work::WorkActs,
        &json!({"session":"a"}),
        &[metadata, turn],
        &env(),
    )
    .outputs;
    let mut events = vec![physical(0), physical(20)];
    for event in &mut events {
        event.payload["extra"]
            .as_object_mut()
            .unwrap()
            .remove("seat");
    }
    events.extend(acts);
    let output = spans(&events, 0);
    let overlap = output
        .iter()
        .find(|span| span.started_at == at(0) && span.ended_at == at(20))
        .unwrap();
    assert_eq!(overlap.agency, Agency::Operator);
    assert!(overlap.delegated);
    assert_eq!(overlap.operator_present_s, 20.0);
}

#[test]
fn changing_civil_time_settings_retires_old_hour_keys() {
    use crate::AnyKeyed;
    use sinex_model::time_settings::TimeSettings;
    let mut env = Env::at(timestamp(1_718_438_400 * SECOND));
    let old = env.time_settings.clone();
    let start = old.hour_start(env.now).unwrap();
    let key = json!({"hour":start});
    assert!(Keyed::inputs(&ActivitySpans, &key, &env).is_some());
    env.time_settings = TimeSettings::new("Asia/Kolkata", 6, 30).unwrap();
    assert!(Keyed::inputs(&ActivitySpans, &key, &env).is_none());
    assert_ne!(
        AnyKeyed::version_for(&ActivitySpans, &old),
        AnyKeyed::version_for(&ActivitySpans, &env.time_settings)
    );
    assert_eq!(
        AnyKeyed::version_for(&crate::library::LibraryItems, &old),
        AnyKeyed::version_for(&crate::library::LibraryItems, &env.time_settings)
    );
}
