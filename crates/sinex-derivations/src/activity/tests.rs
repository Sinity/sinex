use super::*;

// 2024-06-15 10:00 CEST.
const T: i64 = 1_718_438_400;

fn at(secs: i64) -> Timestamp {
    Timestamp::from_unix_secs(T + secs).unwrap()
}

fn parent(secs: i64) -> Event {
    crate::fusion::tests::zsh("x", (T + secs) as f64)
}

fn command(secs: i64, duration_s: i64) -> Event {
    derived(
        &CommandFused {
            record: format!("r{secs}"),
            command: format!("c{secs}"),
            host: "box".into(),
            started_at: at(secs),
            cwd: None,
            session: None,
            duration_ns: Some(duration_s * 1_000_000_000),
            exit_code: Some(0),
            observed_by: vec!["terminal.zsh".into()],
        },
        "command.fusion",
        [&parent(secs)],
    )
}

fn focus(secs: i64, until: Option<i64>, app: &str) -> Event {
    derived(
        &FocusFused {
            record: format!("f{secs}"),
            started_at: at(secs),
            ended_at: until.map(at),
            app: Some(app.into()),
            title: Some("t".into()),
            observed_by: vec!["desktop.hyprland".into()],
        },
        "focus.fusion",
        [&parent(secs)],
    )
}

fn visit(secs: i64) -> Event {
    derived(
        &VisitFused {
            record: format!("v{secs}"),
            url: format!("https://example.org/{secs}"),
            title: None,
            visited_at: at(secs),
            observed_by: vec!["browser.chromium-visits".into()],
        },
        "visit.fusion",
        [&parent(secs)],
    )
}

fn absent(from: i64, until: i64) -> Event {
    derived(
        &PresenceInterval {
            day: "2024-06-15".into(),
            started_at: at(from),
            ended_at: at(until),
            state: "absent".into(),
            sparse: false,
            reported: "afk".into(),
            evidence: 0,
        },
        "presence.intervals",
        [&parent(from)],
    )
}

fn env() -> Env {
    Env::at(at(86_400 * 3))
}

fn spans(inputs: &[Event]) -> Vec<ActivitySpan> {
    ActivitySpans
        .compute(&json!({"day": "2024-06-15"}), inputs, &env())
        .outputs
        .iter()
        .map(|e| e.payload_as::<ActivitySpan>().unwrap())
        .collect()
}

#[devkit::test]
fn activity_joins_across_sources_until_a_long_pause_or_absence() {
    let inputs = vec![
        focus(0, Some(600), "kitty"),
        command(100, 30),
        // 290 s after the focus ended: still the same span.
        visit(890),
        focus(900, Some(1_500), "firefox"),
        // Away for twenty minutes inside the firefox focus.
        absent(1_000, 1_200),
        // A pause longer than five minutes starts a new span.
        command(1_900, 10),
    ];
    let s = spans(&inputs);
    let bounds: Vec<(Timestamp, Timestamp)> =
        s.iter().map(|s| (s.started_at, s.ended_at)).collect();
    assert_eq!(
        bounds,
        [
            (at(0), at(1_000)),
            (at(1_200), at(1_500)),
            (at(1_900), at(1_910))
        ],
        "{s:#?}"
    );
    assert_eq!(s[0].apps, ["kitty", "firefox"]);
    assert_eq!(s[0].records["focus.fused"], 2);
    assert_eq!(s[0].records["command.fused"], 1);
    assert_eq!(s[0].records["visit.fused"], 1);
    assert_eq!(s[2].records["command.fused"], 1);
}

#[devkit::test]
fn the_result_does_not_depend_on_input_order() {
    let mut inputs = vec![
        focus(0, Some(600), "kitty"),
        command(100, 30),
        visit(890),
        absent(1_000, 1_200),
        focus(900, Some(1_500), "firefox"),
        command(1_900, 10),
        visit(1_905),
    ];
    let forward = spans(&inputs);
    inputs.reverse();
    assert_eq!(spans(&inputs), forward);
    inputs.swap(0, 3);
    assert_eq!(spans(&inputs), forward);
}

#[devkit::test]
fn a_moment_while_away_is_not_activity_and_spans_stop_at_the_day_start() {
    // 03:55 CEST: the logical day ends at 04:00.
    let before_day_end = 64_500;
    let s = spans(&[
        visit(500),
        absent(400, 600),
        focus(before_day_end, Some(before_day_end + 900), "kitty"),
    ]);
    assert_eq!(s.len(), 1, "{s:#?}");
    assert_eq!(s[0].started_at, at(before_day_end));
    assert_eq!(s[0].ended_at, at(64_800), "cut at the day start");
    let keys = ActivitySpans.keys_for(
        &KeyView {
            event_type: FocusFused::EVENT_TYPE,
            ts_orig: Some(at(before_day_end)),
            payload: &focus(before_day_end, Some(before_day_end + 900), "kitty").payload,
        },
        &env(),
    );
    assert_eq!(
        keys,
        [json!({"day": "2024-06-15"}), json!({"day": "2024-06-16"})]
    );
}

#[devkit::test]
fn a_focus_still_held_waits_for_its_end() {
    let c = ActivitySpans.compute(
        &json!({"day": "2024-06-15"}),
        &[focus(0, None, "kitty"), command(10, 5)],
        &env(),
    );
    assert!(c.waiting);
    assert_eq!(c.outputs.len(), 1);
}

#[devkit::test]
fn absence_boundaries_exclude_moments_inside_and_keep_the_first_moment_after() {
    let mut absence = absent(10, 20);
    absence.class = sinex_model::Class::Guarded;
    let inputs = [
        focus(0, Some(10), "kitty"),
        visit(10),
        visit(20),
        absence.clone(),
    ];
    let computed = ActivitySpans.compute(&json!({"day": "2024-06-15"}), &inputs, &env());
    let readings: Vec<ActivitySpan> = computed
        .outputs
        .iter()
        .map(|e| e.payload_as().unwrap())
        .collect();
    assert_eq!(readings.len(), 2);
    assert!(!readings[0].records.contains_key("visit.fused"));
    assert_eq!(readings[1].started_at, at(20));
    assert_eq!(readings[1].records["visit.fused"], 1);
    for output in computed.outputs {
        assert_eq!(output.class, sinex_model::Class::Guarded);
        let sinex_model::Origin::Derived { parents } = output.origin else {
            panic!("derived")
        };
        assert!(parents.contains(&absence.id));
    }
}
