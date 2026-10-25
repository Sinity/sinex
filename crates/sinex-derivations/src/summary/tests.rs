use super::*;
use std::collections::BTreeMap as Map;

const T: i64 = 1_718_438_400; // 2024-06-15 10:00 CEST

fn parent() -> Event {
    crate::fusion::tests::zsh("p", T as f64)
}

fn command(secs: i64, cwd: &str, exit: i64) -> Event {
    let p = parent();
    derived(
        &CommandFused {
            record: format!("r{secs}"),
            command: "c".into(),
            host: "box".into(),
            started_at: Timestamp::from_unix_secs(T + secs).unwrap(),
            cwd: Some(cwd.into()),
            session: None,
            duration_ns: None,
            exit_code: Some(exit),
            observed_by: vec![],
        },
        "command.fusion",
        [&p],
    )
}

fn interval(kind: &str, label: &str, from: i64, to: Option<i64>) -> Event {
    let p = parent();
    derived(
        &StateInterval {
            interval_id: format!("{kind}:{label}:{from}"),
            state_kind: kind.into(),
            subject_id: Some(label.into()),
            label: Some(label.into()),
            started_at: Timestamp::from_unix_secs(T + from).unwrap(),
            ended_at: to.map(|t| Timestamp::from_unix_secs(T + t).unwrap()),
            start_event_type: "window.focused".into(),
            end_event_type: None,
            attributes: Map::new(),
        },
        "state.intervals",
        [&p],
    )
}

fn summaries(d: &dyn crate::AnyKeyed, inputs: &[Event], now: i64) -> Vec<ActivitySummary> {
    let env = Env::at(Timestamp::from_unix_secs(T + now).unwrap());
    d.compute(&json!({"day": "2024-06-15"}), inputs, &env)
        .outputs
        .iter()
        .map(|e| e.payload_as::<ActivitySummary>().unwrap())
        .collect()
}

fn hour(inputs: &[Event], now: i64) -> Option<ActivitySummary> {
    summaries(&HourlySummary, inputs, now)
        .into_iter()
        .find(|s| s.period_start == Timestamp::from_unix_secs(T).unwrap())
}

#[devkit::test]
fn an_hour_counts_its_commands_and_state_time_inside_it() {
    let s = hour(
        &[
            command(10, "/a", 0),
            command(20, "/a", 1),
            command(30, "/b", 0),
            command(3700, "/c", 0),
            // 20 minutes before the hour to 10 minutes into it.
            interval("desktop.focus", "vim", -1200, Some(600)),
            interval("desktop.focus", "web", 600, Some(3000)),
            interval("system.systemd.unit", "cron", 0, Some(3600)),
        ],
        100_000,
    )
    .unwrap();
    assert_eq!(s.commands, 3);
    assert_eq!(s.failed_commands, 1);
    assert_eq!(s.top_cwds[0], ("/a".into(), 2));
    assert_eq!(s.state_seconds.get("desktop.focus"), Some(&3000));
    assert!(!s.state_seconds.contains_key("system.systemd.unit"));
    assert_eq!(
        s.top_subjects[0],
        ("desktop.focus".into(), "web".into(), 2400)
    );
    assert_eq!(
        s.period_end.0.unix_timestamp() - s.period_start.0.unix_timestamp(),
        3600
    );
}

#[devkit::test]
fn a_day_key_emits_only_its_busy_hours() {
    let out = summaries(
        &HourlySummary,
        &[command(10, "/a", 0), command(7200, "/a", 0)],
        100_000,
    );
    assert_eq!(out.len(), 2, "hours with nothing counted have no summary");
    assert!(
        out.iter()
            .all(|s| s.grain == "hour" && s.day == "2024-06-15")
    );
}

#[devkit::test]
fn open_states_count_up_to_now() {
    let s = hour(&[interval("desktop.focus", "vim", 0, None)], 900).unwrap();
    assert_eq!(s.state_seconds.get("desktop.focus"), Some(&900));
}

#[devkit::test]
fn an_old_open_state_stops_counting_at_its_day_end() {
    // Opened at 10:00 two years ago and never ended: it covers that day
    // until 04:00 the next morning, not every day until today.
    let env = Env::at(Timestamp::from_unix_secs(T + 2 * 365 * 86_400).unwrap());
    let e = interval("desktop.focus", "vim", 0, None);
    let view = KeyView {
        event_type: e.event_type,
        ts_orig: e.ts_orig.at,
        payload: &e.payload,
    };
    assert_eq!(
        DailySummary.keys_for(&view, &env),
        vec![json!({"day": "2024-06-15"})]
    );
    let s = summaries(&DailySummary, &[e], 2 * 365 * 86_400);
    assert_eq!(s[0].state_seconds.get("desktop.focus"), Some(&(18 * 3600)));
}

#[devkit::test]
fn a_long_state_dirties_every_day_it_covers_and_units_none() {
    let env = Env::at(Timestamp::from_unix_secs(T + 100_000).unwrap());
    let e = interval("desktop.focus", "vim", 0, Some(2 * 86_400));
    let view = KeyView {
        event_type: e.event_type,
        ts_orig: e.ts_orig.at,
        payload: &e.payload,
    };
    assert_eq!(HourlySummary.keys_for(&view, &env).len(), 3);
    let unit = interval("system.systemd.unit", "cron", 0, Some(10));
    let view = KeyView {
        event_type: unit.event_type,
        ts_orig: unit.ts_orig.at,
        payload: &unit.payload,
    };
    assert!(HourlySummary.keys_for(&view, &env).is_empty());
}

#[devkit::test]
fn a_day_asks_for_the_last_state_of_each_kind_before_it() {
    let env = Env::at(Timestamp::now());
    let q = DailySummary
        .inputs(&json!({"day": "2024-06-15"}), &env)
        .unwrap();
    assert_eq!(q.before.len(), desktop_kinds().len());
    assert!(q.before.iter().all(|n| n.count == 1));
}

#[devkit::test]
fn a_day_is_the_logical_day() {
    let out = summaries(
        &DailySummary,
        &[command(0, "/a", 0), command(-7 * 3600, "/early", 0)],
        100_000,
    );
    assert_eq!(out[0].commands, 1, "03:00 belongs to the 14th");
    assert_eq!(out[0].grain, "day");
}
