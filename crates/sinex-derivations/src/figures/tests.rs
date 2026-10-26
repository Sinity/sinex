use super::*;
use sinex_model::event_types::CommandFused;
use sinex_model::event_types::fused::FocusFused;
use sinex_model::event_types::machine::JournalEntry;
use sinex_model::{Class, MaterialId, Origin, ReadingOrder, RecordLocation};

const T: i64 = 1_718_438_400; // 2024-06-15 10:00 CEST

fn at(secs: i64) -> Timestamp {
    Timestamp::from_unix_secs(T + secs).unwrap()
}

fn parent() -> Event {
    crate::fusion::tests::zsh("p", T as f64)
}

fn command(secs: i64, cwd: &str, exit: Option<i64>) -> Event {
    derived(
        &CommandFused {
            record: format!("r{secs}"),
            command: "c".into(),
            host: "box".into(),
            started_at: at(secs),
            cwd: Some(cwd.into()),
            session: None,
            duration_ns: None,
            exit_code: exit,
            observed_by: vec![],
        },
        "command.fusion",
        [&parent()],
    )
}

fn focus(app: &str, from: i64, to: Option<i64>) -> Event {
    derived(
        &FocusFused {
            record: format!("f{from}"),
            started_at: at(from),
            ended_at: to.map(at),
            app: Some(app.into()),
            title: None,
            observed_by: vec![],
        },
        "focus.fusion",
        [&parent()],
    )
}

fn journal(secs: i64, priority: u8) -> Event {
    Event::build(
        &JournalEntry {
            cursor: format!("c{secs}"),
            logged_at: Some(at(secs)),
            boot_id: None,
            hostname: None,
            message: Some("m".into()),
            priority: Some(priority),
            unit: Some("a.service".into()),
            user_unit: None,
            identifier: None,
            pid: None,
            uid: None,
            command: None,
            transport: None,
            fields: Default::default(),
        },
        "machine.journald",
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
}

fn key(metric: &str) -> Value {
    json!({"metric": metric, "day": "2024-06-15"})
}

fn env(now: i64) -> Env {
    Env::at(Timestamp::from_unix_secs(T + now).unwrap())
}

/// The hourly figures of `metric` from `inputs`, as the runner reads them.
fn hourly(metric: &str, inputs: &[Event], now: i64) -> Vec<MetricFigure> {
    let env = env(now);
    let q = HourlyMetrics.inputs(&key(metric), &env).unwrap();
    let window: Vec<Event> = inputs.iter().filter(|e| q.admits(e)).cloned().collect();
    HourlyMetrics
        .compute(&key(metric), &window, &env)
        .outputs
        .iter()
        .map(|e| e.payload_as::<MetricFigure>().unwrap())
        .collect()
}

fn total_at(figures: &[MetricFigure], secs: i64) -> &MetricFigure {
    figures
        .iter()
        .find(|f| f.period_start == at(secs) && f.dimension.is_empty())
        .unwrap()
}

#[devkit::test]
fn the_hourly_inputs_are_every_type_a_metric_counts() {
    let mut declared: Vec<&str> = HourlyMetrics::INPUTS.to_vec();
    let mut counted: Vec<&str> = METRICS
        .iter()
        .flat_map(|m| (m.inputs)())
        .map(|k| k.event_type)
        .collect();
    declared.sort_unstable();
    counted.sort_unstable();
    counted.dedup();
    assert_eq!(declared, counted);
    for t in declared {
        assert!(sinex_model::event_types::find(t).is_some(), "{t}");
    }
}

#[devkit::test]
fn an_hour_counts_its_commands_and_breaks_them_down() {
    let inputs = [
        command(10, "/a", Some(0)),
        command(20, "/a", Some(1)),
        command(30, "/b", None),
        command(3700, "/c", Some(0)),
    ];
    let commands = hourly("shell.commands", &inputs, 100_000);
    let first = total_at(&commands, 0);
    assert_eq!(first.value, Some(3.0));
    assert_eq!(first.rows, 3);
    assert_eq!(first.period_end, at(3600));
    let a = commands
        .iter()
        .find(|f| f.dimension == "cwd" && f.dimension_value == "/a")
        .unwrap();
    assert_eq!(a.value, Some(2.0));
    assert_eq!(total_at(&commands, 3600).value, Some(1.0));

    let failed = hourly("shell.failed_ratio", &inputs, 100_000);
    let first = total_at(&failed, 0);
    assert_eq!(first.value, Some(0.5), "the unknown exit is left out");
    assert_eq!((first.numerator, first.denominator), (Some(1.0), Some(2.0)));
}

#[devkit::test]
fn a_ratio_of_nothing_known_has_no_value() {
    let failed = hourly("shell.failed_ratio", &[command(10, "/a", None)], 100_000);
    assert_eq!(total_at(&failed, 0).value, None);
}

#[devkit::test]
fn a_day_key_emits_only_its_busy_hours() {
    let out = hourly(
        "shell.commands",
        &[command(10, "/a", Some(0)), command(7200, "/a", Some(0))],
        100_000,
    );
    let totals: Vec<&MetricFigure> = out.iter().filter(|f| f.dimension.is_empty()).collect();
    assert_eq!(totals.len(), 2, "hours with nothing counted have no figure");
    assert!(
        totals
            .iter()
            .all(|f| f.grain == "hour" && f.day == "2024-06-15")
    );
}

#[devkit::test]
fn seconds_are_clipped_to_each_hour_and_overlaps_count_once() {
    let out = hourly(
        "focus.seconds",
        &[
            // From the day before the hour to ten minutes into it.
            focus("vim", -1200, Some(600)),
            focus("web", 600, Some(3000)),
            focus("web", 2900, Some(3100)),
        ],
        100_000,
    );
    assert_eq!(total_at(&out, 0).value, Some(3100.0));
    let web = out
        .iter()
        .find(|f| f.period_start == at(0) && f.dimension_value == "web")
        .unwrap();
    assert_eq!(web.value, Some(2500.0));
    assert_eq!(total_at(&out, -3600).value, Some(1200.0));
}

#[devkit::test]
fn an_open_focus_counts_up_to_now() {
    let out = hourly("focus.seconds", &[focus("vim", 0, None)], 900);
    assert_eq!(total_at(&out, 0).value, Some(900.0));
}

#[devkit::test]
fn a_play_of_unknown_length_counts_no_seconds() {
    let play = derived(
        &sinex_model::event_types::fused::PlayFused {
            record: "p".into(),
            title: "Song".into(),
            artist: None,
            album: None,
            player: None,
            started_at: at(0),
            ended_at: None,
            observed_by: vec![],
        },
        "play.fusion",
        [&parent()],
    );
    let seconds = hourly("music.listen_seconds", std::slice::from_ref(&play), 900);
    assert!(seconds.is_empty(), "{seconds:?}");
    let plays = hourly("music.plays", &[play], 900);
    assert_eq!(total_at(&plays, 0).value, Some(1.0));
}

#[devkit::test]
fn an_old_open_interval_stops_counting_at_its_day_end() {
    // Opened at 10:00 two years ago and never ended: it covers that day
    // until 04:00 the next morning, not every day until today.
    let now = 2 * 365 * 86_400;
    let e = focus("vim", 0, None);
    let view = KeyView {
        event_type: e.event_type,
        ts_orig: e.ts_orig.at,
        payload: &e.payload,
    };
    assert_eq!(
        HourlyMetrics.keys_for(&view, &env(now)),
        vec![key("focus.seconds"), key("focus.switches")]
    );
    let out = hourly("focus.seconds", &[e], now);
    let seconds: f64 = out
        .iter()
        .filter(|f| f.dimension.is_empty())
        .filter_map(|f| f.value)
        .sum();
    assert_eq!(seconds, 18.0 * 3600.0);
}

#[devkit::test]
fn a_long_interval_dirties_every_day_it_covers() {
    let e = focus("vim", 0, Some(2 * 86_400));
    let view = KeyView {
        event_type: e.event_type,
        ts_orig: e.ts_orig.at,
        payload: &e.payload,
    };
    let keys = HourlyMetrics.keys_for(&view, &env(3 * 86_400));
    let seconds: Vec<&Value> = keys
        .iter()
        .filter(|k| k["metric"] == "focus.seconds")
        .collect();
    assert_eq!(seconds.len(), 3);
    let switches = keys.iter().filter(|k| k["metric"] == "focus.switches");
    assert_eq!(switches.count(), 1, "a switch is counted on its day only");
}

#[devkit::test]
fn seconds_ask_for_the_interval_running_at_the_day_start() {
    let q = HourlyMetrics
        .inputs(&key("focus.seconds"), &env(0))
        .unwrap();
    assert_eq!(q.before.len(), 1);
    let q = HourlyMetrics
        .inputs(&key("shell.commands"), &env(0))
        .unwrap();
    assert!(q.before.is_empty());
}

#[devkit::test]
fn a_day_is_the_logical_day() {
    let out = hourly(
        "shell.commands",
        &[
            command(0, "/a", Some(0)),
            command(-7 * 3600, "/early", Some(0)),
        ],
        100_000,
    );
    let total: f64 = out
        .iter()
        .filter(|f| f.dimension.is_empty())
        .filter_map(|f| f.value)
        .sum();
    assert_eq!(total, 1.0, "03:00 belongs to the 14th");
}

#[devkit::test]
fn only_error_entries_count_as_journal_errors() {
    let out = hourly(
        "machine.journal_errors",
        &[journal(10, 3), journal(20, 4), journal(30, 0)],
        100_000,
    );
    assert_eq!(total_at(&out, 0).value, Some(2.0));
    assert!(
        out.iter()
            .any(|f| f.dimension == "unit" && f.dimension_value == "a.service")
    );
}

#[devkit::test]
fn a_day_adds_up_its_hours() {
    let env = env(100_000);
    let inputs = [
        command(10, "/a", Some(1)),
        command(3700, "/a", Some(0)),
        command(7300, "/b", Some(0)),
    ];
    let mut out = Vec::new();
    for metric in ["shell.commands", "shell.failed_ratio"] {
        let hours: Vec<Event> = HourlyMetrics.compute(&key(metric), &inputs, &env).outputs;
        let q = DailyMetrics.inputs(&key(metric), &env).unwrap();
        assert!(hours.iter().all(|h| q.admits(h)));
        let view = KeyView {
            event_type: hours[0].event_type,
            ts_orig: hours[0].ts_orig.at,
            payload: &hours[0].payload,
        };
        assert_eq!(DailyMetrics.keys_for(&view, &env), vec![key(metric)]);
        out.extend(
            DailyMetrics
                .compute(&key(metric), &hours, &env)
                .outputs
                .into_iter()
                .map(|e| e.payload_as::<MetricFigure>().unwrap()),
        );
    }
    let total = |metric: &str| {
        out.iter()
            .find(|f| f.metric == metric && f.dimension.is_empty())
            .unwrap()
    };
    assert_eq!(total("shell.commands").value, Some(3.0));
    assert_eq!(total("shell.commands").grain, "day");
    assert_eq!(total("shell.commands").period_start, at(-6 * 3600));
    assert_eq!(total("shell.failed_ratio").value, Some(1.0 / 3.0));
    let a = out
        .iter()
        .find(|f| f.metric == "shell.commands" && f.dimension_value == "/a")
        .unwrap();
    assert_eq!(a.value, Some(2.0));
}
