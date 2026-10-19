use super::*;
use sinex_model::{Class, MaterialId, Origin, ReadingOrder, RecordLocation};

// 2026-08-22 is the documented artifact night: 01:52-03:42 CEST.
fn at(hhmm: &str, day: &str) -> Timestamp {
    let s = format!("{day}T{hhmm}:00+02:00");
    Timestamp(
        time::OffsetDateTime::parse(&s, &time::format_description::well_known::Rfc3339).unwrap(),
    )
}

fn located() -> Origin {
    Origin::Record {
        location: RecordLocation {
            material: MaterialId::new(),
            anchor: 0,
        },
        ordinal: 0,
    }
}

fn span(row: i64, start: Timestamp, end: Timestamp, status: &str) -> Event {
    Event::build(
        &PresenceSpan {
            bucket: "aw-watcher-afk".into(),
            row,
            started_at: start,
            ended_at: end,
            status: status.into(),
        },
        "desktop.activitywatch",
        located(),
        ReadingOrder(row),
        *blake3::hash(&row.to_le_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

fn command(at: Timestamp, n: u64) -> Event {
    Event::build(
        &CommandImported {
            command: format!("c{n}"),
            hostname: "box".into(),
            started_at: at,
            header_elapsed: 0,
            n: 0,
        },
        "terminal.zsh",
        located(),
        ReadingOrder(0),
        *blake3::hash(&n.to_le_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

fn env() -> Env {
    Env::at(at("12:00", "2026-10-01"))
}

/// The intervals of the logical day `of` falls in.
fn intervals(of: Timestamp, inputs: &[Event]) -> Vec<PresenceInterval> {
    let day = env().civil.day(of).unwrap().to_string();
    let mut sorted = inputs.to_vec();
    sorted.sort_by_key(|e| e.ts_orig.at);
    Presence
        .compute(&json!({ "day": day }), &sorted, &env())
        .outputs
        .iter()
        .map(|e| e.payload_as().unwrap())
        .collect()
}

#[test]
fn a_long_not_afk_night_with_no_input_is_uncertain() {
    let d = "2026-08-22";
    let out = intervals(
        at("01:52", d),
        &[span(1, at("01:52", d), at("03:42", d), "not-afk")],
    );
    assert_eq!(out.len(), 1);
    assert_eq!((out[0].state.as_str(), out[0].evidence), ("uncertain", 0));
}

#[test]
fn a_long_night_with_a_few_commands_is_present_and_sparse() {
    let d = "2026-08-20";
    // 343 minutes with 12 real commands.
    // 20:00 to 01:43: inside one logical day (days start at 04:00).
    let start = at("20:00", d);
    let mut inputs = vec![span(
        1,
        start,
        Timestamp(start.0 + time::Duration::minutes(343)),
        "not-afk",
    )];
    for n in 0..12 {
        inputs.push(command(
            Timestamp(start.0 + time::Duration::minutes(10 + 27 * n as i64)),
            n,
        ));
    }
    let out = intervals(start, &inputs);
    assert_eq!(out.len(), 1);
    assert_eq!(
        (out[0].state.as_str(), out[0].sparse, out[0].evidence),
        ("present", true, 12)
    );
}

#[test]
fn short_spans_are_taken_as_reported_and_afk_is_absent() {
    let d = "2026-08-21";
    let out = intervals(
        at("10:00", d),
        &[
            span(1, at("10:00", d), at("10:20", d), "not-afk"),
            span(2, at("10:20", d), at("12:00", d), "afk"),
        ],
    );
    let states: Vec<&str> = out.iter().map(|i| i.state.as_str()).collect();
    assert_eq!(states, ["present", "absent"]);
}

#[test]
fn a_span_is_cut_at_logical_day_boundaries() {
    // 02:00 on the 22nd to 06:00 on the 23rd crosses two day starts.
    let s = span(
        1,
        at("02:00", "2026-08-22"),
        at("06:00", "2026-08-23"),
        "not-afk",
    );
    let view = KeyView {
        event_type: s.event_type,
        ts_orig: s.ts_orig.at,
        payload: &s.payload,
    };
    assert_eq!(Presence.keys_for(&view, &env()).len(), 3);
    let middle = intervals(at("12:00", "2026-08-22"), &[s]);
    assert_eq!(middle.len(), 1);
    let day = env().civil.day(at("12:00", "2026-08-22")).unwrap();
    assert_eq!(middle[0].started_at, env().civil.day_start(day).unwrap());
    assert_eq!(middle[0].ended_at, env().civil.day_end(day).unwrap());
}
