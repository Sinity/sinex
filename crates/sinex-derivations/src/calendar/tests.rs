use super::*;
use sinex_model::{Class, MaterialId, Origin, ReadingOrder, RecordLocation};

fn at(text: &str) -> Timestamp {
    let t: jiff::Timestamp = text.parse().unwrap();
    Timestamp::from_unix_ns(t.as_nanosecond()).unwrap()
}

fn event(uid: &str) -> CalendarEvent {
    CalendarEvent {
        calendar: "me@example.org".into(),
        uid: uid.into(),
        recurrence_id: None,
        summary: Some("Reading".into()),
        description: None,
        location: None,
        start: None,
        end: None,
        all_day: false,
        zone: "Europe/Warsaw".into(),
        local_zone: Some("UTC".into()),
        status: None,
        rrule: None,
        exdates: vec![],
        recurrence_start: None,
        excluded_starts: vec![],
        added_starts: vec![],
        organizer: None,
        attendees: vec![],
        created_at: None,
        last_modified: None,
        sequence: None,
        cancelled_at: None,
        extra: serde_json::Map::new(),
    }
}

fn stored(payload: &CalendarEvent) -> Event {
    Event::build(
        payload,
        "calendar.export",
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

fn compute(inputs: &[Event], now: &str) -> Vec<CalendarInstance> {
    let mut out: Vec<CalendarInstance> = Instances
        .compute(&Value::Null, inputs, &Env::at(at(now)))
        .outputs
        .iter()
        .map(|e| e.payload_as::<CalendarInstance>().unwrap())
        .collect();
    out.sort_by_key(|i| i.series_start);
    out
}

/// A weekly series at 09:00 Warsaw time from Monday 2024-03-18, an hour long.
fn weekly(rule: &str) -> CalendarEvent {
    CalendarEvent {
        start: Some(at("2024-03-18T08:00:00Z")),
        end: Some(at("2024-03-18T09:00:00Z")),
        rrule: Some(rule.into()),
        ..event("w")
    }
}

#[devkit::test]
fn a_series_keeps_its_local_time_across_the_spring_change() {
    let out = compute(
        &[stored(&weekly("FREQ=WEEKLY;COUNT=3"))],
        "2024-03-01T00:00:00Z",
    );
    let starts: Vec<Timestamp> = out.iter().map(|i| i.start).collect();
    assert_eq!(
        starts,
        [
            at("2024-03-18T08:00:00Z"),
            at("2024-03-25T08:00:00Z"),
            at("2024-04-01T07:00:00Z")
        ]
    );
    assert!(
        out.iter()
            .all(|i| i.end.unwrap().unix_ns() - i.start.unix_ns() == 3_600_000_000_000)
    );
    assert!(
        out.iter()
            .all(|i| !i.changed && i.rule_not_expanded.is_none())
    );
}

#[devkit::test]
fn excluded_added_and_changed_instances() {
    let series = CalendarEvent {
        excluded_starts: vec![at("2024-03-25T08:00:00Z")],
        added_starts: vec![at("2024-03-20T12:00:00Z")],
        ..weekly("FREQ=WEEKLY;COUNT=4")
    };
    let moved = CalendarEvent {
        recurrence_id: Some("20240401T090000".into()),
        recurrence_start: Some(at("2024-04-01T07:00:00Z")),
        start: Some(at("2024-04-02T15:00:00Z")),
        end: Some(at("2024-04-02T16:00:00Z")),
        summary: Some("Moved".into()),
        ..event("w")
    };
    let cancelled = CalendarEvent {
        recurrence_id: Some("20240408T090000".into()),
        recurrence_start: Some(at("2024-04-08T07:00:00Z")),
        start: Some(at("2024-04-08T07:00:00Z")),
        status: Some("CANCELLED".into()),
        cancelled_at: Some(at("2024-04-01T00:00:00Z")),
        ..event("w")
    };
    let inputs = [stored(&series), stored(&moved), stored(&cancelled)];
    let out = compute(&inputs, "2024-03-01T00:00:00Z");
    let series_starts: Vec<Timestamp> = out.iter().map(|i| i.series_start).collect();
    assert_eq!(
        series_starts,
        [
            at("2024-03-18T08:00:00Z"),
            at("2024-03-20T12:00:00Z"),
            at("2024-04-01T07:00:00Z")
        ],
        "the excluded start and the cancelled instance plan nothing"
    );
    let changed = &out[2];
    assert!(changed.changed);
    assert_eq!(changed.start, at("2024-04-02T15:00:00Z"));
    assert_eq!(changed.summary.as_deref(), Some("Moved"));
    let parents = |i: usize| match &Instances
        .compute(&Value::Null, &inputs, &Env::at(at("2024-03-01T00:00:00Z")))
        .outputs[i]
        .origin
    {
        Origin::Derived { parents } => parents.len(),
        Origin::Record { .. } => 0,
    };
    assert!(
        (0..3).map(parents).any(|n| n == 2),
        "a changed instance names both readings"
    );
}

#[devkit::test]
fn an_endless_series_stops_at_the_horizon() {
    let out = compute(&[stored(&weekly("FREQ=WEEKLY"))], "2024-03-18T00:00:00Z");
    // 366 days ahead of 2024-03-18: 53 Mondays from the first.
    assert_eq!(out.len(), 53);
    assert!(out.last().unwrap().start <= at("2025-03-19T00:00:00Z"));
}

#[devkit::test]
fn a_whole_day_series_and_an_event_that_does_not_repeat() {
    let birthday = CalendarEvent {
        start: Some(at("2024-07-10T00:00:00Z")),
        end: Some(at("2024-07-11T00:00:00Z")),
        all_day: true,
        zone: "date".into(),
        local_zone: Some("Europe/Warsaw".into()),
        rrule: Some("FREQ=YEARLY;UNTIL=20260710".into()),
        ..event("b")
    };
    let out = compute(&[stored(&birthday)], "2024-01-01T00:00:00Z");
    assert_eq!(out.len(), 3);
    assert_eq!(out[2].start, at("2026-07-10T00:00:00Z"));
    assert_eq!(out[2].end, Some(at("2026-07-11T00:00:00Z")));
    let single = CalendarEvent {
        start: Some(at("2024-05-01T10:00:00Z")),
        ..event("s")
    };
    let out = compute(&[stored(&single)], "2024-01-01T00:00:00Z");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].series_start, out[0].start);
}

#[devkit::test]
fn an_unread_rule_gives_the_first_start_and_says_why() {
    let out = compute(&[stored(&weekly("FREQ=HOURLY"))], "2024-03-01T00:00:00Z");
    assert_eq!(out.len(), 1);
    assert!(
        out[0]
            .rule_not_expanded
            .as_deref()
            .unwrap()
            .contains("HOURLY")
    );
}

#[devkit::test]
fn a_cancelled_series_plans_nothing() {
    let series = CalendarEvent {
        status: Some("CANCELLED".into()),
        cancelled_at: Some(at("2024-03-01T00:00:00Z")),
        ..weekly("FREQ=WEEKLY;COUNT=3")
    };
    assert!(compute(&[stored(&series)], "2024-03-01T00:00:00Z").is_empty());
}

#[devkit::test]
fn a_series_is_one_key_read_whole() {
    let e = stored(&weekly("FREQ=DAILY"));
    let env = Env::at(at("2024-03-01T00:00:00Z"));
    let keys = Instances.keys_for(
        &KeyView {
            event_type: e.event_type,
            ts_orig: e.ts_orig.at,
            payload: &e.payload,
        },
        &env,
    );
    assert_eq!(keys, [json!({"calendar": "me@example.org", "uid": "w"})]);
    let query = Instances.inputs(&keys[0], &env).unwrap();
    assert!(query.admits(&e));
}
