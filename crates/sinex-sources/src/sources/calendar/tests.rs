use super::*;
use serde_json::json;

const ICS: &str = "BEGIN:VEVENT\r\nDTSTART;TZID=Europe/Warsaw:20110128T150000\r\nDTEND;TZID=Europe/Warsaw:20110128T160000\r\nRRULE:FREQ=WEEKLY;COUNT=100;BYDAY=SU,FR\r\nDTSTAMP:20260508T144252Z\r\nUID:a1@google.com\r\nATTENDEE;CUTYPE=INDIVIDUAL;CN=\"A, B\";X-NUM-GUE\r\n STS=0:mailto:a@example.org\r\nCREATED:00010101T000000Z\r\nDESCRIPTION:line one\\nline\\, two\r\nLAST-MODIFIED:20221220T050634Z\r\nSEQUENCE:2\r\nSTATUS:CONFIRMED\r\nSUMMARY:Reading\r\nX-GOOGLE-CONFERENCE:https://meet.example\r\nBEGIN:VALARM\r\nACTION:DISPLAY\r\nEND:VALARM\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";

fn read(ics: &str) -> CalendarEvent {
    let ev = <VEvent as RecordShape>::decode(ics.as_bytes()).unwrap();
    let manifest =
        sinex_model::Manifest::from_json(json!({"path": "Takeout/Calendar/me@example.org.ics"}));
    let ctx = RecordContext {
        manifest: &manifest,
        member: None,
    };
    let parser = CalendarParser {
        zone: jiff::tz::TimeZone::UTC,
    };
    parser.parse(&ev, &(), &ctx).unwrap().0.remove(0)
}

#[devkit::test]
fn reads_a_recurring_zoned_event() {
    let e = read(ICS);
    assert_eq!(e.calendar, "me@example.org");
    assert_eq!(e.uid, "a1@google.com");
    assert_eq!(e.zone, "Europe/Warsaw");
    assert_eq!(
        e.start,
        Timestamp::from_unix_secs(1_296_223_200).ok(),
        "15:00 CET is 14:00 UTC"
    );
    assert_eq!(
        e.end.unwrap().unix_ns() - e.start.unwrap().unix_ns(),
        3_600_000_000_000
    );
    assert_eq!(
        e.rrule.as_deref(),
        Some("FREQ=WEEKLY;COUNT=100;BYDAY=SU,FR")
    );
    assert_eq!(e.description.as_deref(), Some("line one\nline, two"));
    assert_eq!(e.attendees, vec!["mailto:a@example.org"]);
    assert_eq!(e.created_at, None, "year 1 is Google's placeholder");
    assert!(e.extra.contains_key("X-GOOGLE-CONFERENCE"));
    assert!(
        !e.extra.contains_key("ACTION"),
        "the alarm is not the event's own"
    );
    assert!(!e.extra.contains_key("DTSTAMP"));
}

#[devkit::test]
fn whole_days_floating_times_durations_and_cancellations() {
    let ics = "BEGIN:VEVENT\nUID:d\nDTSTART;VALUE=DATE:20240301\nDURATION:P1D\nSTATUS:CANCELLED\nLAST-MODIFIED:20240302T000000Z\nEND:VEVENT\n";
    let e = read(ics);
    assert!(e.all_day);
    assert_eq!(e.zone, "date");
    assert_eq!(
        e.end.unwrap().unix_ns() - e.start.unwrap().unix_ns(),
        86_400_000_000_000
    );
    assert!(e.cancelled_at.is_some());
    let floating = read("BEGIN:VEVENT\nUID:f\nDTSTART:20240301T090000\nEND:VEVENT\n");
    assert_eq!(floating.zone, "floating");
}

#[devkit::test]
fn the_export_stamp_does_not_revise_an_event() {
    let later = ICS.replace("DTSTAMP:20260508T144252Z", "DTSTAMP:20270101T000000Z");
    assert_eq!(
        <VEvent as RecordShape>::hash(ICS.as_bytes(), &[]),
        <VEvent as RecordShape>::hash(later.as_bytes(), &[])
    );
}

#[devkit::test]
fn frames_one_record_per_event() {
    let doc = format!(
        "BEGIN:VCALENDAR\nX-WR-CALNAME:Me\n{ICS}{}",
        ICS.replace("a1@", "a2@")
    );
    let (records, _) = frame(doc.as_bytes());
    assert_eq!(records.len(), 2);
}

fn read_in(ics: &str, zone: &str) -> CalendarEvent {
    let ev = <VEvent as RecordShape>::decode(ics.as_bytes()).unwrap();
    let parser = CalendarParser {
        zone: jiff::tz::TimeZone::get(zone).unwrap(),
    };
    parser
        .parse(&ev, &(), &crate::RecordContext::none())
        .unwrap()
        .0
        .remove(0)
}

fn at(text: &str) -> Timestamp {
    let t: jiff::Timestamp = text.parse().unwrap();
    Timestamp::from_unix_ns(t.as_nanosecond()).unwrap()
}

#[devkit::test]
fn exceptions_are_read_in_their_own_zone() {
    let ics = "BEGIN:VEVENT\nUID:s\nDTSTART;TZID=Europe/Warsaw:20240301T090000\nRRULE:FREQ=DAILY;COUNT=10\nEXDATE;TZID=Europe/Warsaw:20240303T090000,20240305T090000\nEXDATE:20240307T080000Z\nRDATE;VALUE=DATE:20240320\nRDATE:20240321T120000Z/PT1H\nEND:VEVENT\n";
    let e = read_in(ics, "America/New_York");
    assert_eq!(
        e.excluded_starts,
        vec![
            at("2024-03-03T08:00:00Z"),
            at("2024-03-05T08:00:00Z"),
            at("2024-03-07T08:00:00Z")
        ]
    );
    assert_eq!(
        e.exdates,
        vec!["20240303T090000,20240305T090000", "20240307T080000Z"],
        "the values stay as written"
    );
    assert_eq!(
        e.added_starts,
        vec![at("2024-03-20T04:00:00Z"), at("2024-03-21T12:00:00Z")],
        "a date is read in the configured zone; a period counts by its start"
    );
    assert_eq!(e.local_zone.as_deref(), Some("America/New_York"));
}

#[devkit::test]
fn a_changed_instance_names_the_start_it_replaces() {
    let ics = "BEGIN:VEVENT\nUID:s\nRECURRENCE-ID;TZID=Europe/Warsaw:20240331T090000\nDTSTART;TZID=Europe/Warsaw:20240331T113000\nDTEND;TZID=Europe/Warsaw:20240331T123000\nSUMMARY:Moved\nEND:VEVENT\n";
    let e = read_in(ics, "UTC");
    assert_eq!(e.recurrence_id.as_deref(), Some("20240331T090000"));
    // 31 March 2024 is the spring change: 09:00 is already summer time.
    assert_eq!(e.recurrence_start, Some(at("2024-03-31T07:00:00Z")));
    assert_eq!(e.start, Some(at("2024-03-31T09:30:00Z")));
}

#[devkit::test]
fn a_time_in_the_spring_gap_moves_forward() {
    let e = read_in(
        "BEGIN:VEVENT\nUID:g\nDTSTART;TZID=Europe/Warsaw:20240331T023000\nEND:VEVENT\n",
        "UTC",
    );
    assert_eq!(e.start, Some(at("2024-03-31T01:30:00Z")));
}

#[devkit::test]
fn an_unknown_zone_is_a_record_failure() {
    let ev = <VEvent as RecordShape>::decode(
        b"BEGIN:VEVENT\nUID:z\nDTSTART;TZID=Nowhere/Else:20240301T090000\nEND:VEVENT\n",
    )
    .unwrap();
    let parser = CalendarParser {
        zone: jiff::tz::TimeZone::UTC,
    };
    let error = parser
        .parse(&ev, &(), &crate::RecordContext::none())
        .unwrap_err();
    assert!(error.to_string().contains("unknown TZID"), "{error}");
}
