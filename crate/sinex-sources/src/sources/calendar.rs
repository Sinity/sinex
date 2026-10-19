//! Calendars (RFC 5545 `.ics`), on `ExportArchive`: a Google Takeout
//! `Calendar/*.ics` member or any `.ics` export. One record per `VEVENT`, so
//! each event has its own record location. The calendar is named by its file.
//!
//! Times keep their meaning: a `TZID` is read in that zone, a trailing `Z` is
//! UTC, a time with neither is floating and read in the configured zone (the
//! event records which), and a date is a whole day. `DTSTAMP` is when the
//! export was written, so it is left out of the event and of the record hash,
//! or every export would revise every event.
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::{Parser, RecordContext, RecordError, RecordShape};
use serde_json::{Map, Value};
use sinex_model::kinds::account::CalendarEvent;
use sinex_model::{Class, Timestamp};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "calendar",
    driver: Driver::ExportArchive,
    consumer: "planned events beside what happened",
    class: Class::Open,
    parser: "icalendar",
    parser_version: 1,
    event_types: &["calendar.event"],
    framer: frame,
};

const EVENT: &[u8] = b"BEGIN:VEVENT";

/// One `VEVENT`: its properties in order, lines unfolded.
#[derive(Clone, Debug, PartialEq)]
pub struct VEvent {
    pub props: Vec<Prop>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Prop {
    pub name: String,
    pub params: Vec<(String, String)>,
    pub value: String,
}

impl Prop {
    fn param(&self, name: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

impl<'a> RecordShape<'a> for VEvent {
    fn decode(raw: &'a [u8]) -> Result<Self, RecordError> {
        let text = std::str::from_utf8(raw)
            .map_err(|e| RecordError::new(format!("calendar is not UTF-8: {e}")))?;
        let mut props = Vec::new();
        let mut inside = false;
        let mut depth = 0usize;
        for line in unfold(text) {
            let Some(prop) = prop(&line) else { continue };
            match (prop.name.as_str(), prop.value.as_str()) {
                ("BEGIN", "VEVENT") if !inside => inside = true,
                ("END", "VEVENT") if inside && depth == 0 => break,
                // A nested component (a `VALARM`) is not the event's own.
                ("BEGIN", _) if inside => depth += 1,
                ("END", _) if inside && depth > 0 => depth -= 1,
                _ if inside && depth == 0 => props.push(prop),
                _ => {}
            }
        }
        if !inside {
            return Err(RecordError::new("no VEVENT in the record"));
        }
        Ok(VEvent { props })
    }

    fn hash(raw: &[u8], _volatile: &[&str]) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        let text = String::from_utf8_lossy(raw);
        for line in unfold(&text) {
            if !line.starts_with("DTSTAMP") {
                hasher.update(line.as_bytes());
                hasher.update(b"\n");
            }
        }
        *hasher.finalize().as_bytes()
    }
}

/// Logical lines: a line starting with a space or tab continues the one
/// before it.
fn unfold(text: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for raw in text.split('\n') {
        let raw = raw.strip_suffix('\r').unwrap_or(raw);
        match raw.strip_prefix([' ', '\t']) {
            Some(cont) if !out.is_empty() => out.last_mut().expect("checked").push_str(cont),
            _ => out.push(raw.to_owned()),
        }
    }
    out
}

/// `NAME;PARAM=value;PARAM="quoted":value`.
fn prop(line: &str) -> Option<Prop> {
    let mut in_quotes = false;
    let colon = line.char_indices().find_map(|(i, c)| match c {
        '"' => {
            in_quotes = !in_quotes;
            None
        }
        ':' if !in_quotes => Some(i),
        _ => None,
    })?;
    let (head, value) = (&line[..colon], &line[colon + 1..]);
    let mut parts = head.split(';');
    let name = parts.next()?.trim().to_ascii_uppercase();
    if name.is_empty() {
        return None;
    }
    let params = parts
        .filter_map(|p| {
            let (k, v) = p.split_once('=')?;
            Some((k.to_ascii_uppercase(), v.trim_matches('"').to_owned()))
        })
        .collect();
    Some(Prop {
        name,
        params,
        value: value.to_owned(),
    })
}

/// Text values escape `\n`, `\,`, `\;` and `\\`.
fn unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n' | 'N') => out.push('\n'),
                Some(other) => out.push(other),
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

pub struct CalendarParser {
    /// The zone floating times and whole days are read in.
    pub zone: jiff::tz::TimeZone,
}

/// A date-time property read as an instant: `(instant, zone label, whole day)`.
type Moment = (Timestamp, String, bool);

impl CalendarParser {
    fn moment(&self, prop: &Prop) -> Result<Moment, RecordError> {
        let raw = prop.value.trim();
        let date_only = prop.param("VALUE") == Some("DATE") || (raw.len() == 8 && !raw.contains('T'));
        let (civil, zone, label) = if date_only {
            let d: jiff::civil::Date = jiff::civil::Date::strptime("%Y%m%d", raw)
                .map_err(|e| RecordError::new(format!("{}: invalid date {raw:?}: {e}", prop.name)))?;
            (d.to_datetime(jiff::civil::Time::midnight()), self.zone.clone(), "date".to_owned())
        } else {
            let (body, utc) = match raw.strip_suffix('Z') {
                Some(b) => (b, true),
                None => (raw, false),
            };
            let dt = jiff::civil::DateTime::strptime("%Y%m%dT%H%M%S", body).map_err(|e| {
                RecordError::new(format!("{}: invalid date-time {raw:?}: {e}", prop.name))
            })?;
            if utc {
                (dt, jiff::tz::TimeZone::UTC, "UTC".to_owned())
            } else if let Some(tzid) = prop.param("TZID") {
                let tz = jiff::tz::TimeZone::get(tzid).map_err(|e| {
                    RecordError::new(format!("{}: unknown TZID {tzid:?}: {e}", prop.name))
                })?;
                (dt, tz, tzid.to_owned())
            } else {
                (dt, self.zone.clone(), "floating".to_owned())
            }
        };
        let zoned = zone
            .to_ambiguous_zoned(civil)
            .compatible()
            .map_err(|e| RecordError::new(format!("{}: {raw:?}: {e}", prop.name)))?;
        let at = Timestamp::from_unix_ns(zoned.timestamp().as_nanosecond())
            .map_err(|_| RecordError::new(format!("{}: {raw:?} out of range", prop.name)))?;
        Ok((at, label, date_only))
    }

    /// A timestamp property, or `None` for Google's year-1 placeholder.
    fn stamp(&self, prop: Option<&Prop>) -> Result<Option<Timestamp>, RecordError> {
        match prop {
            Some(p) if p.value.starts_with("0001") || p.value.starts_with("0000") => Ok(None),
            Some(p) => self.moment(p).map(|(t, _, _)| Some(t)),
            None => Ok(None),
        }
    }
}

/// Properties read into fields; the rest go to `extra`, as written.
const KNOWN: &[&str] = &[
    "UID",
    "RECURRENCE-ID",
    "SUMMARY",
    "DESCRIPTION",
    "LOCATION",
    "DTSTART",
    "DTEND",
    "DURATION",
    "STATUS",
    "RRULE",
    "EXDATE",
    "ORGANIZER",
    "ATTENDEE",
    "CREATED",
    "LAST-MODIFIED",
    "SEQUENCE",
    "DTSTAMP",
];

impl Parser for CalendarParser {
    type Record<'a> = VEvent;
    type Carry = ();
    type Payload = CalendarEvent;

    fn parse(&self, _: &VEvent, _: &()) -> Result<(Vec<CalendarEvent>, ()), RecordError> {
        Err(RecordError::new("a calendar event needs its file's name"))
    }

    fn parse_record(
        &self,
        ev: &VEvent,
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<CalendarEvent>, ()), RecordError> {
        let calendar = context
            .path()
            .map(|p| {
                let name = p.rsplit('/').next().unwrap_or(p);
                name.strip_suffix(".ics").unwrap_or(name).to_owned()
            })
            .unwrap_or_else(|| "calendar".into());
        let get = |name: &str| ev.props.iter().find(|p| p.name == name);
        let all = |name: &'static str| ev.props.iter().filter(move |p| p.name == name);
        let text = |name: &str| {
            get(name)
                .map(|p| unescape(&p.value))
                .filter(|v| !v.is_empty())
        };
        let uid = text("UID").ok_or_else(|| RecordError::new("VEVENT without a UID"))?;
        let start = get("DTSTART").map(|p| self.moment(p)).transpose()?;
        let end = match (get("DTEND"), get("DURATION"), &start) {
            (Some(p), _, _) => Some(self.moment(p)?.0),
            (None, Some(d), Some((s, _, _))) => Some(add_duration(*s, &d.value)?),
            _ => None,
        };
        let status = text("STATUS");
        let last_modified = self.stamp(get("LAST-MODIFIED"))?;
        let mut extra = Map::new();
        for p in ev.props.iter().filter(|p| !KNOWN.contains(&p.name.as_str())) {
            let entry = extra
                .entry(p.name.clone())
                .or_insert_with(|| Value::Array(vec![]));
            if let Value::Array(a) = entry {
                a.push(Value::String(p.value.clone()));
            }
        }
        Ok((
            vec![CalendarEvent {
                calendar,
                uid,
                recurrence_id: get("RECURRENCE-ID").map(|p| p.value.clone()),
                summary: text("SUMMARY"),
                description: text("DESCRIPTION"),
                location: text("LOCATION"),
                start: start.as_ref().map(|(t, _, _)| *t),
                end,
                all_day: start.as_ref().is_some_and(|(_, _, d)| *d),
                zone: start.map_or_else(|| "unknown".into(), |(_, z, _)| z),
                cancelled_at: if status.as_deref() == Some("CANCELLED") {
                    last_modified.or_else(|| Timestamp::from_unix_secs(0).ok())
                } else {
                    None
                },
                status,
                rrule: get("RRULE").map(|p| p.value.clone()),
                exdates: all("EXDATE").map(|p| p.value.clone()).collect(),
                organizer: get("ORGANIZER").map(|p| p.value.clone()),
                attendees: all("ATTENDEE").map(|p| p.value.clone()).collect(),
                created_at: self.stamp(get("CREATED"))?,
                last_modified,
                sequence: get("SEQUENCE").and_then(|p| p.value.trim().parse().ok()),
                extra,
            }],
            (),
        ))
    }
}

/// `start` plus an RFC 5545 duration (`PT1H30M`, `P1D`, `P2W`).
fn add_duration(start: Timestamp, raw: &str) -> Result<Timestamp, RecordError> {
    let bad = || RecordError::new(format!("invalid DURATION {raw:?}"));
    let (negative, body) = match raw.strip_prefix('-') {
        Some(b) => (true, b),
        None => (false, raw.strip_prefix('+').unwrap_or(raw)),
    };
    let body = body.strip_prefix('P').ok_or_else(bad)?;
    let mut secs: i64 = 0;
    let mut number = String::new();
    for c in body.chars() {
        match c {
            '0'..='9' => number.push(c),
            'T' => {}
            unit => {
                let n: i64 = number.parse().map_err(|_| bad())?;
                number.clear();
                secs += n * match unit {
                    'W' => 604_800,
                    'D' => 86_400,
                    'H' => 3_600,
                    'M' => 60,
                    'S' => 1,
                    _ => return Err(bad()),
                };
            }
        }
    }
    let delta = i128::from(if negative { -secs } else { secs }) * 1_000_000_000;
    Timestamp::from_unix_ns(start.unix_ns() + delta).map_err(|_| bad())
}

pub fn source(zone: jiff::tz::TimeZone) -> Declared<CalendarParser> {
    Declared {
        spec: SPEC,
        parser: CalendarParser { zone },
    }
}

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Delimited(EVENT).frame(bytes, true)
}

pub fn select(member: &str) -> bool {
    member.ends_with(".ics")
}

pub fn driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select,
        framing: Framing::Delimited(EVENT),
        part_bytes: PART_BYTES,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const ICS: &str = "BEGIN:VEVENT\r\nDTSTART;TZID=Europe/Warsaw:20110128T150000\r\nDTEND;TZID=Europe/Warsaw:20110128T160000\r\nRRULE:FREQ=WEEKLY;COUNT=100;BYDAY=SU,FR\r\nDTSTAMP:20260508T144252Z\r\nUID:a1@google.com\r\nATTENDEE;CUTYPE=INDIVIDUAL;CN=\"A, B\";X-NUM-GUE\r\n STS=0:mailto:a@example.org\r\nCREATED:00010101T000000Z\r\nDESCRIPTION:line one\\nline\\, two\r\nLAST-MODIFIED:20221220T050634Z\r\nSEQUENCE:2\r\nSTATUS:CONFIRMED\r\nSUMMARY:Reading\r\nX-GOOGLE-CONFERENCE:https://meet.example\r\nBEGIN:VALARM\r\nACTION:DISPLAY\r\nEND:VALARM\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";

    fn read(ics: &str) -> CalendarEvent {
        let ev = <VEvent as RecordShape>::decode(ics.as_bytes()).unwrap();
        let manifest = json!({"path": "Takeout/Calendar/me@example.org.ics"});
        let ctx = RecordContext { manifest: &manifest, member: None };
        let parser = CalendarParser { zone: jiff::tz::TimeZone::UTC };
        parser.parse_record(&ev, &(), &ctx).unwrap().0.remove(0)
    }

    #[test]
    fn reads_a_recurring_zoned_event() {
        let e = read(ICS);
        assert_eq!(e.calendar, "me@example.org");
        assert_eq!(e.uid, "a1@google.com");
        assert_eq!(e.zone, "Europe/Warsaw");
        assert_eq!(e.start, Timestamp::from_unix_secs(1_296_223_200).ok(), "15:00 CET is 14:00 UTC");
        assert_eq!(e.end.unwrap().unix_ns() - e.start.unwrap().unix_ns(), 3_600_000_000_000);
        assert_eq!(e.rrule.as_deref(), Some("FREQ=WEEKLY;COUNT=100;BYDAY=SU,FR"));
        assert_eq!(e.description.as_deref(), Some("line one\nline, two"));
        assert_eq!(e.attendees, vec!["mailto:a@example.org"]);
        assert_eq!(e.created_at, None, "year 1 is Google's placeholder");
        assert!(e.extra.contains_key("X-GOOGLE-CONFERENCE"));
        assert!(!e.extra.contains_key("ACTION"), "the alarm is not the event's own");
        assert!(!e.extra.contains_key("DTSTAMP"));
    }

    #[test]
    fn whole_days_floating_times_durations_and_cancellations() {
        let ics = "BEGIN:VEVENT\nUID:d\nDTSTART;VALUE=DATE:20240301\nDURATION:P1D\nSTATUS:CANCELLED\nLAST-MODIFIED:20240302T000000Z\nEND:VEVENT\n";
        let e = read(ics);
        assert!(e.all_day);
        assert_eq!(e.zone, "date");
        assert_eq!(e.end.unwrap().unix_ns() - e.start.unwrap().unix_ns(), 86_400_000_000_000);
        assert!(e.cancelled_at.is_some());
        let floating = read("BEGIN:VEVENT\nUID:f\nDTSTART:20240301T090000\nEND:VEVENT\n");
        assert_eq!(floating.zone, "floating");
    }

    #[test]
    fn the_export_stamp_does_not_revise_an_event() {
        let later = ICS.replace("DTSTAMP:20260508T144252Z", "DTSTAMP:20270101T000000Z");
        assert_eq!(
            <VEvent as RecordShape>::hash(ICS.as_bytes(), &[]),
            <VEvent as RecordShape>::hash(later.as_bytes(), &[])
        );
    }

    #[test]
    fn frames_one_record_per_event() {
        let doc = format!("BEGIN:VCALENDAR\nX-WR-CALNAME:Me\n{ICS}{}", ICS.replace("a1@", "a2@"));
        let (records, _) = frame(doc.as_bytes());
        assert_eq!(records.len(), 2);
    }
}
