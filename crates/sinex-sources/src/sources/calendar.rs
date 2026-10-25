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
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordContext, RecordError, RecordShape};
use serde_json::{Map, Value};
use sinex_model::event_types::account::CalendarEvent;
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
    framer: crate::Framer::export(
        frame,
        crate::drivers::export_archive::Framing::Delimited(EVENT),
    ),
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
        let date_only =
            prop.param("VALUE") == Some("DATE") || (raw.len() == 8 && !raw.contains('T'));
        let (civil, zone, label) = if date_only {
            let d: jiff::civil::Date = jiff::civil::Date::strptime("%Y%m%d", raw).map_err(|e| {
                RecordError::new(format!("{}: invalid date {raw:?}: {e}", prop.name))
            })?;
            (
                d.to_datetime(jiff::civil::Time::midnight()),
                self.zone.clone(),
                "date".to_owned(),
            )
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

    fn parse(
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
        for p in ev
            .props
            .iter()
            .filter(|p| !KNOWN.contains(&p.name.as_str()))
        {
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

pub fn source(zone: jiff::tz::TimeZone) -> DeclaredSource<CalendarParser> {
    DeclaredSource {
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
        join: None,
    }
}

/// `--source calendar=PATH`: each calendar export in PATH. Floating times are
/// read in the binding's zone, recorded when it is planned.
pub const KIND: crate::binding::InputKind = crate::binding::InputKind {
    name: "calendar",
    specs: || vec![&SPEC],
    plan: Some(|input, settings| {
        Ok(crate::binding::per_file(
            &SPEC,
            input,
            &settings.clone().with_zone()?,
            false,
            crate::binding::any,
        ))
    }),
    source: |b| Ok(Box::new(source(b.settings.zone()?))),
    reader: Some(|b| {
        Ok(crate::binding::stateless(driver(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;
