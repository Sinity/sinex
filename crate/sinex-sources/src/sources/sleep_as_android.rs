//! Sleep as Android export (`sleep-as-android.csv`), on `ExportArchive`
//! (`spec/port-lynchpin-sources.md` §10.3). Each session is a header row
//! starting `Id,` and a data row; the first 15 columns are fixed and the rest
//! are the session's own (`Event` markers such as `DEEP_START-<ms>` and
//! `HR-<ms>-<bpm>`, or actigraphy samples headed by a time of day).
//!
//! `Id` is the session start in epoch milliseconds; `From` and `To` are naive
//! local times. The `Tz` column is unreliable (IANA names beside POSIX-inverted
//! `GMT-02:00`), so the offset is derived from `From` and `Id`, rounded to a
//! quarter hour, and recorded. `-1` in `Snore`, `Noise`, `Cycles` and
//! `DeepSleep` means absent.
//!
//! A session row can be followed by a row whose first 13 cells are empty: the
//! session's sound-level (noise) samples, not aligned to the header's times.
//! It becomes a `sleep-noise` record of the session before it.
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::sources::fields;
use crate::{Parser, RecordError};
use serde_json::{Map, Value, json};
use sinex_model::kinds::health::HealthRecord;
use sinex_model::{Class, Timestamp};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "health.sleep-as-android",
    driver: Driver::ExportArchive,
    consumer: "sleep sessions and their phases",
    class: Class::Open,
    parser: "sleep-as-android",
    parser_version: 2,
    event_types: &["health.record"],
    framer: frame,
};

pub struct SleepParser;

/// The header row of the current session, and the last session read under it
/// (its ID and span), which a following noise row belongs to.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Carry {
    header: Vec<String>,
    session: Option<(i64, Timestamp, Option<Timestamp>, i32)>,
}

impl Parser for SleepParser {
    type Record<'a> = &'a [u8];
    type Carry = Carry;
    type Payload = HealthRecord;

    fn parse(
        &self,
        record: &&[u8],
        carry: &Carry,
    ) -> Result<(Vec<HealthRecord>, Carry), RecordError> {
        let cells = fields::csv_cells(record)?;
        if cells.first().map(String::as_str) == Some("Id") {
            return Ok((
                vec![],
                Carry {
                    header: cells,
                    session: None,
                },
            ));
        }
        if cells.len() > 13 && cells[..13].iter().all(|c| c.trim().is_empty()) {
            return noise(&cells, carry);
        }
        let header = &carry.header;
        if header.len() < 15 {
            return Err(RecordError::new("a session row before its header row"));
        }
        let cell = |i: usize| cells.get(i).map(|c| c.trim()).unwrap_or("");
        let id_ms: i64 = cell(0)
            .parse()
            .map_err(|_| RecordError::new(format!("Id {:?} is not epoch milliseconds", cell(0))))?;
        let start = fields::from_unix_ms(id_ms)?;
        let from = naive(cell(2))?;
        // The local start written in From, read as if UTC, minus the true
        // start: the offset, rounded to a quarter hour.
        let quarter = 15 * 60;
        let diff = from.unix_ns() / 1_000_000_000 - i128::from(id_ms / 1000);
        let offset_secs = ((diff as f64) / f64::from(quarter)).round() as i64 * i64::from(quarter);
        let end = if cell(3).is_empty() {
            None
        } else {
            let to = naive(cell(3))?;
            Some(
                Timestamp::from_unix_ns(to.unix_ns() - i128::from(offset_secs) * 1_000_000_000)
                    .map_err(|_| RecordError::new("To out of range"))?,
            )
        };
        let mut values = Map::new();
        values.insert("tz_column".into(), json!(cell(1)));
        values.insert("scheduled".into(), json!(cell(4)));
        let num = |s: &str| s.parse::<f64>().ok();
        let present = |s: &str| num(s).filter(|v| *v >= 0.0);
        values.insert("hours".into(), json!(num(cell(5))));
        values.insert("rating".into(), json!(num(cell(6))));
        values.insert("comment".into(), json!(cell(7).trim()));
        values.insert("framerate".into(), json!(num(cell(8))));
        values.insert("snore".into(), json!(present(cell(9))));
        values.insert("noise".into(), json!(present(cell(10))));
        values.insert("cycles".into(), json!(present(cell(11))));
        values.insert("deep_sleep".into(), json!(present(cell(12))));
        values.insert("len_adjust".into(), json!(num(cell(13))));
        values.insert(
            "geo".into(),
            json!(Some(cell(14)).filter(|g| !g.is_empty())),
        );
        let mut events = Vec::new();
        let mut actigraphy = Vec::new();
        for (i, raw) in cells.iter().enumerate().skip(15) {
            let raw = raw.trim();
            if raw.is_empty() {
                continue;
            }
            match header.get(i).map(String::as_str) {
                Some("Event") => events.push(event(raw)?),
                label => actigraphy.push(json!([label, num(raw)])),
            }
        }
        if !actigraphy.is_empty() {
            values.insert("actigraphy".into(), Value::Array(actigraphy));
        }
        Ok((
            vec![HealthRecord {
                provider: "sleep-as-android".into(),
                category: "sleep".into(),
                native_id: id_ms.to_string(),
                start: Some(start),
                end,
                offset_minutes: Some((offset_secs / 60) as i32),
                values,
                events,
            }],
            Carry {
                header: header.clone(),
                session: Some((id_ms, start, end, (offset_secs / 60) as i32)),
            },
        ))
    }
}

/// The noise samples row after a session.
fn noise(cells: &[String], carry: &Carry) -> Result<(Vec<HealthRecord>, Carry), RecordError> {
    let (id_ms, start, end, offset) = carry
        .session
        .ok_or_else(|| RecordError::new("a noise row without a session before it"))?;
    let samples: Vec<Value> = cells[13..]
        .iter()
        .map(|c| c.trim())
        .filter(|c| !c.is_empty())
        .map(|c| {
            c.parse::<f64>()
                .map(|v| json!(v))
                .map_err(|_| RecordError::new(format!("noise sample {c:?} is not a number")))
        })
        .collect::<Result<_, _>>()?;
    let mut values = Map::new();
    values.insert("noise".into(), Value::Array(samples));
    Ok((
        vec![HealthRecord {
            provider: "sleep-as-android".into(),
            category: "sleep-noise".into(),
            native_id: format!("{id_ms}:noise"),
            start: Some(start),
            end,
            offset_minutes: Some(offset),
            values,
            events: vec![],
        }],
        Carry {
            header: carry.header.clone(),
            session: None,
        },
    ))
}

/// `D. M. YYYY HH:MM[:SS]` read as if it were UTC.
fn naive(raw: &str) -> Result<Timestamp, RecordError> {
    let bad = || RecordError::new(format!("invalid local time {raw:?}"));
    let (date, clock) = raw.rsplit_once(' ').ok_or_else(bad)?;
    let mut d = date.split('.').map(|p| p.trim().parse::<i32>());
    let (Some(Ok(day)), Some(Ok(month)), Some(Ok(year))) = (d.next(), d.next(), d.next()) else {
        return Err(bad());
    };
    let mut c = clock.split(':').map(str::parse::<u8>);
    let (Some(Ok(h)), Some(Ok(m))) = (c.next(), c.next()) else {
        return Err(bad());
    };
    let s = c.next().and_then(Result::ok).unwrap_or(0);
    let month = time::Month::try_from(month as u8).map_err(|_| bad())?;
    let date = time::Date::from_calendar_date(year, month, day as u8).map_err(|_| bad())?;
    let time = time::Time::from_hms(h, m, s).map_err(|_| bad())?;
    Ok(Timestamp(
        time::PrimitiveDateTime::new(date, time).assume_utc(),
    ))
}

/// `LIGHT_START-1759646940000` or `HR-1759647300000-88.0`.
fn event(raw: &str) -> Result<Value, RecordError> {
    let mut parts = raw.splitn(3, '-');
    let kind = parts.next().unwrap_or_default();
    let ms: i64 = parts
        .next()
        .and_then(|m| m.parse().ok())
        .ok_or_else(|| RecordError::new(format!("event {raw:?} has no time")))?;
    let at = fields::from_unix_ms(ms)?;
    let value = parts.next().and_then(|v| v.parse::<f64>().ok());
    Ok(json!({"kind": kind, "at": at, "value": value}))
}

pub fn source() -> Declared<SleepParser> {
    Declared {
        spec: SPEC,
        parser: SleepParser,
    }
}

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Csv.frame(bytes, true)
}

pub fn driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select: |m| m.ends_with(".csv"),
        framing: Framing::Csv,
        part_bytes: PART_BYTES,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: &[u8] = b"Id,Tz,From,To,Sched,Hours,Rating,Comment,Framerate,Snore,Noise,Cycles,DeepSleep,LenAdjust,Geo,\"Event\",\"Event\"\n";

    #[test]
    fn derives_the_offset_from_from_and_id_and_drops_sentinels() {
        let (_, header) = SleepParser.parse(&HEADER, &Carry::default()).unwrap();
        // 2025-10-05 02:49 UTC, written as 04:49 local: +2 h, whatever Tz says.
        let row = b"\"1759632540000\",\"GMT-02:00\",\"05. 10. 2025 4:49\",\"05. 10. 2025 6:53\",\"05. 10. 2025 6:53\",\"2.070\",\"0.0\",\" #cloud\",\"10008\",\"-1\",\"-1.0\",\"3\",\"0.48\",\"-40\",\"\",\"LIGHT_START-1759632540000\",\"HR-1759632900000-88.0\"\n";
        let (out, _) = SleepParser.parse(&&row[..], &header).unwrap();
        let r = &out[0];
        assert_eq!(r.offset_minutes, Some(120));
        assert_eq!(r.start, fields::from_unix_ms(1_759_632_540_000).ok());
        assert_eq!(
            r.end.unwrap().unix_ns() - r.start.unwrap().unix_ns(),
            (2 * 3600 + 4 * 60) as i128 * 1_000_000_000
        );
        assert_eq!(r.values["snore"], Value::Null, "-1 is absent");
        assert_eq!(r.values["cycles"], json!(3.0));
        assert_eq!(r.events.len(), 2);
        assert_eq!(r.events[1]["value"], json!(88.0));
    }

    #[test]
    fn a_row_without_its_header_is_an_error() {
        assert!(
            SleepParser
                .parse(&&b"\"1\",\"x\"\n"[..], &Carry::default())
                .is_err()
        );
    }

    #[test]
    fn a_noise_row_belongs_to_the_session_before_it() {
        let (_, header) = SleepParser.parse(&HEADER, &Carry::default()).unwrap();
        let row = b"\"1759632540000\",\"GMT-02:00\",\"05. 10. 2025 4:49\",\"05. 10. 2025 6:53\",\"05. 10. 2025 6:53\",\"2.070\",\"0.0\",\"\",\"10008\",\"-1\",\"-1.0\",\"3\",\"0.48\",\"-40\",\"\",\"\",\"\"\n";
        let (_, after) = SleepParser.parse(&&row[..], &header).unwrap();
        let noise = b",,,,,,,,,,,,,\"20643.4\",\"287.5\"\n";
        let (out, after) = SleepParser.parse(&&noise[..], &after).unwrap();
        assert_eq!(out[0].category, "sleep-noise");
        assert_eq!(out[0].native_id, "1759632540000:noise");
        assert_eq!(out[0].values["noise"], json!([20643.4, 287.5]));
        assert!(
            SleepParser.parse(&&noise[..], &after).is_err(),
            "only one per session"
        );
    }
}
