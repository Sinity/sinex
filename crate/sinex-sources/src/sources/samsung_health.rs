//! Samsung Health in-app export (`com.samsung.<health|shealth>.<type>.<stamp>.csv`,
//! as received in the export's zip), on `ExportArchive`
//! (`spec/port-lynchpin-sources.md` §10.1). Line 1 of each file names the
//! type, line 2 is the header (names may carry the type prefix, stripped to
//! the last dotted part), then rows. Every row has a `datauuid`.
//!
//! Each row becomes one `health.record` whose category is the type and whose
//! values are every column, typed as numbers where they are; splitting types
//! into series samples is a derivation. Times are UTC text
//! (`YYYY-MM-DD HH:MM:SS.fff`), and `time_offset` (`UTC+0200`) is the local
//! offset at the record. Companion `jsons/` files (binned samples) are kept in
//! the material, not read here.
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::sources::fields;
use crate::{Parser, RecordContext, RecordError};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sinex_model::kinds::health::HealthRecord;
use sinex_model::{Class, Timestamp};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "health.samsung-export",
    driver: Driver::ExportArchive,
    consumer: "sleep, heart, activity and body measurements",
    class: Class::Open,
    parser: "samsung-health-export",
    parser_version: 1,
    event_types: &["health.record"],
    framer: frame,
};

/// The member being read, its type (line 1) and header (line 2).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Carry {
    pub member: String,
    pub category: String,
    pub header: Vec<String>,
}

pub struct SamsungParser;

impl Parser for SamsungParser {
    type Record<'a> = &'a [u8];
    type Carry = Carry;
    type Payload = HealthRecord;

    fn parse(&self, _: &&[u8], _: &Carry) -> Result<(Vec<HealthRecord>, Carry), RecordError> {
        Err(RecordError::new(
            "a Samsung Health row needs its file's name",
        ))
    }

    fn parse_record(
        &self,
        record: &&[u8],
        carry: &Carry,
        context: &RecordContext<'_>,
    ) -> Result<(Vec<HealthRecord>, Carry), RecordError> {
        let member = context.path().unwrap_or_default().to_owned();
        let cells = fields::csv_cells(record)?;
        if carry.member != member {
            // Line 1: `com.samsung.health.hrv,7006003,1`.
            let category = cells
                .first()
                .map(|t| category_of(t))
                .ok_or_else(|| RecordError::new("Samsung Health file without its type line"))?;
            return Ok((
                vec![],
                Carry {
                    member,
                    category,
                    header: vec![],
                },
            ));
        }
        if carry.header.is_empty() {
            let header = cells
                .iter()
                .map(|h| h.rsplit('.').next().unwrap_or(h).to_owned())
                .collect();
            return Ok((
                vec![],
                Carry {
                    header,
                    ..carry.clone()
                },
            ));
        }
        let mut values = Map::new();
        for (name, cell) in carry.header.iter().zip(&cells) {
            if !cell.is_empty() {
                values.insert(name.clone(), typed(cell));
            }
        }
        let native_id = values
            .get("datauuid")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| {
                RecordError::new(format!("{} row without a datauuid", carry.category))
            })?;
        let time = |name: &str| -> Result<Option<Timestamp>, RecordError> {
            values
                .get(name)
                .and_then(Value::as_str)
                .map(utc)
                .transpose()
        };
        let start = match time("start_time")? {
            Some(t) => Some(t),
            None => time("day_time")?.or(time("create_time")?),
        };
        let end = time("end_time")?;
        let offset_minutes = values
            .get("time_offset")
            .and_then(Value::as_str)
            .and_then(offset);
        Ok((
            vec![HealthRecord {
                provider: "samsung-health".into(),
                category: carry.category.clone(),
                native_id,
                start,
                end,
                offset_minutes,
                values,
                events: vec![],
            }],
            carry.clone(),
        ))
    }
}

/// `com.samsung.shealth.tracker.heart_rate` → `tracker.heart_rate`.
fn category_of(type_line: &str) -> String {
    let t = type_line.trim_start_matches('\u{feff}').trim();
    t.strip_prefix("com.samsung.shealth.")
        .or_else(|| t.strip_prefix("com.samsung.health."))
        .unwrap_or(t)
        .to_owned()
}

/// A number where the cell is one, text otherwise. A zero-padded cell
/// (`0042`) is an identifier and stays text.
pub(crate) fn typed(cell: &str) -> Value {
    let digits = cell.trim_start_matches('-');
    let padded = digits.len() > 1 && digits.starts_with('0') && !digits.starts_with("0.");
    if !padded {
        if let Ok(i) = cell.parse::<i64>() {
            return json!(i);
        }
        if let Ok(f) = cell.parse::<f64>()
            && f.is_finite()
        {
            return json!(f);
        }
    }
    Value::String(cell.to_owned())
}

/// `2025-05-21 04:00:00.000`, UTC.
fn utc(raw: &str) -> Result<Timestamp, RecordError> {
    let civil = jiff::civil::DateTime::strptime("%Y-%m-%d %H:%M:%S%.f", raw.trim())
        .or_else(|_| jiff::civil::DateTime::strptime("%Y-%m-%d %H:%M:%S", raw.trim()))
        .map_err(|e| RecordError::new(format!("invalid time {raw:?}: {e}")))?;
    let z = civil
        .to_zoned(jiff::tz::TimeZone::UTC)
        .map_err(|e| RecordError::new(e.to_string()))?;
    Timestamp::from_unix_ns(z.timestamp().as_nanosecond())
        .map_err(|_| RecordError::new("time out of range"))
}

/// `UTC+0200` → 120.
fn offset(raw: &str) -> Option<i32> {
    let body = raw.trim().strip_prefix("UTC")?;
    let (sign, digits) = match body.as_bytes().first()? {
        b'+' => (1, &body[1..]),
        b'-' => (-1, &body[1..]),
        _ => return None,
    };
    if digits.len() != 4 {
        return None;
    }
    let h: i32 = digits[..2].parse().ok()?;
    let m: i32 = digits[2..].parse().ok()?;
    Some(sign * (h * 60 + m))
}

pub fn source() -> Declared<SamsungParser> {
    Declared {
        spec: SPEC,
        parser: SamsungParser,
    }
}

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Csv.frame(bytes, true)
}

pub fn select(member: &str) -> bool {
    let name = member.rsplit('/').next().unwrap_or(member);
    name.starts_with("com.samsung.") && name.ends_with(".csv") && !member.contains("/jsons/")
}

pub fn driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select,
        framing: Framing::Csv,
        part_bytes: PART_BYTES,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_line_header_and_rows() {
        let manifest = json!({"path": "com.samsung.health.hrv.20260807162074.csv"});
        let ctx = RecordContext {
            manifest: &manifest,
            member: None,
        };
        let (_, c) = SamsungParser
            .parse_record(
                &&b"\xef\xbb\xbfcom.samsung.health.hrv,7006003,1\n"[..],
                &Carry::default(),
                &ctx,
            )
            .unwrap();
        assert_eq!(c.category, "hrv");
        let (_, c) = SamsungParser
            .parse_record(
                &&b"com.samsung.health.hrv.start_time,time_offset,datauuid,end_time,score\n"[..],
                &c,
                &ctx,
            )
            .unwrap();
        assert_eq!(c.header[0], "start_time");
        let (out, _) = SamsungParser
            .parse_record(
                &&b"2025-05-21 04:00:00.000,UTC+0200,a291,2025-05-21 05:00:00.000,7\n"[..],
                &c,
                &ctx,
            )
            .unwrap();
        let r = &out[0];
        assert_eq!(r.native_id, "a291");
        assert_eq!(
            r.start,
            fields::rfc3339("2025-05-21T04:00:00Z").ok(),
            "the text is UTC"
        );
        assert_eq!(r.offset_minutes, Some(120));
        assert_eq!(r.values["score"], json!(7));
    }

    #[test]
    fn categories_and_values() {
        assert_eq!(
            category_of("com.samsung.shealth.tracker.heart_rate"),
            "tracker.heart_rate"
        );
        assert_eq!(typed("12"), json!(12));
        assert_eq!(typed("0.5"), json!(0.5));
        assert_eq!(typed("gOU20c3N2O"), json!("gOU20c3N2O"));
        assert_eq!(typed("0042"), json!("0042"), "zero-padded stays text");
        assert_eq!(typed("0"), json!(0));
        assert_eq!(typed("-1.5"), json!(-1.5));
        assert_eq!(offset("UTC-0130"), Some(-90));
    }
}
