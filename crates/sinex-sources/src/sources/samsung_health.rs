//! Samsung Health in-app export
//! (`com.samsung.<health|shealth>.<type>.<stamp>.csv`, as received in the
//! export's zip), on `ExportArchive`. Line 1 of each file names the type, line
//! 2 is the header (names may carry the type prefix, stripped to the last
//! dotted part), then rows. Every row has a `datauuid`.
//!
//! Each row becomes one `health.record` whose category is the type and whose
//! values are every column, typed as numbers where they are; splitting types
//! into series samples is a derivation. Times are UTC text
//! (`YYYY-MM-DD HH:MM:SS.fff`), and `time_offset` (`UTC+0200`) is the local
//! offset at the record.
//!
//! A row's larger columns are separate `jsons/<type>/<x>/<datauuid>.<column>.json`
//! members (binned heart-rate or stress samples, sleep stages, daily
//! achievements). A second binding of the same export reads them: an array
//! member gives one `health.record` per element (one bin) and an object member
//! one record, each of category `<type>.<column>` and identified by the row's
//! `datauuid`, the column and the element's position.
use crate::sources::fields;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sinex_model::Timestamp;
use sinex_model::event_types::health::HealthRecord;
use sinex_model::json::JsonExt;
use sinex_source_core::discovery::{DiscoverySpec, Location};
use sinex_source_core::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use sinex_source_core::registry::{DeclaredSource, Expectation, Origin, Shape, SourceSpec};
use sinex_source_core::stage::Structure;
use sinex_source_core::{
    Framer, Parser, RecordContext, RecordError, RecordResult, Stateless, StatelessParser, binding,
};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "health.samsung-export",
    origin: Origin::Input(Shape::ExportArchive),
    expect: Expectation::Export,
    purpose: "sleep, heart, activity and body measurements",
    parser: "samsung-health-export",
    parser_version: 2,
    event_types: &["health.record"],
    framer: Framer::export(frame, Framing::Csv),
    ..SourceSpec::DEFAULT
};

/// The `jsons/` members: the columns Samsung stores beside a row.
pub const ATTACHMENTS: SourceSpec = SourceSpec {
    name: "health.samsung-export",
    origin: Origin::Input(Shape::ExportArchive),
    expect: Expectation::Export,
    purpose: "sleep, heart, activity and body measurements",
    parser: "samsung-health-attachments",
    event_types: &["health.record"],
    framer: Framer::export(whole, Framing::Whole),
    ..SourceSpec::DEFAULT
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

    fn parse(
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
                .record("Samsung Health file without its type line")?;
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
            .str("datauuid")
            .map(str::to_owned)
            .record_with(|| format!("{} row without a datauuid", carry.category))?;
        let time = |name: &str| -> Result<Option<Timestamp>, RecordError> {
            values.str(name).map(utc).transpose()
        };
        let start = match time("start_time")? {
            Some(t) => Some(t),
            None => time("day_time")?.or(time("create_time")?),
        };
        let end = time("end_time")?;
        let offset_minutes = values.str("time_offset").and_then(offset);
        Ok((
            vec![
                HealthRecord::new(
                    "samsung-health".into(),
                    carry.category.clone(),
                    native_id,
                    start,
                    end,
                    offset_minutes,
                    values,
                    vec![],
                )
                .record("invalid health record")?,
            ],
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
        .record_with(|| format!("invalid time {raw:?}"))?;
    let z = civil
        .to_zoned(jiff::tz::TimeZone::UTC)
        .record("invalid time")?;
    Timestamp::from_unix_ns(z.timestamp().as_nanosecond()).record("time out of range")
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

/// Reads one `jsons/<type>/<x>/<datauuid>.<column>.json` member.
pub struct AttachmentParser;

impl StatelessParser for AttachmentParser {
    type Record<'a> = &'a [u8];
    type Payload = HealthRecord;

    fn parse(
        &self,
        record: &&[u8],
        context: &RecordContext<'_>,
    ) -> Result<Vec<HealthRecord>, RecordError> {
        let path = context.path().unwrap_or_default();
        let (category, row, column) = attachment_of(path).record_with(|| {
            format!("{path}: not a jsons/<type>/<x>/<datauuid>.<column>.json member")
        })?;
        let value: Value = serde_json::from_slice(record).record_with(|| format!("{path}"))?;
        let category = format!("{category}.{column}");
        let millis = |v: &Map<String, Value>, name: &str| {
            v.i64(name)
                .and_then(|ms| Timestamp::from_unix_ns(i128::from(ms) * 1_000_000).ok())
        };
        let record_of = |native_id: String, value: Value| {
            let values = match value {
                Value::Object(map) => map,
                other => Map::from_iter([("value".to_owned(), other)]),
            };
            HealthRecord::new(
                "samsung-health".into(),
                category.clone(),
                native_id,
                millis(&values, "start_time"),
                millis(&values, "end_time"),
                None,
                values,
                vec![],
            )
            .record("invalid health record")
        };
        let records = match value {
            Value::Array(items) => items
                .into_iter()
                .enumerate()
                .map(|(i, item)| record_of(format!("{row}:{column}:{i}"), item))
                .collect::<Result<_, _>>()?,
            other => vec![record_of(format!("{row}:{column}"), other)?],
        };
        Ok(records)
    }
}

/// `jsons/com.samsung.shealth.stress/0/<datauuid>.binning_data.json` →
/// (`stress`, `<datauuid>`, `binning_data`). The column may carry the type
/// prefix (`com.samsung.health.heart_rate.binning_data`), stripped to its last
/// dotted part as in the CSV header.
fn attachment_of(path: &str) -> Option<(String, &str, &str)> {
    let mut parts = path.rsplit('/');
    let file = parts.next()?.strip_suffix(".json")?;
    parts.next()?;
    let category = category_of(parts.next()?);
    let (row, column) = file.split_once('.')?;
    Some((category, row, column.rsplit('.').next().unwrap_or(column)))
}

pub fn whole(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Whole.frame(bytes, true)
}

pub fn select_attachments(member: &str) -> bool {
    member.contains("jsons/") && member.ends_with(".json")
}

pub fn attachments_driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select: select_attachments,
        framing: Framing::Whole,
        part_bytes: PART_BYTES,
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

/// `stage --parser samsung-health PATH`: each Samsung Health export ZIP under PATH.
super::input_format! { pub FORMAT = "samsung-health" {
    specs: [SPEC, ATTACHMENTS],
    source: |b| {
        if b.parser == ATTACHMENTS.parser {
            Ok(Box::new(DeclaredSource {
                spec: ATTACHMENTS,
                parser: Stateless(AttachmentParser),
            }))
        } else {
            Ok(Box::new(DeclaredSource {
                spec: SPEC,
                parser: SamsungParser,
            }))
        }
    },
    discovery: DiscoverySpec::system(&[Location::new("input", "samsung-health")]),
    accepts: [Structure::ArchiveLayout(select)],
    plan: |input, settings| {
        let zip = |p: &std::path::Path| p.extension().is_some_and(|e| e == "zip");
        let mut out = binding::per_file(&SPEC, input, settings, true, zip);
        out.extend(
            binding::per_file(&ATTACHMENTS, input, settings, true, zip)
                .into_iter()
                .map(|b| b.named("attachments")),
        );
        Ok(out)
    },
    reader: |b| {
        let read = if b.parser == ATTACHMENTS.parser {
            attachments_driver
        } else {
            driver
        };
        Ok(binding::shared(read(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    },
} }

#[cfg(test)]
mod tests;
