//! The operator's own records (`spec/port-lynchpin-sources.md` §12): the raw
//! log of quick notes and the steering store.
//!
//! The raw log (`raw-log.md`) is a Markdown list, one entry per item,
//! `- **YYYY-MM-DD HH:MM:SS** text`, local time, continued on following lines.
//! The operator edits it, so it is read on `AppendFile`, whose alignment keeps
//! an entry's place across rewrites. The steering store is a SQLite file of
//! commitments, activities and reviews, read on `SqliteRows`, one table per
//! binding; every row can change (a commitment's status), so each poll
//! compares them all.
use crate::drivers::append_file::AppendFile;
use crate::drivers::sqlite_rows::SqliteRows;
use crate::registry::{Declared, Driver, SourceSpec};
use crate::sources::fields;
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::kinds::operator::{LogEntry, SteeringRecord};
use sinex_model::{Class, Timestamp};
use std::path::PathBuf;

pub const RAW_LOG: SourceSpec = SourceSpec {
    name: "operator.raw-log",
    driver: Driver::AppendFile,
    consumer: "the operator's quick notes, as primary data",
    class: Class::Open,
    parser: "operator-raw-log",
    parser_version: 2,
    event_types: &["operator.log.entry"],
    framer: frame_log,
};

const ENTRY: &[u8] = b"\n- **";

/// One record per entry. Front matter and anything before the first entry is
/// not a record; the last entry is read up to its last complete line.
pub fn frame_log(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    let mut starts = Vec::new();
    if bytes.starts_with(&ENTRY[1..]) {
        starts.push(0);
    }
    let mut i = 0;
    while let Some(p) = bytes[i..].windows(ENTRY.len()).position(|w| w == ENTRY) {
        starts.push(i + p + 1);
        i += p + 1;
    }
    let complete = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |p| p + 1);
    let mut out: Vec<(usize, usize)> = starts.windows(2).map(|w| (w[0], w[1])).collect();
    match starts.last() {
        Some(&last) if complete > last => {
            out.push((last, complete));
            (out, complete)
        }
        Some(&last) => (out, last),
        None => (out, complete),
    }
}

pub struct RawLogParser {
    pub zone: jiff::tz::TimeZone,
}

impl Parser for RawLogParser {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = LogEntry;

    fn parse(&self, record: &&[u8], _: &()) -> Result<(Vec<LogEntry>, ()), RecordError> {
        let text = std::str::from_utf8(record)
            .map_err(|e| RecordError::new(format!("entry is not UTF-8: {e}")))?;
        let rest = text
            .strip_prefix("- **")
            .ok_or_else(|| RecordError::new("not a log entry"))?;
        let (stamp, body) = rest
            .split_once("**")
            .ok_or_else(|| RecordError::new("entry without a closing ** after its time"))?;
        // A heading that is a label or only a day (`SPECIAL:`, `2025-08-18`):
        // the entry is kept with an unknown time and the heading as its
        // label. A time without seconds reads as `:00`.
        let stamp = stamp.trim();
        let civil = jiff::civil::DateTime::strptime("%Y-%m-%d %H:%M:%S", stamp)
            .or_else(|_| jiff::civil::DateTime::strptime("%Y-%m-%d %H:%M", stamp));
        let Ok(civil) = civil else {
            let label = stamp.trim_end_matches(':').trim();
            if label.is_empty()
                || label.contains(' ') && label.starts_with(|c: char| c.is_ascii_digit())
            {
                return Err(RecordError::new(format!(
                    "entry heading {stamp:?} is not a time or a label"
                )));
            }
            return Ok((
                vec![LogEntry {
                    written_at: None,
                    label: Some(label.to_owned()),
                    text: body.trim().to_owned(),
                }],
                (),
            ));
        };
        let zoned = self
            .zone
            .to_ambiguous_zoned(civil)
            .compatible()
            .map_err(|e| RecordError::new(format!("{stamp:?}: {e}")))?;
        let written_at = Timestamp::from_unix_ns(zoned.timestamp().as_nanosecond())
            .map_err(|_| RecordError::new("time out of range"))?;
        Ok((
            vec![LogEntry {
                written_at: Some(written_at),
                label: None,
                text: body.trim().to_owned(),
            }],
            (),
        ))
    }
}

pub fn raw_log(zone: jiff::tz::TimeZone) -> Declared<RawLogParser> {
    Declared {
        spec: RAW_LOG,
        parser: RawLogParser { zone },
    }
}

pub fn raw_log_driver(path: PathBuf, host: String) -> AppendFile {
    AppendFile {
        path,
        host,
        framer: frame_log,
    }
}

/// The steering tables, each its own binding and parser name, so a replay
/// knows which table a stored row came from.
pub const STEERING: [SourceSpec; 3] = [
    steering_spec("commitments", "steering-commitments"),
    steering_spec("activities", "steering-activities"),
    steering_spec("reviews", "steering-reviews"),
];

const fn steering_spec(_table: &'static str, parser: &'static str) -> SourceSpec {
    SourceSpec {
        name: "operator.steering",
        driver: Driver::SqliteRows,
        consumer: "commitments, forecasts and reviews the operator keeps",
        class: Class::Open,
        parser,
        parser_version: 1,
        event_types: &["steering.record"],
        framer: crate::drivers::append_file::lines,
    }
}

pub struct SteeringParser {
    pub table: &'static str,
}

impl Parser for SteeringParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = SteeringRecord;

    fn parse(
        &self,
        row: &Map<String, Value>,
        _: &(),
    ) -> Result<(Vec<SteeringRecord>, ()), RecordError> {
        let id = fields::text(row, "id")?;
        let time_field = match self.table {
            "commitments" => Some("created_at"),
            "reviews" => Some("ts"),
            _ => None,
        };
        let at = time_field
            .and_then(|f| fields::optional_text(row, f))
            .map(|t| fields::rfc3339(&t))
            .transpose()?;
        let mut values = row.clone();
        values.remove("rowid");
        Ok((
            vec![SteeringRecord {
                table: self.table.into(),
                record_id: id,
                at,
                values,
            }],
            (),
        ))
    }
}

pub fn steering(table: &'static str) -> Option<Declared<SteeringParser>> {
    let spec = STEERING.iter().find(|s| s.parser.ends_with(table))?.clone();
    Some(Declared {
        spec,
        parser: SteeringParser { table },
    })
}

pub fn steering_driver(path: PathBuf, host: String, table: &'static str) -> SqliteRows {
    SqliteRows {
        path,
        host,
        table,
        from: None,
        required: &["id"],
        // A small store whose every row can change: all of it, each poll.
        mutable: Some("1 = 1"),
        full_pass_secs: 3600,
    }
}

#[cfg(test)]
mod tests;
