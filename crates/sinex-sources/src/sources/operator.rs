//! The operator's own records: the raw log of quick notes and the steering
//! store.
//!
//! The raw log (`raw-log.md`) is a Markdown list, one entry per item,
//! `- **YYYY-MM-DD HH:MM:SS** text`, local time, continued on following lines.
//! The operator edits it, so it is read on `AppendFile`, whose alignment keeps
//! an entry's place across rewrites. The steering store is a SQLite file of
//! commitments, activities and reviews, read on `SqliteRows`, one table per
//! binding; every row can change (a commitment's status), so each poll
//! compares them all.
use serde_json::{Map, Value};
use sinex_model::{
    Timestamp,
    event_types::operator::{LogEntry, SteeringRow},
};
use sinex_source_core::{
    Binding, DeclaredSource, Expectation, Framer, Origin, RecordContext, RecordError, RecordResult,
    Shape, Source, SourceSpec, Stateless, StatelessParser, binding,
    discovery::{DiscoverySpec, Location},
    drivers::{
        append_file::AppendFile,
        sqlite_rows::{Changes, SqliteRows},
    },
};

pub const RAW_LOG: SourceSpec = SourceSpec {
    name: "operator.raw-log",
    origin: Origin::Input(Shape::AppendFile),
    expect: Expectation::continuous_days(7),
    purpose: "the operator's quick notes, as primary data",
    parser: "operator-raw-log",
    parser_version: 2,
    event_types: &["operator_log.entry"],
    framer: Framer::incremental(frame_log),
    ..SourceSpec::DEFAULT
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

impl StatelessParser for RawLogParser {
    type Record<'a> = &'a [u8];
    type Payload = LogEntry;

    fn parse(
        &self,
        record: &&[u8],
        _context: &RecordContext<'_>,
    ) -> Result<Vec<LogEntry>, RecordError> {
        let text = std::str::from_utf8(record).record("entry is not UTF-8")?;
        let rest = text.strip_prefix("- **").record("not a log entry")?;
        let (stamp, body) = rest
            .split_once("**")
            .record("entry without a closing ** after its time")?;
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
            return Ok(vec![LogEntry {
                written_at: None,
                label: Some(label.to_owned()),
                text: body.trim().to_owned(),
            }]);
        };
        let zoned = self
            .zone
            .to_ambiguous_zoned(civil)
            .compatible()
            .record_with(|| format!("{stamp:?}"))?;
        let written_at = Timestamp::from_unix_ns(zoned.timestamp().as_nanosecond())
            .record("time out of range")?;
        Ok(vec![LogEntry {
            written_at: Some(written_at),
            label: None,
            text: body.trim().to_owned(),
        }])
    }
}

/// The steering tables, each its own binding and parser name, so a replay
/// knows which table a stored row came from.
const STEERING: [SourceSpec; 3] = [
    steering_spec("steering-commitments"),
    steering_spec("steering-activities"),
    steering_spec("steering-reviews"),
];

const fn steering_spec(parser: &'static str) -> SourceSpec {
    SourceSpec {
        name: "operator.steering",
        origin: Origin::Input(Shape::SqliteRows),
        expect: Expectation::continuous_days(30),
        purpose: "commitments, forecasts and reviews the operator keeps",
        parser,
        parser_version: 3,
        event_types: &["steering.row"],
        framer: Framer::LINES,
        ..SourceSpec::DEFAULT
    }
}

pub struct SteeringParser {
    pub table: &'static str,
}

impl StatelessParser for SteeringParser {
    type Record<'a> = Map<String, Value>;
    type Payload = SteeringRow;

    fn parse(
        &self,
        row: &Map<String, Value>,
        _context: &RecordContext<'_>,
    ) -> Result<Vec<SteeringRow>, RecordError> {
        let mut values = row.clone();
        // The SQLite-row driver's own field, not a column of the table.
        values.remove("_rowid");
        let payload =
            SteeringRow::from_row(self.table, Value::Object(values)).map_err(RecordError::new)?;
        Ok(vec![payload])
    }
}

pub fn steering(table: &'static str) -> Option<DeclaredSource<Stateless<SteeringParser>>> {
    let spec = STEERING
        .iter()
        .find(|s| s.parser.strip_prefix("steering-") == Some(table))?
        .clone();
    Some(DeclaredSource {
        spec,
        parser: Stateless(SteeringParser { table }),
    })
}

input_format! {
    /// `stage --parser raw-log PATH`: the operator's raw log, read in the binding's
    /// zone, recorded when it is planned.
    pub RAW_LOG_FORMAT = "raw-log" {
    specs: [RAW_LOG], parser: |b| Stateless(RawLogParser { zone: b.settings.zone()? }),
    discovery: DiscoverySpec::system(&[Location::new("declared", "raw-log")]),
    plan: |input, settings| Ok(binding::single(&RAW_LOG, input, &settings.clone().with_zone()?)),
    reader: |b| Ok(binding::appended(AppendFile { path: b.input.clone(), host: b.settings.host.clone(), framer: Framer::incremental(frame_log) })),
} }

/// The steering table a binding reads, from its spec name.
fn steering_table(b: &Binding) -> Result<&'static str, String> {
    ["commitments", "activities", "reviews"]
        .into_iter()
        .find(|t| b.parser.strip_prefix("steering-") == Some(*t))
        .ok_or_else(|| format!("{}: not a steering table", b.parser))
}

input_format! {
    /// `stage --parser steering PATH`: the steering store's three tables.
    pub STEERING_FORMAT = "steering" {
    specs: || STEERING.iter().collect(), source: |b| {
        let table = steering_table(b)?;
        steering(table).map(|s| Box::new(s) as Box<dyn Source>)
            .ok_or_else(|| format!("{table}: not a steering table"))
    },
    discovery: DiscoverySpec::system(&[Location::new("declared", "steering")]),
    plan: |input, settings| Ok(STEERING.iter().map(|spec| {
        let table = spec.parser.strip_prefix("steering-").unwrap_or(spec.parser);
        Binding::of(spec, input, settings.clone()).named(table)
    }).collect()),
    reader: |b| Ok(binding::rows(SqliteRows {
        path: b.input.clone(), host: b.settings.host.clone(), table: steering_table(b)?,
        from: None, required: &["id"],
        changes: Changes::InPlace { mutable: "1 = 1", full_pass_secs: 3600 },
    })),
} }

#[cfg(test)]
mod tests;
