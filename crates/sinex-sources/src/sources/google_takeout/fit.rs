//! Google Fit (`Fit/All Data/<data source>.json`), on `ExportArchive`: each
//! file is one data source, and its `Data Points` are its samples. The
//! driver joins consecutive points of one data type that start on the same
//! local day into one record, and each record is one `health.record` of
//! provider `google-fit`: the category is the data type, the native ID the
//! file (the data source) and the day, and the values the day's point count
//! and, for each `fitValue` position, the count, sum, minimum, maximum and
//! mean of its numbers. Its start and end are the earliest point start and
//! the latest point end. Points of one day that are not consecutive in the
//! file make two records with one native ID, and the later replaces the
//! earlier.
//!
//! The points stay in the stored material as exported; [`points`] reads a
//! record's points back. Fit's points carry no zone, so the day is reckoned in
//! the binding's zone, recorded when the binding is planned.
use crate::drivers::export_archive::{ExportArchive, Framing, Join, PART_BYTES};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::sources::fields;
use crate::{Parser, RecordContext, RecordError};
use jiff::tz::TimeZone;
use serde_json::{Map, Number, Value};
use sinex_model::event_types::health::HealthRecord;
use sinex_model::event_types::health::fit::{FitDay, FitField};
use sinex_model::{Class, Timestamp};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "google.fit",
    driver: Driver::ExportArchive,
    consumer: "daily steps, activity, heart and location figures Fit recorded",
    class: Class::Open,
    parser: "google-fit",
    parser_version: 3,
    event_types: &["health.record"],
    framer: crate::Framer::export(
        frame,
        crate::drivers::export_archive::Framing::JsonArray("/Data Points"),
    ),
};

pub struct FitParser {
    pub zone: TimeZone,
}

impl Parser for FitParser {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = HealthRecord;

    fn parse(
        &self,
        record: &&[u8],
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<HealthRecord>, ()), RecordError> {
        let file = context
            .path()
            .and_then(|p| p.rsplit('/').next())
            .unwrap_or_default()
            .trim_end_matches(".json")
            .to_owned();
        let mut data_type: Option<String> = None;
        let mut day = None;
        let mut start: Option<i64> = None;
        let mut end: Option<i64> = None;
        let mut count = 0;
        let mut fields: Vec<FitField> = Vec::new();
        for point in points(record) {
            let point = Point::read(&point?, &self.zone)?;
            match &data_type {
                None => {
                    data_type = Some(point.data_type);
                    day = point.day;
                }
                Some(first) if *first != point.data_type || day != point.day => {
                    return Err(RecordError::new(
                        "a Fit record holds points of different data types or days",
                    ));
                }
                Some(_) => {}
            }
            start = min_some(start, point.start_ns);
            end = end.max(point.end_ns);
            count += 1;
            if fields.len() < point.numbers.len() {
                fields.resize_with(point.numbers.len(), FitField::default);
            }
            for (field, number) in fields.iter_mut().zip(&point.numbers) {
                if let Some(number) = number {
                    field.add(number);
                }
            }
        }
        let Some(data_type) = data_type else {
            return Err(RecordError::new("a Fit record without points"));
        };
        let offset_minutes = start
            .and_then(|ns| jiff::Timestamp::from_nanosecond(i128::from(ns)).ok())
            .map(|at| self.zone.to_offset(at).seconds() / 60);
        let values = FitDay {
            day: day.map(|d| d.to_string()),
            zone: self.zone.iana_name().unwrap_or_default().to_owned(),
            points: count,
            fields,
        };
        let Value::Object(values) = serde_json::to_value(values).expect("a Fit day serializes")
        else {
            unreachable!("a Fit day is an object")
        };
        let native_id = match day {
            Some(day) => format!("{file}:{day}"),
            None => format!("{file}:undated"),
        };
        let record = HealthRecord::new(
            "google-fit".into(),
            data_type,
            native_id,
            start.map(timestamp).transpose()?,
            end.map(timestamp).transpose()?,
            offset_minutes,
            values,
            vec![],
        )
        .map_err(|error| RecordError::new(error.to_string()))?;
        Ok((vec![record], ()))
    }
}

/// The points of a Fit record, each as its stored JSON object, in order.
pub fn points(record: &[u8]) -> impl Iterator<Item = Result<Map<String, Value>, RecordError>> + '_ {
    serde_json::Deserializer::from_slice(record)
        .into_iter::<Map<String, Value>>()
        .map(|point| point.map_err(|e| RecordError::new(format!("a Fit point: {e}"))))
}

/// What a record needs of one point: what joins it and what it adds.
struct Point {
    data_type: String,
    day: Option<jiff::civil::Date>,
    start_ns: Option<i64>,
    end_ns: Option<i64>,
    /// The number at each `fitValue` position, or `None` for text and maps.
    numbers: Vec<Option<Number>>,
}

impl Point {
    fn read(point: &Map<String, Value>, zone: &TimeZone) -> Result<Self, RecordError> {
        let nanos = |name: &str| match point.get(name) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => value
                .as_i64()
                .map(Some)
                .ok_or_else(|| RecordError::new(format!("{name} is not an integer: {value}"))),
        };
        let start_ns = nanos("startTimeNanos")?;
        let day = start_ns
            .map(|ns| {
                jiff::Timestamp::from_nanosecond(i128::from(ns))
                    .map(|at| at.to_zoned(zone.clone()).date())
                    .map_err(|_| RecordError::new("startTimeNanos out of range"))
            })
            .transpose()?;
        let numbers = match point.get("fitValue") {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::Array(values)) => values.iter().map(number).collect::<Result<_, _>>()?,
            Some(other) => {
                return Err(RecordError::new(format!(
                    "fitValue is not an array: {other}"
                )));
            }
        };
        Ok(Self {
            data_type: fields::text(point, "dataTypeName")?,
            day,
            start_ns,
            end_ns: nanos("endTimeNanos")?,
            numbers,
        })
    }

    /// The key the driver joins points by.
    fn key(&self) -> String {
        match self.day {
            Some(day) => format!("{}\n{day}", self.data_type),
            None => format!("{}\n", self.data_type),
        }
    }
}

/// The number in one `fitValue` entry: `intVal` or `fpVal`, directly or
/// inside `value`.
fn number(entry: &Value) -> Result<Option<Number>, RecordError> {
    let value = entry.get("value").unwrap_or(entry);
    match value.get("intVal").or_else(|| value.get("fpVal")) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => Ok(Some(n.clone())),
        Some(other) => Err(RecordError::new(format!(
            "a fitValue number is not a number: {other}"
        ))),
    }
}

fn timestamp(ns: i64) -> Result<Timestamp, RecordError> {
    Timestamp::from_unix_ns(i128::from(ns)).map_err(|_| RecordError::new("Fit time out of range"))
}

fn min_some(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

pub fn source(zone: TimeZone) -> DeclaredSource<FitParser> {
    DeclaredSource {
        spec: SPEC,
        parser: FitParser { zone },
    }
}

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::JsonArray("/Data Points").frame(bytes, true)
}

/// Points join when they have the same data type and local day; a point
/// that cannot be read stays alone, so its failure is its own.
pub fn driver(path: PathBuf, host: String, zone: TimeZone) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select: |m| m.contains("/Fit/All Data/") && m.ends_with(".json"),
        framing: Framing::JsonArray("/Data Points"),
        part_bytes: PART_BYTES,
        join: Some(Join(std::sync::Arc::new(move |bytes| {
            let point = serde_json::from_slice(bytes).ok()?;
            Point::read(&point, &zone).ok().map(|point| point.key())
        }))),
    }
}

#[cfg(test)]
mod tests;
