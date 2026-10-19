//! Google Fit (`Fit/All Data/<data source>.json`), on `ExportArchive`: each
//! file is one data source, and each element of its `Data Points` one
//! `health.record` of provider `google-fit` whose category is the point's
//! `dataTypeName`. The file (the data source) and the point's start and end
//! identify it. `fitValue` is kept as exported, one entry per field.
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::sources::fields;
use crate::{Parser, RecordContext, RecordError};
use serde_json::{Map, Value};
use sinex_model::kinds::health::HealthRecord;
use sinex_model::{Class, Timestamp};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "google.fit",
    driver: Driver::ExportArchive,
    consumer: "steps, activity, heart and location samples Fit recorded",
    class: Class::Open,
    parser: "google-fit",
    parser_version: 1,
    event_types: &["health.record"],
    framer: frame,
};

pub struct FitParser;

impl Parser for FitParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = HealthRecord;

    fn parse(
        &self,
        _: &Map<String, Value>,
        _: &(),
    ) -> Result<(Vec<HealthRecord>, ()), RecordError> {
        Err(RecordError::new("a Fit data point needs its file's name"))
    }

    fn parse_record(
        &self,
        point: &Map<String, Value>,
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<HealthRecord>, ()), RecordError> {
        let file = context
            .path()
            .and_then(|p| p.rsplit('/').next())
            .unwrap_or_default()
            .trim_end_matches(".json")
            .to_owned();
        let nanos = |name: &str| -> Result<Option<Timestamp>, RecordError> {
            point
                .get(name)
                .and_then(Value::as_i64)
                .map(|n| {
                    Timestamp::from_unix_ns(i128::from(n))
                        .map_err(|_| RecordError::new(format!("{name} out of range")))
                })
                .transpose()
        };
        let start_ns = point.get("startTimeNanos").and_then(Value::as_i64);
        let end_ns = point.get("endTimeNanos").and_then(Value::as_i64);
        let category = fields::text(point, "dataTypeName")?;
        let mut values = Map::new();
        for (k, v) in point {
            if !matches!(
                k.as_str(),
                "startTimeNanos" | "endTimeNanos" | "dataTypeName"
            ) {
                values.insert(k.clone(), v.clone());
            }
        }
        Ok((
            vec![HealthRecord {
                provider: "google-fit".into(),
                category,
                native_id: format!(
                    "{file}:{}:{}",
                    start_ns.unwrap_or_default(),
                    end_ns.unwrap_or_default()
                ),
                start: nanos("startTimeNanos")?,
                end: nanos("endTimeNanos")?,
                offset_minutes: None,
                values,
                events: vec![],
            }],
            (),
        ))
    }
}

pub fn source() -> Declared<FitParser> {
    Declared {
        spec: SPEC,
        parser: FitParser,
    }
}

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::JsonArray("/Data Points").frame(bytes, true)
}

pub fn driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select: |m| m.contains("/Fit/All Data/") && m.ends_with(".json"),
        framing: Framing::JsonArray("/Data Points"),
        part_bytes: PART_BYTES,
    }
}

#[cfg(test)]
mod tests;
