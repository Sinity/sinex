//! Google Location History (`Location History/Location History.json` in older
//! Takeouts, `Records.json` in newer ones), on `ExportArchive`: each element of
//! `locations` is one fix, a `location.sample`. Times are `timestampMs` (epoch
//! milliseconds as text) or `timestamp` (RFC 3339); coordinates are degrees ×
//! 10⁷. A later Takeout restates the same fixes, which admission recognizes by
//! device and instant.
use crate::sources::fields;
use serde_json::{Map, Value};
use sinex_model::event_types::account::LocationSample;
use sinex_model::json::JsonExt;
use sinex_source_core::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use sinex_source_core::registry::{DeclaredSource, Expectation, Origin, Shape, SourceSpec};
use sinex_source_core::{Framer, RecordError, RecordResult, Stateless, StatelessParser};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "google.location-history",
    origin: Origin::Input(Shape::ExportArchive),
    expect: Expectation::Export,
    purpose: "where the operator was",
    parser: "google-location-history",
    event_types: &["location.sample"],
    framer: Framer::export(frame, Framing::JsonArray("/locations")),
    ..SourceSpec::DEFAULT
};

const KNOWN: &[&str] = &[
    "timestampMs",
    "timestamp",
    "latitudeE7",
    "longitudeE7",
    "accuracy",
    "altitude",
    "velocity",
    "heading",
    "activity",
    "deviceTag",
];

pub struct LocationParser;

impl StatelessParser for LocationParser {
    type Record<'a> = Map<String, Value>;
    type Payload = LocationSample;

    fn parse(
        &self,
        fix: &Map<String, Value>,
        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<Vec<LocationSample>, RecordError> {
        let at = match fix.get("timestampMs") {
            Some(Value::String(ms)) => {
                fields::from_unix_ms(ms.parse().record_with(|| format!("timestampMs {ms:?}"))?)?
            }
            Some(Value::Number(n)) => {
                fields::from_unix_ms(n.as_i64().record("timestampMs out of range")?)?
            }
            _ => fields::rfc3339(&fields::text(fix, "timestamp")?)?,
        };
        let e7 = |name: &str| -> Result<f64, RecordError> {
            fix.i64(name)
                .map(|v| v as f64 / 1e7)
                .record_with(|| format!("fix without {name}"))
        };
        let num = |name: &str| fix.f64(name);
        let device = fix.get("deviceTag").map(|d| match d {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        });
        Ok(vec![LocationSample {
            provider: "google".into(),
            device,
            at,
            latitude: e7("latitudeE7")?,
            longitude: e7("longitudeE7")?,
            accuracy_m: num("accuracy"),
            altitude_m: num("altitude"),
            velocity_mps: num("velocity"),
            heading_deg: num("heading"),
            activity: fix.get("activity").cloned(),
            extra: fields::extra(fix, KNOWN),
        }])
    }
}

pub fn source() -> DeclaredSource<Stateless<LocationParser>> {
    DeclaredSource {
        spec: SPEC,
        parser: Stateless(LocationParser),
    }
}

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::JsonArray("/locations").frame(bytes, true)
}

pub fn driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select: |m| {
            m.contains("Location History")
                && (m.ends_with("/Records.json") || m.ends_with("/Location History.json"))
        },
        framing: Framing::JsonArray("/locations"),
        part_bytes: PART_BYTES,
    }
}

#[cfg(test)]
mod tests;
