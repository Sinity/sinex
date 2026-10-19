//! Google Location History (`Location History/Location History.json` in older
//! Takeouts, `Records.json` in newer ones), on `ExportArchive`: each element of
//! `locations` is one fix, a `location.sample`. Times are `timestampMs` (epoch
//! milliseconds as text) or `timestamp` (RFC 3339); coordinates are degrees ×
//! 10⁷. A later Takeout restates the same fixes, which admission recognizes by
//! device and instant.
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::sources::fields;
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::Class;
use sinex_model::kinds::account::LocationSample;
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "google.location-history",
    driver: Driver::ExportArchive,
    consumer: "where the operator was",
    class: Class::Open,
    parser: "google-location-history",
    parser_version: 1,
    event_types: &["location.sample"],
    framer: frame,
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

impl Parser for LocationParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = LocationSample;

    fn parse(
        &self,
        fix: &Map<String, Value>,
        _: &(),
    ) -> Result<(Vec<LocationSample>, ()), RecordError> {
        let at = match fix.get("timestampMs") {
            Some(Value::String(ms)) => fields::from_unix_ms(
                ms.parse()
                    .map_err(|_| RecordError::new(format!("timestampMs {ms:?}")))?,
            )?,
            Some(Value::Number(n)) => fields::from_unix_ms(
                n.as_i64()
                    .ok_or_else(|| RecordError::new("timestampMs out of range"))?,
            )?,
            _ => fields::rfc3339(&fields::text(fix, "timestamp")?)?,
        };
        let e7 = |name: &str| -> Result<f64, RecordError> {
            fix.get(name)
                .and_then(Value::as_i64)
                .map(|v| v as f64 / 1e7)
                .ok_or_else(|| RecordError::new(format!("fix without {name}")))
        };
        let num = |name: &str| fix.get(name).and_then(Value::as_f64);
        let device = fix.get("deviceTag").map(|d| match d {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        });
        Ok((
            vec![LocationSample {
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
            }],
            (),
        ))
    }
}

pub fn source() -> Declared<LocationParser> {
    Declared {
        spec: SPEC,
        parser: LocationParser,
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
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn old_and_new_fix_shapes() {
        let (out, ()) = LocationParser
            .parse(
                json!({"timestampMs": "1398348676787", "latitudeE7": 497031663,
                    "longitudeE7": 190826466, "accuracy": 3097})
                .as_object()
                .unwrap(),
                &(),
            )
            .unwrap();
        assert!((out[0].latitude - 49.7031663).abs() < 1e-9);
        assert_eq!(out[0].at, fields::from_unix_ms(1_398_348_676_787).unwrap());
        assert_eq!(
            (out[0].device.as_ref(), out[0].accuracy_m),
            (None, Some(3097.0))
        );
        let (out, ()) = LocationParser
            .parse(
                json!({"timestamp": "2022-01-02T03:04:05.678Z", "latitudeE7": 1,
                    "longitudeE7": 2, "deviceTag": 12345, "source": "WIFI"})
                .as_object()
                .unwrap(),
                &(),
            )
            .unwrap();
        assert_eq!(out[0].device.as_deref(), Some("12345"));
        assert_eq!(out[0].extra["source"], json!("WIFI"));
    }
}
