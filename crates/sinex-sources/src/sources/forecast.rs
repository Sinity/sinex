//! Authenticated forecast requests stored as ordinary JSON-line materials.
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sinex_model::event_types::forecast::{ForecastRecord, Issued, Resolved};
use sinex_model::{
    Class, InputRef, MaterialId, ObservationFacts, ReadingOrder, RevisionChange, Timestamp,
};
use sinex_source_core::drivers::{Acquired, NewMaterial};
use sinex_source_core::{DeclaredSource, Driver, Parser, RecordContext, RecordError, SourceSpec};

pub const REQUEST: SourceSpec = SourceSpec {
    name: "forecast.request",
    driver: Driver::ApiRequest,
    excisable: true,
    purpose: "submitted probability forecasts",
    class: Class::Guarded,
    parser: "forecast-request",
    parser_version: 1,
    event_types: &["forecast.issued", "forecast.resolved"],
    framer: sinex_source_core::Framer::export(
        sinex_source_core::drivers::export_archive::frame_lines,
        sinex_source_core::drivers::export_archive::Framing::Lines,
    ),
};

#[derive(Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case")]
enum Line {
    Issued(Issued),
    Resolved(Resolved),
}
pub struct RequestParser;
impl Parser for RequestParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = ForecastRecord;
    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<ForecastRecord>, ()), RecordError> {
        let actor = context
            .manifest
            .details
            .get("actor")
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| RecordError::new("forecast manifest has no actor"))?;
        let at = context
            .manifest
            .details
            .get("written_at_ns")
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<i128>().ok())
            .and_then(|ns| Timestamp::from_unix_ns(ns).ok())
            .ok_or_else(|| RecordError::new("forecast manifest has no time"))?;
        let mut object = record.clone();
        object.insert("by".into(), json!(actor));
        object.insert("at".into(), json!(at));
        let line: Line = serde_json::from_value(Value::Object(object))
            .map_err(|e| RecordError::new(e.to_string()))?;
        let payload = match line {
            Line::Issued(x) => {
                if !x.probability.is_finite()
                    || !(0.0..=1.0).contains(&x.probability)
                    || x.target.trim().is_empty()
                    || x.criteria.trim().is_empty()
                    || x.information_cutoff > x.at
                    || x.horizon <= x.at
                {
                    return Err(RecordError::new(
                        "invalid forecast probability, target, criteria or temporal bounds",
                    ));
                }
                ForecastRecord::Issued(x)
            }
            Line::Resolved(x) => {
                if x.evidence.is_empty() || x.outcome_at.is_some_and(|t| t > x.at) {
                    return Err(RecordError::new(
                        "resolution needs evidence and a nonfuture outcome time",
                    ));
                }
                ForecastRecord::Resolved(x)
            }
        };
        Ok((vec![payload], ()))
    }
}
pub fn request_source() -> DeclaredSource<RequestParser> {
    DeclaredSource {
        spec: REQUEST,
        parser: RequestParser,
    }
}

/// Both acts keep their exact bytes with server-owned authorship and time.
pub fn request_material(
    record: ForecastRecord,
    actor: &str,
    host: &str,
    at: Timestamp,
) -> Acquired {
    let line = match record {
        ForecastRecord::Issued(x) => Line::Issued(x),
        ForecastRecord::Resolved(x) => Line::Resolved(x),
    };
    let mut bytes = serde_json::to_vec(&line).expect("forecast record serializes");
    bytes.push(b'\n');
    let length = bytes.len();
    let order = i64::try_from(at.unix_ns()).expect("current timestamp fits reading order");
    Acquired {
        input: InputRef {
            kind: "forecast".into(),
            locator: "requests".into(),
            host: host.into(),
        },
        observation: ObservationFacts {
            observed_at: Some(at),
            changed: true,
            ..Default::default()
        },
        revision: RevisionChange::Appended,
        material: Some(NewMaterial {
            id: MaterialId::new(),
            bytes,
            stored: None,
            records: vec![(0, length)],
            part_index: 0,
            previous_part: None,
            manifest: sinex_model::Manifest::from_json(
                json!({"path":"forecast", "host":host, "actor":actor, "written_at_ns":at.unix_ns().to_string(), "read_at_ns":order, "media_type":"application/x-sinex-forecast-request"}),
            ),
            reading_order: Box::new(move |_| ReadingOrder(order)),
            cursor_at: Box::new(|_| Ok(Value::Null)),
            aligned: false,
            removed: vec![],
            memory: None,
            owned_files: Vec::new(),
        }),
        cursor_without_material: Value::Null,
    }
}
pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "forecast",
        discovery: sinex_source_core::discovery::DiscoverySpec {
            owner: sinex_source_core::discovery::Owner::System,
            locations: &[],
            capabilities: &[],
            credentials: &[],
            streams: &[],
            requests: &[],
            request_only: true,
        },
        discover: crate::discovery::discover_format,
        specs: || vec![&REQUEST],
        plan: None,
        source: |_| Ok(Box::new(request_source())),
        reader: None,
        capture: None,
    };

#[cfg(test)]
mod tests;
