//! Authenticated moment marks and saved queries stored as JSON-line materials.
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sinex_model::event_types::query::{Mark, QueryRecord, Saved};
use sinex_model::{
    Class, InputRef, MaterialId, ObservationFacts, ReadingOrder, RevisionChange, Timestamp,
};
use sinex_source_core::drain::RecordContext;
use sinex_source_core::drivers::{Acquired, NewMaterial};
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordError};

pub const REQUEST: SourceSpec = SourceSpec {
    name: "query.request",
    driver: Driver::ApiRequest,
    excisable: false,
    purpose: "moment marks and saved queries",
    class: Class::Guarded,
    parser: "query-request",
    parser_version: 1,
    event_types: &["operator.mark", "query.saved"],
    framer: sinex_source_core::Framer::export(
        sinex_source_core::drivers::export_archive::frame_lines,
        sinex_source_core::drivers::export_archive::Framing::Lines,
    ),
};

#[derive(Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case")]
enum Line {
    Mark(Mark),
    Saved(Saved),
}
pub struct RequestParser;
impl Parser for RequestParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = QueryRecord;
    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<QueryRecord>, ()), RecordError> {
        let actor = context
            .manifest
            .details
            .get("actor")
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| RecordError::new("query manifest has no actor"))?;
        let at = context
            .manifest
            .details
            .get("written_at_ns")
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<i128>().ok())
            .and_then(|ns| Timestamp::from_unix_ns(ns).ok())
            .ok_or_else(|| RecordError::new("query manifest has no time"))?;
        let host = context
            .manifest
            .detail("host")
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| RecordError::new("query manifest has no host"))?;
        let mut object = record.clone();
        object.insert("by".into(), json!(actor));
        object.insert("written_at".into(), json!(at));
        let line: Line = serde_json::from_value(Value::Object(object))
            .map_err(|e| RecordError::new(e.to_string()))?;
        let payload = match line {
            Line::Mark(mut x) => {
                x.host = host.into();
                x.by = actor.into();
                x.written_at = at;
                QueryRecord::Mark(x)
            }
            Line::Saved(mut x) => {
                x.by = actor.into();
                x.written_at = at;
                // Authoring resolves other saved questions. Replay checks the
                // stored declarations independently of today's definitions.
                sinex_model::query::saved::SavedQuery {
                    name: x.name.clone(),
                    query: x.query.clone(),
                    parameters: x.parameters.clone(),
                }
                .validate_parameters()
                .map_err(|e| RecordError::new(e.to_string()))?;
                QueryRecord::Saved(x)
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

/// Keep request bytes with server-owned authorship and recording time.
pub fn request_material(record: QueryRecord, actor: &str, host: &str, at: Timestamp) -> Acquired {
    let line = match record {
        QueryRecord::Mark(x) => Line::Mark(x),
        QueryRecord::Saved(x) => Line::Saved(x),
    };
    let mut bytes = serde_json::to_vec(&line).expect("query record serializes");
    bytes.push(b'\n');
    let length = bytes.len();
    let order = i64::try_from(at.unix_ns()).expect("current timestamp fits reading order");
    Acquired {
        input: InputRef {
            kind: "query".into(),
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
                json!({"path":"query", "host":host, "actor":actor, "written_at_ns":at.unix_ns().to_string(), "read_at_ns":order, "media_type":"application/x-sinex-query-request"}),
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
        name: "query",
        specs: || vec![&REQUEST],
        plan: None,
        source: |_| Ok(Box::new(request_source())),
        reader: None,
        capture: None,
    };

#[cfg(test)]
mod tests;
