//! Authenticated moment marks and saved queries stored as JSON-line materials.
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sinex_model::{
    Class, InputRef, MaterialId, ObservationFacts, ReadingOrder, RevisionChange, Timestamp,
    event_types::query::{Mark, QueryRecord, Saved},
    json::JsonExt,
};
use sinex_source_core::{
    DeclaredSource, Expectation, Framer, Origin, RecordContext, RecordError, RecordResult,
    SourceSpec, Stateless, StatelessParser,
    discovery::DiscoverySpec,
    drivers::{
        Acquired, NewMaterial,
        export_archive::{Framing, frame_lines},
    },
};

pub const REQUEST: SourceSpec = SourceSpec {
    name: "query.request",
    origin: Origin::Request,
    expect: Expectation::OnDemand,
    purpose: "moment marks and saved queries",
    class: Class::Guarded,
    parser: "query-request",
    parser_version: 2,
    event_types: &["operator.mark", "query.saved"],
    framer: Framer::export(frame_lines, Framing::Lines),
    ..SourceSpec::DEFAULT
};

#[derive(Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case")]
enum Line {
    Mark(Mark),
    Saved(Saved),
}
pub struct RequestParser;
impl StatelessParser for RequestParser {
    type Record<'a> = Map<String, Value>;
    type Payload = QueryRecord;
    fn parse(
        &self,
        record: &Map<String, Value>,
        context: &RecordContext<'_>,
    ) -> Result<Vec<QueryRecord>, RecordError> {
        let actor = context
            .manifest
            .details
            .str("actor")
            .filter(|s| !s.trim().is_empty())
            .record("query manifest has no actor")?;
        let at = context
            .manifest
            .details
            .str("written_at_ns")
            .and_then(|s| s.parse::<i128>().ok())
            .and_then(|ns| Timestamp::from_unix_ns(ns).ok())
            .record("query manifest has no time")?;
        let host = context
            .manifest
            .detail("host")
            .as_str()
            .filter(|s| !s.is_empty())
            .record("query manifest has no host")?;
        let mut object = record.clone();
        object.insert("by".into(), json!(actor));
        object.insert("written_at".into(), json!(at));
        let line: Line = serde_json::from_value(Value::Object(object)).record("invalid record")?;
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
                sinex_query::saved::validate_parameters(
                    &sinex_model::query_definition::SavedQuery {
                        name: x.name.clone(),
                        query: x.query.clone(),
                        template: x.template.clone(),
                        parameters: x.parameters.clone(),
                    },
                )
                .record("invalid record")?;
                QueryRecord::Saved(x)
            }
        };
        Ok(vec![payload])
    }
}
pub fn request_source() -> DeclaredSource<Stateless<RequestParser>> {
    DeclaredSource {
        spec: REQUEST,
        parser: Stateless(RequestParser),
    }
}

/// Keep request bytes with the author and time Sinex records.
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
        memory: None,
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

            owned_files: Vec::new(),
        }),
        cursor_without_material: Value::Null,
    }
}
input_format! { pub FORMAT = "query" {
    specs: [REQUEST], parser: |_b| Stateless(RequestParser),
    discovery: DiscoverySpec::DEFAULT,
    plan: none, reader: none,
} }

#[cfg(test)]
mod tests;
