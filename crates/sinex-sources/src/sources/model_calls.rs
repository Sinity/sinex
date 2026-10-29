//! Authenticated model policy, request and recorded response material. The
//! parser records evidence; writer authorization makes a policy effective.
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sinex_model::event_types::model_call::{
    ModelPolicy, ModelRecord, ModelRequest, RecordedResponse,
};
use sinex_model::{
    Class, InputRef, MaterialId, ObservationFacts, ReadingOrder, RevisionChange, Timestamp,
};
use sinex_source_core::drain::RecordContext;
use sinex_source_core::drivers::{Acquired, NewMaterial};
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordError};

pub const RECORDS: SourceSpec = SourceSpec {
    name: "model.records",
    driver: Driver::ApiRequest,
    excisable: false,
    purpose: "configured model policy and recorded effects",
    class: Class::Open,
    parser: "model-records",
    parser_version: 1,
    event_types: &["model.policy", "model.request", "model.response"],
    framer: sinex_source_core::Framer::export(
        sinex_source_core::drivers::export_archive::frame_lines,
        sinex_source_core::drivers::export_archive::Framing::Lines,
    ),
};

#[derive(Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case")]
enum Line {
    Policy(ModelPolicy),
    Request(ModelRequest),
    Response(RecordedResponse),
}

pub struct RecordParser;
impl Parser for RecordParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = ModelRecord;
    const VOLATILE: &'static [&'static str] = &[
        "configured_at",
        "configured_by",
        "requested_at",
        "requested_by",
        "received_at",
    ];
    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<ModelRecord>, ()), RecordError> {
        let actor = context
            .manifest
            .detail("actor")
            .as_str()
            .filter(|actor| !actor.trim().is_empty())
            .ok_or_else(|| RecordError::new("model record has no authenticated actor"))?
            .to_owned();
        let at = context
            .manifest
            .detail("written_at_ns")
            .as_str()
            .and_then(|value| value.parse::<i128>().ok())
            .and_then(|value| Timestamp::from_unix_ns(value).ok())
            .ok_or_else(|| RecordError::new("model record has no recording time"))?;
        let line: Line = serde_json::from_value(Value::Object(record.clone()))
            .map_err(|error| RecordError::new(error.to_string()))?;
        let record = match line {
            Line::Policy(mut policy) => {
                if actor != "operator" {
                    return Err(RecordError::new(
                        "model policy authoring requires the operator",
                    ));
                }
                policy.configured_by = actor;
                policy.configured_at = at;
                policy.validate().map_err(RecordError::new)?;
                ModelRecord::Policy(policy)
            }
            Line::Request(mut request) => {
                request.requested_by = actor;
                request.requested_at = at;
                request.validate().map_err(RecordError::new)?;
                ModelRecord::Request(request)
            }
            Line::Response(mut response) => {
                response.received_at = at;
                response.validate().map_err(RecordError::new)?;
                ModelRecord::Response(response)
            }
        };
        Ok((vec![record], ()))
    }
}

pub fn record_source() -> DeclaredSource<RecordParser> {
    DeclaredSource {
        spec: RECORDS,
        parser: RecordParser,
    }
}

pub fn record_material(record: ModelRecord, actor: &str, host: &str, at: Timestamp) -> Acquired {
    let line = match record {
        ModelRecord::Policy(policy) => Line::Policy(policy),
        ModelRecord::Request(request) => Line::Request(request),
        ModelRecord::Response(response) => Line::Response(response),
    };
    let mut bytes = serde_json::to_vec(&line).expect("model record serializes");
    bytes.push(b'\n');
    let length = bytes.len();
    let order = i64::try_from(at.unix_ns()).expect("recording timestamp fits reading order");
    Acquired {
        input: InputRef {
            kind: "model-records".into(),
            locator: "records".into(),
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
                json!({"path":"model-records","host":host,"actor":actor,
                "written_at_ns":at.unix_ns().to_string(),"read_at_ns":order,"media_type":"application/x-sinex-model-records"}),
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
        name: "model-records",
        discovery: sinex_source_core::discovery::DiscoverySpec {
            owner: sinex_source_core::discovery::Owner::System,
            locations: &[],
            capabilities: &[],
            credentials: &[],
            streams: &[],
            requests: &["model-policy", "model-record"],
            request_only: true,
        },
        discover: crate::discovery::discover_format,
        specs: || vec![&RECORDS],
        plan: None,
        source: |_| Ok(Box::new(record_source())),
        reader: None,
        capture: None,
    };

#[cfg(test)]
mod tests;
