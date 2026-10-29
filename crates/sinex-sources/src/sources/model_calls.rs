//! Authenticated model policy, request and recorded response material. The
//! parser records evidence; writer authorization makes a policy effective.
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sinex_model::event_types::model_call::{
    ModelPolicy, ModelReceipt, ModelRecord, ModelRequest, RecordedResponse,
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
    excisable: true,
    purpose: "configured model policy and recorded effects",
    class: Class::Open,
    parser: "model-records",
    parser_version: 1,
    event_types: &[
        "model.policy",
        "model.request",
        "model.response",
        "model.receipt",
    ],
    framer: sinex_source_core::Framer::export(
        sinex_source_core::drivers::export_archive::frame_whole,
        sinex_source_core::drivers::export_archive::Framing::Whole,
    ),
};

#[derive(Serialize, Deserialize)]
#[serde(tag = "record", content = "value", rename_all = "snake_case")]
enum Line {
    Policy(ModelPolicy),
    Request(ModelRequest),
    Response(RecordedResponse),
    Receipt(ModelReceipt),
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
        "recorded_at",
        "recorded_by",
    ];
    fn record_hash(&self, raw: &[u8], _: &RecordContext<'_>) -> [u8; 32] {
        match serde_json::from_slice::<Map<String, Value>>(raw) {
            Ok(record) => record_identity(&record),
            Err(_) => *blake3::hash(raw).as_bytes(),
        }
    }
    fn streamed_record_hash(
        &self,
        record: &Map<String, Value>,
        _raw_hash: [u8; 32],
        _: &RecordContext<'_>,
    ) -> [u8; 32] {
        record_identity(record)
    }
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
            Line::Receipt(mut receipt) => {
                if actor != "operator" {
                    return Err(RecordError::new(
                        "actual receipt authoring requires the operator",
                    ));
                }
                receipt.recorded_by = actor;
                receipt.recorded_at = at;
                receipt.validate().map_err(RecordError::new)?;
                ModelRecord::Receipt(receipt)
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

fn record_identity(record: &Map<String, Value>) -> [u8; 32] {
    let mut value = Value::Object(record.clone());
    if let Some(fields) = value.get_mut("value").and_then(Value::as_object_mut) {
        for field in RecordParser::VOLATILE {
            fields.remove(*field);
        }
    }
    *blake3::hash(&sinex_model::json_identity::bytes(&value)).as_bytes()
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
        ModelRecord::Receipt(receipt) => Line::Receipt(receipt),
    };
    let mut bytes = serde_json::to_vec(&line).expect("model record serializes");
    bytes.push(b'\n');
    material(bytes, actor, host, at)
}

/// The supplied definition remains verbatim source bytes inside its typed
/// envelope. Server provenance lives in the manifest and is applied by parse.
pub fn record_original(
    record: ModelRecord,
    value: &str,
    actor: &str,
    host: &str,
    at: Timestamp,
) -> Acquired {
    let kind = match record {
        ModelRecord::Policy(_) => "policy",
        ModelRecord::Request(_) => "request",
        ModelRecord::Response(_) => "response",
        ModelRecord::Receipt(_) => "receipt",
    };
    let mut bytes = format!("{{\"record\":\"{kind}\",\"value\":").into_bytes();
    bytes.extend(value.as_bytes());
    bytes.extend(b"}\n");
    material(bytes, actor, host, at)
}
fn material(bytes: Vec<u8>, actor: &str, host: &str, at: Timestamp) -> Acquired {
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
            requests: &["model-policy", "model-record", "model-receipt"],
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
