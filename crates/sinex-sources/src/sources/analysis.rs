//! Authenticated analysis requests retain their own source material.
use serde_json::{Map, Value, json};
use sinex_model::event_types::analysis::AnalysisRequest;
use sinex_model::{
    Class, InputRef, MaterialId, ObservationFacts, ReadingOrder, RevisionChange, Timestamp,
};
use sinex_source_core::drain::RecordContext;
use sinex_source_core::drivers::{Acquired, NewMaterial};
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordError};

pub const REQUEST: SourceSpec = SourceSpec {
    name: "analysis.request",
    driver: Driver::ApiRequest,
    excisable: false,
    purpose: "canonical analysis questions",
    class: Class::Open,
    parser: "analysis-request",
    parser_version: 1,
    event_types: &["analysis.request"],
    framer: sinex_source_core::Framer::export(
        sinex_source_core::drivers::export_archive::frame_lines,
        sinex_source_core::drivers::export_archive::Framing::Lines,
    ),
};
pub struct RequestParser;
impl Parser for RequestParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = AnalysisRequest;
    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<AnalysisRequest>, ()), RecordError> {
        let mut request: AnalysisRequest = serde_json::from_value(Value::Object(record.clone()))
            .map_err(|e| RecordError::new(e.to_string()))?;
        request.by = context
            .manifest
            .details
            .get("actor")
            .and_then(Value::as_str)
            .ok_or_else(|| RecordError::new("analysis request has no authenticated actor"))?
            .into();
        Ok((vec![request], ()))
    }
}
pub fn request_source() -> DeclaredSource<RequestParser> {
    DeclaredSource {
        spec: REQUEST,
        parser: RequestParser,
    }
}
pub fn request_material(
    record: AnalysisRequest,
    actor: &str,
    host: &str,
    at: Timestamp,
) -> Acquired {
    let line = record;
    let mut bytes = serde_json::to_vec(&line).expect("analysis request serializes");
    bytes.push(b'\n');
    let length = bytes.len();
    let order = i64::try_from(at.unix_ns()).expect("current timestamp fits reading order");
    Acquired {
        input: InputRef {
            kind: "analysis".into(),
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
                json!({"path":"analysis", "host":host, "actor":actor, "written_at_ns":at.unix_ns().to_string(), "read_at_ns":order, "media_type":"application/x-sinex-analysis-request"}),
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
        name: "analysis",
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
