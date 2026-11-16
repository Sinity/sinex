//! Authenticated analysis requests keep their own source material.
use crate::sources::input_format;
use serde_json::{Map, Value, json};
use sinex_model::{
    InputRef, MaterialId, ObservationFacts, ReadingOrder, RevisionChange, Timestamp,
    event_types::analysis::AnalysisRequest, json::JsonExt,
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
    name: "analysis.request",
    origin: Origin::Request,
    expect: Expectation::OnDemand,
    purpose: "authored analysis questions",
    parser: "analysis-request",
    parser_version: 2,
    event_types: &["analysis.request"],
    framer: Framer::export(frame_lines, Framing::Lines),
    ..SourceSpec::DEFAULT
};
pub struct RequestParser;
impl StatelessParser for RequestParser {
    type Record<'a> = Map<String, Value>;
    type Payload = AnalysisRequest;
    fn parse(
        &self,
        record: &Map<String, Value>,
        context: &RecordContext<'_>,
    ) -> Result<Vec<AnalysisRequest>, RecordError> {
        let mut request: AnalysisRequest =
            serde_json::from_value(Value::Object(record.clone())).record("invalid record")?;
        request.by = context
            .manifest
            .details
            .str("actor")
            .record("analysis request has no authenticated actor")?
            .into();
        Ok(vec![request])
    }
}
pub fn request_source() -> DeclaredSource<Stateless<RequestParser>> {
    DeclaredSource {
        spec: REQUEST,
        parser: Stateless(RequestParser),
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
        memory: None,
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

            owned_files: Vec::new(),
        }),
        cursor_without_material: Value::Null,
    }
}

input_format! { pub FORMAT = "analysis" {
    specs: [REQUEST], parser: |_b| Stateless(RequestParser),
    discovery: DiscoverySpec::DEFAULT,
    plan: none, reader: none,
} }

#[cfg(test)]
mod tests;
