//! Source reader outcomes stored as JSON-line materials.
use crate::sources::input_format;
use serde_json::{Map, Value, json};
use sinex_model::{
    InputRef, MaterialId, ObservationFacts, ReadingOrder, RevisionChange,
    event_types::coverage::{CoverageState, SourceCoverage},
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

pub const OBSERVATION: SourceSpec = SourceSpec {
    name: "source.observation",
    origin: Origin::Request,
    expect: Expectation::OnDemand,
    purpose: "source read outcomes and coverage observations",
    parser: "source-observation",
    event_types: &["source.coverage"],
    framer: Framer::export(frame_lines, Framing::Lines),
    ..SourceSpec::DEFAULT
};

pub struct ObservationParser;
impl StatelessParser for ObservationParser {
    type Record<'a> = Map<String, Value>;
    type Payload = SourceCoverage;

    fn parse(
        &self,
        record: &Map<String, Value>,
        context: &RecordContext<'_>,
    ) -> Result<Vec<SourceCoverage>, RecordError> {
        let observation: SourceCoverage =
            serde_json::from_value(Value::Object(record.clone())).record("invalid record")?;
        if observation.observed_source.is_empty()
            || observation.binding.is_empty()
            || observation.started_at > observation.ended_at
            || context.manifest.detail("host").as_str() != Some(observation.host.as_str())
            || (observation.state == CoverageState::Missing && observation.reason.is_none())
            || (observation.state == CoverageState::Observed && observation.failures != 0)
        {
            return Err(RecordError::new("invalid source coverage observation"));
        }
        Ok(vec![observation])
    }
}

pub fn observation_source() -> DeclaredSource<Stateless<ObservationParser>> {
    DeclaredSource {
        spec: OBSERVATION,
        parser: Stateless(ObservationParser),
    }
}

pub fn observation_material(observation: SourceCoverage) -> Acquired {
    let host = observation.host.clone();
    let binding = observation.binding.clone();
    let at = observation.ended_at;
    let mut bytes = serde_json::to_vec(&observation).expect("coverage serializes");
    bytes.push(b'\n');
    let length = bytes.len();
    let order = i64::try_from(at.unix_ns()).expect("current timestamp fits reading order");
    Acquired {
        memory: None,
        input: InputRef {
            kind: "source-observation".into(),
            locator: binding,
            host: host.clone(),
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
            manifest: sinex_model::Manifest::from_json(json!({
                "path":"source-observation", "host":host,
                "read_at_ns":order, "media_type":"application/x-sinex-source-observation"
            })),
            reading_order: Box::new(move |_| ReadingOrder(order)),
            cursor_at: Box::new(|_| Ok(Value::Null)),
            aligned: false,
            removed: vec![],

            owned_files: vec![],
        }),
        cursor_without_material: Value::Null,
    }
}

input_format! { pub FORMAT = "source-observation" {
    specs: [OBSERVATION], parser: |_b| Stateless(ObservationParser),
    discovery: DiscoverySpec::DEFAULT,
    plan: none, reader: none,
} }

#[cfg(test)]
mod tests;
