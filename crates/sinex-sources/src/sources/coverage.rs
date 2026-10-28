//! Source reader outcomes retained as ordinary JSON-line materials.
use serde_json::{Map, Value, json};
use sinex_model::event_types::coverage::{CoverageState, SourceCoverage};
use sinex_model::{Class, InputRef, MaterialId, ObservationFacts, ReadingOrder, RevisionChange};
use sinex_source_core::drivers::{Acquired, NewMaterial};
use sinex_source_core::{DeclaredSource, Driver, Parser, RecordContext, RecordError, SourceSpec};

pub const OBSERVATION: SourceSpec = SourceSpec {
    name: "source.observation",
    driver: Driver::ApiRequest,
    excisable: false,
    purpose: "source read outcomes and coverage evidence",
    class: Class::Open,
    parser: "source-observation",
    parser_version: 1,
    event_types: &["source.coverage"],
    framer: sinex_source_core::Framer::export(
        sinex_source_core::drivers::export_archive::frame_lines,
        sinex_source_core::drivers::export_archive::Framing::Lines,
    ),
};

pub struct ObservationParser;
impl Parser for ObservationParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = SourceCoverage;

    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<SourceCoverage>, ()), RecordError> {
        let observation: SourceCoverage = serde_json::from_value(Value::Object(record.clone()))
            .map_err(|error| RecordError::new(error.to_string()))?;
        if observation.observation.is_empty()
            || observation.source.is_empty()
            || observation.binding.is_empty()
            || observation.started_at > observation.ended_at
            || context.manifest.detail("host").as_str() != Some(observation.host.as_str())
            || (observation.state == CoverageState::Missing && observation.reason.is_none())
            || (observation.state == CoverageState::Observed && observation.failures != 0)
        {
            return Err(RecordError::new("invalid source coverage evidence"));
        }
        Ok((vec![observation], ()))
    }
}

pub fn observation_source() -> DeclaredSource<ObservationParser> {
    DeclaredSource { spec: OBSERVATION, parser: ObservationParser }
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
        input: InputRef { kind: "source-observation".into(), locator: binding, host: host.clone() },
        observation: ObservationFacts { observed_at: Some(at), changed: true, ..Default::default() },
        revision: RevisionChange::Appended,
        material: Some(NewMaterial {
            id: MaterialId::new(), bytes, stored: None, records: vec![(0, length)],
            part_index: 0, previous_part: None,
            manifest: sinex_model::Manifest::from_json(json!({
                "path":"source-observation", "host":host,
                "read_at_ns":order, "media_type":"application/x-sinex-source-observation"
            })),
            reading_order: Box::new(move |_| ReadingOrder(order)),
            cursor_at: Box::new(|_| Ok(Value::Null)), aligned: false, removed: vec![],
            memory: None, owned_files: vec![],
        }),
        cursor_without_material: Value::Null,
    }
}

pub const FORMAT: sinex_source_core::binding::InputFormat = sinex_source_core::binding::InputFormat {
    name: "source-observation",
    discovery: sinex_source_core::discovery::DiscoverySpec {
        owner: sinex_source_core::discovery::Owner::System,
        locations: &[], capabilities: &[], credentials: &[], streams: &[], requests: &[],
        request_only: true,
    },
    discover: crate::discovery::discover_format,
    specs: || vec![&OBSERVATION], plan: None,
    source: |_| Ok(Box::new(observation_source())), reader: None, capture: None,
};

#[cfg(test)]
mod tests;
