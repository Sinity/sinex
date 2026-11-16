//! Duplicate decisions (`same`, `distinct`, `withdraw-decision`) kept as
//! JSON-line request materials. The server supplies the author, channel and
//! time from the request's manifest; a line cannot claim another author.
use crate::sources::input_format;
use serde_json::{Map, Value, json};
use sinex_model::{
    Class, InputRef, MaterialId, ObservationFacts, ReadingOrder, RevisionChange, Timestamp,
    event_types::duplicate::Duplicate, json::JsonExt,
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
    name: "duplicate.request",
    origin: Origin::Request,
    expect: Expectation::OnDemand,
    purpose: "decisions on whether two records are one occurrence",
    class: Class::Guarded,
    parser: "duplicate-request",
    event_types: &["curation.duplicate"],
    framer: Framer::export(frame_lines, Framing::Lines),
    ..SourceSpec::DEFAULT
};

pub struct RequestParser;

impl StatelessParser for RequestParser {
    type Record<'a> = Map<String, Value>;
    type Payload = Duplicate;

    fn parse(
        &self,
        record: &Map<String, Value>,
        context: &RecordContext<'_>,
    ) -> Result<Vec<Duplicate>, RecordError> {
        let detail = |name: &str| {
            context
                .manifest
                .details
                .str(name)
                .filter(|s| !s.trim().is_empty())
                .record_with(|| format!("decision manifest has no {name}"))
        };
        let actor = detail("actor")?;
        let via = detail("via")?;
        let written_ns = detail("written_at_ns")?
            .parse::<i128>()
            .record("decision manifest has no time")?;
        let mut object = record.clone();
        object.insert("author".into(), json!(actor));
        object.insert("via".into(), json!(via));
        object.insert(
            "at".into(),
            json!(i64::try_from(written_ns / 1_000_000).unwrap_or(i64::MAX)),
        );
        let decision: Duplicate =
            serde_json::from_value(Value::Object(object)).record("invalid record")?;
        if decision.pair.len() != 2 || decision.pair[0] >= decision.pair[1] {
            return Err(RecordError::new(
                "a duplicate decision names two different records, sorted",
            ));
        }
        decision.header().record("invalid record")?;
        if decision.decision.is_none() && decision.withdraws.is_empty() {
            return Err(RecordError::new(
                "a record without a decision must withdraw one",
            ));
        }
        Ok(vec![decision])
    }
}

pub fn request_source() -> DeclaredSource<Stateless<RequestParser>> {
    DeclaredSource {
        spec: REQUEST,
        parser: Stateless(RequestParser),
    }
}

/// Keep a decision's request bytes with the author, channel and time Sinex
/// records. `author`, `via` and `at` in `decision` are replaced by the
/// manifest's.
pub fn request_material(
    decision: &Duplicate,
    actor: &str,
    via: &str,
    host: &str,
    at: Timestamp,
) -> Acquired {
    let mut line = serde_json::to_value(decision).expect("decisions serialize");
    if let Some(object) = line.as_object_mut() {
        object.remove("author");
        object.remove("via");
        object.remove("at");
    }
    let mut bytes = serde_json::to_vec(&line).expect("decisions serialize");
    bytes.push(b'\n');
    let length = bytes.len();
    let order = i64::try_from(at.unix_ns()).expect("current timestamp fits reading order");
    Acquired {
        memory: None,
        input: InputRef {
            kind: "duplicate".into(),
            locator: "decisions".into(),
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
                json!({"path":"duplicate", "host":host, "actor":actor, "via":via, "written_at_ns":at.unix_ns().to_string(), "read_at_ns":order, "media_type":"application/x-sinex-duplicate-request"}),
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

input_format! { pub FORMAT = "duplicate" {
    specs: [REQUEST], parser: |_b| Stateless(RequestParser),
    discovery: DiscoverySpec::DEFAULT,
    plan: none, reader: none,
} }

#[cfg(test)]
mod tests;
