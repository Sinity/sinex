//! Duplicate decisions (`same`, `distinct`, `withdraw-decision`) kept as
//! JSON-line request materials. The server supplies the author, channel and
//! time from the request's manifest; a line cannot claim another author.
use serde_json::{Map, Value, json};
use sinex_model::event_types::duplicate::Duplicate;
use sinex_model::{
    Class, InputRef, MaterialId, ObservationFacts, ReadingOrder, RevisionChange, Timestamp,
};
use sinex_source_core::drain::RecordContext;
use sinex_source_core::drivers::{Acquired, NewMaterial};
use sinex_source_core::registry::{DeclaredSource, Expectation, Origin, SourceSpec};
use sinex_source_core::{Parser, RecordError};

pub const REQUEST: SourceSpec = SourceSpec {
    name: "duplicate.request",
    origin: Origin::Request,
    expect: Expectation::OnDemand,
    expires_after: None,
    purpose: "decisions on whether two records are one occurrence",
    class: Class::Guarded,
    parser: "duplicate-request",
    parser_version: 1,
    event_types: &["curation.duplicate"],
    framer: sinex_source_core::Framer::export(
        sinex_source_core::drivers::export_archive::frame_lines,
        sinex_source_core::drivers::export_archive::Framing::Lines,
    ),
};

pub struct RequestParser;

impl Parser for RequestParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = Duplicate;

    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<Duplicate>, ()), RecordError> {
        let detail = |name: &str| {
            context
                .manifest
                .details
                .get(name)
                .and_then(Value::as_str)
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| RecordError::new(format!("decision manifest has no {name}")))
        };
        let actor = detail("actor")?;
        let via = detail("via")?;
        let written_ns = detail("written_at_ns")?
            .parse::<i128>()
            .map_err(|_| RecordError::new("decision manifest has no time"))?;
        let mut object = record.clone();
        object.insert("author".into(), json!(actor));
        object.insert("via".into(), json!(via));
        object.insert(
            "at".into(),
            json!(i64::try_from(written_ns / 1_000_000).unwrap_or(i64::MAX)),
        );
        let decision: Duplicate = serde_json::from_value(Value::Object(object))
            .map_err(|e| RecordError::new(e.to_string()))?;
        if decision.pair.len() != 2 || decision.pair[0] >= decision.pair[1] {
            return Err(RecordError::new(
                "a duplicate decision names two different records, sorted",
            ));
        }
        decision
            .header()
            .map_err(|e| RecordError::new(e.to_string()))?;
        if decision.decision.is_none() && decision.withdraws.is_empty() {
            return Err(RecordError::new(
                "a record without a decision must withdraw one",
            ));
        }
        Ok((vec![decision], ()))
    }
}

pub fn request_source() -> DeclaredSource<RequestParser> {
    DeclaredSource {
        spec: REQUEST,
        parser: RequestParser,
    }
}

/// Keep a decision's request bytes with server-owned authorship, channel and
/// time. `author`, `via` and `at` in `decision` are replaced by the
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
            memory: None,
            owned_files: Vec::new(),
        }),
        cursor_without_material: Value::Null,
    }
}

pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "duplicate",
        accepts: &[],
        discovery: sinex_source_core::discovery::DiscoverySpec {
            owner: sinex_source_core::discovery::Owner::System,
            locations: &[],
            capabilities: &[],
            credentials: &[],
            streams: &[],
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
