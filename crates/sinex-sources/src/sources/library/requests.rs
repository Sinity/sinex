//! Library requests: what the operator or an agent writes to the library,
//! stored as JSON lines and read back with the author and time taken from the
//! material's manifest.
use super::REQUEST;
use crate::drain::RecordContext;
use crate::drivers::{Acquired, NewMaterial};
use crate::registry::DeclaredSource;
use crate::{Parser, RecordError};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sinex_model::event_types::library::{
    Assignment, Collection, Comparison, Dismissal, Entry, Item, LibraryRecord, Rating, Rendition,
    View,
};
use sinex_model::{
    Class, InputRef, MaterialId, ObservationFacts, ReadingOrder, RevisionChange, Timestamp,
};

/// One request line: a record tagged with its kind. `by` and `at` in the line
/// are ignored; the parser fills them from the manifest.
#[derive(Deserialize, Serialize)]
#[serde(tag = "record", rename_all = "snake_case")]
enum Line {
    Item(Item),
    Rendition(Rendition),
    Assignment(Assignment),
    Collection(Collection),
    Entry(Entry),
    Rating(Rating),
    Comparison(Comparison),
    View(View),
    Dismissal(Dismissal),
}

/// The bytes of a request: one JSON line per record, in order. Admission
/// stores them as the request's material.
fn request_bytes(records: &[LibraryRecord]) -> (Vec<u8>, Vec<(usize, usize)>) {
    let mut bytes = Vec::new();
    let mut spans = Vec::new();
    for r in records {
        let line = match r.clone() {
            LibraryRecord::Item(x) => Line::Item(x),
            LibraryRecord::Rendition(x) => Line::Rendition(x),
            LibraryRecord::Assignment(x) => Line::Assignment(x),
            LibraryRecord::Collection(x) => Line::Collection(x),
            LibraryRecord::Entry(x) => Line::Entry(x),
            LibraryRecord::Rating(x) => Line::Rating(x),
            LibraryRecord::Comparison(x) => Line::Comparison(x),
            LibraryRecord::View(x) => Line::View(x),
            LibraryRecord::Dismissal(x) => Line::Dismissal(x),
        };
        let start = bytes.len();
        serde_json::to_writer(&mut bytes, &line).expect("library records serialize");
        bytes.push(b'\n');
        spans.push((start, bytes.len()));
    }
    (bytes, spans)
}

/// The manifest of a request material: who wrote it and when.
fn request_manifest(actor: &str, host: &str, at: Timestamp, action: &str) -> sinex_model::Manifest {
    sinex_model::Manifest::from_json(json!({
        "path": "library",
        "host": host,
        "media_type": "application/x-sinex-library-request",
        "actor": actor,
        "action": action,
        "written_at_ns": at.unix_ns().to_string(),
        "read_at_ns": i64::try_from(at.unix_ns()).unwrap_or(i64::MAX),
    }))
}

/// Payload bytes stay the registered library shape. A request's copy
/// facts also make its event cite the prepared rendition.
pub struct RequestPayload {
    record: LibraryRecord,
    citation: Option<sinex_model::OwnedBlob>,
}
impl sinex_model::Payloads for RequestPayload {
    fn build(
        &self,
        source: &str,
        origin: sinex_model::Origin,
        order: ReadingOrder,
        hash: [u8; 32],
        class: Class,
    ) -> Result<sinex_model::Event, sinex_model::BuildError> {
        let mut event = self.record.build(source, origin, order, hash, class)?;
        event.owned_blobs.extend(self.citation.clone());
        Ok(event)
    }
}

pub struct RequestParser;

impl Parser for RequestParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = RequestPayload;

    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<RequestPayload>, ()), RecordError> {
        let by = context
            .manifest
            .details
            .get("actor")
            .and_then(Value::as_str)
            .ok_or_else(|| RecordError::new("library request manifest has no actor"))?
            .to_owned();
        let at = context
            .manifest
            .details
            .get("written_at_ns")
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<i128>().ok())
            .and_then(|ns| Timestamp::from_unix_ns(ns).ok())
            .ok_or_else(|| RecordError::new("library request manifest has no time"))?;
        let mut object = record.clone();
        object.insert("by".into(), Value::String(by.clone()));
        if !matches!(object.get("record").and_then(Value::as_str), Some("view")) {
            object.insert(
                "at".into(),
                serde_json::to_value(at).expect("time serializes"),
            );
        }
        let line: Line = serde_json::from_value(Value::Object(object))
            .map_err(|e| RecordError::new(format!("not a library record: {e}")))?;
        let record = match line {
            Line::Item(x) => LibraryRecord::Item(x),
            Line::Rendition(x) => LibraryRecord::Rendition(x),
            Line::Assignment(x) => LibraryRecord::Assignment(x),
            Line::Collection(x) => LibraryRecord::Collection(x),
            Line::Entry(x) => LibraryRecord::Entry(x),
            Line::Rating(x) => LibraryRecord::Rating(x),
            Line::Comparison(x) => LibraryRecord::Comparison(x),
            Line::View(x) => LibraryRecord::View(x),
            Line::Dismissal(x) => LibraryRecord::Dismissal(x),
        };
        let citation = match context.manifest.details.get("owned_blobs") {
            None => None,
            Some(facts) => {
                let facts: Vec<sinex_model::OwnedBlob> = serde_json::from_value(facts.clone())
                    .map_err(|_| RecordError::new("invalid library request citation facts"))?;
                let mut seen = std::collections::BTreeSet::new();
                if facts.is_empty()
                    || facts.iter().any(|fact| {
                        !seen.insert(fact.blob.clone())
                            || i64::try_from(fact.size).is_err()
                            || fact
                                .blob
                                .strip_prefix("blake3:")
                                .is_none_or(|hash| blake3::Hash::from_hex(hash).is_err())
                    })
                {
                    return Err(RecordError::new(
                        "invalid or duplicate library request citation facts",
                    ));
                }
                match &record {
                    LibraryRecord::Rendition(rendition) => Some(
                        facts
                            .into_iter()
                            .find(|fact| fact.blob == rendition.content)
                            .ok_or_else(|| {
                                RecordError::new("rendition content differs from prepared citation")
                            })?,
                    ),
                    _ => None,
                }
            }
        };
        Ok((vec![RequestPayload { record, citation }], ()))
    }
}

pub fn request_source() -> DeclaredSource<RequestParser> {
    DeclaredSource {
        spec: REQUEST,
        parser: RequestParser,
    }
}

/// A request as a material ready for `drivers::batches`: the request lines
/// and their manifest.
pub fn request_material(
    records: &[LibraryRecord],
    actor: &str,
    host: &str,
    action: &str,
) -> Acquired {
    let at = Timestamp::now();
    let (bytes, spans) = request_bytes(records);
    let manifest = request_manifest(actor, host, at, action);
    let order = i64::try_from(at.unix_ns()).unwrap_or(i64::MAX);
    Acquired {
        input: InputRef {
            kind: "operator".into(),
            locator: "library".into(),
            host: host.to_owned(),
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
            records: spans,
            part_index: 0,
            previous_part: None,
            manifest,
            reading_order: Box::new(move |_| ReadingOrder(order)),
            cursor_at: Box::new({
                let cursor = |_| Value::Null;
                move |end| Ok(cursor(end))
            }),
            aligned: false,
            removed: Vec::new(),
            memory: None,
            owned_files: Vec::new(),
        }),
        cursor_without_material: Value::Null,
    }
}
