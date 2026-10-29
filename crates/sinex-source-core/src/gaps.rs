//! One material record and parser for every capture gap. Capture callers
//! persist these records with their spool and supply the stream's class floor.
use crate::{Parser, RecordContext, RecordError};
pub use sinex_model::event_types::capture::Gap;

pub struct GapParser;

impl Parser for GapParser {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = Gap;

    fn parse(
        &self,
        record: &&[u8],
        _: &(),
        _: &RecordContext<'_>,
    ) -> Result<(Vec<Gap>, ()), RecordError> {
        let gap: Gap =
            serde_json::from_slice(record).map_err(|_| RecordError::new("invalid capture gap"))?;
        if gap.stream.is_empty()
            || gap.reason.trim().is_empty()
            || gap
                .sequence
                .as_ref()
                .is_some_and(|sequence| sequence.last_missing < sequence.first_missing)
            || matches!((gap.start, gap.end), (Some(start), Some(end)) if end < start)
        {
            return Err(RecordError::new("invalid capture gap interval"));
        }
        Ok((vec![gap], ()))
    }
}

/// Callers supply the affected stream's class, including configured floors.
pub fn source(class: sinex_model::Class) -> crate::DeclaredSource<GapParser> {
    crate::DeclaredSource {
        spec: crate::SourceSpec {
            name: "capture.gap",
            driver: crate::Driver::AppendFile,
            excisable: false,
            purpose: "intervals when a capture stream could not record",
            class,
            parser: "capture-gap",
            parser_version: 1,
            event_types: &["capture.gap"],
            framer: crate::Framer::incremental(crate::drivers::append_file::lines),
        },
        parser: GapParser,
    }
}

/// A gap is an ordinary JSON-line material. Its boundaries are supplied by the
/// stream; acquisition time does not fill an unknown boundary.
pub fn material(
    gap: &Gap,
    host: &str,
    observed_at: sinex_model::Timestamp,
) -> crate::drivers::Acquired {
    use sinex_model::{InputRef, MaterialId, ObservationFacts, ReadingOrder, RevisionChange};
    let mut bytes = serde_json::to_vec(gap).expect("capture gaps serialize");
    bytes.push(b'\n');
    let length = bytes.len();
    crate::drivers::Acquired {
        input: InputRef {
            kind: "capture-gap".into(),
            locator: gap.stream.clone(),
            host: host.into(),
        },
        observation: ObservationFacts {
            observed_at: Some(observed_at),
            changed: true,
            ..Default::default()
        },
        revision: RevisionChange::Appended,
        material: Some(crate::drivers::NewMaterial {
            id: MaterialId::new(),
            bytes,
            stored: None,
            records: vec![(0, length)],
            part_index: 0,
            previous_part: None,
            manifest: sinex_model::Manifest::from_json(
                serde_json::json!({"media_type":"application/jsonl"}),
            ),
            reading_order: Box::new(|_| ReadingOrder(0)),
            cursor_at: Box::new(|_| Ok(serde_json::Value::Null)),
            aligned: false,
            removed: vec![],
            memory: None,
            owned_files: vec![],
        }),
        cursor_without_material: serde_json::Value::Null,
    }
}

#[cfg(test)]
mod tests;
