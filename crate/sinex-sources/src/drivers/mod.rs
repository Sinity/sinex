//! Drivers (architecture §5.3): each obtains bytes for one input shape and
//! frames them, written once and shared by every source of that shape.
//!
//! A driver's poll returns [`Acquired`]: the observation, the new material's
//! bytes and framed records, and how to compute the cursor after any record.
//! [`batches`] turns that into admission batches through a source's drain.
use crate::{DrainInput, Limits, Source};
use serde_json::Value;
use sinex_model::{
    Batch, BatchKind, InputRef, MaterialId, MaterialRef, ObservationFacts, ReadingOrder,
    RecordOutcome, RevisionChange, RunId, SourceProgress,
};

pub mod append_file;
pub mod sqlite_rows;
pub mod todo;

/// Where drivers put a material's bytes: the content store. Implemented by the
/// daemon over `sinex-store`'s `Blobs`, so this crate does no database IO.
pub trait MaterialSink: Send + Sync {
    /// Store `bytes` and return their name, `blake3:<hex>`.
    fn put(&self, bytes: &[u8]) -> Result<String, String>;
}

#[derive(Debug, thiserror::Error)]
pub enum DriverError {
    #[error("{0}")]
    Io(String),
    #[error("stored cursor does not decode: {0}")]
    Cursor(String),
    #[error("content store: {0}")]
    Store(String),
    #[error("parser carry: {0}")]
    Carry(String),
}

/// What one poll of an input produced.
pub struct Acquired {
    pub input: InputRef,
    pub observation: ObservationFacts,
    pub revision: RevisionChange,
    /// The new material, if the poll found new complete records.
    pub material: Option<NewMaterial>,
    /// The cursor when no material was produced (an unchanged look, or only a
    /// partial record so far).
    pub cursor_unchanged: Value,
}

pub struct NewMaterial {
    pub id: MaterialId,
    pub bytes: Vec<u8>,
    pub records: Vec<(usize, usize)>,
    pub part_index: u32,
    pub previous_part: Option<MaterialId>,
    pub manifest: Value,
    /// Which of two readings is newer, by the record's anchor.
    pub reading_order: Box<dyn Fn(usize) -> ReadingOrder + Send + Sync>,
    /// The cursor after the record ending at the given byte of the material.
    pub cursor_at: Box<dyn Fn(usize) -> Value + Send + Sync>,
}

/// Turn one poll into admission batches. The material's bytes go to the
/// content store first; the first batch registers the material, and every
/// batch carries the cursor and carry after its last record, so a crash
/// between batches resumes exactly.
pub fn batches(
    source: &dyn Source,
    binding: &str,
    run: RunId,
    acquired: Acquired,
    carry: Value,
    sink: &dyn MaterialSink,
    limits: Limits,
) -> Result<Vec<Batch>, DriverError> {
    let spec = source.spec();
    let progress = |material: Option<MaterialRef>,
                    cursor: Value,
                    carry: Value,
                    observation: ObservationFacts,
                    revision| {
        BatchKind::Source(Box::new(SourceProgress {
            binding: binding.to_owned(),
            input: acquired.input.clone(),
            observation,
            revision,
            material,
            cursor,
            carry,
        }))
    };
    let Some(m) = acquired.material else {
        return Ok(vec![Batch {
            run,
            kind: progress(
                None,
                acquired.cursor_unchanged,
                carry,
                acquired.observation,
                acquired.revision,
            ),
            records: vec![],
            archive: vec![],
        }]);
    };
    let blob = sink.put(&m.bytes).map_err(DriverError::Store)?;
    let material = MaterialRef {
        id: m.id,
        input: None,
        blob,
        length: m.bytes.len() as u64,
        part_index: m.part_index,
        previous_part: m.previous_part,
        parser: spec.parser.to_owned(),
        parser_version: spec.parser_version,
        manifest: m.manifest.clone(),
    };
    let chunks = source
        .drain(DrainInput {
            source: spec.name,
            class: spec.class,
            material: m.id,
            bytes: &m.bytes,
            records: &m.records,
            carry,
            reading_order: &*m.reading_order,
            limits,
        })
        .map_err(|e| DriverError::Carry(e.0))?;
    let mut out = Vec::with_capacity(chunks.len());
    let mut first = true;
    for chunk in chunks {
        let (observation, revision) = if first {
            (acquired.observation.clone(), acquired.revision)
        } else {
            (
                ObservationFacts {
                    changed: false,
                    ..acquired.observation.clone()
                },
                RevisionChange::Same,
            )
        };
        let records: Vec<RecordOutcome> = chunk.outcomes;
        out.push(Batch {
            run,
            kind: progress(
                first.then(|| material.clone()),
                (m.cursor_at)(chunk.end),
                chunk.carry,
                observation,
                revision,
            ),
            records,
            archive: vec![],
        });
        first = false;
    }
    Ok(out)
}

/// File facts for an observation.
pub fn file_facts(path: &std::path::Path) -> std::io::Result<ObservationFacts> {
    use std::os::unix::fs::MetadataExt;
    let m = std::fs::metadata(path)?;
    Ok(ObservationFacts {
        observed_at: Some(sinex_model::Timestamp::now()),
        size: Some(m.len()),
        mtime_ns: Some(i128::from(m.mtime()) * 1_000_000_000 + i128::from(m.mtime_nsec())),
        ctime_ns: Some(i128::from(m.ctime()) * 1_000_000_000 + i128::from(m.ctime_nsec())),
        inode: Some(m.ino()),
        device: Some(m.dev()),
        changed: true,
        unavailable: None,
    })
}
