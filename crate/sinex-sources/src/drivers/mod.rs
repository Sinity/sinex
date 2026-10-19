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

pub mod api_cursor;
pub mod append_file;
pub mod dir_snapshot;
pub mod export_archive;
pub mod live_stream;
pub mod producer;
pub mod sqlite_rows;
pub mod media;

/// Where drivers put a material's bytes: the content store. Implemented by the
/// daemon over `sinex-store`'s `Blobs`, so this crate does no database IO.
pub trait MaterialSink: Send + Sync {
    /// Store `bytes` and return their name, `blake3:<hex>`.
    fn put(&self, bytes: &[u8]) -> Result<String, String>;

    /// Store a file's bytes as they are and return their name. Sinks that can
    /// stream override this, so an archive of any size is stored without
    /// holding it in memory.
    fn put_file(&self, path: &std::path::Path) -> Result<String, String> {
        let bytes = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
        self.put(&bytes)
    }
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
    /// The bytes the records index into.
    pub bytes: Vec<u8>,
    /// When set, the material stored is this file as received (an export
    /// archive) and `bytes` is the stream decoded from it as the manifest
    /// describes, so a replay decodes the stored file the same way.
    pub stored_file: Option<std::path::PathBuf>,
    pub records: Vec<(usize, usize)>,
    pub part_index: u32,
    pub previous_part: Option<MaterialId>,
    pub manifest: Value,
    /// Which of two readings is newer, by the record's anchor.
    pub reading_order: Box<dyn Fn(usize) -> ReadingOrder + Send + Sync>,
    /// The cursor after the record ending at the given byte of the material.
    pub cursor_at: Box<dyn Fn(usize) -> Value + Send + Sync>,
    /// A rewrite aligned against the previous revision: `records` holds only
    /// the records not read before. Its records are admitted as one batch, so
    /// the cursor never stops inside it (a resumed read would take the
    /// records skipped as already read for new ones).
    pub aligned: bool,
    /// Records the previous revision held that a rewrite dropped, by where
    /// they were read: each becomes a removed-at-source reading.
    pub vanished: Vec<sinex_model::RecordLocation>,
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
    let (blob, length) = match &m.stored_file {
        Some(path) => {
            let length = std::fs::metadata(path)
                .map_err(|e| DriverError::Io(format!("{}: {e}", path.display())))?
                .len();
            (sink.put_file(path).map_err(DriverError::Store)?, length)
        }
        None => (
            sink.put(&m.bytes).map_err(DriverError::Store)?,
            m.bytes.len() as u64,
        ),
    };
    let material = MaterialRef {
        id: m.id,
        input: None,
        blob,
        length,
        part_index: m.part_index,
        previous_part: m.previous_part,
        parser: spec.parser.to_owned(),
        parser_version: spec.parser_version,
        manifest: m.manifest.clone(),
    };
    let limits = if m.aligned {
        Limits {
            max_events: usize::MAX,
            max_bytes: usize::MAX,
        }
    } else {
        limits
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
            manifest: &m.manifest,
        })
        .map_err(|e| DriverError::Carry(e.0))?;
    let mut out = Vec::with_capacity(chunks.len());
    let mut first = true;
    let count = chunks.len();
    for (i, chunk) in chunks.into_iter().enumerate() {
        // After the last chunk the whole material is read, records skipped as
        // already read included.
        let end = if i + 1 == count {
            m.bytes.len()
        } else {
            chunk.end
        };
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
                (m.cursor_at)(end),
                chunk.carry,
                observation,
                revision,
            ),
            records,
            archive: if first && !m.vanished.is_empty() {
                vec![sinex_model::ArchiveRequest::RemovedAtSource {
                    locations: m.vanished.clone(),
                }]
            } else {
                vec![]
            },
        });
        first = false;
    }
    Ok(out)
}

/// Which of two readings of a record is newer, as the material's manifest
/// records it: a database snapshot's base order plus the row's position in it
/// (`order_base`), the time a snapshot was read (`read_at_ns`), or the
/// record's byte offset in its file (`file_offset` plus the anchor). The
/// driver and a replay both compute it here, so a replayed reading keeps the
/// order of the reading it replaces.
pub fn reading_order(manifest: &Value) -> Box<dyn Fn(usize) -> ReadingOrder + Send + Sync> {
    if let Some(base) = manifest.get("order_base").and_then(Value::as_i64) {
        return Box::new(move |anchor| ReadingOrder(base.saturating_add(anchor as i64)));
    }
    if let Some(at) = manifest.get("read_at_ns").and_then(Value::as_i64) {
        return Box::new(move |_| ReadingOrder(at));
    }
    let offset = manifest
        .get("file_offset")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    Box::new(move |anchor| ReadingOrder(offset.saturating_add(anchor as u64) as i64))
}

/// The records of a stored material the driver read: every framed record,
/// or, for a rewrite aligned against the previous revision, only those the
/// manifest lists as new (`records`), since the rest were read before.
pub fn stored_records(
    manifest: &Value,
    bytes: &[u8],
    framer: crate::Framer,
) -> Vec<(usize, usize)> {
    match manifest.get("records").and_then(Value::as_array) {
        Some(listed) => listed
            .iter()
            .filter_map(|r| Some((r.get(0)?.as_u64()? as usize, r.get(1)?.as_u64()? as usize)))
            .collect(),
        None => framer(bytes).0,
    }
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
