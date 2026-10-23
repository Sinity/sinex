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
    RevisionChange, RunId, SourceProgress,
};

pub mod api_cursor;
pub mod append_file;
pub mod dir_snapshot;
pub mod export_archive;
pub mod live_stream;
pub mod media;
pub mod producer;
pub mod sqlite_rows;

/// Where drivers put a material's bytes: the content store. Implemented by the
/// daemon over `sinex-store`'s `Blobs`, so this crate does no database IO.
pub trait MaterialSink: Send + Sync {
    /// Store `bytes` and return their name, `blake3:<hex>`.
    fn put(&self, bytes: &[u8]) -> Result<String, String>;

    /// Store a file's bytes without holding the complete file in memory.
    fn put_file(&self, path: &std::path::Path) -> Result<String, String>;
}

#[derive(Debug, thiserror::Error)]
pub enum DriverError {
    #[error("capture was cancelled")]
    Cancelled,
    #[error("material: {0}")]
    InvalidMaterial(String),
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

/// Bytes already retained independently from the current decoded records.
#[derive(Clone, Debug)]
pub enum StoredBytes {
    File(std::path::PathBuf),
    /// Authoritative framed bytes, staged in an unnamed owned file. The
    /// decoded record body is read from this file, not `NewMaterial::bytes`.
    FramedFile(std::sync::Arc<std::fs::File>),
    Blob {
        name: String,
        length: u64,
    },
}

pub struct NewMaterial {
    pub id: MaterialId,
    /// In-memory decoded record bytes. `FramedFile` supplies these ranges
    /// from its owned file instead; its `bytes` vector stays empty.
    pub bytes: Vec<u8>,
    /// Retained backing. `File` and `Blob` may hold an original export while
    /// `bytes` holds its decoded stream. `FramedFile` supplies both the stored
    /// body and the record ranges, without a second in-memory copy.
    pub stored: Option<StoredBytes>,
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

impl NewMaterial {
    fn decoded_len(&self, retained_len: u64) -> usize {
        match self.stored {
            Some(StoredBytes::FramedFile(_)) => retained_len as usize,
            _ => self.bytes.len(),
        }
    }

    pub(crate) fn record_bytes(&self) -> crate::drain::RecordBytes<'_> {
        match &self.stored {
            Some(StoredBytes::FramedFile(file)) => crate::drain::RecordBytes::File(file),
            _ => self.bytes.as_slice().into(),
        }
    }
}

/// Turn one poll into admission batches. The material's bytes go to the
/// content store first; the first batch registers the material, and every
/// batch carries the cursor and carry after its last record, so a crash
/// between batches resumes exactly. Collects [`Prepared::batches`]; a caller
/// that submits as it goes uses [`prepare`] and the iterator, so memory holds
/// one batch rather than a whole material's events.
pub fn batches(
    source: &dyn Source,
    binding: &str,
    run: RunId,
    acquired: Acquired,
    carry: Value,
    sink: &dyn MaterialSink,
    limits: Limits,
) -> Result<Vec<Batch>, DriverError> {
    let prepared = prepare(source, binding, run, acquired, carry, sink, limits)?;
    prepared.batches()?.collect()
}

/// One poll with its material stored, ready to be drained into batches.
pub struct Prepared<'s> {
    source: &'s dyn Source,
    binding: String,
    run: RunId,
    input: InputRef,
    observation: ObservationFacts,
    revision: RevisionChange,
    cursor_unchanged: Value,
    carry: Value,
    limits: Limits,
    material: Option<(NewMaterial, MaterialRef)>,
}

/// Store the poll's material (if any) and return it ready to drain.
pub fn prepare<'s>(
    source: &'s dyn Source,
    binding: &str,
    run: RunId,
    acquired: Acquired,
    carry: Value,
    sink: &dyn MaterialSink,
    limits: Limits,
) -> Result<Prepared<'s>, DriverError> {
    let spec = source.spec();
    let reset = acquired
        .material
        .as_ref()
        .map(|m| sinex_model::MaterialRole::of(&m.manifest))
        .transpose()
        .map_err(|e| DriverError::InvalidMaterial(e.to_string()))?
        == Some(sinex_model::MaterialRole::OriginalArchive);
    let carry = if reset { Value::Null } else { carry };
    let material = match acquired.material {
        None => None,
        Some(m) => {
            let (blob, length) = match &m.stored {
                Some(StoredBytes::File(path)) => {
                    let length = std::fs::metadata(path)
                        .map_err(|e| DriverError::Io(format!("{}: {e}", path.display())))?
                        .len();
                    (sink.put_file(path).map_err(DriverError::Store)?, length)
                }
                Some(StoredBytes::FramedFile(file)) => {
                    use std::os::fd::AsRawFd;
                    let length = file
                        .metadata()
                        .map_err(|e| DriverError::Io(e.to_string()))?
                        .len();
                    // Linux exposes the live unnamed file without giving it a crash-leaking
                    // directory entry. The Arc retains it through storage and draining.
                    let path =
                        std::path::PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()));
                    (sink.put_file(&path).map_err(DriverError::Store)?, length)
                }
                Some(StoredBytes::Blob { name, length }) => (name.clone(), *length),
                None => (
                    sink.put(&m.bytes).map_err(DriverError::Store)?,
                    m.bytes.len() as u64,
                ),
            };
            let reference = MaterialRef {
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
            Some((m, reference))
        }
    };
    Ok(Prepared {
        source,
        binding: binding.to_owned(),
        run,
        input: acquired.input,
        observation: acquired.observation,
        revision: acquired.revision,
        cursor_unchanged: acquired.cursor_unchanged,
        carry,
        limits,
        material,
    })
}

impl Prepared<'_> {
    /// The decoded record body length, if the poll produced a material.
    pub fn material_len(&self) -> Option<usize> {
        self.material
            .as_ref()
            .map(|(material, reference)| material.decoded_len(reference.length))
    }

    fn progress(
        &self,
        material: Option<MaterialRef>,
        cursor: Value,
        carry: Value,
        observation: ObservationFacts,
        revision: RevisionChange,
    ) -> BatchKind {
        BatchKind::Source(Box::new(SourceProgress {
            binding: self.binding.clone(),
            input: self.input.clone(),
            observation,
            revision,
            material,
            cursor,
            carry,
        }))
    }

    /// The admission batches, produced one at a time as they are taken.
    pub fn batches(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<Batch, DriverError>> + Send + '_>, DriverError> {
        let Some((m, material)) = &self.material else {
            let batch = Batch {
                run: self.run,
                kind: self.progress(
                    None,
                    self.cursor_unchanged.clone(),
                    self.carry.clone(),
                    self.observation.clone(),
                    self.revision,
                ),
                records: vec![],
                archive: vec![],
            };
            return Ok(Box::new(std::iter::once(Ok(batch))));
        };
        let spec = self.source.spec();
        let limits = if m.aligned {
            Limits {
                max_events: usize::MAX,
                max_bytes: usize::MAX,
            }
        } else {
            self.limits
        };
        let mut chunks = self
            .source
            .drainer(DrainInput {
                source: spec.name,
                class: spec.class,
                material: m.id,
                bytes: m.record_bytes(),
                records: &m.records,
                carry: self.carry.clone(),
                reading_order: &*m.reading_order,
                limits,
                manifest: &m.manifest,
            })
            .map_err(|e| DriverError::Carry(e.0))?;
        // Only the last chunk ends at the last record's end. Told this way,
        // not by peeking, so the next chunk is not built before this one is
        // taken.
        let last_end = m.records.last().map_or(0, |r| r.1);
        let mut first = true;
        Ok(Box::new(std::iter::from_fn(move || {
            let chunk = match chunks.next()? {
                Ok(chunk) => chunk,
                Err(error) => return Some(Err(DriverError::Io(error.to_string()))),
            };
            // After the last chunk the whole material is read, records
            // skipped as already read included.
            let end = if chunk.end >= last_end {
                m.decoded_len(material.length)
            } else {
                chunk.end
            };
            let (observation, revision) = if first {
                (self.observation.clone(), self.revision)
            } else {
                (
                    ObservationFacts {
                        changed: false,
                        ..self.observation.clone()
                    },
                    RevisionChange::Same,
                )
            };
            let batch = Batch {
                run: self.run,
                kind: self.progress(
                    first.then(|| material.clone()),
                    (m.cursor_at)(end),
                    chunk.carry,
                    observation,
                    revision,
                ),
                records: chunk.outcomes,
                archive: if first && !m.vanished.is_empty() {
                    vec![sinex_model::ArchiveRequest::RemovedAtSource {
                        locations: m.vanished.clone(),
                    }]
                } else {
                    vec![]
                },
            };
            first = false;
            Some(Ok(batch))
        })))
    }
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
        None => framer.frame(bytes).0,
    }
}

/// Stored explicit ranges take precedence over legacy source framing.
pub fn stored_records_from(
    manifest: &Value,
    reader: &mut dyn std::io::Read,
    framer: crate::Framer,
) -> std::io::Result<Vec<(usize, usize)>> {
    if manifest.get("records").and_then(Value::as_array).is_some() {
        Ok(stored_records(manifest, &[], framer))
    } else {
        framer.ranges(reader)
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

#[cfg(test)]
mod tests;
