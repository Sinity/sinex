//! `AppendFile`: a file that grows by appending (shell histories, journal
//! exports, logs). An unchanged stat tuple skips the file without reading it.
//! Otherwise the window before the stored offset is hashed: if it still
//! matches, only the appended complete records are read, as a new part; if it
//! does not, the file was rewritten and is read again as a new revision.
//!
//! A rewritten file (zsh expiring duplicates, an editor saving the whole
//! file) is aligned against the previous revision: records whose bytes the
//! previous revision already held were read before, and only the others are
//! read as new. The file has no record IDs, so without this every record
//! would come back as a new event at a new location. Records the previous
//! revision held and the rewrite dropped are reported by where they were read,
//! and become removed-at-source readings.
use super::{Acquired, DriverError, NewMaterial, file_facts};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sinex_model::{InputRef, MaterialId, ObservationFacts, RecordLocation, RevisionChange};
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

/// Bytes before the offset whose hash proves the prefix is unchanged.
const WINDOW: u64 = 4096;

pub use crate::registry::Framer;

pub struct AppendFile {
    pub path: PathBuf,
    pub host: String,
    pub framer: Framer,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    pub offset: u64,
    pub size: u64,
    pub mtime_ns: i128,
    pub inode: u64,
    /// BLAKE3 of the up-to-`WINDOW` bytes before `offset`.
    pub window: String,
    pub material: Option<MaterialId>,
    pub part_index: u32,
    /// How many bytes of `material` the stored readings cover. A restart
    /// rebuilds the driver's memory from those bytes only: the rest of the
    /// material may never have been admitted.
    #[serde(default)]
    pub through: usize,
}

/// The records of the current revision, by content hash, with where each
/// occurrence was read: what a rewrite is aligned against. Kept for the life
/// of the source task and rebuilt from the stored materials after a restart
/// ([`AppendMemory::rebuild`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AppendMemory {
    records: HashMap<[u8; 32], Vec<RecordLocation>>,
}

impl AppendMemory {
    /// The memory after reading `materials` (the current revision's parts, in
    /// order) with `framer`.
    pub fn rebuild<'a>(
        materials: impl IntoIterator<Item = (MaterialId, &'a [u8])>,
        framer: Framer,
    ) -> Self {
        let mut memory = Self::default();
        for (material, bytes) in materials {
            for (s, e) in framer(bytes).0 {
                memory.insert(&bytes[s..e], material, s);
            }
        }
        memory
    }

    fn insert(&mut self, record: &[u8], material: MaterialId, anchor: usize) {
        self.records
            .entry(*blake3::hash(record).as_bytes())
            .or_default()
            .push(RecordLocation {
                material,
                anchor: anchor as u64,
            });
    }

    /// Whether an equal record was held, consuming one occurrence of it.
    fn take(&mut self, record: &[u8]) -> bool {
        match self.records.get_mut(blake3::hash(record).as_bytes()) {
            Some(held) if !held.is_empty() => {
                held.remove(0);
                true
            }
            _ => false,
        }
    }

    /// Where the records still held were read: after a rewrite is aligned,
    /// the ones it dropped.
    fn remaining(self) -> Vec<RecordLocation> {
        let mut out: Vec<RecordLocation> = self.records.into_values().flatten().collect();
        out.sort_by_key(|l| (l.material, l.anchor));
        out
    }

    pub fn is_empty(&self) -> bool {
        self.records.values().all(Vec::is_empty)
    }
}

impl AppendFile {
    fn input(&self) -> InputRef {
        InputRef {
            kind: "file".into(),
            locator: self.path.display().to_string(),
            host: self.host.clone(),
        }
    }

    pub fn poll(&self, cursor: &Value, memory: &mut AppendMemory) -> Result<Acquired, DriverError> {
        let previous: Cursor = if cursor.is_null() {
            Cursor::default()
        } else {
            serde_json::from_value(cursor.clone())
                .map_err(|e| DriverError::Cursor(e.to_string()))?
        };
        let facts = match file_facts(&self.path) {
            Ok(f) => f,
            Err(e) => {
                return Ok(Acquired {
                    input: self.input(),
                    observation: ObservationFacts {
                        unavailable: Some(format!("{}: {e}", self.path.display())),
                        changed: false,
                        observed_at: Some(sinex_model::Timestamp::now()),
                        ..Default::default()
                    },
                    revision: RevisionChange::Same,
                    material: None,
                    cursor_unchanged: cursor.clone(),
                });
            }
        };
        let size = facts.size.unwrap_or(0);
        let mtime = facts.mtime_ns.unwrap_or(0);
        let inode = facts.inode.unwrap_or(0);
        if previous.size == size
            && previous.mtime_ns == mtime
            && previous.inode == inode
            && previous.offset <= size
            && !cursor.is_null()
        {
            return Ok(Acquired {
                input: self.input(),
                observation: ObservationFacts {
                    changed: false,
                    ..facts
                },
                revision: RevisionChange::Same,
                material: None,
                cursor_unchanged: cursor.clone(),
            });
        }
        let mut file =
            std::fs::File::open(&self.path).map_err(|e| DriverError::Io(e.to_string()))?;
        let intact = !cursor.is_null()
            && previous.inode == inode
            && previous.offset <= size
            && window_hash(&mut file, previous.offset)? == previous.window;
        let (start, revision) = if cursor.is_null() {
            (0, RevisionChange::Appended)
        } else if intact {
            (previous.offset, RevisionChange::Appended)
        } else if size < previous.offset {
            (0, RevisionChange::Truncated)
        } else {
            (0, RevisionChange::Rewritten)
        };
        file.seek(SeekFrom::Start(start))
            .map_err(|e| DriverError::Io(e.to_string()))?;
        let mut data = Vec::with_capacity((size - start) as usize);
        (&mut file)
            .take(size - start)
            .read_to_end(&mut data)
            .map_err(|e| DriverError::Io(e.to_string()))?;
        let (records, consumed) = (self.framer)(&data);
        let restarted = start == 0 && !cursor.is_null();
        let base = Cursor {
            offset: start,
            size,
            mtime_ns: mtime,
            inode,
            window: String::new(),
            material: if restarted { None } else { previous.material },
            part_index: if restarted { 0 } else { previous.part_index },
            through: if restarted { 0 } else { previous.through },
        };
        if consumed == 0 {
            // Nothing complete yet; keep the old offset and window, note the new facts.
            let window = if start == previous.offset {
                previous.window.clone()
            } else {
                blake3::hash(b"").to_hex().to_string()
            };
            let unchanged = Cursor { window, ..base };
            return Ok(Acquired {
                input: self.input(),
                observation: facts,
                revision: RevisionChange::Same,
                material: None,
                cursor_unchanged: serde_json::to_value(unchanged).expect("cursor serializes"),
            });
        }
        data.truncate(consumed);
        // A restart from the beginning (rewritten or truncated) is aligned
        // against what the previous revision held; then the memory becomes
        // the new revision's records.
        let restarted_records = start == 0 && !cursor.is_null();
        let id = MaterialId::new();
        let (records, aligned, vanished) = if restarted_records {
            let mut previous = std::mem::take(memory);
            let aligning = !previous.is_empty();
            let mut fresh = Vec::new();
            for &(s, e) in &records {
                if !previous.take(&data[s..e]) {
                    fresh.push((s, e));
                }
                memory.insert(&data[s..e], id, s);
            }
            (fresh, aligning, previous.remaining())
        } else {
            for &(s, e) in &records {
                memory.insert(&data[s..e], id, s);
            }
            (records, false, Vec::new())
        };
        let part_index = if base.material.is_some() {
            base.part_index + 1
        } else {
            0
        };
        let previous_part = base.material;
        let bytes = data;
        // The window for any cursor inside this material: hash the bytes
        // before that point, taking the tail of the old window when the
        // material alone is shorter.
        let mut head = Vec::new();
        if start > 0 {
            let from = start.saturating_sub(WINDOW);
            file.seek(SeekFrom::Start(from))
                .map_err(|e| DriverError::Io(e.to_string()))?;
            (&mut file)
                .take(start - from)
                .read_to_end(&mut head)
                .map_err(|e| DriverError::Io(e.to_string()))?;
        }
        let tail_source = bytes.clone();
        let cursor_at = move |end: usize| -> Value {
            let mut joined = head.clone();
            joined.extend_from_slice(&tail_source[..end]);
            let w = &joined[joined.len().saturating_sub(WINDOW as usize)..];
            serde_json::to_value(Cursor {
                offset: start + end as u64,
                // Only a cursor that covers everything this poll read may
                // skip an unchanged file. One left between two batches of the
                // same read has records after it, so it records no file size
                // and the next poll reads on from its offset.
                size: if end == tail_source.len() { size } else { 0 },
                mtime_ns: mtime,
                inode,
                window: blake3::hash(w).to_hex().to_string(),
                material: Some(id),
                part_index,
                through: end,
            })
            .expect("cursor serializes")
        };
        let mut manifest = json!({"path": self.path.display().to_string(), "host": self.host, "file_offset": start, "media_type": "text/plain"});
        if aligned {
            manifest["records"] = json!(records);
        }
        Ok(Acquired {
            input: self.input(),
            observation: facts,
            revision,
            material: Some(NewMaterial {
                id,
                bytes,
                stored_file: None,
                records,
                part_index,
                previous_part,
                reading_order: super::reading_order(&manifest),
                manifest,
                cursor_at: Box::new(cursor_at),
                aligned,
                vanished,
            }),
            cursor_unchanged: Value::Null,
        })
    }
}

fn window_hash(file: &mut std::fs::File, offset: u64) -> Result<String, DriverError> {
    let from = offset.saturating_sub(WINDOW);
    file.seek(SeekFrom::Start(from))
        .map_err(|e| DriverError::Io(e.to_string()))?;
    let mut buf = Vec::new();
    file.take(offset - from)
        .read_to_end(&mut buf)
        .map_err(|e| DriverError::Io(e.to_string()))?;
    Ok(blake3::hash(&buf).to_hex().to_string())
}

/// Lines ending in `\n`.
pub fn lines(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    let mut out = Vec::new();
    let mut start = 0;
    for (i, b) in bytes.iter().enumerate() {
        if *b == b'\n' {
            out.push((start, i + 1));
            start = i + 1;
        }
    }
    (out, start)
}

#[cfg(test)]
mod tests;
