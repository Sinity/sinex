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

/// Bytes before the offset whose hash shows the prefix is unchanged.
const WINDOW: u64 = 4096;
/// The most bytes one poll reads past its cursor (more only for a single
/// record longer than this): a file of any size is read in bounded parts.
const PART: u64 = 64 << 20;
mod tail;
pub use tail::AppendTail;

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
    pub window: Option<String>,
    /// A rewrite seen before any of its records was complete; the next material
    /// records it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unrecorded_rewrite: Option<RevisionChange>,
    pub material: Option<MaterialId>,
    pub part_index: u32,
    /// How many bytes of `material` the stored readings cover. A restart
    /// rebuilds the driver's state from those bytes only: the rest of the
    /// material may never have been admitted.
    #[serde(default)]
    pub through: usize,
}

/// How much of the current revision the state keeps: its newest records, at
/// most this many record bytes and this many records (a record costs about
/// 100 bytes of memory whatever its length, so short lines are bounded by
/// count). A rewrite is aligned against that tail; a record older than it
/// that a rewrite drops is not recognized as read before (rewrites such as a
/// shell's duplicate expiry touch recent history), and one it keeps is read
/// again as a repeat. Bounds the state and a restart's rebuild alike.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TailBounds {
    pub bytes: usize,
    pub records: usize,
}

impl Default for TailBounds {
    fn default() -> Self {
        Self {
            bytes: 16 << 20,
            records: 200_000,
        }
    }
}

/// The records of the current revision's tail, by content hash, with where
/// each occurrence was read: what a rewrite is aligned against. Kept for the
/// life of the source task and rebuilt from the stored materials after a
/// restart ([`AppendState::rebuild`]); bounded by its [`TailBounds`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AppendState {
    bounds: TailBounds,
    records: HashMap<[u8; 32], std::collections::VecDeque<RecordLocation>>,
    /// Every held record in reading order, with its length, so the oldest
    /// is dropped first when the tail grows past a bound.
    order: std::collections::VecDeque<([u8; 32], RecordLocation, usize)>,
    bytes: usize,
}

impl AppendState {
    /// An empty state kept within `bounds`.
    pub fn bounded(bounds: TailBounds) -> Self {
        Self {
            bounds,
            ..Self::default()
        }
    }

    /// The state after reading `materials` (the current revision's parts, in
    /// order) with `framer`.
    pub fn rebuild<'a>(
        materials: impl IntoIterator<Item = (MaterialId, &'a [u8])>,
        framer: Framer,
    ) -> Self {
        let mut state = Self::default();
        for (material, bytes) in materials {
            for (s, e) in framer.frame(bytes).0 {
                state.insert(&bytes[s..e], material, s);
            }
        }
        state
    }

    fn insert(&mut self, record: &[u8], material: MaterialId, anchor: usize) {
        self.insert_digest(
            *blake3::hash(record).as_bytes(),
            record.len(),
            material,
            anchor,
        );
    }

    pub(crate) fn insert_digest(
        &mut self,
        hash: [u8; 32],
        length: usize,
        material: MaterialId,
        anchor: usize,
    ) {
        let location = RecordLocation {
            material,
            anchor: anchor as u64,
        };
        self.records.entry(hash).or_default().push_back(location);
        self.order.push_back((hash, location, length));
        self.bytes = self.bytes.saturating_add(length);
        while self.order.len() > self.bounds.records
            || (self.bytes > self.bounds.bytes && self.order.len() > 1)
        {
            let (hash, location, n) = self.order.pop_front().expect("not empty");
            self.bytes -= n;
            if let Some(held) = self.records.get_mut(&hash) {
                held.retain(|l| *l != location);
                if held.is_empty() {
                    self.records.remove(&hash);
                }
            }
        }
    }

    /// Whether an equal record was held, consuming one occurrence of it.
    fn take(&mut self, record: &[u8]) -> bool {
        match self.records.get_mut(blake3::hash(record).as_bytes()) {
            Some(held) if !held.is_empty() => {
                held.pop_front();
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
        self.records.values().all(|locations| locations.is_empty())
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

    pub fn poll(&self, cursor: &Value, state: &mut AppendState) -> Result<Acquired, DriverError> {
        self.poll_in_parts(cursor, state, PART)
    }

    /// [`poll`](Self::poll) reading at most `part` bytes past the cursor.
    pub fn poll_in_parts(
        &self,
        cursor: &Value,
        state: &mut AppendState,
        part: u64,
    ) -> Result<Acquired, DriverError> {
        let previous: Cursor = if cursor.is_null() {
            Cursor::default()
        } else {
            serde_json::from_value(cursor.clone())
                .map_err(|e| DriverError::Cursor(e.to_string()))?
        };
        if previous.unrecorded_rewrite.is_some_and(|kind| {
            !matches!(
                kind,
                RevisionChange::Rewritten | RevisionChange::Replaced | RevisionChange::Truncated
            )
        }) {
            return Err(DriverError::Cursor(
                "invalid unrecorded rewrite kind".into(),
            ));
        }
        if !cursor.is_null() && previous.window.is_none() {
            return Err(DriverError::Cursor(
                "the append cursor has no prefix window".into(),
            ));
        }
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
                    cursor_without_material: cursor.clone(),
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
                cursor_without_material: cursor.clone(),
            });
        }
        let mut file =
            std::fs::File::open(&self.path).map_err(|e| DriverError::Io(e.to_string()))?;
        let intact = !cursor.is_null()
            && previous.inode == inode
            && previous.offset <= size
            && Some(window_hash(&mut file, previous.offset)?) == previous.window;
        let (start, revision) = if cursor.is_null() {
            (0, RevisionChange::Appended)
        } else if intact {
            (
                previous.offset,
                previous
                    .unrecorded_rewrite
                    .unwrap_or(RevisionChange::Appended),
            )
        } else if previous.inode != inode {
            (0, RevisionChange::Replaced)
        } else if size < previous.offset {
            (0, RevisionChange::Truncated)
        } else {
            (0, RevisionChange::Rewritten)
        };
        file.seek(SeekFrom::Start(start))
            .map_err(|e| DriverError::Io(e.to_string()))?;
        // Read in parts of at most PART bytes, ending on a record boundary,
        // so a file of any size is read in bounded state. A record longer
        // than a part grows the read until it completes; no record is
        // refused. A rewrite is read whole: aligning it against the previous
        // revision needs every record to tell which ones removed.
        let restart = start == 0 && !cursor.is_null();
        let available = size - start;
        let mut want = if restart {
            available
        } else {
            available.min(part.max(1))
        };
        let mut data = Vec::new();
        let (records, consumed) = loop {
            data.clear();
            file.seek(SeekFrom::Start(start))
                .map_err(|e| DriverError::Io(e.to_string()))?;
            (&mut file)
                .take(want)
                .read_to_end(&mut data)
                .map_err(|e| DriverError::Io(e.to_string()))?;
            let framed = self.framer.frame(&data);
            if framed.1 > 0 || want >= available {
                break framed;
            }
            want = (want * 2).min(available);
        };
        let read_to_end = start + consumed as u64 == size || want >= available;
        let restarted = start == 0 && !cursor.is_null();
        let base = Cursor {
            offset: start,
            size,
            mtime_ns: mtime,
            inode,
            window: None,
            material: previous.material,
            part_index: previous.part_index,
            through: previous.through,
            unrecorded_rewrite: if restarted
                && matches!(
                    revision,
                    RevisionChange::Rewritten
                        | RevisionChange::Replaced
                        | RevisionChange::Truncated
                ) {
                Some(revision)
            } else {
                previous.unrecorded_rewrite
            },
        };
        if consumed == 0 {
            // A fresh poll has covered a known empty prefix, even when the file
            // contains only a partial record.
            let window = if !cursor.is_null() && start == previous.offset {
                previous.window.clone()
            } else {
                Some(blake3::hash(b"").to_hex().to_string())
            };
            let unchanged = Cursor { window, ..base };
            return Ok(Acquired {
                input: self.input(),
                observation: facts,
                revision: RevisionChange::Same,
                material: None,
                cursor_without_material: serde_json::to_value(unchanged)
                    .expect("cursor serializes"),
            });
        }
        data.truncate(consumed);
        // A restart from the beginning (rewritten or truncated) is aligned
        // against what the previous revision held; then the state becomes
        // the new revision's records.
        let restarted_records = start == 0 && !cursor.is_null();
        let id = MaterialId::new();
        let (records, aligned, removed) = if restarted_records {
            let mut previous = std::mem::take(state);
            let aligning = !previous.is_empty();
            let mut fresh = Vec::new();
            for &(s, e) in &records {
                if !previous.take(&data[s..e]) {
                    fresh.push((s, e));
                }
                state.insert(&data[s..e], id, s);
            }
            (fresh, aligning, previous.remaining())
        } else {
            for &(s, e) in &records {
                state.insert(&data[s..e], id, s);
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
        // Parts are capped at PART bytes, so this copy is bounded. Each call
        // hashes only the WINDOW bytes before `end`, never the whole prefix.
        let tail_source = bytes.clone();
        let material_len = bytes.len();
        let cursor_at = move |end: usize| -> Value {
            let window = WINDOW as usize;
            let mut w = Vec::with_capacity(window);
            if end < window {
                w.extend_from_slice(&head[head.len().saturating_sub(window - end)..]);
            }
            w.extend_from_slice(&tail_source[end.saturating_sub(window)..end]);
            let w = w.as_slice();
            serde_json::to_value(Cursor {
                offset: start + end as u64,
                // Only a cursor that covers everything this poll read may
                // skip an unchanged file. One left between two batches of the
                // same read has records after it, so it records no file size
                // and the next poll reads on from its offset.
                size: if end == material_len && read_to_end {
                    size
                } else {
                    0
                },
                mtime_ns: mtime,
                inode,
                window: Some(blake3::hash(w).to_hex().to_string()),
                material: Some(id),
                part_index,
                through: end,
                unrecorded_rewrite: None,
            })
            .expect("cursor serializes")
        };
        let mut manifest = sinex_model::Manifest::from_json(
            json!({"path": self.path.display().to_string(), "host": self.host, "file_offset": start, "media_type": "text/plain"}),
        );
        if aligned {
            manifest.records = Some(records.clone());
        }
        Ok(Acquired {
            input: self.input(),
            observation: facts,
            revision,
            material: Some(NewMaterial {
                id,
                bytes,
                stored: None,
                records,
                part_index,
                previous_part,
                reading_order: super::reading_order(&manifest),
                manifest,
                cursor_at: Box::new({
                    let cursor = cursor_at;
                    move |end| Ok(cursor(end))
                }),
                aligned,
                removed,
                memory: None,
                owned_files: Vec::new(),
            }),
            cursor_without_material: Value::Null,
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
pub(crate) mod tests;
