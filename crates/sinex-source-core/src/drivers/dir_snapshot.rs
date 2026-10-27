//! `DirSnapshot`: a directory of files read as a whole (a notes vault, a
//! Maildir).
//!
//! Each poll walks the tree comparing file facts with what was already read.
//! New and changed files stream into an unnamed temporary bundle, stored as one
//! material: one framing byte precedes each record, outside its exact source
//! byte range. The prefix gives even empty files distinct anchors and cursor
//! ends. A manifest member describes each file
//! (path, range in the material, size, modification time). One file is one
//! record. Draining reads one record at a time; an oversized single file still
//! needs its complete decoded record in memory. Unchanged content is skipped.
//!
//! When a walk completes without reaching the part size, files read before but
//! not seen now are recorded as removed: a member holding a short marker line,
//! so each has its own record location and the parser reads the removal.
//!
//! The admitted cursor confirms cached file facts. Until its final byte commits,
//! a retry offers the same bundle again. Each member records the file's size,
//! modification time and content hash, and the reader saves its file facts
//! with the cursor on the cadence of [`super::memory_due`]. A restart takes
//! the facts from that saved memory and the bundles after it, so an unchanged
//! file is not read or stored again and a file removed while Sinex was down is
//! still recorded as removed.
use super::{Acquired, DriverError, NewMaterial, StoredBytes, file_facts};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sinex_model::{InputRef, MaterialId, ObservationFacts, RevisionChange, Timestamp};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

type FileMemory = HashMap<String, Option<(u64, i128, [u8; 32])>>;

mod format;
mod restore;
pub(crate) use format::validate as validate_records;

pub struct DirSnapshot {
    pub root: PathBuf,
    pub host: String,
    /// Which files to read, by path relative to the root.
    pub select: fn(&str) -> bool,
    /// Bytes per material; a walk that reaches it continues on the next poll.
    pub part_bytes: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    pub material: Option<MaterialId>,
    pub part_index: u32,
    /// When the last walk that saw every file finished.
    pub last_complete_scan_ns: Option<i128>,
    /// Admitted byte end within the current bundle; none before the first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub through: Option<usize>,
    /// The part at whose end the reader last saved its memory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_part: Option<u32>,
    /// Files stored or recorded as removed since that part.
    #[serde(default)]
    pub changed_since_memory: u64,
}

/// What was already read, by relative path: size, modification time, hash.
#[derive(Default)]
pub struct DirState {
    files: FileMemory,
    /// The newest bundle and the file facts it records once admitted.
    held: Option<(super::held::HeldMaterial, FileMemory)>,
    /// Paths a restart has already decided from a newer bundle.
    remembered: HashSet<String>,
}

/// The marker a removed file's member holds.
pub fn removed_marker(path: &str) -> Vec<u8> {
    let mut line = json!({"removed": path}).to_string().into_bytes();
    line.push(b'\n');
    line
}

impl DirSnapshot {
    fn input(&self) -> InputRef {
        InputRef {
            kind: "directory".into(),
            locator: self.root.display().to_string(),
            host: self.host.clone(),
        }
    }

    pub fn poll(&self, cursor: &Value, state: &mut DirState) -> Result<Acquired, DriverError> {
        self.poll_excluding(cursor, state, &[])
    }

    pub fn poll_excluding(
        &self,
        cursor: &Value,
        state: &mut DirState,
        excluded: &[PathBuf],
    ) -> Result<Acquired, DriverError> {
        let previous: Cursor = if cursor.is_null() {
            Cursor::default()
        } else {
            serde_json::from_value(cursor.clone())
                .map_err(|e| DriverError::Cursor(e.to_string()))?
        };
        if let Some((held, files)) = state.held.take() {
            if held.admitted(cursor) {
                state.files = files;
            } else {
                let acquired = held.acquired(cursor)?;
                state.held = Some((held, files));
                return Ok(acquired);
            }
        }
        let mut facts = match file_facts(&self.root) {
            Ok(f) => f,
            Err(e) => {
                return Ok(Acquired {
                    input: self.input(),
                    observation: ObservationFacts {
                        unavailable: Some(format!("{}: {e}", self.root.display())),
                        changed: false,
                        observed_at: Some(Timestamp::now()),
                        ..Default::default()
                    },
                    revision: RevisionChange::Same,
                    material: None,
                    cursor_without_material: cursor.clone(),
                });
            }
        };
        let now = Timestamp::now();
        // Sorted walk: a stable order makes materials and part cuts repeatable.
        let mut found = BTreeMap::new();
        walk(&self.root, &self.root, self.select, excluded, &mut found)?;

        let mut bundle = tempfile::tempfile().map_err(io)?;
        let mut length = 0usize;
        let mut next_files = state.files.clone();
        let mut records = Vec::new();
        let mut members = Vec::new();
        let mut complete = true;
        let mut seen = HashSet::new();
        for (relative, (size, mtime)) in &found {
            seen.insert(relative.clone());
            if let Some(Some((s, m, _))) = state.files.get(relative)
                && *s == *size
                && *m == *mtime
            {
                continue;
            }
            if length > 0
                && (size.saturating_add(1) > self.part_bytes.saturating_sub(length) as u64)
            {
                complete = false;
                break;
            }
            let mut file = File::open(self.root.join(relative))
                .map_err(|error| DriverError::Io(format!("{relative}: {error}")))?;
            let before = file.metadata().map_err(io)?;
            bundle.write_all(&[format::PREFIX]).map_err(io)?;
            let (actual_size, hash) = copy_hashed(&mut file, &mut bundle).map_err(io)?;
            let after = file.metadata().map_err(io)?;
            if !same_file(&before, &after)
                || actual_size != *size
                || metadata_mtime(&before) != *mtime
            {
                return Err(DriverError::Io(format!(
                    "{relative}: changed during capture"
                )));
            }
            let same = state
                .files
                .get(relative)
                .is_some_and(|facts| facts.as_ref().is_some_and(|(_, _, h)| *h == hash));
            next_files.insert(relative.clone(), Some((*size, *mtime, hash)));
            if same {
                bundle.set_len(length as u64).map_err(io)?;
                bundle
                    .seek(std::io::SeekFrom::Start(length as u64))
                    .map_err(io)?;
                continue;
            }
            let offset = length.checked_add(1).ok_or_else(|| {
                DriverError::Io("directory bundle exceeds addressable size".into())
            })?;
            members.push(json!({
                "path": relative,
                "stream_offset": offset,
                "length": actual_size,
                "mtime_ns": mtime.to_string(),
                "hash": blake3::Hash::from(hash).to_hex().as_str(),
            }));
            length =
                offset
                    .checked_add(usize::try_from(actual_size).map_err(|_| {
                        DriverError::Io("file exceeds addressable record size".into())
                    })?)
                    .ok_or_else(|| {
                        DriverError::Io("directory bundle exceeds addressable size".into())
                    })?;
            records.push((offset, length));
        }
        if complete {
            let mut gone: Vec<String> = state
                .files
                .keys()
                .filter(|p| !seen.contains(*p))
                .cloned()
                .collect();
            gone.sort();
            for relative in gone {
                next_files.remove(&relative);
                let marker = removed_marker(&relative);
                bundle.write_all(&[format::PREFIX]).map_err(io)?;
                let offset = length + 1;
                members.push(json!({
                    "path": relative,
                    "stream_offset": offset,
                    "length": marker.len(),
                    "removed_at_ns": now.unix_ns().to_string(),
                }));
                records.push((offset, offset + marker.len()));
                bundle.write_all(&marker).map_err(io)?;
                length = offset + marker.len();
            }
        }
        let mut scanned = Cursor {
            last_complete_scan_ns: if complete {
                Some(now.unix_ns())
            } else {
                previous.last_complete_scan_ns
            },
            ..previous.clone()
        };
        if records.is_empty() {
            state.files = next_files;
            facts.changed = false;
            return Ok(Acquired {
                input: self.input(),
                observation: facts,
                revision: RevisionChange::Same,
                material: None,
                cursor_without_material: serde_json::to_value(scanned).expect("cursor serializes"),
            });
        }
        // Every file read by one walk is as new as the walk; the records are
        // listed so a replay or an excision finds them in the bundle.
        let manifest = sinex_model::Manifest::from_json(json!({
            "path": self.root.display().to_string(),
            "host": self.host,
            "media_type": format::MEDIA_TYPE,
            "scan_complete": complete,
            "members": members,
            "records": records,
            "read_at_ns": i64::try_from(now.unix_ns()).unwrap_or(i64::MAX),
        }));
        let id = MaterialId::new();
        let part_index = if previous.material.is_some() {
            previous.part_index + 1
        } else {
            0
        };
        let changed = previous
            .changed_since_memory
            .saturating_add(members.len() as u64);
        let due = super::memory_due(
            part_index,
            previous.memory_part,
            changed,
            next_files.len() as u64,
        );
        scanned.memory_part = if due {
            Some(part_index)
        } else {
            previous.memory_part
        };
        scanned.changed_since_memory = if due { 0 } else { changed };
        let memory = due.then(|| restore::memory_of(&next_files));
        let previous_part = previous.material;
        let cursor_at = move |end: usize| {
            let last = end == length;
            serde_json::to_value(Cursor {
                material: Some(id),
                part_index,
                through: Some(end),
                last_complete_scan_ns: if last {
                    scanned.last_complete_scan_ns
                } else {
                    previous.last_complete_scan_ns
                },
                memory_part: if last {
                    scanned.memory_part
                } else {
                    previous.memory_part
                },
                changed_since_memory: if last {
                    scanned.changed_since_memory
                } else {
                    previous.changed_since_memory
                },
            })
            .expect("cursor serializes")
        };
        let acquired = Acquired {
            input: self.input(),
            observation: facts,
            revision: RevisionChange::Snapshot,
            material: Some(NewMaterial {
                id,
                bytes: Vec::new(),
                stored: Some(StoredBytes::FramedFile(Arc::new(bundle))),
                records,
                part_index,
                previous_part,
                reading_order: super::reading_order(&manifest),
                manifest,
                cursor_at: Box::new(move |end| Ok(cursor_at(end))),
                aligned: false,
                removed: Vec::new(),
                memory,
                owned_files: Vec::new(),
            }),
            cursor_without_material: Value::Null,
        };
        let held = super::held::HeldMaterial::hold(acquired, cursor)?;
        let acquired = held.acquired(cursor)?;
        state.held = Some((held, next_files));
        Ok(acquired)
    }
}

fn io(error: std::io::Error) -> DriverError {
    DriverError::Io(error.to_string())
}

fn copy_hashed(input: &mut File, output: &mut File) -> std::io::Result<(u64, [u8; 32])> {
    let mut hash = blake3::Hasher::new();
    let mut length = 0u64;
    let mut bytes = [0; 64 * 1024];
    loop {
        let count = match input.read(&mut bytes) {
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if count == 0 {
            return Ok((length, *hash.finalize().as_bytes()));
        }
        output.write_all(&bytes[..count])?;
        hash.update(&bytes[..count]);
        length += count as u64;
    }
}

fn metadata_mtime(metadata: &std::fs::Metadata) -> i128 {
    use std::os::unix::fs::MetadataExt;
    i128::from(metadata.mtime()) * 1_000_000_000 + i128::from(metadata.mtime_nsec())
}

fn same_file(before: &std::fs::Metadata, after: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    before.dev() == after.dev()
        && before.ino() == after.ino()
        && before.len() == after.len()
        && before.mtime() == after.mtime()
        && before.mtime_nsec() == after.mtime_nsec()
        && before.ctime() == after.ctime()
        && before.ctime_nsec() == after.ctime_nsec()
}

fn walk(
    root: &Path,
    dir: &Path,
    select: fn(&str) -> bool,
    excluded: &[PathBuf],
    found: &mut BTreeMap<String, (u64, i128)>,
) -> Result<(), DriverError> {
    let entries =
        std::fs::read_dir(dir).map_err(|e| DriverError::Io(format!("{}: {e}", dir.display())))?;
    for entry in entries {
        let entry = entry.map_err(io)?;
        let kind = entry.file_type().map_err(io)?;
        let path = entry.path();
        if excluded.iter().any(|root| {
            std::fs::canonicalize(&path)
                .unwrap_or_else(|_| path.clone())
                .starts_with(root)
        }) {
            continue;
        }
        if kind.is_dir() {
            walk(root, &path, select, excluded, found)?;
        } else if kind.is_file() {
            let Ok(relative) = path.strip_prefix(root) else {
                continue;
            };
            let relative = relative.to_string_lossy().into_owned();
            if !select(&relative) {
                continue;
            }
            let facts = file_facts(&path).map_err(io)?;
            found.insert(
                relative,
                (facts.size.unwrap_or(0), facts.mtime_ns.unwrap_or(0)),
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
