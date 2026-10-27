//! `DirSnapshot`: a directory of files read as a whole (a notes vault, a
//! Maildir).
//!
//! Each poll walks the tree comparing file facts with what was already read.
//! New and changed files stream into one unnamed PAX tar material, retaining
//! native file names, ownership, times, permissions, extended attributes, ACLs
//! and sparse ranges. The manifest indexes the stored member data offsets.
//! One member is one record; sparse records expand before parsing and replay.
//!
//! When a walk completes without reaching the part size, files read before but
//! not seen now are recorded as removed: a member holding a short marker line,
//! so each has its own record location and the parser reads the removal. A
//! source whose records are not identified by their path (a Maildir message
//! by its Message-ID) asks the driver to record the removal at the source
//! instead (`removed_at_source`): the reader remembers where each file was
//! last stored, and the file's reading there gets a newer reading, removed.
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
use sinex_model::{
    InputRef, MaterialId, ObservationFacts, RecordLocation, RevisionChange, Timestamp,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

type EntryFacts = Result<(u64, i128), String>;
type FileMemory = HashMap<PathBuf, Option<(u64, i128, [u8; 32])>>;

mod format;
mod paths;
mod restore;
mod tar;
pub(crate) use format::{decode as decode_record, validate as validate_records};

pub struct DirSnapshot {
    pub root: PathBuf,
    pub host: String,
    /// Which files to read, by display path relative to the root.
    pub select: fn(&str) -> bool,
    /// Bytes per material; a walk that reaches it continues on the next poll.
    pub part_bytes: usize,
    /// Whether a removed file's last reading is recorded as removed at the
    /// source, for parsers that cannot read the removal from its marker.
    pub removed_at_source: bool,
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
    locations: HashMap<PathBuf, RecordLocation>,
    /// The newest bundle and the file facts it records once admitted.
    held: Option<HeldSnapshot>,
    failures: HashMap<PathBuf, String>,
    /// Unstable files need a read even if their size and time return to cached values.
    retries: HashSet<PathBuf>,
    /// Paths a restart has already decided from a newer bundle.
    remembered: HashSet<PathBuf>,
    located: HashSet<PathBuf>,
}

struct HeldSnapshot {
    material: super::held::HeldMaterial,
    files: FileMemory,
    locations: HashMap<PathBuf, RecordLocation>,
    failures: HashMap<PathBuf, String>,
    retries: HashSet<PathBuf>,
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
            locator: paths::display(&self.root),
            host: self.host.clone(),
        }
    }

    pub fn poll(&self, cursor: &Value, state: &mut DirState) -> Result<Acquired, DriverError> {
        let previous: Cursor = if cursor.is_null() {
            Cursor::default()
        } else {
            serde_json::from_value(cursor.clone())
                .map_err(|e| DriverError::Cursor(e.to_string()))?
        };
        if let Some(held) = state.held.take() {
            if held.material.admitted(cursor) {
                state.files = held.files;
                state.failures = held.failures;
                state.retries = held.retries;
                state.locations = held.locations;
            } else {
                let acquired = held.material.acquired(cursor)?;
                state.held = Some(held);
                return Ok(acquired);
            }
        }
        let facts = match file_facts(&self.root) {
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
        // Sorted walk: a stable order makes materials and part cuts repeatable.
        let mut found = BTreeMap::new();
        walk(&self.root, &self.root, self.select, &mut found);
        self.poll_entries(cursor, state, previous, facts, found)
    }

    fn poll_entries(
        &self,
        cursor: &Value,
        state: &mut DirState,
        previous: Cursor,
        mut facts: ObservationFacts,
        found: BTreeMap<PathBuf, EntryFacts>,
    ) -> Result<Acquired, DriverError> {
        let now = Timestamp::now();
        let failed: Vec<_> = found
            .iter()
            .filter(|(_, facts)| facts.is_err())
            .map(|(path, _)| path.clone())
            .collect();

        let id = MaterialId::new();
        let mut bundle = tar::Archive::new(tempfile::tempfile().map_err(io)?);
        let mut length = 0usize;
        let mut next_files = state.files.clone();
        let mut next_locations = state.locations.clone();
        let mut removed = Vec::new();
        let mut next_failures = state.failures.clone();
        let mut next_retries = state.retries.clone();
        let mut records = Vec::new();
        let mut members = Vec::new();
        let mut complete = true;
        let mut seen = HashSet::new();
        let mut files_read = false;
        for (relative, entry) in &found {
            seen.insert(relative.clone());
            let display = paths::display(relative);
            let (size, changed) = match entry {
                Ok(facts) => facts,
                Err(reason) => {
                    if next_failures.get(relative) == Some(reason) {
                        continue;
                    }
                    next_failures.insert(relative.clone(), reason.clone());
                    entry_failure(
                        &mut bundle,
                        &mut length,
                        &mut members,
                        &mut records,
                        relative,
                        reason,
                    )?;
                    continue;
                }
            };
            if let Some(Some((s, m, _))) = state.files.get(relative)
                && *s == *size
                && *m == *changed
                && !state.failures.contains_key(relative)
                && !state.retries.contains(relative)
            {
                next_failures.remove(relative);
                continue;
            }
            if files_read
                && (size.saturating_add(1) > self.part_bytes.saturating_sub(length) as u64)
            {
                complete = false;
                break;
            }
            // Source failures leave this file's remembered facts unchanged, so
            // the next poll retries it. Bundle writes still fail the poll.
            let captured = tar::capture(
                &self.root.join(relative),
                relative,
                &mut bundle,
                *size,
                *changed,
            )?;
            let captured = match captured {
                Ok(Some(captured)) => captured,
                Ok(None) => {
                    next_retries.insert(relative.clone());
                    tar::rewind(&mut bundle, length)?;
                    continue;
                }
                Err(reason) => {
                    tar::rewind(&mut bundle, length)?;
                    if state.failures.get(relative) == Some(&reason) {
                        next_failures.insert(relative.clone(), reason);
                        continue;
                    }
                    next_failures.insert(relative.clone(), reason.clone());
                    entry_failure(
                        &mut bundle,
                        &mut length,
                        &mut members,
                        &mut records,
                        relative,
                        &reason,
                    )?;
                    continue;
                }
            };
            next_failures.remove(relative);
            next_retries.remove(relative);
            next_files.insert(relative.clone(), Some((*size, *changed, captured.hash)));
            let offset = captured.offset;
            let end = offset
                .checked_add(captured.length)
                .ok_or_else(|| DriverError::Io("tar exceeds addressable size".into()))?;
            members.push(json!({
                "path": display,
                "stream_offset": offset,
                "length": captured.length,
                "mtime_ns": captured.node.mtime_ns,
                "hash": blake3::Hash::from(captured.hash).to_hex().as_str(),
                "node": captured.node,
            }));
            paths::add_native(members.last_mut().expect("member added"), relative);
            length = tar::position(&mut bundle)?;
            records.push((offset, end));
            if self.removed_at_source {
                next_locations.insert(
                    relative.clone(),
                    RecordLocation {
                        material: id,
                        anchor: offset as u64,
                    },
                );
            }
            files_read = true;
        }
        if complete {
            let mut gone: Vec<PathBuf> = state
                .files
                .keys()
                .filter(|p| {
                    !seen.contains(*p) && !failed.iter().any(|prefix| p.starts_with(prefix))
                })
                .cloned()
                .collect();
            gone.sort();
            for relative in gone {
                next_files.remove(&relative);
                if let Some(location) = next_locations.remove(&relative) {
                    removed.push(location);
                }
                next_retries.remove(&relative);
                let display = paths::display(&relative);
                let marker = removed_marker(&display);
                let offset = tar::marker(
                    &mut bundle,
                    &format!(".sinex-membership/{}", members.len()),
                    &marker,
                )?;
                members.push(json!({
                    "path": display,
                    "stream_offset": offset,
                    "length": marker.len(),
                    "removed_at_ns": now.unix_ns().to_string(),
                }));
                paths::add_native(members.last_mut().expect("member added"), &relative);
                records.push((offset, offset + marker.len()));
                length = tar::position(&mut bundle)?;
            }
        }
        if complete {
            next_retries.retain(|path| {
                seen.contains(path) || failed.iter().any(|prefix| path.starts_with(prefix))
            });
            next_failures.retain(|path, _| {
                seen.contains(path) || failed.iter().any(|prefix| path.starts_with(prefix))
            });
        }
        let mut scanned = Cursor {
            last_complete_scan_ns: if complete && failed.is_empty() {
                Some(now.unix_ns())
            } else {
                previous.last_complete_scan_ns
            },
            ..previous.clone()
        };
        if records.is_empty() {
            state.files = next_files;
            state.locations = next_locations;
            state.failures = next_failures;
            state.retries = next_retries;
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
        let mut manifest = json!({
            "path": paths::display(&self.root),
            "host": self.host,
            "media_type": format::MEDIA_TYPE,
            "scan_complete": complete && failed.is_empty(),
            "members": members,
            "records": records,
            "read_at_ns": i64::try_from(now.unix_ns()).unwrap_or(i64::MAX),
        });
        paths::add_native(&mut manifest, &self.root);
        let manifest = sinex_model::Manifest::from_json(manifest);
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
        let memory = due.then(|| restore::memory_of(&next_files, &next_locations));
        let previous_part = previous.material;
        let record_end = records.last().expect("records present").1;
        let cursor_at = move |end: usize| {
            let last = end >= record_end;
            let end = end.min(record_end);
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
                stored: Some(StoredBytes::FramedFile(Arc::new(
                    bundle.into_inner().map_err(io)?,
                ))),
                records,
                part_index,
                previous_part,
                reading_order: super::reading_order(&manifest),
                manifest,
                cursor_at: Box::new(move |end| Ok(cursor_at(end))),
                aligned: false,
                removed,
                memory,
                owned_files: Vec::new(),
            }),
            cursor_without_material: Value::Null,
        };
        let held = super::held::HeldMaterial::hold(acquired, cursor)?;
        let acquired = held.acquired(cursor)?;
        state.held = Some(HeldSnapshot {
            material: held,
            files: next_files,
            locations: next_locations,
            failures: next_failures,
            retries: next_retries,
        });
        Ok(acquired)
    }
}

fn io(error: std::io::Error) -> DriverError {
    DriverError::Io(error.to_string())
}

fn entry_failure(
    bundle: &mut tar::Archive,
    length: &mut usize,
    members: &mut Vec<Value>,
    records: &mut Vec<(usize, usize)>,
    path: &Path,
    reason: &str,
) -> Result<(), DriverError> {
    let display = paths::display(path);
    let marker = json!({"path": display, "read_error": reason}).to_string();
    let offset = tar::marker(
        bundle,
        &format!(".sinex-membership/{}", members.len()),
        marker.as_bytes(),
    )?;
    *length = tar::position(bundle)?;
    members.push(json!({
        "path": display,
        "stream_offset": offset,
        "length": marker.len(),
        "read_error": reason,
    }));
    paths::add_native(members.last_mut().expect("member added"), path);
    records.push((offset, offset + marker.len()));
    Ok(())
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
    found: &mut BTreeMap<PathBuf, EntryFacts>,
) {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) => {
            found.insert(
                dir.strip_prefix(root).expect("within root").to_owned(),
                Err(error.to_string()),
            );
            return;
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                found.insert(
                    dir.strip_prefix(root).expect("within root").to_owned(),
                    Err(error.to_string()),
                );
                continue;
            }
        };
        let path = entry.path();
        let relative = path.strip_prefix(root).expect("within root").to_owned();
        let kind = match entry.file_type() {
            Ok(kind) => kind,
            Err(error) => {
                found.insert(relative, Err(error.to_string()));
                continue;
            }
        };
        if kind.is_dir() {
            walk(root, &path, select, found);
        } else if kind.is_file() && select(&paths::display(&relative)) {
            let facts = std::fs::metadata(&path)
                .map(|facts| (facts.len(), tar::change_time(&facts)))
                .map_err(|error| error.to_string());
            found.insert(relative, facts);
        }
    }
}

#[cfg(test)]
mod tests;
