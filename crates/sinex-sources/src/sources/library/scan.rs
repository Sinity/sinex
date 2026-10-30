//! Scans of a library root, its changed paths and completed walks.
use super::SCAN;
use super::files::{Digests, digest, dimensions, ffprobe};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sinex_model::event_types::library::FileCopy;
use sinex_model::{
    InputRef, MaterialId, ObservationFacts, ReadingOrder, RevisionChange, Timestamp,
};
use sinex_source_core::drivers::{Acquired, DriverError, NewMaterial, file_facts};
use sinex_source_core::registry::DeclaredSource;
use sinex_source_core::{Parser, RecordError};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};

/// One scanned file as the driver describes it: the stored record.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct ScannedFile {
    pub item_key: String,
    pub host: String,
    pub root: String,
    pub path: String,
    pub content: String,
    pub size: u64,
    pub sha256: String,
    pub md5: String,
    pub media_type: String,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub duration_ms: Option<u64>,
    pub stored: bool,
    pub device: Option<u64>,
    pub inode: Option<u64>,
    /// Decimal nanoseconds, as text (beyond JSON's safe integers).
    pub mtime_ns: Option<String>,
    pub moved_from: Option<String>,
    pub move_candidates: Vec<String>,
    pub previous_content: Option<String>,
    pub vanished_at_ns: Option<String>,
}

/// File changes and the end of a completed walk share its stored material.
#[derive(Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case")]
enum ScanLine {
    File(Box<ScannedFile>),
    Unreadable {
        path: String,
        reason: String,
    },
    Complete {
        host: String,
        root: String,
        only: Option<String>,
        unreadable: Vec<String>,
    },
}

fn nanos(text: &Option<String>) -> Option<Timestamp> {
    text.as_deref()
        .and_then(|s| s.parse::<i128>().ok())
        .and_then(|ns| Timestamp::from_unix_ns(ns).ok())
}

pub struct ScanParser;

impl Parser for ScanParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = FileCopy;

    /// Location and file facts do not change the hash of the captured bytes.
    const VOLATILE: &'static [&'static str] = &[
        "host",
        "root",
        "path",
        "device",
        "inode",
        "mtime_ns",
        "moved_from",
        "previous_content",
        "stored",
    ];

    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<(Vec<FileCopy>, ()), RecordError> {
        let line: ScanLine = serde_json::from_value(Value::Object(record.clone()))
            .map_err(|e| RecordError::new(format!("not a library scan line: {e}")))?;
        let d = match line {
            ScanLine::File(d) => d,
            ScanLine::Unreadable { path, reason } => {
                return Err(RecordError::new(format!(
                    "partial library scan: {path}: {reason}"
                )));
            }
            ScanLine::Complete { .. } => return Ok((Vec::new(), ())),
        };
        let d = *d;
        let key = d.item_key.clone();
        let scanned_at = context
            .manifest
            .read_at_ns
            .and_then(|n| Timestamp::from_unix_ns(i128::from(n)).ok())
            .ok_or_else(|| RecordError::new("library scan has no acquisition time"))?;
        let scan = context
            .manifest
            .details
            .get("scan")
            .and_then(Value::as_str)
            .ok_or_else(|| RecordError::new("library scan has no identity"))?
            .to_owned();
        let part_index = context
            .manifest
            .details
            .get("part_index")
            .and_then(Value::as_u64)
            .and_then(|index| u32::try_from(index).ok())
            .ok_or_else(|| RecordError::new("library scan has no part index"))?;
        let payload = FileCopy {
            scan,
            part_index,
            item: sinex_model::event_types::library::item_id(&key),
            item_key: key,
            scanned_at,
            modified_at: nanos(&d.mtime_ns),
            vanished_at: nanos(&d.vanished_at_ns),
            host: d.host,
            root: d.root,
            path: d.path,
            content: d.content,
            size: d.size,
            sha256: d.sha256,
            md5: d.md5,
            media_type: d.media_type,
            width: d.width,
            height: d.height,
            duration_ms: d.duration_ms,
            stored: d.stored,
            device: d.device,
            inode: d.inode,
            moved_from: d.moved_from,
            move_candidates: d.move_candidates,
            previous_content: d.previous_content,
        };
        Ok((vec![payload], ()))
    }
}

pub fn scan_source() -> DeclaredSource<ScanParser> {
    DeclaredSource {
        spec: SCAN,
        parser: ScanParser,
    }
}

/// A library root and how to read it.
pub struct LibraryScan {
    pub root: PathBuf,
    pub host: String,
    /// Store the bytes in the content store (`false`: index by reference).
    pub keep_bytes: bool,
    /// Ask `ffprobe` for dimensions and duration of video and audio.
    pub probe: bool,
    /// File bytes described per poll, so one poll's material stays a sensible size
    /// and progress commits as it goes. A file larger than this is read alone.
    pub bytes_per_poll: u64,
    /// Scan only this file or directory within `root`; paths outside the
    /// selection are neither read nor reported missing.
    pub only: Option<String>,
}

/// What a scan knows about a file it already read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Known {
    pub item_key: String,
    pub sha256: String,
    pub md5: String,
    pub media_type: String,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub duration_ms: Option<u64>,
    pub stored: bool,
    pub size: u64,
    pub mtime_ns: i128,
    pub device: Option<u64>,
    pub inode: Option<u64>,
    pub content: String,
}

/// A file the walk found: relative path, size, mtime, device, inode.
type Pending = (String, u64, i128, Option<u64>, Option<u64>);

/// The scan's state: the files read, by path, and the work left from the
/// current walk. A restart rebuilds the files from the saved memory and the
/// scan parts after it, so it does not hash the whole library again.
#[derive(Default)]
pub struct ScanState {
    pub files: HashMap<String, Known>,
    queue: VecDeque<Pending>,
    /// Paths missing from the current walk, with the file identity they had.
    missing: BTreeMap<String, Known>,
    prepared: HashMap<String, Prepared>,
    moves: HashMap<String, String>,
    move_candidates: HashMap<String, Vec<String>>,
    keys: std::collections::HashSet<String>,
    unreadable: BTreeMap<String, String>,
    problems: Vec<(String, String)>,
    walk_pending: bool,
    walk_changed: bool,
    completed: bool,
    #[cfg(test)]
    files_hashed: usize,
    /// Paths a restart has already decided from a newer part.
    remembered: std::collections::HashSet<String>,
}

impl ScanState {
    #[cfg(test)]
    pub(super) fn files_hashed(&self) -> usize {
        self.files_hashed
    }
    /// What the scan knows, as saved with the cursor.
    fn memory(&self) -> std::sync::Arc<[u8]> {
        let mut files: Vec<(&String, &Known)> = self.files.iter().collect();
        files.sort_by_key(|(path, _)| *path);
        serde_json::to_vec(&files)
            .expect("scan memory serializes")
            .into()
    }

    /// Remember the files a scan part described, up to `admitted` bytes of
    /// it. Parts come newest first; a path a newer part decided keeps that.
    fn remember_part(
        &mut self,
        root: &Path,
        bytes: &dyn sinex_source_core::drain::RecordRead,
        admitted: u64,
    ) -> Result<(), DriverError> {
        let ranges = sinex_source_core::drivers::record_ranges(bytes, SCAN.framer)
            .map_err(|e| DriverError::Io(e.to_string()))?;
        for (start, end) in ranges.into_iter().rev() {
            if end as u64 > admitted {
                continue;
            }
            let mut line = vec![0; end - start];
            bytes
                .read_exact_at(start as u64, &mut line)
                .map_err(|e| DriverError::Io(e.to_string()))?;
            let line: ScanLine = serde_json::from_slice(&line)
                .map_err(|e| DriverError::Cursor(format!("stored scan line: {e}")))?;
            let ScanLine::File(d) = line else {
                continue;
            };
            let d = *d;
            let Ok(relative) = Path::new(&d.path).strip_prefix(root) else {
                continue;
            };
            let relative = relative.to_string_lossy().into_owned();
            // A move left nothing at the path it came from.
            if let Some(from) = d.moved_from.as_deref().map(Path::new)
                && let Ok(from) = from.strip_prefix(root)
            {
                self.remembered.insert(from.to_string_lossy().into_owned());
            }
            if !self.remembered.insert(relative.clone()) || d.vanished_at_ns.is_some() {
                continue;
            }
            let mtime_ns = d
                .mtime_ns
                .as_deref()
                .and_then(|n| n.parse().ok())
                .ok_or_else(|| DriverError::Cursor("stored scan line has no mtime".into()))?;
            self.files.insert(
                relative,
                Known {
                    item_key: d.item_key,
                    sha256: d.sha256,
                    md5: d.md5,
                    media_type: d.media_type,
                    width: d.width,
                    height: d.height,
                    duration_ms: d.duration_ms,
                    stored: d.stored,
                    size: d.size,
                    mtime_ns,
                    device: d.device,
                    inode: d.inode,
                    content: d.content,
                },
            );
        }
        Ok(())
    }

    /// Remember the files of a saved memory that no newer part decided.
    fn remember_saved(&mut self, memory: &[u8]) -> Result<(), DriverError> {
        let files: Vec<(String, Known)> = serde_json::from_slice(memory)
            .map_err(|e| DriverError::Cursor(format!("saved scan memory: {e}")))?;
        for (path, known) in files {
            if self.remembered.insert(path.clone()) {
                self.files.insert(path, known);
            }
        }
        Ok(())
    }
}

/// A library root read through the reader framework: the scan, what it knows,
/// and its newest material until that is admitted.
pub struct LibraryReader {
    scan: LibraryScan,
    state: ScanState,
    held: Option<sinex_source_core::drivers::held::HeldMaterial>,
}

impl LibraryReader {
    #[cfg(test)]
    pub(super) fn files_hashed(&self) -> usize {
        self.state.files_hashed
    }
    pub fn new(scan: LibraryScan) -> Self {
        Self {
            scan,
            state: ScanState::default(),
            held: None,
        }
    }
}

impl sinex_source_core::binding::Reader for LibraryReader {
    fn history_from(&self, cursor: &Value) -> Option<MaterialId> {
        sinex_source_core::drivers::cursor_material(cursor).map(|(material, _)| material)
    }

    fn restore(
        &mut self,
        cursor: &Value,
        material: sinex_source_core::binding::StoredMaterial,
    ) -> Result<bool, DriverError> {
        let admitted = match sinex_source_core::drivers::cursor_material(cursor) {
            Some((id, Some(through))) if id == material.reference.id => {
                through.min(material.reference.length)
            }
            _ => material.reference.length,
        };
        self.state
            .remember_part(&self.scan.root, material.bytes.as_ref(), admitted)?;
        Ok(true)
    }

    fn restore_memory(&mut self, memory: &[u8]) -> Result<(), DriverError> {
        self.state.remember_saved(memory)
    }

    fn restored(&mut self) -> Result<(), DriverError> {
        self.state.remembered = Default::default();
        Ok(())
    }

    fn poll(&mut self, cursor: &Value) -> Result<Acquired, DriverError> {
        if let Some(held) = &self.held {
            if !held.admitted(cursor) {
                return held.acquired(cursor);
            }
            self.held = None;
        }
        let acquired = self.scan.poll(cursor, &mut self.state)?;
        if acquired.material.is_none() {
            return Ok(acquired);
        }
        let held = sinex_source_core::drivers::held::HeldMaterial::hold(acquired, cursor)?;
        let acquired = held.acquired(cursor)?;
        self.held = Some(held);
        Ok(acquired)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct ScanCursor {
    /// How much of `material` is admitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub through: Option<u64>,
    pub material: Option<MaterialId>,
    pub part_index: u32,
    pub last_complete_walk_ns: Option<String>,
    /// The part the scan last saved what it knows at.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_part: Option<u32>,
    /// Files described since then.
    #[serde(default)]
    pub changed_since_memory: u64,
}

/// Paths a library walk skips: hidden files and folders, and files other
/// programs keep beside media (thumbnails databases, partial downloads).
pub fn skipped(relative: &str) -> bool {
    relative.split('/').any(|part| part.starts_with('.'))
        || relative.ends_with(".part")
        || relative.ends_with(".crdownload")
        || relative.ends_with("Thumbs.db")
}

/// Whether `relative` is a metadata sidecar of another file `exists` knows:
/// `<file>.json` (gallery-dl's `--write-metadata`), `<file>.txt` (Hydrus tag
/// sidecars), or `<stem>.json` beside `<stem>.<ext>` (gallery-dl's
/// `--write-metadata` with a filename format). `stems` holds the paths of
/// the other files without their extension.
fn is_sidecar(
    relative: &str,
    exists: impl Fn(&str) -> bool,
    stems: &std::collections::BTreeSet<String>,
) -> bool {
    [".json", ".txt"]
        .iter()
        .filter_map(|ext| relative.strip_suffix(ext))
        .any(|base| exists(base) || stems.contains(base))
}

/// `a/b.jpg` → `a/b`; a path without an extension has no stem here.
fn stem(relative: &str) -> Option<&str> {
    let name_at = relative.rfind('/').map_or(0, |i| i + 1);
    relative[name_at..]
        .rfind('.')
        .filter(|&dot| dot > 0)
        .map(|dot| &relative[..name_at + dot])
}

impl LibraryScan {
    fn input(&self) -> InputRef {
        InputRef {
            kind: "library-root".into(),
            locator: self.root.display().to_string(),
            host: self.host.clone(),
        }
    }

    fn prepare(&self, path: &Path) -> std::io::Result<Prepared> {
        let mut digests = digest(path)?;
        let (mut width, mut height, mut duration_ms) =
            dimensions(&digests.head, &digests.media_type);
        if self.probe
            && (digests.media_type.starts_with("video/")
                || digests.media_type.starts_with("audio/"))
            && let Some(probed) = ffprobe(path)
        {
            width = width.or(probed.0);
            height = height.or(probed.1);
            duration_ms = duration_ms.or(probed.2);
        }
        digests.head = Vec::new();
        Ok(Prepared {
            content: format!("blake3:{}", digests.blake3),
            digests,
            width,
            height,
            duration_ms,
        })
    }

    /// Walk the root and queue what changed; note what is missing.
    fn walk(&self, state: &mut ScanState) {
        let mut found = BTreeMap::new();
        let mut unreadable = BTreeMap::new();
        if let Some(only) = &self.only {
            let path = self.root.join(only);
            match std::fs::symlink_metadata(&path) {
                Ok(metadata) if !skipped(only) && metadata.is_file() => {
                    found.insert(only.clone(), file_identity(&metadata));
                }
                Ok(metadata) if metadata.is_dir() => {
                    walk(&self.root, &path, &mut found, &mut unreadable);
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    unreadable.insert(only.clone(), error.to_string());
                }
            }
        } else {
            walk(&self.root, &self.root, &mut found, &mut unreadable);
        }
        // Sidecars (`a.jpg.json`, `a.jpg.txt`, `a.json`) describe the file
        // beside them; library import reads them. They are not items.
        let stems: std::collections::BTreeSet<String> = found
            .keys()
            .filter(|p| !p.ends_with(".json") && !p.ends_with(".txt"))
            .filter_map(|p| stem(p).map(str::to_owned))
            .collect();
        let sidecars: Vec<String> = found
            .keys()
            .filter(|p| is_sidecar(p, |other| found.contains_key(other), &stems))
            .cloned()
            .collect();
        for s in sidecars {
            found.remove(&s);
        }
        let mut missing = BTreeMap::new();
        for (path, known) in &state.files {
            if self
                .only
                .as_ref()
                .is_some_and(|only| !Path::new(path).starts_with(only))
            {
                continue;
            }
            if !found.contains_key(path)
                && !unreadable
                    .keys()
                    .any(|blocked| Path::new(path).starts_with(blocked))
            {
                missing.insert(path.clone(), known.clone());
            }
        }
        for (path, (size, mtime, dev, ino)) in found {
            if state.files.get(&path).is_none_or(|known| {
                known.size != size
                    || known.mtime_ns != mtime
                    || known.device != dev
                    || known.inode != ino
                    || known.stored != self.keep_bytes
            }) {
                state.queue.push_back((path, size, mtime, dev, ino));
            }
        }
        state.keys = state
            .files
            .values()
            .map(|known| known.item_key.clone())
            .collect();
        state.prepared.clear();
        state.moves.clear();
        state.move_candidates.clear();
        // Compare every possible new copy before choosing a move, including
        // copies that will be emitted in later parts of this walk.
        let missing_sizes: std::collections::HashSet<_> =
            missing.values().map(|known| known.size).collect();
        for (path, size, _, _, _) in &state.queue {
            if state.files.contains_key(path) || !missing_sizes.contains(size) {
                continue;
            }
            match self.prepare(&self.root.join(path)) {
                Ok(prepared) => {
                    #[cfg(test)]
                    {
                        state.files_hashed += 1;
                    }
                    state.prepared.insert(path.clone(), prepared);
                }
                Err(error) => {
                    unreadable.insert(path.clone(), error.to_string());
                }
            }
        }
        let mut by_content: BTreeMap<&str, Vec<&String>> = BTreeMap::new();
        let mut by_identity: BTreeMap<(&str, Option<u64>, u64), Vec<&String>> = BTreeMap::new();
        for (path, _, _, device, inode) in &state.queue {
            if let Some(prepared) = state.prepared.get(path) {
                by_content.entry(&prepared.content).or_default().push(path);
                if let Some(inode) = inode {
                    by_identity
                        .entry((&prepared.content, *device, *inode))
                        .or_default()
                        .push(path);
                }
            }
        }
        let mut old_content: BTreeMap<&str, Vec<&String>> = BTreeMap::new();
        let mut old_identity: BTreeMap<(&str, Option<u64>, u64), Vec<&String>> = BTreeMap::new();
        for (path, known) in &missing {
            old_content.entry(&known.content).or_default().push(path);
            if let Some(inode) = known.inode {
                old_identity
                    .entry((&known.content, known.device, inode))
                    .or_default()
                    .push(path);
            }
        }
        let mut matched = std::collections::HashSet::new();
        for (identity, paths) in &by_identity {
            if let Some(old) = old_identity.get(identity)
                && paths.len() == 1
                && old.len() == 1
            {
                state.moves.insert((*paths[0]).clone(), (*old[0]).clone());
                matched.insert((*old[0]).clone());
            }
        }
        for (content, paths) in by_content {
            let remaining: Vec<_> = paths
                .into_iter()
                .filter(|p| !state.moves.contains_key(*p))
                .collect();
            let old: Vec<_> = old_content
                .get(content)
                .into_iter()
                .flatten()
                .filter(|p| !matched.contains(**p))
                .copied()
                .collect();
            if remaining.len() == 1 && old.len() == 1 {
                state.moves.insert(remaining[0].clone(), old[0].clone());
            } else if !old.is_empty() {
                let candidates: Vec<_> = old
                    .iter()
                    .chain(remaining.iter())
                    .map(|p| self.root.join(p).display().to_string())
                    .collect();
                for path in remaining {
                    state
                        .move_candidates
                        .insert(path.clone(), candidates.clone());
                }
            }
        }
        state.problems = unreadable
            .iter()
            .filter(|(path, reason)| state.unreadable.get(*path) != Some(*reason))
            .map(|(path, reason)| (path.clone(), reason.clone()))
            .collect();
        state.unreadable = unreadable;
        state.missing = missing;
        state.walk_pending = true;
    }

    /// One poll: continue the current walk's queue, or start a new walk.
    pub fn poll(&self, cursor: &Value, state: &mut ScanState) -> Result<Acquired, DriverError> {
        let previous: ScanCursor = if cursor.is_null() {
            ScanCursor::default()
        } else {
            serde_json::from_value(cursor.clone())
                .map_err(|e| DriverError::Cursor(e.to_string()))?
        };
        let mut facts = match file_facts(&self.root) {
            Ok(f) => f,
            Err(e) => {
                // An inaccessible root never means deletion.
                return Ok(Acquired {
                    input: self.input(),
                    observation: ObservationFacts {
                        unavailable: Some(format!("{}: {e}", self.root.display())),
                        observed_at: Some(Timestamp::now()),
                        ..Default::default()
                    },
                    revision: RevisionChange::Same,
                    material: None,
                    cursor_without_material: cursor.clone(),
                });
            }
        };
        if !state.walk_pending {
            self.walk(state);
        }
        let now = Timestamp::now();
        let id = MaterialId::new();
        let root = self.root.display().to_string();
        let mut lines: Vec<u8> = Vec::new();
        let mut records = Vec::new();
        let mut files_described = 0u64;
        for (path, reason) in std::mem::take(&mut state.problems) {
            push_line(
                &mut lines,
                &mut records,
                &ScanLine::Unreadable {
                    path: self.root.join(path).display().to_string(),
                    reason,
                },
            );
        }
        let mut moved_away: Vec<(String, Known)> = Vec::new();
        let mut owned_files = Vec::new();
        let mut budget = 0u64;
        while let Some((relative, size, mtime, dev, ino)) = state.queue.front().cloned() {
            if budget > 0 && budget + size > self.bytes_per_poll {
                break;
            }
            state.queue.pop_front();
            budget += size.max(1);
            let full = self.root.join(&relative);
            if state.unreadable.contains_key(&relative) {
                continue;
            }
            let before = state.files.get(&relative).cloned();
            if let Some(known) = &before
                && known.size == size
                && known.mtime_ns == mtime
                && known.device == dev
                && known.inode == ino
            {
                let line = ScannedFile {
                    item_key: known.item_key.clone(),
                    host: self.host.clone(),
                    root: root.clone(),
                    path: full.display().to_string(),
                    content: known.content.clone(),
                    size,
                    sha256: known.sha256.clone(),
                    md5: known.md5.clone(),
                    media_type: known.media_type.clone(),
                    width: known.width,
                    height: known.height,
                    duration_ms: known.duration_ms,
                    stored: self.keep_bytes,
                    device: dev,
                    inode: ino,
                    mtime_ns: Some(mtime.to_string()),
                    moved_from: None,
                    move_candidates: Vec::new(),
                    previous_content: None,
                    vanished_at_ns: None,
                };
                if self.keep_bytes && !known.stored {
                    owned_files.push(sinex_source_core::drivers::OwnedFile {
                        path: full,
                        blob: known.content.clone(),
                    });
                }
                push_line(
                    &mut lines,
                    &mut records,
                    &ScanLine::File(Box::new(line.clone())),
                );
                files_described += 1;
                state.files.get_mut(&relative).expect("known file").stored = self.keep_bytes;
                continue;
            }
            let prepared = match state.prepared.remove(&relative) {
                Some(prepared) => prepared,
                None => match self.prepare(&full) {
                    Ok(prepared) => {
                        #[cfg(test)]
                        {
                            state.files_hashed += 1;
                        }
                        prepared
                    }
                    Err(error) => {
                        let reason = error.to_string();
                        if state.unreadable.get(&relative) != Some(&reason) {
                            push_line(
                                &mut lines,
                                &mut records,
                                &ScanLine::Unreadable {
                                    path: full.display().to_string(),
                                    reason: reason.clone(),
                                },
                            );
                        }
                        state.unreadable.insert(relative, reason);
                        continue;
                    }
                },
            };
            let content = prepared.content;
            let digests = prepared.digests;
            let stored = self.keep_bytes;
            let moved_from = state.moves.remove(&relative);
            let item_key = before
                .as_ref()
                .map(|k| k.item_key.clone())
                .or_else(|| {
                    moved_from
                        .as_ref()
                        .and_then(|p| state.missing.get(p))
                        .map(|k| k.item_key.clone())
                })
                .unwrap_or_else(|| {
                    let key =
                        sinex_model::event_types::library::item_key(&self.host, &root, &relative);
                    if state.keys.contains(&key) {
                        format!("{key}@{id}")
                    } else {
                        key
                    }
                });
            state.keys.insert(item_key.clone());
            if let Some(old) = &moved_from {
                // The old path is gone; its line comes after this one's, so
                // whoever reads the move sees the new path first.
                if let Some(known) = state.missing.remove(old) {
                    moved_away.push((old.clone(), known));
                }
                state.files.remove(old);
            }
            let previous_content = before
                .as_ref()
                .filter(|known| known.content != content)
                .map(|k| k.content.clone());
            let line = ScannedFile {
                item_key: item_key.clone(),
                host: self.host.clone(),
                root: root.clone(),
                path: full.display().to_string(),
                content: content.clone(),
                size,
                sha256: digests.sha256,
                md5: digests.md5,
                media_type: digests.media_type,
                width: prepared.width,
                height: prepared.height,
                duration_ms: prepared.duration_ms,
                stored,
                device: dev,
                inode: ino,
                mtime_ns: Some(mtime.to_string()),
                moved_from: moved_from.map(|p| self.root.join(p).display().to_string()),
                move_candidates: state.move_candidates.remove(&relative).unwrap_or_default(),
                previous_content,
                vanished_at_ns: None,
            };
            push_line(
                &mut lines,
                &mut records,
                &ScanLine::File(Box::new(line.clone())),
            );
            files_described += 1;
            if stored {
                owned_files.push(sinex_source_core::drivers::OwnedFile {
                    path: full.clone(),
                    blob: content.clone(),
                });
            }
            state.files.insert(
                relative,
                Known {
                    item_key,
                    sha256: line.sha256,
                    md5: line.md5,
                    media_type: line.media_type,
                    width: line.width,
                    height: line.height,
                    duration_ms: line.duration_ms,
                    stored: line.stored,
                    size,
                    mtime_ns: mtime,
                    device: dev,
                    inode: ino,
                    content,
                },
            );
        }
        let walk_done = state.queue.is_empty();
        let mut gone: Vec<(String, Known)> = moved_away;
        if walk_done {
            // Whatever is still missing after the walk's files were read was
            // not moved: it vanished.
            gone.extend(std::mem::take(&mut state.missing));
            state.walk_pending = false;
        }
        {
            for (relative, known) in gone {
                state.files.remove(&relative);
                let line = ScannedFile {
                    item_key: known.item_key,
                    host: self.host.clone(),
                    root: root.clone(),
                    path: self.root.join(&relative).display().to_string(),
                    content: known.content,
                    size: known.size,
                    device: known.device,
                    inode: known.inode,
                    mtime_ns: Some(known.mtime_ns.to_string()),
                    vanished_at_ns: Some(now.unix_ns().to_string()),
                    media_type: String::new(),
                    ..Default::default()
                };
                push_line(
                    &mut lines,
                    &mut records,
                    &ScanLine::File(Box::new(line.clone())),
                );
                files_described += 1;
            }
        }
        state.walk_changed |= !records.is_empty();
        if walk_done && (state.walk_changed || (previous.material.is_none() && !state.completed)) {
            push_line(
                &mut lines,
                &mut records,
                &ScanLine::Complete {
                    host: self.host.clone(),
                    root: root.clone(),
                    only: self.only.clone(),
                    unreadable: state
                        .unreadable
                        .keys()
                        .map(|path| self.root.join(path).display().to_string())
                        .collect(),
                },
            );
            state.walk_changed = false;
        }
        state.completed |= walk_done;
        let scanned = ScanCursor {
            last_complete_walk_ns: if walk_done && state.unreadable.is_empty() {
                Some(now.unix_ns().to_string())
            } else {
                previous.last_complete_walk_ns.clone()
            },
            ..previous.clone()
        };
        if records.is_empty() {
            facts.changed = false;
            return Ok(Acquired {
                input: self.input(),
                observation: facts,
                revision: RevisionChange::Same,
                material: None,
                cursor_without_material: serde_json::to_value(scanned).expect("cursor serializes"),
            });
        }
        let read_at = i64::try_from(now.unix_ns()).unwrap_or(i64::MAX);
        let part_index = if previous.material.is_some() {
            previous.part_index + 1
        } else {
            0
        };
        let manifest = sinex_model::Manifest::from_json(json!({
            "path": root, "host": self.host,
            "media_type": "application/x-sinex-library-scan", "keep_bytes": self.keep_bytes,
            "walk_complete": walk_done, "only": self.only, "read_at_ns": read_at,
            "scan": id.to_string(), "part_index": part_index,
        }));
        let changed = previous
            .changed_since_memory
            .saturating_add(files_described);
        let due = sinex_source_core::drivers::memory_due(
            part_index,
            previous.memory_part,
            changed,
            state.files.len() as u64,
        );
        let memory = due.then(|| state.memory());
        let length = lines.len();
        let finished = ScanCursor {
            through: Some(length as u64),
            material: Some(id),
            part_index,
            memory_part: if due {
                Some(part_index)
            } else {
                previous.memory_part
            },
            changed_since_memory: if due { 0 } else { changed },
            ..scanned.clone()
        };
        Ok(Acquired {
            input: self.input(),
            observation: facts,
            revision: RevisionChange::Snapshot,
            material: Some(NewMaterial {
                id,
                bytes: lines,
                stored: None,
                records,
                part_index,
                previous_part: previous.material,
                manifest,
                reading_order: Box::new(move |_| ReadingOrder(read_at)),
                cursor_at: Box::new({
                    let cursor = move |end: usize| {
                        let at = if end == length {
                            finished.clone()
                        } else {
                            ScanCursor {
                                through: Some(end as u64),
                                material: Some(id),
                                part_index,
                                ..previous.clone()
                            }
                        };
                        serde_json::to_value(at).expect("cursor serializes")
                    };
                    move |end| Ok(cursor(end))
                }),
                aligned: false,
                removed: Vec::new(),
                memory,
                owned_files,
            }),
            cursor_without_material: Value::Null,
        })
    }
}

fn push_line(lines: &mut Vec<u8>, records: &mut Vec<(usize, usize)>, line: &ScanLine) {
    let start = lines.len();
    serde_json::to_writer(&mut *lines, line).expect("scan lines serialize");
    lines.push(b'\n');
    records.push((start, lines.len()));
}

type Found = BTreeMap<String, (u64, i128, Option<u64>, Option<u64>)>;

fn file_identity(metadata: &std::fs::Metadata) -> (u64, i128, Option<u64>, Option<u64>) {
    use std::os::unix::fs::MetadataExt;
    let modified = i128::from(metadata.mtime()) * 1_000_000_000 + i128::from(metadata.mtime_nsec());
    (
        metadata.len(),
        modified,
        Some(metadata.dev()),
        Some(metadata.ino()),
    )
}

fn walk(root: &Path, dir: &Path, found: &mut Found, unreadable: &mut BTreeMap<String, String>) {
    let relative = |path: &Path| {
        path.strip_prefix(root)
            .unwrap_or(path)
            .to_string_lossy()
            .into_owned()
    };
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) => {
            unreadable.insert(relative(dir), error.to_string());
            return;
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                unreadable.insert(relative(dir), error.to_string());
                continue;
            }
        };
        let path = entry.path();
        let name = relative(&path);
        if skipped(&name) {
            continue;
        }
        // Symlinks may point outside the root and are not followed.
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) => {
                unreadable.insert(name, error.to_string());
                continue;
            }
        };
        if metadata.is_dir() {
            walk(root, &path, found, unreadable);
        } else if metadata.is_file() {
            found.insert(name, file_identity(&metadata));
        }
    }
}

struct Prepared {
    content: String,
    digests: Digests,
    width: Option<u32>,
    height: Option<u32>,
    duration_ms: Option<u64>,
}

#[cfg(test)]
mod tests;
