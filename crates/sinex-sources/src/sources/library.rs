//! The media library's two sources.
//!
//! **Scan** (`library.scan`): a walk of a library root. Every changed file is hashed once (BLAKE3, SHA-256, MD5), its type sniffed from its
//! bytes, its bytes streamed into the content store unless the root is
//! indexed by reference only, and described by one JSON line. The material is
//! those lines; a file line is one `library.copy`. A completion line closes
//! each changed scan once, without another copy event. A file that left its
//! path and whose bytes turned up at a new path is a move (`moved_from`);
//! matching device and inode are considered first. An edited file names the
//! bytes it held (`previous_content`). A file gone from a completed walk, and
//! not moved, gets a vanished line.
//!
//! **Requests** (`library.request`): what the operator or an agent writes to
//! the library (previews, assignments, collections, ratings, views), stored as
//! the request's own JSON lines and read back by the request parser. The
//! parser takes the author and time from the material's manifest, which the
//! daemon fills from the authenticated caller, never from the lines.
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sinex_model::event_types::library::{
    Assignment, Collection, Comparison, Dismissal, Entry, FileCopy, LibraryRecord, Preview, Rating,
    View,
};
use sinex_model::{
    Class, InputRef, MaterialId, ObservationFacts, ReadingOrder, RevisionChange, Timestamp,
};
use sinex_source_core::drain::RecordContext;
use sinex_source_core::drivers::{Acquired, DriverError, NewMaterial, file_facts};
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordError};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::Read;
use std::path::{Path, PathBuf};

pub const SCAN: SourceSpec = SourceSpec {
    name: "library.scan",
    driver: Driver::DirSnapshot,
    excisable: false,
    purpose: "the media library",
    class: Class::Open,
    parser: "library-scan",
    parser_version: 2,
    event_types: &["library.copy"],
    framer: sinex_source_core::Framer::export(
        sinex_source_core::drivers::export_archive::frame_lines,
        sinex_source_core::drivers::export_archive::Framing::Lines,
    ),
};

pub const REQUEST: SourceSpec = SourceSpec {
    name: "library.request",
    driver: Driver::ApiRequest,
    excisable: false,
    purpose: "the media library",
    class: Class::Open,
    parser: "library-request",
    parser_version: 2,
    event_types: sinex_model::event_types::library::REQUEST_TYPES,
    framer: sinex_source_core::Framer::export(
        sinex_source_core::drivers::export_archive::frame_lines,
        sinex_source_core::drivers::export_archive::Framing::Lines,
    ),
};

// ---------------------------------------------------------------------------
// Scan

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

pub struct Digests {
    pub blake3: String,
    pub sha256: String,
    pub md5: String,
    /// The first bytes, for sniffing the type and reading image dimensions.
    pub head: Vec<u8>,
    pub media_type: String,
}

/// Hash a file once with all three digests, streaming it.
pub fn digest(path: &Path) -> std::io::Result<Digests> {
    use md5::Digest as _;
    let mut file = std::fs::File::open(path)?;
    let mut b3 = blake3::Hasher::new();
    let mut sha = sha2::Sha256::new();
    let mut md = md5::Md5::new();
    let mut head = Vec::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        let chunk = &buf[..n];
        if head.len() < 64 * 1024 {
            let take = (64 * 1024 - head.len()).min(n);
            head.extend_from_slice(&chunk[..take]);
        }
        b3.update(chunk);
        sha.update(chunk);
        md.update(chunk);
    }
    let media_type = sniff(&head).to_owned();
    Ok(Digests {
        blake3: b3.finalize().to_hex().to_string(),
        sha256: hex(&sha.finalize()),
        md5: hex(&md.finalize()),
        head,
        media_type,
    })
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// The media type from a file's first bytes. Names are never trusted: a
/// `.jpg` that is a PNG is a PNG.
fn sniff(head: &[u8]) -> &'static str {
    let starts = |magic: &[u8]| head.starts_with(magic);
    let at = |offset: usize, magic: &[u8]| head.get(offset..offset + magic.len()) == Some(magic);
    if starts(b"\xFF\xD8\xFF") {
        "image/jpeg"
    } else if starts(b"\x89PNG\r\n\x1a\n") {
        "image/png"
    } else if starts(b"GIF87a") || starts(b"GIF89a") {
        "image/gif"
    } else if starts(b"RIFF") && at(8, b"WEBP") {
        "image/webp"
    } else if starts(b"RIFF") && at(8, b"WAVE") {
        "audio/wav"
    } else if starts(b"RIFF") && at(8, b"AVI ") {
        "video/x-msvideo"
    } else if at(4, b"ftyp") {
        match head.get(8..12) {
            Some(b"avif") | Some(b"avis") => "image/avif",
            Some(b"heic") | Some(b"heix") | Some(b"mif1") => "image/heic",
            Some(b"qt  ") => "video/quicktime",
            Some(b"M4A ") => "audio/mp4",
            _ => "video/mp4",
        }
    } else if starts(b"\x1A\x45\xDF\xA3") {
        if head.windows(4).any(|w| w == b"webm") {
            "video/webm"
        } else {
            "video/x-matroska"
        }
    } else if starts(b"%PDF-") {
        "application/pdf"
    } else if starts(b"ID3") || (head.len() > 1 && head[0] == 0xFF && head[1] & 0xE0 == 0xE0) {
        "audio/mpeg"
    } else if starts(b"fLaC") {
        "audio/flac"
    } else if starts(b"OggS") {
        "audio/ogg"
    } else if starts(b"BM") && head.len() > 26 {
        "image/bmp"
    } else if starts(b"II*\0") || starts(b"MM\0*") {
        "image/tiff"
    } else if starts(b"PK\x03\x04") {
        "application/zip"
    } else if head.iter().take(4096).all(|b| *b != 0) && std::str::from_utf8(head).is_ok() {
        if head.starts_with(b"WEBVTT") {
            "text/vtt"
        } else {
            "text/plain"
        }
    } else {
        "application/octet-stream"
    }
}

/// Image dimensions from the header bytes; `(width, height, duration)`.
pub fn dimensions(head: &[u8], media_type: &str) -> (Option<u32>, Option<u32>, Option<u64>) {
    let be16 = |o: usize| {
        head.get(o..o + 2)
            .map(|b| u16::from_be_bytes([b[0], b[1]]) as u32)
    };
    let le16 = |o: usize| {
        head.get(o..o + 2)
            .map(|b| u16::from_le_bytes([b[0], b[1]]) as u32)
    };
    let be32 = |o: usize| {
        head.get(o..o + 4)
            .map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    };
    let le24 = |o: usize| {
        head.get(o..o + 3)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], 0]))
    };
    let (w, h) = match media_type {
        "image/png" => (be32(16), be32(20)),
        "image/gif" => (le16(6), le16(8)),
        "image/bmp" => (
            head.get(18..22)
                .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]])),
            head.get(22..26)
                .map(|b| i32::from_le_bytes([b[0], b[1], b[2], b[3]]).unsigned_abs()),
        ),
        "image/webp" => match head.get(12..16) {
            Some(b"VP8 ") => (le16(26).map(|v| v & 0x3FFF), le16(28).map(|v| v & 0x3FFF)),
            Some(b"VP8L") => head
                .get(21..25)
                .map(|b| {
                    let v = u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
                    (Some((v & 0x3FFF) + 1), Some(((v >> 14) & 0x3FFF) + 1))
                })
                .unwrap_or((None, None)),
            Some(b"VP8X") => (le24(24).map(|v| v + 1), le24(27).map(|v| v + 1)),
            _ => (None, None),
        },
        "image/jpeg" => {
            // Walk the segments to the first start-of-frame marker.
            let mut i = 2;
            let mut found = (None, None);
            while i + 9 < head.len() {
                if head[i] != 0xFF {
                    break;
                }
                let marker = head[i + 1];
                let len = be16(i + 2).unwrap_or(0) as usize;
                if matches!(marker, 0xC0..=0xC3 | 0xC5..=0xC7 | 0xC9..=0xCB | 0xCD..=0xCF) {
                    found = (be16(i + 7), be16(i + 5));
                    break;
                }
                if len < 2 {
                    break;
                }
                i += 2 + len;
            }
            found
        }
        _ => (None, None),
    };
    (w.filter(|v| *v > 0), h.filter(|v| *v > 0), None)
}

/// Width, height and duration from `ffprobe`, when it is installed.
fn ffprobe(path: &Path) -> Option<(Option<u32>, Option<u32>, Option<u64>)> {
    let out = std::process::Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-print_format",
            "json",
            "-show_entries",
            "format=duration:stream=width,height",
        ])
        .arg(path)
        .stdin(std::process::Stdio::null())
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let v: Value = serde_json::from_slice(&out.stdout).ok()?;
    let stream = v
        .get("streams")
        .and_then(Value::as_array)
        .and_then(|s| s.iter().find(|s| s.get("width").is_some()));
    let dim = |k: &str| {
        stream
            .and_then(|s| s.get(k))
            .and_then(Value::as_u64)
            .and_then(|n| u32::try_from(n).ok())
    };
    let duration = v
        .pointer("/format/duration")
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<f64>().ok())
        .map(|s| (s * 1000.0).round() as u64);
    Some((dim("width"), dim("height"), duration))
}

// ---------------------------------------------------------------------------
// Requests

/// One request line: a record tagged with its kind. `by` and `at` in the line
/// are ignored; the parser fills them from the manifest.
#[derive(Deserialize, Serialize)]
#[serde(tag = "record", rename_all = "snake_case")]
enum Line {
    Preview(Preview),
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
            LibraryRecord::Preview(x) => Line::Preview(x),
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
            Line::Preview(x) => LibraryRecord::Preview(x),
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
                    LibraryRecord::Preview(rendition) => Some(
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

/// Library roots are ordinary bindings; library requests are supplied by the API.
pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "library",
        discovery: sinex_source_core::discovery::DiscoverySpec {
            owner: sinex_source_core::discovery::Owner::System,
            locations: &[sinex_source_core::discovery::Location {
                root: "declared",
                path: "library",
            }],
            capabilities: &[],
            credentials: &[],
            streams: &[],
            requests: &["library.request"],
            request_only: false,
        },
        discover: crate::discovery::discover_format,
        specs: || vec![&SCAN, &REQUEST],
        plan: Some(|path, settings| {
            let path =
                std::fs::canonicalize(path).map_err(|e| format!("{}: {e}", path.display()))?;
            let mut settings = settings.clone();
            let root = if let Some(root) = settings.option("root") {
                std::fs::canonicalize(root).map_err(|error| format!("{root}: {error}"))?
            } else if path.is_dir() {
                path.clone()
            } else {
                path.parent()
                    .ok_or("library input has no parent")?
                    .to_path_buf()
            };
            let selection = path
                .strip_prefix(&root)
                .map_err(|_| "library selection is outside its root")?;
            if !selection.as_os_str().is_empty() {
                if !settings.options.is_object() {
                    settings.options = json!({});
                }
                settings.options["only"] = json!(selection.to_string_lossy());
            }
            Ok(sinex_source_core::binding::single(&SCAN, &root, &settings))
        }),
        source: |b| {
            if b.source == SCAN.name {
                Ok(Box::new(scan_source()))
            } else {
                Ok(Box::new(request_source()))
            }
        },
        reader: Some(|b| {
            if b.source != SCAN.name {
                return Err("library requests are supplied by the API".into());
            }
            Ok(sinex_source_core::binding::polled(LibraryReader::new(
                LibraryScan {
                    root: b.input.clone(),
                    host: b.settings.host.clone(),
                    keep_bytes: !b.settings.boolean("reference")?.unwrap_or(false),
                    probe: true,
                    bytes_per_poll: 1 << 30,
                    only: b.settings.option("only").map(str::to_owned),
                },
            )))
        }),
        capture: None,
    };

#[cfg(test)]
mod tests;
