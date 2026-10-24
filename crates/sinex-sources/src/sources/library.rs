//! The media library's two sources (architecture §6.6).
//!
//! **Scan** (`library.scan`): a walk of a library root. Each new or changed
//! file is hashed once (BLAKE3, SHA-256, MD5), its type sniffed from its
//! bytes, its bytes streamed into the content store unless the root is
//! indexed by reference only, and described by one JSON line. The material is
//! those lines; one line is one `library.acquisition`. A file that left its
//! path but whose device and inode turned up at a new path with the same bytes
//! is a rename (`moved_from`); a file whose bytes changed in place names the
//! bytes it held (`previous_content`). A file gone from a completed walk, and
//! not moved, gets a vanished line.
//!
//! **Requests** (`library.request`): what the operator or an agent writes to
//! the library (items, assignments, collections, ratings, views), stored as
//! the request's own JSON lines and read back by the request parser. The
//! parser takes the author and time from the material's manifest, which the
//! daemon fills from the authenticated caller, never from the lines.
use crate::drain::RecordContext;
use crate::drivers::{Acquired, DriverError, MaterialSink, NewMaterial, file_facts};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sinex_model::event_types::library::{
    Acquisition, Assignment, Collection, Comparison, Dismissal, Entry, Item, LibraryRecord, Rating,
    Rendition, View,
};
use sinex_model::{
    Class, InputRef, MaterialId, ObservationFacts, ReadingOrder, RevisionChange, Timestamp,
};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::Read;
use std::path::{Path, PathBuf};

pub const SCAN: SourceSpec = SourceSpec {
    name: "library.scan",
    driver: Driver::DirSnapshot,
    consumer: "the media library",
    class: Class::Open,
    parser: "library-scan",
    parser_version: 1,
    event_types: &["library.acquisition"],
    framer: crate::Framer::export(
        crate::drivers::export_archive::frame_lines,
        crate::drivers::export_archive::Framing::Lines,
    ),
};

pub const REQUEST: SourceSpec = SourceSpec {
    name: "library.request",
    driver: Driver::ApiRequest,
    consumer: "the media library",
    class: Class::Open,
    parser: "library-request",
    parser_version: 1,
    event_types: sinex_model::event_types::library::REQUEST_TYPES,
    framer: crate::Framer::export(
        crate::drivers::export_archive::frame_lines,
        crate::drivers::export_archive::Framing::Lines,
    ),
};

// ---------------------------------------------------------------------------
// Scan

/// One scanned file as the driver describes it: the stored record.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Described {
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
    pub previous_content: Option<String>,
    pub vanished_at_ns: Option<String>,
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
    type Payload = Acquisition;

    /// Where a file is and how it was found are left out of the record hash,
    /// so equal bytes hash equal at any path: a rescan of an unchanged file is
    /// a repeat, and an excised file's bytes stay excised wherever they turn
    /// up again.
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
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<Acquisition>, ()), RecordError> {
        let d: Described = serde_json::from_value(Value::Object(record.clone()))
            .map_err(|e| RecordError::new(format!("not a library scan line: {e}")))?;
        let payload = Acquisition {
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
    /// File bytes hashed per poll, so one poll's material stays a sensible size
    /// and progress commits as it goes. A file larger than this is read alone.
    pub bytes_per_poll: u64,
    /// Scan only this file (a path relative to `root`); the rest of the root
    /// is neither read nor reported missing.
    pub only: Option<String>,
}

/// What a scan knows about a file it already read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Known {
    pub size: u64,
    pub mtime_ns: i128,
    pub device: Option<u64>,
    pub inode: Option<u64>,
    pub content: String,
}

/// A file the walk found changed: relative path, size, mtime, device, inode.
type Pending = (String, u64, i128, Option<u64>, Option<u64>);

/// The scan's state: the files read, by path, and the work left from the
/// current walk. Seeded from the live acquisitions after a restart, so a
/// restart does not hash the whole library again.
#[derive(Default)]
pub struct ScanState {
    pub files: HashMap<String, Known>,
    queue: VecDeque<Pending>,
    /// Paths missing from the current walk, with the file identity they had.
    missing: BTreeMap<String, Known>,
    walk_pending: bool,
}

impl ScanState {
    pub fn seed(files: impl IntoIterator<Item = (String, Known)>) -> Self {
        Self {
            files: files.into_iter().collect(),
            ..Self::default()
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanCursor {
    pub material: Option<MaterialId>,
    pub part_index: u32,
    pub last_complete_walk_ns: Option<String>,
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
pub fn is_sidecar(
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

    /// Walk the root and queue what changed; note what is missing.
    fn walk(&self, state: &mut ScanState) -> Result<(), DriverError> {
        let mut found = BTreeMap::new();
        walk(&self.root, &self.root, &mut found)?;
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
        if let Some(only) = &self.only {
            found.retain(|path, _| path == only);
        }
        let mut missing = BTreeMap::new();
        for (path, known) in &state.files {
            if self.only.as_ref().is_some_and(|o| o != path) {
                continue;
            }
            if !found.contains_key(path) {
                missing.insert(path.clone(), known.clone());
            }
        }
        for (path, (size, mtime, dev, ino)) in found {
            let unchanged = state
                .files
                .get(&path)
                .is_some_and(|k| k.size == size && k.mtime_ns == mtime);
            if !unchanged {
                state.queue.push_back((path, size, mtime, dev, ino));
            }
        }
        state.missing = missing;
        state.walk_pending = true;
        Ok(())
    }

    /// One poll: continue the current walk's queue, or start a new walk.
    pub fn poll(
        &self,
        cursor: &Value,
        state: &mut ScanState,
        sink: &dyn MaterialSink,
    ) -> Result<Acquired, DriverError> {
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
                    image: None,
                    input: self.input(),
                    observation: ObservationFacts {
                        unavailable: Some(format!("{}: {e}", self.root.display())),
                        observed_at: Some(Timestamp::now()),
                        ..Default::default()
                    },
                    revision: RevisionChange::Same,
                    material: None,
                    cursor_unchanged: cursor.clone(),
                });
            }
        };
        if !state.walk_pending {
            self.walk(state)?;
        }
        let now = Timestamp::now();
        let root = self.root.display().to_string();
        let mut lines: Vec<u8> = Vec::new();
        let mut records = Vec::new();
        let mut moved_away: Vec<(String, Known)> = Vec::new();
        let mut budget = 0u64;
        while let Some((relative, size, mtime, dev, ino)) = state.queue.front().cloned() {
            if budget > 0 && budget + size > self.bytes_per_poll {
                break;
            }
            state.queue.pop_front();
            budget += size.max(1);
            let full = self.root.join(&relative);
            let digests = match digest(&full) {
                Ok(d) => d,
                // Gone or unreadable between the walk and the read: the next
                // walk sees it again.
                Err(_) => continue,
            };
            let content = format!("blake3:{}", digests.blake3);
            let stored = if self.keep_bytes {
                let name = sink.put_file(&full).map_err(DriverError::Store)?;
                if name != content {
                    // The file changed while it was read; read it next walk.
                    continue;
                }
                true
            } else {
                false
            };
            let before = state.files.get(&relative).cloned();
            // Moved: a missing path held this very file (device and inode)
            // with these bytes.
            let moved_from = state
                .missing
                .iter()
                .find(|(_, k)| {
                    k.content == content && k.device == dev && k.inode == ino && ino.is_some()
                })
                .map(|(p, _)| p.clone());
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
                .filter(|k| k.content != content && k.device == dev && k.inode == ino)
                .map(|k| k.content.clone());
            if before.as_ref().is_some_and(|k| k.content == content) {
                // Touched, not changed.
                state.files.insert(
                    relative.clone(),
                    Known {
                        size,
                        mtime_ns: mtime,
                        device: dev,
                        inode: ino,
                        content,
                    },
                );
                continue;
            }
            let (width, height, mut duration_ms) = dimensions(&digests.head, &digests.media_type);
            let (mut width, mut height) = (width, height);
            if self.probe
                && (digests.media_type.starts_with("video/")
                    || digests.media_type.starts_with("audio/"))
                && let Some(p) = ffprobe(&full)
            {
                width = width.or(p.0);
                height = height.or(p.1);
                duration_ms = duration_ms.or(p.2);
            }
            let line = Described {
                host: self.host.clone(),
                root: root.clone(),
                path: full.display().to_string(),
                content: content.clone(),
                size,
                sha256: digests.sha256,
                md5: digests.md5,
                media_type: digests.media_type,
                width,
                height,
                duration_ms,
                stored,
                device: dev,
                inode: ino,
                mtime_ns: Some(mtime.to_string()),
                moved_from: moved_from.map(|p| self.root.join(p).display().to_string()),
                previous_content,
                vanished_at_ns: None,
            };
            push_line(&mut lines, &mut records, &line);
            state.files.insert(
                relative,
                Known {
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
                let line = Described {
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
                push_line(&mut lines, &mut records, &line);
            }
        }
        let scanned = ScanCursor {
            last_complete_walk_ns: if walk_done {
                Some(now.unix_ns().to_string())
            } else {
                previous.last_complete_walk_ns.clone()
            },
            ..previous.clone()
        };
        if records.is_empty() {
            facts.changed = false;
            return Ok(Acquired {
                image: None,
                input: self.input(),
                observation: facts,
                revision: RevisionChange::Same,
                material: None,
                cursor_unchanged: serde_json::to_value(scanned).expect("cursor serializes"),
            });
        }
        let read_at = i64::try_from(now.unix_ns()).unwrap_or(i64::MAX);
        let manifest = json!({
            "path": root,
            "host": self.host,
            "media_type": "application/x-sinex-library-scan",
            "keep_bytes": self.keep_bytes,
            "walk_complete": walk_done,
            "read_at_ns": read_at,
        });
        let id = MaterialId::new();
        let part_index = if previous.material.is_some() {
            previous.part_index + 1
        } else {
            0
        };
        Ok(Acquired {
            image: None,
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
                    let cursor = move |_| {
                        serde_json::to_value(ScanCursor {
                            material: Some(id),
                            part_index,
                            ..scanned.clone()
                        })
                        .expect("cursor serializes")
                    };
                    move |end| Ok(cursor(end))
                }),
                aligned: false,
                vanished: Vec::new(),
            }),
            cursor_unchanged: Value::Null,
        })
    }

    /// Poll until a walk completes, returning every material along the way.
    /// Callers that admit as they go use `poll` directly.
    pub fn pending(&self, state: &ScanState) -> usize {
        state.queue.len()
    }
}

fn push_line(lines: &mut Vec<u8>, records: &mut Vec<(usize, usize)>, line: &Described) {
    let start = lines.len();
    serde_json::to_writer(&mut *lines, line).expect("scan lines serialize");
    lines.push(b'\n');
    records.push((start, lines.len()));
}

type Found = BTreeMap<String, (u64, i128, Option<u64>, Option<u64>)>;

fn walk(root: &Path, dir: &Path, found: &mut Found) -> Result<(), DriverError> {
    use std::os::unix::fs::MetadataExt;
    let entries =
        std::fs::read_dir(dir).map_err(|e| DriverError::Io(format!("{}: {e}", dir.display())))?;
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(relative) = path.strip_prefix(root) else {
            continue;
        };
        let relative = relative.to_string_lossy().into_owned();
        if skipped(&relative) {
            continue;
        }
        // Symlinks are not followed: a link can point anywhere, including
        // outside the root (the predecessor's containment rule, `kv6l`).
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if meta.is_dir() {
            walk(root, &path, found)?;
        } else if meta.is_file() {
            let mtime = i128::from(meta.mtime()) * 1_000_000_000 + i128::from(meta.mtime_nsec());
            found.insert(
                relative,
                (meta.len(), mtime, Some(meta.dev()), Some(meta.ino())),
            );
        }
    }
    Ok(())
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
pub fn sniff(head: &[u8]) -> &'static str {
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

/// Width, height and duration from `ffprobe`, when it is installed. Media
/// decoding uses existing implementations (spec `library.md` §1).
pub fn ffprobe(path: &Path) -> Option<(Option<u32>, Option<u32>, Option<u64>)> {
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
    Item(Item),
    Rendition(Rendition),
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
pub fn request_bytes(records: &[LibraryRecord]) -> (Vec<u8>, Vec<(usize, usize)>) {
    let mut bytes = Vec::new();
    let mut spans = Vec::new();
    for r in records {
        let line = match r.clone() {
            LibraryRecord::Item(x) => Line::Item(x),
            LibraryRecord::Rendition(x) => Line::Rendition(x),
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
pub fn request_manifest(actor: &str, host: &str, at: Timestamp, action: &str) -> Value {
    json!({
        "path": "library",
        "host": host,
        "media_type": "application/x-sinex-library-request",
        "actor": actor,
        "action": action,
        "written_at_ns": at.unix_ns().to_string(),
        "read_at_ns": i64::try_from(at.unix_ns()).unwrap_or(i64::MAX),
    })
}

pub struct RequestParser;

impl Parser for RequestParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = LibraryRecord;

    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<LibraryRecord>, ()), RecordError> {
        let by = context
            .manifest
            .get("actor")
            .and_then(Value::as_str)
            .ok_or_else(|| RecordError::new("library request manifest has no actor"))?
            .to_owned();
        let at = context
            .manifest
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
            Line::Item(x) => LibraryRecord::Item(x),
            Line::Rendition(x) => LibraryRecord::Rendition(x),
            Line::Assignment(x) => LibraryRecord::Assignment(x),
            Line::Collection(x) => LibraryRecord::Collection(x),
            Line::Entry(x) => LibraryRecord::Entry(x),
            Line::Rating(x) => LibraryRecord::Rating(x),
            Line::Comparison(x) => LibraryRecord::Comparison(x),
            Line::View(x) => LibraryRecord::View(x),
            Line::Dismissal(x) => LibraryRecord::Dismissal(x),
        };
        Ok((vec![record], ()))
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
        image: None,
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
            vanished: Vec::new(),
        }),
        cursor_unchanged: Value::Null,
    }
}

/// The library's sources: scans of library roots and library requests,
/// written through `library-add` and the API rather than read from a path.
pub const KIND: crate::binding::InputKind = crate::binding::InputKind {
    name: "library",
    specs: || vec![&SCAN, &REQUEST],
    plan: None,
    source: |b| {
        if b.source == SCAN.name {
            Ok(Box::new(scan_source()))
        } else {
            Ok(Box::new(request_source()))
        }
    },
    reader: None,
    capture: None,
};

#[cfg(test)]
mod tests;
