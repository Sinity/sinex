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
use crate::registry::{Declared, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sinex_model::kinds::library::{
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
    framer: crate::drivers::export_archive::frame_lines,
};

pub const REQUEST: SourceSpec = SourceSpec {
    name: "library.request",
    driver: Driver::Operator,
    consumer: "the media library",
    class: Class::Open,
    parser: "library-request",
    parser_version: 1,
    event_types: sinex_model::kinds::library::REQUEST_TYPES,
    framer: crate::drivers::export_archive::frame_lines,
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

pub fn scan_source() -> Declared<ScanParser> {
    Declared {
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

/// The scan's memory: the files read, by path, and the work left from the
/// current walk. Seeded from the live acquisitions after a restart, so a
/// restart does not hash the whole library again.
#[derive(Default)]
pub struct ScanMemory {
    pub files: HashMap<String, Known>,
    queue: VecDeque<Pending>,
    /// Paths missing from the current walk, with the file identity they had.
    missing: BTreeMap<String, Known>,
    walk_pending: bool,
}

impl ScanMemory {
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
/// `<file>.json` (gallery-dl's `--write-metadata`) or `<file>.txt` (Hydrus
/// tag sidecars).
pub fn is_sidecar(relative: &str, exists: impl Fn(&str) -> bool) -> bool {
    [".json", ".txt"]
        .iter()
        .filter_map(|ext| relative.strip_suffix(ext))
        .any(exists)
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
    fn walk(&self, memory: &mut ScanMemory) -> Result<(), DriverError> {
        let mut found = BTreeMap::new();
        walk(&self.root, &self.root, &mut found)?;
        // Sidecars (`a.jpg.json`, `a.jpg.txt`, `a.json`) describe the file
        // beside them; library import reads them. They are not items.
        let sidecars: Vec<String> = found
            .keys()
            .filter(|p| is_sidecar(p, |other| found.contains_key(other)))
            .cloned()
            .collect();
        for s in sidecars {
            found.remove(&s);
        }
        let mut missing = BTreeMap::new();
        for (path, known) in &memory.files {
            if !found.contains_key(path) {
                missing.insert(path.clone(), known.clone());
            }
        }
        for (path, (size, mtime, dev, ino)) in found {
            let unchanged = memory
                .files
                .get(&path)
                .is_some_and(|k| k.size == size && k.mtime_ns == mtime);
            if !unchanged {
                memory.queue.push_back((path, size, mtime, dev, ino));
            }
        }
        memory.missing = missing;
        memory.walk_pending = true;
        Ok(())
    }

    /// One poll: continue the current walk's queue, or start a new walk.
    pub fn poll(
        &self,
        cursor: &Value,
        memory: &mut ScanMemory,
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
        if !memory.walk_pending {
            self.walk(memory)?;
        }
        let now = Timestamp::now();
        let root = self.root.display().to_string();
        let mut lines: Vec<u8> = Vec::new();
        let mut records = Vec::new();
        let mut moved_away: Vec<(String, Known)> = Vec::new();
        let mut budget = 0u64;
        while let Some((relative, size, mtime, dev, ino)) = memory.queue.front().cloned() {
            if budget > 0 && budget + size > self.bytes_per_poll {
                break;
            }
            memory.queue.pop_front();
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
            let before = memory.files.get(&relative).cloned();
            // Moved: a missing path held this very file (device and inode)
            // with these bytes.
            let moved_from = memory
                .missing
                .iter()
                .find(|(_, k)| {
                    k.content == content && k.device == dev && k.inode == ino && ino.is_some()
                })
                .map(|(p, _)| p.clone());
            if let Some(old) = &moved_from {
                // The old path is gone; its line comes after this one's, so
                // whoever reads the move sees the new path first.
                if let Some(known) = memory.missing.remove(old) {
                    moved_away.push((old.clone(), known));
                }
                memory.files.remove(old);
            }
            let previous_content = before
                .as_ref()
                .filter(|k| k.content != content && k.device == dev && k.inode == ino)
                .map(|k| k.content.clone());
            if before.as_ref().is_some_and(|k| k.content == content) {
                // Touched, not changed.
                memory.files.insert(
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
            memory.files.insert(
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
        let walk_done = memory.queue.is_empty();
        let mut gone: Vec<(String, Known)> = moved_away;
        if walk_done {
            // Whatever is still missing after the walk's files were read was
            // not moved: it vanished.
            gone.extend(std::mem::take(&mut memory.missing));
            memory.walk_pending = false;
        }
        {
            for (relative, known) in gone {
                memory.files.remove(&relative);
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
            input: self.input(),
            observation: facts,
            revision: RevisionChange::Snapshot,
            material: Some(NewMaterial {
                id,
                bytes: lines,
                stored_file: None,
                records,
                part_index,
                previous_part: previous.material,
                manifest,
                reading_order: Box::new(move |_| ReadingOrder(read_at)),
                cursor_at: Box::new(move |_| {
                    serde_json::to_value(ScanCursor {
                        material: Some(id),
                        part_index,
                        ..scanned.clone()
                    })
                    .expect("cursor serializes")
                }),
                aligned: false,
                vanished: Vec::new(),
            }),
            cursor_unchanged: Value::Null,
        })
    }

    /// Poll until a walk completes, returning every material along the way.
    /// Callers that admit as they go use `poll` directly.
    pub fn pending(&self, memory: &ScanMemory) -> usize {
        memory.queue.len()
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
        _: &Map<String, Value>,
        _: &(),
    ) -> Result<(Vec<LibraryRecord>, ()), RecordError> {
        Err(RecordError::new("a library request needs its manifest"))
    }

    fn parse_record(
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

pub fn request_source() -> Declared<RequestParser> {
    Declared {
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
            stored_file: None,
            records: spans,
            part_index: 0,
            previous_part: None,
            manifest,
            reading_order: Box::new(move |_| ReadingOrder(order)),
            cursor_at: Box::new(|_| Value::Null),
            aligned: false,
            vanished: Vec::new(),
        }),
        cursor_unchanged: Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DrainInput, Limits, drain};
    use sinex_model::kinds::library::{Level, Target};
    use sinex_model::{RecordOutcome, RecordState};

    struct Memory(std::sync::Mutex<HashMap<String, Vec<u8>>>);
    impl MaterialSink for Memory {
        fn put(&self, bytes: &[u8]) -> Result<String, String> {
            let name = format!("blake3:{}", blake3::hash(bytes).to_hex());
            self.0.lock().unwrap().insert(name.clone(), bytes.to_vec());
            Ok(name)
        }
    }

    fn tmp() -> PathBuf {
        let d = std::env::temp_dir().join(format!("sinex-library-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn png(w: u32, h: u32) -> Vec<u8> {
        let mut b = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
        b.extend_from_slice(&w.to_be_bytes());
        b.extend_from_slice(&h.to_be_bytes());
        b.extend_from_slice(&[8, 6, 0, 0, 0, 0, 0, 0, 0]);
        b
    }

    fn read(a: &Acquired, parser: &ScanParser) -> Vec<Acquisition> {
        let m = a.material.as_ref().expect("a material");
        let out = drain(
            parser,
            DrainInput {
                source: "library.scan",
                class: Class::Open,
                material: m.id,
                bytes: &m.bytes,
                records: &m.records,
                carry: Value::Null,
                reading_order: &*m.reading_order,
                limits: Limits::default(),
                manifest: &m.manifest,
            },
        )
        .unwrap();
        out.into_iter()
            .flat_map(|c| c.outcomes)
            .flat_map(|o| match o {
                RecordOutcome::Events { events, .. } => events,
                RecordOutcome::Failure { reason, .. } => panic!("{reason}"),
            })
            .map(|e| {
                assert_eq!(e.event_type, "library.acquisition");
                e.payload_as::<Acquisition>().unwrap()
            })
            .collect()
    }

    fn scan(root: &Path) -> LibraryScan {
        LibraryScan {
            root: root.to_path_buf(),
            host: "h".into(),
            keep_bytes: true,
            probe: false,
            bytes_per_poll: 1 << 30,
        }
    }

    #[test]
    fn scan_describes_stores_and_follows_renames_edits_and_removals() {
        let root = tmp();
        std::fs::create_dir_all(root.join("a")).unwrap();
        std::fs::write(root.join("a/one.png"), png(640, 480)).unwrap();
        std::fs::write(root.join("two.bin"), b"\0\x01\x02 not media").unwrap();
        std::fs::write(root.join(".hidden"), b"x").unwrap();
        let sink = Memory(Default::default());
        let s = scan(&root);
        let mut mem = ScanMemory::default();
        let parser = ScanParser;

        let first = s.poll(&Value::Null, &mut mem, &sink).unwrap();
        let seen = read(&first, &parser);
        assert_eq!(seen.len(), 2);
        let one = seen.iter().find(|a| a.path.ends_with("one.png")).unwrap();
        assert_eq!(one.media_type, "image/png");
        assert_eq!((one.width, one.height), (Some(640), Some(480)));
        assert!(one.stored && one.content.starts_with("blake3:"));
        assert_eq!(one.sha256.len(), 64);
        assert_eq!(one.md5.len(), 32);
        assert!(sink.0.lock().unwrap().contains_key(&one.content));
        let other = seen.iter().find(|a| a.path.ends_with("two.bin")).unwrap();
        assert_eq!(other.media_type, "application/octet-stream");

        // Nothing changed: no material.
        let again = s.poll(&Value::Null, &mut mem, &sink).unwrap();
        assert!(again.material.is_none());

        // Rename one.png, edit two.bin in place, delete nothing else.
        std::fs::rename(root.join("a/one.png"), root.join("renamed.png")).unwrap();
        std::fs::write(root.join("two.bin"), b"\0\x01\x02 edited").unwrap();
        let third = s.poll(&Value::Null, &mut mem, &sink).unwrap();
        let seen = read(&third, &parser);
        let moved = seen
            .iter()
            .find(|a| a.path.ends_with("renamed.png"))
            .unwrap();
        assert!(moved.moved_from.as_deref().unwrap().ends_with("a/one.png"));
        let edited = seen.iter().find(|a| a.path.ends_with("two.bin")).unwrap();
        assert_eq!(
            edited.previous_content.as_deref(),
            Some(other.content.as_str())
        );
        // The old path's copy is gone, after the line that names the move.
        let gone: Vec<_> = seen.iter().filter(|a| a.vanished_at.is_some()).collect();
        assert_eq!(gone.len(), 1);
        assert!(gone[0].path.ends_with("a/one.png"));
        let moved_at = seen
            .iter()
            .position(|a| a.path.ends_with("renamed.png"))
            .unwrap();
        let gone_at = seen.iter().position(|a| a.vanished_at.is_some()).unwrap();
        assert!(moved_at < gone_at);

        std::fs::remove_file(root.join("renamed.png")).unwrap();
        let fourth = s.poll(&Value::Null, &mut mem, &sink).unwrap();
        let seen = read(&fourth, &parser);
        assert_eq!(seen.len(), 1);
        assert!(seen[0].vanished_at.is_some());
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn equal_bytes_hash_equal_wherever_they_are() {
        let a = json!({"host": "h", "root": "/r", "path": "/r/a", "content": "blake3:x", "size": 1,
            "sha256": "s", "md5": "m", "media_type": "image/png", "stored": true});
        let b = json!({"host": "h", "root": "/q", "path": "/q/b", "content": "blake3:x", "size": 1,
            "sha256": "s", "md5": "m", "media_type": "image/png", "stored": false});
        let hash = |v: &Value| {
            <Map<String, Value> as crate::RecordShape>::hash(
                v.to_string().as_bytes(),
                ScanParser::VOLATILE,
            )
        };
        assert_eq!(hash(&a), hash(&b));
    }

    #[test]
    fn sniffs_types_from_bytes_not_names() {
        assert_eq!(sniff(b"\xFF\xD8\xFF\xE0"), "image/jpeg");
        assert_eq!(sniff(b"GIF89a.."), "image/gif");
        assert_eq!(sniff(b"RIFF\0\0\0\0WEBPVP8 "), "image/webp");
        assert_eq!(sniff(b"\0\0\0\x20ftypisom"), "video/mp4");
        assert_eq!(sniff(b"\x1A\x45\xDF\xA3 webm"), "video/webm");
        assert_eq!(sniff(b"%PDF-1.7"), "application/pdf");
        assert_eq!(sniff(b"hello"), "text/plain");
        let gif = b"GIF89a\x40\x01\xF0\x00";
        assert_eq!(dimensions(gif, "image/gif").0, Some(320));
        assert_eq!(dimensions(gif, "image/gif").1, Some(240));
    }

    #[test]
    fn requests_take_author_and_time_from_the_manifest() {
        let item = Item {
            item: "i1".into(),
            revision: 1,
            replaces: None,
            content: "blake3:x".into(),
            media_type: "image/png".into(),
            acquisitions: vec!["h:/r/a.png".into()],
            label: Some("a.png".into()),
            by: "forged".into(),
            at: Timestamp::from_unix_secs(1).unwrap(),
            withdrawn_at: None,
        };
        let tag = Assignment {
            assignment: "as1".into(),
            target: Target {
                item: "i1".into(),
                ..Target::default()
            },
            field: "tag".into(),
            value: Some("place:croatia".into()),
            level: Level::Override,
            source_ref: None,
            by: "forged".into(),
            at: Timestamp::from_unix_secs(1).unwrap(),
            withdrawn_at: Some(Timestamp::from_unix_secs(2).unwrap()),
        };
        let a = request_material(
            &[LibraryRecord::Item(item), LibraryRecord::Assignment(tag)],
            "operator",
            "h",
            "test",
        );
        let m = a.material.as_ref().unwrap();
        let out = drain(
            &RequestParser,
            DrainInput {
                source: "library.request",
                class: Class::Open,
                material: m.id,
                bytes: &m.bytes,
                records: &m.records,
                carry: Value::Null,
                reading_order: &*m.reading_order,
                limits: Limits::default(),
                manifest: &m.manifest,
            },
        )
        .unwrap();
        let events: Vec<_> = out
            .into_iter()
            .flat_map(|c| c.outcomes)
            .flat_map(|o| match o {
                RecordOutcome::Events { events, .. } => events,
                RecordOutcome::Failure { reason, .. } => panic!("{reason}"),
            })
            .collect();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "library.item");
        assert_eq!(events[0].payload["by"], "operator");
        assert_ne!(events[0].payload["at"], json!("1970-01-01T00:00:01Z"));
        assert_eq!(events[1].event_type, "library.assignment");
        assert_eq!(events[1].record_state, RecordState::Removed);
    }
}
