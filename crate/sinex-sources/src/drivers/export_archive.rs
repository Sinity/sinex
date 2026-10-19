//! `ExportArchive`: an export as received (Takeout, Spotify, an mbox file, a
//! Raindrop CSV), architecture §5.3.
//!
//! A compressed export (ZIP, tar, gzipped tar, gzip) is stored whole, exactly
//! as received. Its records are addressed in the *member stream*: the selected
//! members, decoded and concatenated in archive order. The manifest lists each
//! member's path and its range in that stream, so a replay decodes the stored
//! archive the same way and every anchor reads back its record.
//!
//! A plain file is stored as itself. One whose records can be cut anywhere
//! between records (lines, CSV rows, mbox messages) is read in parts, so an
//! export of any size is read in bounded steps; a JSON document is read whole,
//! since its records are only known once it is parsed.
//!
//! An unchanged stat tuple skips the file. A changed one whose content hash is
//! the same is a touch, not a new export. Anything else is a new revision,
//! read from the start; record keys make the records already held repeats.
use super::{Acquired, DriverError, NewMaterial, file_facts};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sinex_model::{InputRef, MaterialId, ObservationFacts, RevisionChange};
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

/// How records are framed inside one member (or a plain file).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Framing {
    /// One record per line; a final line without a newline counts at the end.
    Lines,
    /// The elements of the JSON array at this JSON pointer (`""` for a
    /// top-level array, `"/Browser History"` for one inside an object).
    JsonArray(&'static str),
    /// CSV rows, the header row included (the parser carries it).
    Csv,
    /// mbox messages, each starting at a `From ` line.
    Mbox,
    /// The whole member is one record.
    Whole,
    /// A record starts at each occurrence of this byte pattern and runs to the
    /// next (an HTML export's repeated cell); bytes before the first are not a
    /// record.
    Delimited(&'static [u8]),
}

impl Framing {
    /// Whether records can be framed from a prefix of the bytes, so a plain
    /// file can be read in parts.
    fn streamable(self) -> bool {
        matches!(
            self,
            Self::Lines | Self::Csv | Self::Mbox | Self::Delimited(_)
        )
    }

    fn name(self) -> String {
        match self {
            Self::Lines => "lines".into(),
            Self::JsonArray(p) => format!("json-array:{p}"),
            Self::Csv => "csv".into(),
            Self::Mbox => "mbox".into(),
            Self::Whole => "whole".into(),
            Self::Delimited(d) => format!("delimited:{}", String::from_utf8_lossy(d)),
        }
    }

    /// Record ranges in `bytes`, and how many bytes they cover. When `eof` is
    /// false a trailing record that may be incomplete is left out.
    pub fn frame(self, bytes: &[u8], eof: bool) -> (Vec<(usize, usize)>, usize) {
        match self {
            Self::Lines => lines(bytes, eof),
            Self::JsonArray(pointer) => match json_array(bytes, pointer) {
                Ok(records) => (records, bytes.len()),
                // Not the expected document: one record, which the parser
                // then fails, so the bytes are kept and the failure is named.
                Err(_) if !bytes.is_empty() => (vec![(0, bytes.len())], bytes.len()),
                Err(_) => (vec![], 0),
            },
            Self::Csv => csv_rows(bytes, eof),
            Self::Mbox => mbox(bytes, eof),
            Self::Whole if bytes.is_empty() => (vec![], 0),
            Self::Whole => (vec![(0, bytes.len())], bytes.len()),
            Self::Delimited(delimiter) => delimited(bytes, delimiter, eof),
        }
    }
}

/// The container a file is, from its first bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Container {
    Plain,
    Gzip,
    Zip,
    Tar,
    TarGzip,
}

pub struct ExportArchive {
    pub path: PathBuf,
    pub host: String,
    /// Which archive members to read, by path inside the archive. A plain
    /// file is always read.
    pub select: fn(&str) -> bool,
    pub framing: Framing,
    /// Bytes per part when a plain file is read in parts.
    pub part_bytes: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    pub size: u64,
    pub mtime_ns: i128,
    pub inode: u64,
    /// BLAKE3 of the whole file, once read to the end.
    pub content: Option<String>,
    /// For a plain file read in parts: how far it is read.
    pub offset: u64,
    /// Whether the current revision is read to the end.
    pub done: bool,
    pub material: Option<MaterialId>,
    pub part_index: u32,
    /// For a compressed export read in parts: the next selected member.
    #[serde(default)]
    pub member: usize,
}

/// Default part size for plain files read in parts.
pub const PART_BYTES: usize = 64 << 20;

impl ExportArchive {
    fn input(&self) -> InputRef {
        InputRef {
            kind: "export".into(),
            locator: self.path.display().to_string(),
            host: self.host.clone(),
        }
    }

    pub fn poll(&self, cursor: &Value) -> Result<Acquired, DriverError> {
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
        let same_stat = !cursor.is_null()
            && previous.size == size
            && previous.mtime_ns == mtime
            && previous.inode == inode;
        let unchanged = |cursor: Cursor, observation: ObservationFacts| Acquired {
            input: self.input(),
            observation,
            revision: RevisionChange::Same,
            material: None,
            cursor_unchanged: serde_json::to_value(cursor).expect("cursor serializes"),
        };
        if same_stat && previous.done {
            return Ok(unchanged(
                previous,
                ObservationFacts {
                    changed: false,
                    ..facts
                },
            ));
        }
        let continuing = same_stat && !previous.done;
        if !continuing && previous.done {
            // The stat tuple changed; a touch keeps the same content.
            let content = hash_file(&self.path)?;
            if previous.content.as_deref() == Some(content.as_str()) {
                return Ok(unchanged(
                    Cursor {
                        size,
                        mtime_ns: mtime,
                        inode,
                        ..previous
                    },
                    ObservationFacts {
                        changed: false,
                        ..facts
                    },
                ));
            }
        }
        let container = sniff(&self.path)?;
        // Every reading of one export file is ordered by the file's time, so a
        // later export of the same records is the newer reading.
        let order = i64::try_from(mtime).unwrap_or(i64::MAX);
        let base = Cursor {
            size,
            mtime_ns: mtime,
            inode,
            content: None,
            offset: if continuing { previous.offset } else { 0 },
            done: false,
            material: if continuing { previous.material } else { None },
            part_index: if continuing { previous.part_index } else { 0 },
            member: if continuing { previous.member } else { 0 },
        };
        let revision = if continuing {
            RevisionChange::Appended
        } else if cursor.is_null() {
            RevisionChange::Snapshot
        } else {
            RevisionChange::Replaced
        };
        let read = match container {
            Container::Plain if self.framing.streamable() => self.read_part(&base, size)?,
            Container::Plain => self.read_whole()?,
            _ => self.read_archive(container, base.member)?,
        };
        let Some(read) = read else {
            let content = hash_file(&self.path)?;
            return Ok(unchanged(
                Cursor {
                    done: true,
                    content: Some(content),
                    offset: size,
                    ..base
                },
                facts,
            ));
        };
        let id = MaterialId::new();
        let part_index = if base.material.is_some() {
            base.part_index + 1
        } else {
            0
        };
        let previous_part = base.material;
        let content = if read.reaches_end {
            Some(hash_file(&self.path)?)
        } else {
            None
        };
        let file_offset = read.file_offset;
        let consumed = read.consumed;
        let last = read.records.last().map_or(0, |(_, e)| *e);
        let reaches_end = read.reaches_end;
        let archive = read.members.is_some();
        let first_member = base.member;
        let next_member = read.next_member;
        let cursor_at = move |end: usize| -> Value {
            // Within a plain file's part, the cursor is the byte after the
            // record; a whole read is done only after its last record.
            let reaches = reaches_end && end >= last;
            serde_json::to_value(Cursor {
                size,
                mtime_ns: mtime,
                inode,
                content: if reaches { content.clone() } else { None },
                offset: if archive {
                    if reaches { size } else { 0 }
                } else if reaches {
                    file_offset + consumed as u64
                } else {
                    file_offset + end as u64
                },
                done: reaches,
                material: Some(id),
                part_index,
                // A crash inside a part re-reads the part (its records come
                // back as repeats); after the part's last record the next
                // part starts at the member after it.
                member: if end >= last {
                    next_member
                } else {
                    first_member
                },
            })
            .expect("cursor serializes")
        };
        let mut manifest = json!({
            "path": self.path.display().to_string(),
            "host": self.host,
            "container": container,
            "framing": self.framing.name(),
            // Every reading of one export file is as new as the file.
            "order_base": order,
        });
        if read.members.is_some() && read.stored_file.is_none() {
            // A later part of an archive stores its decoded member stream,
            // so it is read as a plain stream of the listed members.
            manifest["container"] = json!(Container::Plain);
        }
        if let Some(members) = read.members {
            manifest["members"] = Value::Array(members);
        } else {
            manifest["file_offset"] = json!(file_offset);
        }
        Ok(Acquired {
            input: self.input(),
            observation: facts,
            revision,
            material: Some(NewMaterial {
                id,
                bytes: read.bytes,
                stored_file: read.stored_file,
                records: read.records,
                part_index,
                previous_part,
                reading_order: super::reading_order(&manifest),
                manifest,
                cursor_at: Box::new(cursor_at),
                aligned: false,
                vanished: Vec::new(),
            }),
            cursor_unchanged: Value::Null,
        })
    }

    /// The next part of a plain file, cut at a record boundary.
    fn read_part(&self, base: &Cursor, size: u64) -> Result<Option<Part>, DriverError> {
        let mut file = std::fs::File::open(&self.path).map_err(io)?;
        let start = base.offset.min(size);
        if start == size {
            return Ok(None);
        }
        file.seek(SeekFrom::Start(start)).map_err(io)?;
        let mut bytes = Vec::new();
        let mut want = self.part_bytes.max(1) as u64;
        loop {
            bytes.clear();
            file.seek(SeekFrom::Start(start)).map_err(io)?;
            (&mut file).take(want).read_to_end(&mut bytes).map_err(io)?;
            let eof = start + bytes.len() as u64 >= size;
            let (records, consumed) = self.framing.frame(&bytes, eof);
            if consumed > 0 || eof {
                bytes.truncate(consumed);
                return Ok(if records.is_empty() {
                    None
                } else {
                    Some(Part {
                        reaches_end: eof,
                        bytes,
                        records,
                        consumed,
                        file_offset: start,
                        stored_file: None,
                        members: None,
                        next_member: 0,
                    })
                });
            }
            // One record longer than a part: read further rather than refuse it.
            want = want.saturating_mul(2);
        }
    }

    fn read_whole(&self) -> Result<Option<Part>, DriverError> {
        let bytes = std::fs::read(&self.path).map_err(io)?;
        let (records, consumed) = self.framing.frame(&bytes, true);
        if records.is_empty() {
            return Ok(None);
        }
        Ok(Some(Part {
            reaches_end: true,
            bytes,
            records,
            consumed,
            file_offset: 0,
            stored_file: None,
            members: None,
            next_member: 0,
        }))
    }

    /// The next part of a compressed export: the selected members from
    /// `first` on, decoded into one stream, stopping once the stream reaches
    /// a part's size (a member is never split; one larger than a part is a
    /// part alone). The first part stores the archive as received; later
    /// parts store their decoded stream, so memory holds one part.
    fn read_archive(
        &self,
        container: Container,
        first: usize,
    ) -> Result<Option<Part>, DriverError> {
        let mut stream = Vec::new();
        let mut records = Vec::new();
        let mut members = Vec::new();
        let mut next = first;
        let mut more = false;
        let part = self.part_bytes.max(1);
        visit_members(
            &self.path,
            container,
            self.select,
            &mut |n, path, reader| {
                if n < first {
                    return Ok(true);
                }
                if !members.is_empty() && stream.len() >= part {
                    more = true;
                    return Ok(false);
                }
                let mut data = Vec::new();
                reader.read_to_end(&mut data).map_err(io)?;
                let offset = stream.len();
                let (framed, _) = self.framing.frame(&data, true);
                records.extend(framed.into_iter().map(|(s, e)| (offset + s, offset + e)));
                members.push(json!({"path": path, "stream_offset": offset, "length": data.len()}));
                stream.extend_from_slice(&data);
                next = n + 1;
                Ok(true)
            },
        )?;
        if members.is_empty() {
            return Ok(None);
        }
        let consumed = stream.len();
        Ok(Some(Part {
            reaches_end: !more,
            bytes: stream,
            records,
            consumed,
            file_offset: 0,
            stored_file: (first == 0).then(|| self.path.clone()),
            members: Some(members),
            next_member: next,
        }))
    }
}

struct Part {
    reaches_end: bool,
    bytes: Vec<u8>,
    records: Vec<(usize, usize)>,
    consumed: usize,
    file_offset: u64,
    stored_file: Option<PathBuf>,
    members: Option<Vec<Value>>,
    /// For an archive read in parts: the selected member after this part.
    next_member: usize,
}

/// Visit the selected members of an export, in archive order, numbered from
/// 0 among the selected. `visit` gets each member's number, path and a reader
/// of its decoded bytes; it reads what it wants (a skipped member is never
/// decoded) and returns whether to go on. The same decoding rebuilds the
/// member stream from the stored archive.
pub fn visit_members(
    path: &std::path::Path,
    container: Container,
    select: fn(&str) -> bool,
    visit: &mut dyn FnMut(usize, String, &mut dyn Read) -> Result<bool, DriverError>,
) -> Result<(), DriverError> {
    let file = std::fs::File::open(path).map_err(io)?;
    match container {
        Container::Plain => {
            let name = file_name(path);
            let mut reader = std::io::BufReader::new(file);
            visit(0, name, &mut reader)?;
        }
        Container::Gzip => {
            let name = file_name(path);
            let name = name.strip_suffix(".gz").unwrap_or(&name).to_owned();
            let mut reader = flate2::read::MultiGzDecoder::new(std::io::BufReader::new(file));
            visit(0, name, &mut reader)?;
        }
        Container::Zip => {
            let mut archive = zip::ZipArchive::new(std::io::BufReader::new(file)).map_err(|e| {
                DriverError::Io(format!("{}: not a readable ZIP: {e}", path.display()))
            })?;
            let mut n = 0;
            for i in 0..archive.len() {
                let mut entry = archive
                    .by_index(i)
                    .map_err(|e| DriverError::Io(format!("{}: member {i}: {e}", path.display())))?;
                if entry.is_dir() || !select(entry.name()) {
                    continue;
                }
                let name = entry.name().to_owned();
                let go_on = visit(n, name, &mut entry)?;
                n += 1;
                if !go_on {
                    break;
                }
            }
        }
        Container::Tar | Container::TarGzip => {
            let reader: Box<dyn Read> = if container == Container::TarGzip {
                Box::new(flate2::read::MultiGzDecoder::new(std::io::BufReader::new(
                    file,
                )))
            } else {
                Box::new(std::io::BufReader::new(file))
            };
            let mut archive = tar::Archive::new(reader);
            let mut n = 0;
            for entry in archive.entries().map_err(io)? {
                let mut entry = entry.map_err(io)?;
                if !entry.header().entry_type().is_file() {
                    continue;
                }
                let name = entry.path().map_err(io)?.to_string_lossy().into_owned();
                if !select(&name) {
                    continue;
                }
                let go_on = visit(n, name, &mut entry)?;
                n += 1;
                if !go_on {
                    break;
                }
            }
        }
    }
    Ok(())
}

/// Decode every selected member of an export, in archive order.
pub fn decode_members(
    path: &std::path::Path,
    container: Container,
    select: fn(&str) -> bool,
    add: &mut dyn FnMut(String, Vec<u8>),
) -> Result<(), DriverError> {
    visit_members(path, container, select, &mut |_, name, reader| {
        let mut data = Vec::new();
        reader.read_to_end(&mut data).map_err(io)?;
        add(name, data);
        Ok(true)
    })
}

/// The container `path` is, from its first bytes.
pub fn sniff(path: &std::path::Path) -> Result<Container, DriverError> {
    let mut head = [0u8; 512];
    let mut file = std::fs::File::open(path).map_err(io)?;
    let n = read_up_to(&mut file, &mut head)?;
    let head = &head[..n];
    if head.starts_with(b"PK\x03\x04") || head.starts_with(b"PK\x05\x06") {
        return Ok(Container::Zip);
    }
    if is_tar(head) {
        return Ok(Container::Tar);
    }
    if head.starts_with(&[0x1f, 0x8b]) {
        file.seek(SeekFrom::Start(0)).map_err(io)?;
        let mut inner = [0u8; 512];
        let mut gz = flate2::read::MultiGzDecoder::new(std::io::BufReader::new(file));
        let m = read_up_to(&mut gz, &mut inner).unwrap_or(0);
        return Ok(if is_tar(&inner[..m]) {
            Container::TarGzip
        } else {
            Container::Gzip
        });
    }
    Ok(Container::Plain)
}

fn is_tar(head: &[u8]) -> bool {
    head.len() >= 262 && &head[257..262] == b"ustar"
}

fn read_up_to(reader: &mut dyn Read, buf: &mut [u8]) -> Result<usize, DriverError> {
    let mut n = 0;
    while n < buf.len() {
        match reader.read(&mut buf[n..]).map_err(io)? {
            0 => break,
            k => n += k,
        }
    }
    Ok(n)
}

fn hash_file(path: &std::path::Path) -> Result<String, DriverError> {
    let mut hasher = blake3::Hasher::new();
    hasher
        .update_reader(std::fs::File::open(path).map_err(io)?)
        .map_err(io)?;
    Ok(format!("blake3:{}", hasher.finalize().to_hex()))
}

fn file_name(path: &std::path::Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn io(e: impl std::fmt::Display) -> DriverError {
    DriverError::Io(e.to_string())
}

fn lines(bytes: &[u8], eof: bool) -> (Vec<(usize, usize)>, usize) {
    let mut out = Vec::new();
    let mut start = 0;
    for (i, b) in bytes.iter().enumerate() {
        if *b == b'\n' {
            out.push((start, i + 1));
            start = i + 1;
        }
    }
    if eof && start < bytes.len() {
        out.push((start, bytes.len()));
        start = bytes.len();
    }
    (out, start)
}

/// The byte ranges of the elements of the JSON array at `pointer`.
fn json_array(bytes: &[u8], pointer: &str) -> Result<Vec<(usize, usize)>, String> {
    use serde_json::value::RawValue;
    let text = std::str::from_utf8(bytes).map_err(|e| e.to_string())?;
    let mut current: &RawValue = serde_json::from_str(text).map_err(|e| e.to_string())?;
    for segment in pointer.split('/').skip(1) {
        let object: std::collections::HashMap<String, &RawValue> =
            serde_json::from_str(current.get()).map_err(|e| e.to_string())?;
        current = object
            .get(segment)
            .copied()
            .ok_or_else(|| format!("no {segment} in the document"))?;
    }
    let items: Vec<&RawValue> = serde_json::from_str(current.get()).map_err(|e| e.to_string())?;
    let base = text.as_ptr() as usize;
    Ok(items
        .iter()
        .map(|item| {
            let start = item.get().as_ptr() as usize - base;
            (start, start + item.get().len())
        })
        .collect())
}

fn csv_rows(bytes: &[u8], eof: bool) -> (Vec<(usize, usize)>, usize) {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(bytes);
    let mut out = Vec::new();
    let mut record = csv::ByteRecord::new();
    loop {
        let start = reader.position().byte() as usize;
        match reader.read_byte_record(&mut record) {
            Ok(true) => out.push((start, reader.position().byte() as usize)),
            Ok(false) => break,
            // A malformed row: keep it as one record so it fails by name.
            Err(_) => {
                let end = reader.position().byte() as usize;
                if end <= start {
                    break;
                }
                out.push((start, end));
            }
        }
    }
    // Without the end of the file, the last row may be cut short.
    if !eof {
        out.pop();
    }
    let consumed = out.last().map_or(0, |(_, e)| *e);
    (out, consumed)
}

/// Records starting at each `delimiter`. Before `eof` the last one may be
/// incomplete, so it is left for the next part.
fn delimited(bytes: &[u8], delimiter: &[u8], eof: bool) -> (Vec<(usize, usize)>, usize) {
    let mut starts = Vec::new();
    let mut i = 0;
    while let Some(p) = find(&bytes[i..], delimiter) {
        starts.push(i + p);
        i += p + delimiter.len();
    }
    let mut out: Vec<(usize, usize)> = starts.windows(2).map(|w| (w[0], w[1])).collect();
    match starts.last() {
        Some(&last) if eof => {
            out.push((last, bytes.len()));
            (out, bytes.len())
        }
        Some(&last) => (out, last),
        // No record yet: everything before a delimiter could still be one, so
        // only the bytes that cannot hold the start of a delimiter are covered.
        None if eof => (out, bytes.len()),
        None => (out, bytes.len().saturating_sub(delimiter.len())),
    }
}

fn mbox(bytes: &[u8], eof: bool) -> (Vec<(usize, usize)>, usize) {
    let mut starts = Vec::new();
    if bytes.starts_with(b"From ") {
        starts.push(0);
    }
    let mut i = 0;
    while let Some(p) = find(&bytes[i..], b"\nFrom ") {
        starts.push(i + p + 1);
        i += p + 1;
    }
    let mut out = Vec::new();
    for w in starts.windows(2) {
        out.push((w[0], w[1]));
    }
    match starts.last() {
        Some(&last) if eof => {
            out.push((last, bytes.len()));
            (out, bytes.len())
        }
        Some(&last) => (out, last),
        None => (out, 0),
    }
}

/// Framers for a source's stored plain-file parts, so a replay or an
/// excision finds the records the driver read. (A compressed export's records
/// are in its decoded member stream, which these do not decode.)
pub fn frame_lines(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Lines.frame(bytes, true)
}

pub fn frame_csv(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Csv.frame(bytes, true)
}

pub fn frame_mbox(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Mbox.frame(bytes, true)
}

pub fn frame_json_array(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::JsonArray("").frame(bytes, true)
}

/// One record per stored material; for directory snapshots the manifest
/// lists the records instead.
pub fn frame_whole(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Whole.frame(bytes, true)
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests;
