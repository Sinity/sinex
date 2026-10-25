//! Export declarations and record framing. `group` traverses a stored archive
//! and stores exact decoded record parts, separately from its captured
//! original. Callers use that traversal for one binding or compatible bindings.
pub mod group;
pub(crate) mod records;

use super::DriverError;
use serde::{Deserialize, Serialize};
use sinex_model::InputRef;
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
    Stateless,
    Gzip,
    Zip,
    Tar,
    TarGzip,
}

#[derive(Clone)]
pub struct ExportArchive {
    pub path: PathBuf,
    pub host: String,
    /// Which archive members to read, by path inside the archive. A plain
    /// file is always read.
    pub select: fn(&str) -> bool,
    pub framing: Framing,
    /// Target bytes per decoded record part.
    pub part_bytes: usize,
    /// Joins consecutive framed records into one; `None` reads each alone.
    pub join: Option<Join>,
}

/// The key of a framed record: consecutive records with the same key are read
/// as one record (Fit's points of one data type and local day). A record
/// without a key stays alone. A joined record is never split across parts.
#[derive(Clone)]
pub struct Join(pub std::sync::Arc<dyn Fn(&[u8]) -> Option<String> + Send + Sync>);

/// Default target size for decoded record parts. One record can exceed it.
pub const PART_BYTES: usize = 64 << 20;

impl ExportArchive {
    fn input(&self) -> InputRef {
        InputRef {
            kind: "export".into(),
            locator: self.path.display().to_string(),
            host: self.host.clone(),
        }
    }
}

fn is_tar(head: &[u8]) -> bool {
    head.len() >= 262 && &head[257..262] == b"ustar"
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
