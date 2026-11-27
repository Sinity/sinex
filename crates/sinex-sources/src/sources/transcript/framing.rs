//! Cue blocks include their identifiers and settings, with exact byte ranges.
use sinex_source_core::drivers::export_archive::DocumentIndex;
use sinex_source_core::{RecordError, RecordResult};

fn index(bytes: &[u8], nested: bool) -> Result<DocumentIndex, RecordError> {
    let (rows, defaults) = super::borrowed_json_records(bytes, nested)?;
    let range = |raw: &serde_json::value::RawValue| {
        let raw = raw.get().as_bytes();
        // RawValue borrows the exact slice of bytes the provider supplied.
        let start = raw.as_ptr().addr() - bytes.as_ptr().addr();
        (start, start + raw.len())
    };
    let mut metadata = std::collections::BTreeMap::new();
    for &key in super::DOCUMENT_DEFAULTS {
        if let Some(&raw) = defaults.get(key) {
            serde_json::from_str::<serde_json::Value>(raw.get())
                .record_with(|| format!("transcript document field {key}"))?;
            metadata.insert(key, range(raw));
        }
    }
    let records = rows.into_iter().map(range).collect();
    Ok(DocumentIndex { records, metadata })
}

pub(super) fn json(bytes: &[u8]) -> Result<DocumentIndex, RecordError> {
    index(bytes, false)
}

pub(super) fn whisper(bytes: &[u8]) -> Result<DocumentIndex, RecordError> {
    index(bytes, true)
}

pub(super) fn frame_json(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    super::JSON_FRAMING.frame(bytes, true)
}

pub(super) fn frame_whisper(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    super::WHISPER_FRAMING.frame(bytes, true)
}

pub fn frame(bytes: &[u8], eof: bool) -> (Vec<(usize, usize)>, usize) {
    let mut records = Vec::new();
    let mut start = 0;
    let mut line_start = 0;
    for (i, byte) in bytes.iter().enumerate() {
        if *byte != b'\n' {
            continue;
        }
        if bytes[line_start..i]
            .iter()
            .all(|b| matches!(b, b' ' | b'\t' | b'\r'))
        {
            if bytes[start..line_start]
                .iter()
                .any(|b| !b.is_ascii_whitespace())
            {
                records.push((start, i + 1));
            }
            start = i + 1;
        }
        line_start = i + 1;
    }
    if eof && start < bytes.len() {
        if bytes[start..].iter().any(|b| !b.is_ascii_whitespace()) {
            records.push((start, bytes.len()));
        }
        start = bytes.len();
    }
    (records, start)
}
pub fn whole(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    frame(bytes, true)
}

#[cfg(test)]
mod tests;
