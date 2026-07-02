//! `docs-library-index` source — document library metadata index.
//!
//! Emits one `document.indexed` event per file found under the configured
//! library root(s) using [`DirectoryWalkAdapter`].  Each event carries
//! filesystem metadata and filename-derived hints; **no document content
//! is extracted** — that is the domain of `document.staging` and the
//! document-layer parsers.
//!
//! ## Scope split
//!
//! | Concern | Source |
//! |---|---|
//! | Content extraction (PDF/EPUB body text) | `document.staging` |
//! | Library metadata index (filename, size, mtime, hints) | **this unit** |
//!
//! ## Filename heuristics
//!
//! Heuristics are applied in order and all are optional:
//!
//! - **Author**: segment before the first ` - ` separator.
//! - **Title**: segment after the last ` - ` and before the year/extension.
//! - **Year**: first `\d{4}` match in the range 1900–2030.
//! - **External id**: first 32-character lowercase hex sequence
//!   (MD5 / Anna's Archive identifier).
//!
//! When no heuristic fires the corresponding field is `None`.
//!
//! ## Occurrence identity
//!
//! `(path, content_hash)` — re-imports only republish on content change.
//! The adapter's fingerprint dedup (`size_bytes`, `modified_ms`) guards
//! the cursor; the occurrence key guards cross-run idempotency.

use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult};
use sinex_macros::SourceMeta;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape,
};
use sinex_primitives::{
    domain::{EventSource, EventType},
    parser::{
        InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
        ParserManifest, SourceId, SourceRecord, TimingEvidence,
    },
    privacy::ProcessingContext,
    temporal::Timestamp,
};

// ---------------------------------------------------------------------------
// Parser config
// ---------------------------------------------------------------------------

/// Configuration for [`DocsLibraryParser`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DocsLibraryParserConfig;

// ---------------------------------------------------------------------------
// Filename heuristics
// ---------------------------------------------------------------------------

/// Extract a four-digit year (1900–2030) from a filename stem.
///
/// Returns the first matching year found left-to-right.
fn extract_year(stem: &str) -> Option<u16> {
    let bytes = stem.as_bytes();
    let mut i = 0;
    while i + 4 <= bytes.len() {
        let slice = &bytes[i..i + 4];
        if slice.iter().all(u8::is_ascii_digit) {
            // Check that neither adjacent byte is also a digit (avoid matching
            // longer numeric sequences at the boundary).
            let before_ok = i == 0 || !bytes[i - 1].is_ascii_digit();
            let after_ok = i + 4 >= bytes.len() || !bytes[i + 4].is_ascii_digit();
            if before_ok && after_ok {
                let s = std::str::from_utf8(slice).ok()?;
                if let Ok(y) = s.parse::<u16>()
                    && (1900..=2030).contains(&y)
                {
                    return Some(y);
                }
            }
        }
        i += 1;
    }
    None
}

/// Extract the first 32-character lowercase hex sequence from a filename.
///
/// Interprets such a sequence as an MD5 / Anna's Archive identifier.
fn extract_external_id(stem: &str) -> Option<String> {
    let bytes = stem.as_bytes();
    let mut run_start: Option<usize> = None;
    let mut run_len = 0usize;

    for (i, &b) in bytes.iter().enumerate() {
        let is_hex_lower = matches!(b, b'0'..=b'9' | b'a'..=b'f');
        if is_hex_lower {
            if run_start.is_none() {
                run_start = Some(i);
            }
            run_len += 1;
            if run_len == 32 {
                // Check the run terminates (not longer than 32 hex chars).
                let after = i + 1;
                let after_is_hex =
                    after < bytes.len() && matches!(bytes[after], b'0'..=b'9' | b'a'..=b'f');
                if !after_is_hex {
                    let start = run_start?;
                    return Some(stem[start..start + 32].to_string());
                }
            }
        } else {
            run_start = None;
            run_len = 0;
        }
    }
    None
}

/// Derive title and author hints from a filename stem.
///
/// The convention observed in the library is:
///   `Author Name - Title Text (YEAR)` or
///   `Author Name - Title Text-Publisher (YEAR)`
///
/// Split on ` - ` (with spaces): the first segment is the author, the
/// remainder (minus year / parenthesised suffixes) is the title.
fn extract_author_title(stem: &str) -> (Option<String>, Option<String>) {
    // Strip any trailing parenthesised year group: ` (YYYY)`.
    let stem_clean = {
        let s = stem.trim_end();
        if let Some(pos) = s.rfind(" (") {
            let rest = &s[pos + 2..];
            if rest.starts_with(|c: char| c.is_ascii_digit()) && rest.ends_with(')') {
                &s[..pos]
            } else {
                s
            }
        } else {
            s
        }
    };

    const SEP: &str = " - ";
    let Some(sep_pos) = stem_clean.find(SEP) else {
        // No ` - ` separator — skip both hints.
        return (None, None);
    };

    let author_raw = stem_clean[..sep_pos].trim();
    let title_raw = stem_clean[sep_pos + SEP.len()..].trim();

    let author = if author_raw.is_empty() {
        None
    } else {
        Some(author_raw.to_string())
    };

    let title = if title_raw.is_empty() {
        None
    } else {
        Some(title_raw.to_string())
    };

    (author, title)
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Parses directory entries from the document library into
/// `document.indexed` metadata events.
///
/// Each `SourceRecord` carries the full file bytes (from
/// [`DirectoryWalkAdapter`]). The parser:
///
/// 1. Extracts the path + extension from the anchor.
/// 2. Uses `bytes.len()` for `byte_size`.
/// 3. Reads mtime from the live filesystem (path from anchor).
/// 4. Applies filename heuristics to derive optional fields.
/// 5. Emits `ProcessingContext::Metadata` for DB admission policy.
#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "docs-library-index",
    namespace = "library",
    event_source = "docs-library",
    event_type = "document.indexed",
    adapter = "DirectoryWalkAdapter",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(source, path, content_hash)"),
    access_scope = AccessScope::LibraryRoot,
    implementation = "sinexd",
    privacy_context = ProcessingContext::Metadata,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::OnDemand,
)]
pub struct DocsLibraryParser;

#[async_trait]
impl MaterialParser for DocsLibraryParser {
    type Config = DocsLibraryParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("docs-library-index"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::DirectoryWalk],
            source_id: SourceId::from_static("docs-library-index"),
            declared_event_types: vec![(
                EventSource::from_static("docs-library"),
                EventType::from_static("document.indexed"),
            )],
            privacy_contexts: vec![ProcessingContext::Metadata],
            sensitivity_hints: Vec::new(),
            description: "Indexes the local document library as metadata events.".into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        // Extract path from anchor.
        let (path_buf, content_hash) = match &record.anchor {
            MaterialAnchor::DirectoryEntry { path, content_hash } => {
                (path.clone(), content_hash.clone())
            }
            other => {
                return Err(ParserError::InvalidInput(format!(
                    "docs-library-index: expected DirectoryEntry anchor (anchor_kind={other:?})"
                )));
            }
        };

        let filename = path_buf
            .file_name()
            .unwrap_or(path_buf.as_str())
            .to_string();

        let extension = path_buf.extension().unwrap_or("").to_lowercase();

        let byte_size = record.bytes.len() as u64;

        // Read mtime from the live filesystem.  Fallback to now() on any error
        // so the event is not dropped; timing confidence is recorded accordingly.
        let (mtime, mtime_confidence) = read_mtime(&path_buf);

        // Filename stem for heuristics (filename without extension).
        let stem = path_buf.file_stem().unwrap_or(filename.as_str());

        let year_hint = extract_year(stem);
        let external_id = extract_external_id(&stem.to_lowercase());
        let (author_hint, title_hint) = extract_author_title(stem);

        let payload = serde_json::json!({
            "path": path_buf.as_str(),
            "filename": filename,
            "extension": extension,
            "byte_size": byte_size,
            "mtime": mtime,
            "content_hash": content_hash,
            "title_hint": title_hint,
            "author_hint": author_hint,
            "year_hint": year_hint,
            "external_id": external_id,
        });

        let occurrence_key = {
            let hash_field = content_hash.clone().unwrap_or_default();
            OccurrenceKey {
                source_id: SourceId::from_static("docs-library-index"),
                fields: vec![
                    ("path".into(), path_buf.to_string()),
                    ("content_hash".into(), hash_field),
                ],
            }
        };

        let intent = ParsedEventIntent::builder()
            .source_id(ctx.source_id.clone())
            .parser_id(ParserId::from_static("docs-library-index"))
            .parser_version("1.0.0")
            .event_type(EventType::from_static("document.indexed"))
            .event_source(EventSource::from_static("docs-library"))
            .payload(payload)
            .ts_orig(mtime)
            .timing(mtime_confidence)
            .anchor(record.anchor.clone())
            .occurrence_key(occurrence_key)
            .privacy_context(ProcessingContext::Metadata)
            .build();

        Ok(vec![intent])
    }
}

/// Read mtime from the filesystem.  Returns `(Timestamp, TimingEvidence)`.
/// On failure, falls back to `Timestamp::now()` with `Atemporal` evidence.
fn read_mtime(path: &Utf8PathBuf) -> (Timestamp, TimingEvidence) {
    let meta_result = std::fs::metadata(path.as_std_path());
    match meta_result {
        Ok(meta) => {
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .and_then(|d| {
                    let nanos = i128::try_from(d.as_nanos()).ok()?;
                    Timestamp::from_unix_timestamp_nanos(nanos)
                })
                .unwrap_or_else(Timestamp::now);
            let evidence = TimingEvidence::InferredMtime {
                path: path.clone(),
                mtime,
            };
            (mtime, evidence)
        }
        Err(_) => (Timestamp::now(), TimingEvidence::Atemporal),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "library_test.rs"]
mod tests;
