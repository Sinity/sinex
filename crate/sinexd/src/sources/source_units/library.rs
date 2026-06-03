//! `docs-library-index` source unit — document library metadata index.
//!
//! Emits one `document.indexed` event per file found under the configured
//! library root(s) using [`DirectoryWalkAdapter`].  Each event carries
//! filesystem metadata and filename-derived hints; **no document content
//! is extracted** — that is the domain of `document.staging` and the
//! document-layer parsers.
//!
//! ## Scope split
//!
//! | Concern | Source unit |
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

use crate::node_sdk::parser::{DirectoryWalkAdapter, MaterialParser, ParserError, ParserResult};
use sinex_primitives::{
    domain::{EventSource, EventType},
    parser::{
        InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
        ParserManifest, SourceRecord, SourceUnitId, TimingEvidence,
    },
    privacy::ProcessingContext,
    proof::{
        CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
        SourceUnitBinding, SourceUnitBuildImpact, SourceUnitDescriptor, SubjectRef,
    },
    temporal::Timestamp,
};
use sinex_primitives::{register_source_unit, register_source_unit_binding};

// ---------------------------------------------------------------------------
// Source unit descriptor
// ---------------------------------------------------------------------------

register_source_unit! {
    SourceUnitDescriptor {
        id: "docs-library-index",
        namespace: "library",
        event_types: &[
            ("docs-library", "document.indexed"),
        ],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "anchor_directory_entry",
            "timestamp_inferred_mtime",
            "occurrence_key_path_hash",
            "filename_heuristics_best_effort",
            "privacy_context_declared",
        ],
        occurrence_identity: OccurrenceIdentity::Uuid5From(
            "(source_unit, path, content_hash)",
        ),
        access_policy: "local_library_root",
    }
}

// ---------------------------------------------------------------------------
// Source unit binding
// ---------------------------------------------------------------------------

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:docs-library-index"),
        "docs-library-index",
        "library",
    )
    .implementation("sinex-source-worker")
    .adapter("DirectoryWalkAdapter")
    .output_event_type("document.indexed")
    .privacy_context("Metadata")
    .material_policy("directory_walk_fingerprint")
    .checkpoint_policy("directory_walk_cursor")
    .resource_shape("file_reader")
    .source_unit_id("docs-library-index")
    .runner_pack("source-worker")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .package_impact("docs_library_index_source_unit")
    .implementation_mode("rust_in_pack:source-worker")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

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
#[derive(Debug, Clone, Default)]
pub struct DocsLibraryParser;

#[async_trait]
impl MaterialParser for DocsLibraryParser {
    type Config = DocsLibraryParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("docs-library-index"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::DirectoryWalk],
            source_unit_id: SourceUnitId::from_static("docs-library-index"),
            declared_event_types: vec![(
                EventSource::from_static("docs-library"),
                EventType::from_static("document.indexed"),
            )],
            privacy_contexts: vec![ProcessingContext::Metadata],
            proof_obligations: vec![
                "anchor_directory_entry".into(),
                "timestamp_inferred_mtime".into(),
                "occurrence_key_path_hash".into(),
                "filename_heuristics_best_effort".into(),
                "privacy_context_declared".into(),
            ],
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
                source_unit_id: SourceUnitId::from_static("docs-library-index"),
                fields: vec![
                    ("path".into(), path_buf.to_string()),
                    ("content_hash".into(), hash_field),
                ],
            }
        };

        let intent = ParsedEventIntent::builder()
            .source_unit_id(ctx.source_unit_id.clone())
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
// Node factory registration
// ---------------------------------------------------------------------------

crate::register_adapter_ingestor!(
    source_unit_id: "docs-library-index",
    adapter: DirectoryWalkAdapter,
    parser: DocsLibraryParser,
);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use sinex_primitives::ids::Id;
    use sinex_primitives::parser::{MaterialAnchor, ParserContext, SourceRecord, SourceUnitId};
    use sinex_primitives::temporal::Timestamp;
    use std::io::Write;
    use tempfile::TempDir;
    use xtask::sandbox::prelude::*;

    fn make_ctx() -> ParserContext {
        ParserContext {
            source_unit_id: SourceUnitId::from_static("docs-library-index"),
            source_material_id: Id::from_uuid(uuid::Uuid::new_v4()),
            record_anchor: MaterialAnchor::DirectoryEntry {
                path: Utf8PathBuf::from("/test/file.pdf"),
                content_hash: None,
            },
            operation_id: uuid::Uuid::new_v4(),
            job_id: uuid::Uuid::new_v4(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn make_record(path: &str, bytes: Vec<u8>, content_hash: Option<String>) -> SourceRecord {
        SourceRecord {
            material_id: Id::from_uuid(uuid::Uuid::new_v4()),
            anchor: MaterialAnchor::DirectoryEntry {
                path: Utf8PathBuf::from(path),
                content_hash,
            },
            bytes,
            logical_path: Some(Utf8PathBuf::from(path)),
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        }
    }

    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn parses_pdf_into_one_intent() -> xtask::sandbox::TestResult<()> {
        let dir = TempDir::new().unwrap();
        let file = dir
            .path()
            .join("Aaron Clarey - Bachelor Pad Economics (2013).pdf");
        let mut f = std::fs::File::create(&file).unwrap();
        f.write_all(b"pdf content").unwrap();
        drop(f);

        let path = camino::Utf8PathBuf::from_path_buf(file).unwrap();
        let record = make_record(path.as_str(), b"pdf content".to_vec(), None);
        let ctx = make_ctx();

        let mut parser = DocsLibraryParser;
        let intents = parser.parse_record(record, &ctx).await?;

        assert_eq!(intents.len(), 1, "one intent per file");
        assert_eq!(intents[0].event_source.as_static_str(), "docs-library");
        assert_eq!(intents[0].event_type.as_static_str(), "document.indexed");
        Ok(())
    }

    #[sinex_test]
    async fn payload_contains_core_fields() -> xtask::sandbox::TestResult<()> {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("test_document.epub");
        let mut f = std::fs::File::create(&file).unwrap();
        f.write_all(b"epub bytes here").unwrap();
        drop(f);

        let path = camino::Utf8PathBuf::from_path_buf(file.clone()).unwrap();
        let record = make_record(path.as_str(), b"epub bytes here".to_vec(), None);
        let ctx = make_ctx();

        let mut parser = DocsLibraryParser;
        let intents = parser.parse_record(record, &ctx).await?;

        let payload = &intents[0].payload;
        assert_eq!(payload["filename"].as_str(), Some("test_document.epub"));
        assert_eq!(payload["extension"].as_str(), Some("epub"));
        assert_eq!(payload["byte_size"].as_u64(), Some(15));
        Ok(())
    }

    #[sinex_test]
    async fn year_hint_extracted_from_filename() -> xtask::sandbox::TestResult<()> {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("Author Name - Some Book (2018).pdf");
        let mut f = std::fs::File::create(&file).unwrap();
        f.write_all(b"x").unwrap();
        drop(f);

        let path = camino::Utf8PathBuf::from_path_buf(file).unwrap();
        let record = make_record(path.as_str(), b"x".to_vec(), None);
        let ctx = make_ctx();

        let mut parser = DocsLibraryParser;
        let intents = parser.parse_record(record, &ctx).await?;

        let payload = &intents[0].payload;
        assert_eq!(payload["year_hint"].as_u64(), Some(2018));
        Ok(())
    }

    #[sinex_test]
    async fn author_and_title_hints_extracted() -> xtask::sandbox::TestResult<()> {
        let dir = TempDir::new().unwrap();
        let file = dir
            .path()
            .join("Jordan B. Peterson - 12 Rules for Life (2018).pdf");
        let mut f = std::fs::File::create(&file).unwrap();
        f.write_all(b"x").unwrap();
        drop(f);

        let path = camino::Utf8PathBuf::from_path_buf(file).unwrap();
        let record = make_record(path.as_str(), b"x".to_vec(), None);
        let ctx = make_ctx();

        let mut parser = DocsLibraryParser;
        let intents = parser.parse_record(record, &ctx).await?;

        let payload = &intents[0].payload;
        assert_eq!(payload["author_hint"].as_str(), Some("Jordan B. Peterson"),);
        assert_eq!(payload["title_hint"].as_str(), Some("12 Rules for Life"),);
        Ok(())
    }

    #[sinex_test]
    async fn external_id_extracted_from_md5_in_name() -> xtask::sandbox::TestResult<()> {
        // Anna's Archive filename pattern: "... -- e73bc86a05ff4661d735d35f844c9650 -- Anna's Archive.pdf"
        let dir = TempDir::new().unwrap();
        let file = dir
            .path()
            .join("Some Book -- e73bc86a05ff4661d735d35f844c9650 -- Anna's Archive.pdf");
        let mut f = std::fs::File::create(&file).unwrap();
        f.write_all(b"x").unwrap();
        drop(f);

        let path = camino::Utf8PathBuf::from_path_buf(file).unwrap();
        let record = make_record(path.as_str(), b"x".to_vec(), None);
        let ctx = make_ctx();

        let mut parser = DocsLibraryParser;
        let intents = parser.parse_record(record, &ctx).await?;

        let payload = &intents[0].payload;
        assert_eq!(
            payload["external_id"].as_str(),
            Some("e73bc86a05ff4661d735d35f844c9650"),
        );
        Ok(())
    }

    #[sinex_test]
    async fn no_hints_for_bare_filename() -> xtask::sandbox::TestResult<()> {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("965.jpg");
        let mut f = std::fs::File::create(&file).unwrap();
        f.write_all(b"jpeg").unwrap();
        drop(f);

        let path = camino::Utf8PathBuf::from_path_buf(file).unwrap();
        let record = make_record(path.as_str(), b"jpeg".to_vec(), None);
        let ctx = make_ctx();

        let mut parser = DocsLibraryParser;
        let intents = parser.parse_record(record, &ctx).await?;

        let payload = &intents[0].payload;
        assert!(
            payload["author_hint"].is_null(),
            "no author hint for bare filename"
        );
        assert!(
            payload["title_hint"].is_null(),
            "no title hint for bare filename"
        );
        assert!(
            payload["year_hint"].is_null(),
            "no year hint for bare filename"
        );
        assert!(
            payload["external_id"].is_null(),
            "no external_id for bare filename"
        );
        Ok(())
    }

    #[sinex_test]
    async fn content_hash_preserved_from_anchor() -> xtask::sandbox::TestResult<()> {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("book.pdf");
        let mut f = std::fs::File::create(&file).unwrap();
        f.write_all(b"bytes").unwrap();
        drop(f);

        let path = camino::Utf8PathBuf::from_path_buf(file).unwrap();
        let hash = "deadbeefdeadbeef".to_string();
        let record = make_record(path.as_str(), b"bytes".to_vec(), Some(hash.clone()));
        let ctx = make_ctx();

        let mut parser = DocsLibraryParser;
        let intents = parser.parse_record(record, &ctx).await?;

        let payload = &intents[0].payload;
        assert_eq!(payload["content_hash"].as_str(), Some(hash.as_str()));
        Ok(())
    }

    #[sinex_test]
    async fn occurrence_key_has_path_and_hash_fields() -> xtask::sandbox::TestResult<()> {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("book.pdf");
        let mut f = std::fs::File::create(&file).unwrap();
        f.write_all(b"x").unwrap();
        drop(f);

        let path = camino::Utf8PathBuf::from_path_buf(file).unwrap();
        let record = make_record(path.as_str(), b"x".to_vec(), None);
        let ctx = make_ctx();

        let mut parser = DocsLibraryParser;
        let intents = parser.parse_record(record, &ctx).await?;

        let key = intents[0]
            .occurrence_key
            .as_ref()
            .expect("occurrence_key present");
        assert_eq!(key.source_unit_id.as_str(), "docs-library-index");
        let field_names: Vec<&str> = key.fields.iter().map(|(k, _)| k.as_str()).collect();
        assert!(field_names.contains(&"path"), "key has 'path' field");
        assert!(
            field_names.contains(&"content_hash"),
            "key has 'content_hash' field"
        );
        Ok(())
    }

    #[sinex_test]
    async fn rejects_non_directory_entry_anchor() -> xtask::sandbox::TestResult<()> {
        let record = SourceRecord {
            material_id: Id::from_uuid(uuid::Uuid::new_v4()),
            anchor: MaterialAnchor::ByteRange { start: 0, len: 1 },
            bytes: b"x".to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        };
        let ctx = make_ctx();

        let mut parser = DocsLibraryParser;
        let result = parser.parse_record(record, &ctx).await;
        assert!(result.is_err(), "must error on wrong anchor kind");
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Unit tests for heuristics (no I/O)
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn extract_year_finds_4digit_year() -> xtask::sandbox::TestResult<()> {
        assert_eq!(extract_year("Book Title (2023)"), Some(2023));
        assert_eq!(extract_year("Title-Publisher (2018)"), Some(2018));
        assert_eq!(extract_year("no year here"), None);
        // Out-of-range years are not matched.
        assert_eq!(extract_year("old 1800 book"), None);
        Ok(())
    }

    #[sinex_test]
    async fn extract_year_ignores_longer_sequences() -> xtask::sandbox::TestResult<()> {
        // 5+ digit numbers should not match.
        assert_eq!(extract_year("12345 is not a year"), None);
        // Exactly 4 digits followed by non-digit is fine.
        assert_eq!(extract_year("prefix2021suffix"), Some(2021));
        Ok(())
    }

    #[sinex_test]
    async fn extract_external_id_finds_32hex() -> xtask::sandbox::TestResult<()> {
        let name = "Some Book -- e73bc86a05ff4661d735d35f844c9650 -- Anna's Archive";
        assert_eq!(
            extract_external_id(name),
            Some("e73bc86a05ff4661d735d35f844c9650".to_string()),
        );
        Ok(())
    }

    #[sinex_test]
    async fn extract_external_id_none_for_short_hex() -> xtask::sandbox::TestResult<()> {
        assert_eq!(extract_external_id("abc123"), None);
        Ok(())
    }

    #[sinex_test]
    async fn extract_author_title_splits_on_separator() -> xtask::sandbox::TestResult<()> {
        let (author, title) = extract_author_title("Jordan B. Peterson - 12 Rules for Life (2018)");
        assert_eq!(author.as_deref(), Some("Jordan B. Peterson"));
        assert_eq!(title.as_deref(), Some("12 Rules for Life"));
        Ok(())
    }

    #[sinex_test]
    async fn extract_author_title_no_separator_returns_none() -> xtask::sandbox::TestResult<()> {
        let (author, title) = extract_author_title("DocumentWithNoSeparator");
        assert!(author.is_none());
        assert!(title.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn extract_author_title_preserves_inner_dashes() -> xtask::sandbox::TestResult<()> {
        // Titles like "The XX Factor - How the Rise ... (2013)" have " - " only once.
        let stem = "Alison Wolf - The XX Factor (2013)";
        let (author, title) = extract_author_title(stem);
        assert_eq!(author.as_deref(), Some("Alison Wolf"));
        assert_eq!(title.as_deref(), Some("The XX Factor"));
        Ok(())
    }
}
