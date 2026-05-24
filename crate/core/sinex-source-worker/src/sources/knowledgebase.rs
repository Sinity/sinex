//! Knowledgebase vault parser (#1075).
//!
//! Mirrors an Obsidian-style PKM vault into sinex by walking the vault root
//! directory and emitting one `knowledgebase`/`note.observed` event per `.md`
//! file.  The adapter is [`DirectoryWalkAdapter`] with a glob filter of
//! `**/*.md`.  Each file is a self-contained record; the parser extracts:
//!
//! - YAML front-matter (opaque JSON pass-through + structured `tags` list)
//! - `[[wikilink]]` references from the body
//! - Inline `#tag` tokens from the body
//! - A BLAKE3 hex digest of the body for content-change detection
//!
//! Privacy tier: `Sensitive` + `ProcessingContext::Document` — personal notes.

use async_trait::async_trait;
use camino::Utf8PathBuf;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

use sinex_node_sdk::parser::{DirectoryWalkAdapter, MaterialParser, ParserError, ParserResult};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
    ParserManifest, SourceRecord, SourceUnitId, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitBinding, SourceUnitBuildImpact, SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{register_source_unit, register_source_unit_binding};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SOURCE_UNIT_ID: &str = "knowledgebase-vault";
const EVENT_SOURCE: &str = "knowledgebase";
const EVENT_TYPE: &str = "note.observed";

// ---------------------------------------------------------------------------
// Regex helpers (compiled once)
// ---------------------------------------------------------------------------

fn wikilink_re() -> &'static Result<Regex, regex::Error> {
    static RE: OnceLock<Result<Regex, regex::Error>> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\[\[([^\]|#]+)(?:#[^\]|]*)?(?:\|[^\]]*)?]]"))
}

fn body_tag_re() -> &'static Result<Regex, regex::Error> {
    static RE: OnceLock<Result<Regex, regex::Error>> = OnceLock::new();
    // Match a # followed by word chars (including Unicode letters/digits/underscores).
    // Require either start-of-string or a word-break so we don't match markdown headings
    // that appear mid-line with `##`. Headings start the line with `#`.
    RE.get_or_init(|| Regex::new(r"(?:^|\s)#([A-Za-z][A-Za-z0-9_/-]*)"))
}

// ---------------------------------------------------------------------------
// Markdown parsing helpers
// ---------------------------------------------------------------------------

/// Split raw file bytes into (`front_matter_str`, `body_str`).
///
/// Recognises the `---` YAML fence. Returns `("", full_content)` if no fence
/// is found (uncommon for the KB vault but handled gracefully).
fn split_front_matter(content: &str) -> (&str, &str) {
    let trimmed = content.trim_start();
    if !trimmed.starts_with("---") {
        return ("", content);
    }
    // Find the closing `---` on its own line (skip the opening fence first).
    let rest = &trimmed[3..];
    // Skip the newline after the opening fence.
    let rest = rest.trim_start_matches('\n').trim_start_matches("\r\n");

    // Look for the closing delimiter.
    if let Some(pos) = find_closing_fence(rest) {
        let fm = &rest[..pos];
        let body_start = pos + 3; // skip `---`
        let body = rest[body_start..]
            .trim_start_matches('\n')
            .trim_start_matches("\r\n");
        (fm, body)
    } else {
        // Unclosed fence — treat everything as body.
        ("", content)
    }
}

/// Find the byte position of `---` at the start of a line within `s`.
fn find_closing_fence(s: &str) -> Option<usize> {
    let mut pos = 0;
    for line in s.lines() {
        if line.starts_with("---") && line.trim() == "---" {
            return Some(pos);
        }
        pos += line.len() + 1; // +1 for '\n'
    }
    None
}

/// Parse YAML front-matter into a `serde_json::Value`.
///
/// On parse failure falls back to `Value::Object({})` so the rest of the
/// event still lands.
fn parse_front_matter(fm: &str) -> serde_json::Value {
    if fm.trim().is_empty() {
        return serde_json::Value::Object(serde_json::Map::new());
    }
    match serde_yml::from_str::<serde_json::Value>(fm) {
        Ok(v) => v,
        Err(_) => serde_json::Value::Object(serde_json::Map::new()),
    }
}

/// Extract the `tags:` list from an already-parsed front-matter JSON value.
fn tags_from_front_matter(fm: &serde_json::Value) -> Vec<String> {
    fm.get("tags")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|t| t.as_str().map(|s| s.trim_start_matches('#').to_owned()))
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

/// Extract inline `#tag` tokens from the note body.
///
/// Skips markdown headings (`# Heading`) — those are whole lines starting with
/// `#`.
fn body_tags(body: &str) -> Vec<String> {
    let Ok(re) = body_tag_re() else {
        return Vec::new();
    };
    let mut tags = Vec::new();
    for line in body.lines() {
        let trimmed = line.trim();
        // Skip heading lines (start with one or more `#` then space/end).
        if trimmed.starts_with('#') {
            let after = trimmed.trim_start_matches('#');
            if after.is_empty() || after.starts_with(' ') {
                continue;
            }
        }
        for cap in re.captures_iter(line) {
            if let Some(m) = cap.get(1) {
                tags.push(m.as_str().to_owned());
            }
        }
    }
    tags
}

/// Extract `[[wikilink]]` targets from the note body.
///
/// Strips alias suffixes (`[[note|Alias]]` → `"note"`) and `#header` anchors
/// (`[[note#heading]]` → `"note"`).
fn wikilinks(body: &str) -> Vec<String> {
    let Ok(re) = wikilink_re() else {
        return Vec::new();
    };
    re.captures_iter(body)
        .filter_map(|cap| cap.get(1))
        .map(|m| {
            let raw = m.as_str().trim();
            // Strip `#header` anchors.
            let without_anchor = raw.split('#').next().unwrap_or(raw);
            without_anchor.trim().to_owned()
        })
        .filter(|s| !s.is_empty())
        .collect()
}

/// Collect and deduplicate tags from both sources; sort for stability.
fn merged_tags(fm_tags: Vec<String>, body_tags: Vec<String>) -> Vec<String> {
    let mut all: std::collections::BTreeSet<String> = fm_tags.into_iter().collect();
    all.extend(body_tags);
    all.into_iter().collect()
}

/// Deduplicate and sort wikilinks.
fn dedup_sorted(v: Vec<String>) -> Vec<String> {
    let set: std::collections::BTreeSet<String> = v.into_iter().collect();
    set.into_iter().collect()
}

/// Derive a note title:
/// 1. front-matter `title:` field
/// 2. front-matter `id:` field — strip leading path prefix, use last segment
/// 3. filename stem as fallback
fn derive_title(fm: &serde_json::Value, path: &str) -> String {
    if let Some(title) = fm.get("title").and_then(|v| v.as_str())
        && !title.trim().is_empty()
    {
        return title.to_owned();
    }
    if let Some(id) = fm.get("id").and_then(|v| v.as_str()) {
        // Dendron id: `area.subarea.note` — last segment is the note name.
        let stem = id.rsplit('.').next().unwrap_or(id);
        if !stem.trim().is_empty() {
            return stem.replace(['-', '_'], " ");
        }
    }
    // Fallback: strip extension from the last path segment.
    let filename = path.rsplit('/').next().unwrap_or(path);
    filename.trim_end_matches(".md").replace(['-', '_'], " ")
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KnowledgebaseParserConfig;

#[derive(Debug, Clone, Default)]
pub struct KnowledgebaseVaultParser;

#[async_trait]
impl MaterialParser for KnowledgebaseVaultParser {
    type Config = KnowledgebaseParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("knowledgebase-vault"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::DirectoryWalk],
            source_unit_id: SourceUnitId::from_static(SOURCE_UNIT_ID),
            declared_event_types: vec![(
                EventSource::from_static(EVENT_SOURCE),
                EventType::from_static(EVENT_TYPE),
            )],
            privacy_contexts: vec![ProcessingContext::Document],
            proof_obligations: vec![
                "timestamp_from_mtime_or_now".into(),
                "anchor_directory_entry_content_hash".into(),
                "occurrence_key_path_body_hash".into(),
                "front_matter_opaque_passthrough".into(),
                "personal_note_content_sensitive".into(),
            ],
            description: "Mirrors an Obsidian-style PKM vault into sinex. \
                One note.observed event per .md file; extracts front-matter, \
                inline tags, wikilink graph edges, and a BLAKE3 body hash for \
                change detection."
                .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        // Only process .md files — adapter glob should enforce this, but guard
        // defensively so a misconfigured adapter doesn't corrupt the stream.
        let logical_path = record
            .logical_path
            .as_ref()
            .map(|p| p.as_str().to_owned())
            .unwrap_or_default();

        if !logical_path.ends_with(".md") {
            return Ok(vec![]);
        }

        let content = String::from_utf8(record.bytes.clone()).map_err(|e| {
            ParserError::Parse(format!(
                "knowledgebase: note at {logical_path:?} is not valid UTF-8: {e}"
            ))
        })?;

        let (fm_str, body) = split_front_matter(&content);
        let fm_value = parse_front_matter(fm_str);

        let fm_tags = tags_from_front_matter(&fm_value);
        let body_tag_list = body_tags(body);
        let tags = merged_tags(fm_tags, body_tag_list);

        let raw_wikilinks = wikilinks(body);
        let wikilinks = dedup_sorted(raw_wikilinks);

        let body_bytes = body.as_bytes();
        let body_hash = {
            let mut hasher = blake3::Hasher::new();
            hasher.update(body_bytes);
            hasher.finalize().to_hex().to_string()
        };
        let body_byte_size = body_bytes.len() as u64;

        // Derive a relative path (strip vault root prefix if the full path is
        // available via logical_path). We store the full path as-is since the
        // vault root is a deployment-time concern; consumers can strip it.
        let relative_path = logical_path.clone();

        let title = derive_title(&fm_value, &relative_path);

        // ts_orig: use mtime from front-matter `revised:` last entry, then
        // `created:`, then fall back to acquisition time.
        let (ts_orig, mtime_str, timing) = pick_timestamp(&fm_value, ctx);

        let payload = serde_json::json!({
            "path": relative_path,
            "title": title,
            "front_matter": fm_value,
            "tags": tags,
            "wikilinks": wikilinks,
            "body_text_hash": body_hash,
            "body_byte_size": body_byte_size,
            "mtime": mtime_str,
        });

        let occurrence_key = OccurrenceKey {
            source_unit_id: SourceUnitId::from_static(SOURCE_UNIT_ID),
            fields: vec![
                ("path".into(), relative_path.clone()),
                ("body_text_hash".into(), body_hash.clone()),
            ],
        };

        let anchor = MaterialAnchor::DirectoryEntry {
            path: Utf8PathBuf::from(&relative_path),
            content_hash: Some(body_hash),
        };

        let intent = ParsedEventIntent::builder()
            .source_unit_id(ctx.source_unit_id.clone())
            .parser_id(ParserId::from_static("knowledgebase-vault"))
            .parser_version("1.0.0")
            .event_type(EventType::from_static(EVENT_TYPE))
            .event_source(EventSource::from_static(EVENT_SOURCE))
            .payload(payload)
            .ts_orig(ts_orig)
            .timing(timing)
            .anchor(anchor)
            .occurrence_key(occurrence_key)
            .privacy_context(ProcessingContext::Document)
            .build();

        Ok(vec![intent])
    }
}

/// Choose the best timestamp from the front-matter for `ts_orig`.
///
/// Priority:
/// 1. Last entry of `revised:` list (most recent edit)
/// 2. `created:` scalar (note creation date)
/// 3. Acquisition time from context (always available)
///
/// Returns `(ts_orig, mtime_str, TimingEvidence)`.
fn pick_timestamp(
    fm: &serde_json::Value,
    ctx: &ParserContext,
) -> (Timestamp, Option<String>, TimingEvidence) {
    // Try the last `revised` date.
    if let Some(last_revised) = fm
        .get("revised")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.last())
        .and_then(|v| v.as_str())
        && let Some(ts) = parse_date(last_revised)
    {
        return (
            ts,
            Some(last_revised.to_owned()),
            TimingEvidence::Intrinsic {
                field: "revised".into(),
                confidence: TimingConfidence::Intrinsic,
            },
        );
    }

    // Try `created`.
    if let Some(created) = fm.get("created").and_then(|v| v.as_str())
        && let Some(ts) = parse_date(created)
    {
        return (
            ts,
            Some(created.to_owned()),
            TimingEvidence::Intrinsic {
                field: "created".into(),
                confidence: TimingConfidence::Intrinsic,
            },
        );
    }

    // Fall back to acquisition time.
    (ctx.acquisition_time, None, TimingEvidence::StagedAtFallback)
}

/// Parse a date string — accepts `YYYY-MM-DD` and RFC 3339.
fn parse_date(s: &str) -> Option<Timestamp> {
    use time::OffsetDateTime;
    use time::format_description::well_known::Rfc3339;
    use time::macros::format_description;

    // Try RFC 3339 first.
    if let Ok(dt) = OffsetDateTime::parse(s, &Rfc3339) {
        return Some(Timestamp::new(dt));
    }

    // Try `YYYY-MM-DD` as midnight UTC.
    let fmt = format_description!("[year]-[month]-[day]");
    if let Ok(date) = time::Date::parse(s, &fmt) {
        let dt = date.with_hms(0, 0, 0).ok()?.assume_utc();
        return Some(Timestamp::new(dt));
    }

    None
}

// ---------------------------------------------------------------------------
// Source-unit descriptor + binding + registration
// ---------------------------------------------------------------------------

register_source_unit! {
    SourceUnitDescriptor {
        id: "knowledgebase-vault",
        namespace: "knowledge",
        event_types: &[("knowledgebase", "note.observed")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "timestamp_from_mtime_or_now",
            "anchor_directory_entry_content_hash",
            "occurrence_key_path_body_hash",
            "front_matter_opaque_passthrough",
            "personal_note_content_sensitive",
        ],
        occurrence_identity: OccurrenceIdentity::Uuid5From("(path, body_text_hash)"),
        access_policy: "personal_knowledgebase",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:knowledgebase-vault"),
        "knowledgebase-vault",
        "knowledge",
    )
    .implementation("sinex-source-worker")
    .adapter("DirectoryWalkAdapter")
    .output_event_type("note.observed")
    .privacy_context("Document")
    .material_policy("directory_walk")
    .checkpoint_policy("directory_walk_cursor")
    .resource_shape("file_reader")
    .source_unit_id("knowledgebase-vault")
    .runner_pack("source-worker")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .package_impact("knowledgebase_vault_source_unit")
    .implementation_mode("rust_in_pack:source-worker")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

crate::register_adapter_ingestor!(
    source_unit_id: "knowledgebase-vault",
    adapter: DirectoryWalkAdapter,
    parser: KnowledgebaseVaultParser,
);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::Uuid;
    use sinex_primitives::ids::Id;

    use xtask::sandbox::prelude::sinex_test;

    fn test_ctx() -> ParserContext {
        ParserContext {
            source_unit_id: SourceUnitId::from_static(SOURCE_UNIT_ID),
            source_material_id: Id::new(),
            record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
            operation_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn record_for(path: &str, bytes: &[u8]) -> SourceRecord {
        SourceRecord {
            material_id: Id::new(),
            anchor: MaterialAnchor::DirectoryEntry {
                path: Utf8PathBuf::from(path),
                content_hash: None,
            },
            bytes: bytes.to_vec(),
            logical_path: Some(Utf8PathBuf::from(path)),
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        }
    }

    // -----------------------------------------------------------------------
    // 1. Basic happy-path: front-matter + body → 1 intent
    // -----------------------------------------------------------------------

    const BASIC_NOTE: &str = "\
---
id: permanent.concept.test
created: 2025-03-15
tags:
  - concept
  - ai
---
This is the body. It has a [[wikilink]] and a #body-tag.
";

    #[sinex_test]
    async fn parses_basic_note_into_one_intent() -> TestResult<()> {
        let mut parser = KnowledgebaseVaultParser;
        let intents = parser
            .parse_record(
                record_for("permanent.concept.test.md", BASIC_NOTE.as_bytes()),
                &test_ctx(),
            )
            .await
            .unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].event_source.as_str(), EVENT_SOURCE);
        assert_eq!(intents[0].event_type.as_str(), EVENT_TYPE);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // 2. Tags: merged from front-matter + body, deduplicated + sorted
    // -----------------------------------------------------------------------

    #[sinex_test]
    async fn merges_fm_tags_and_body_tags() -> TestResult<()> {
        let mut parser = KnowledgebaseVaultParser;
        let intents = parser
            .parse_record(
                record_for("permanent.concept.test.md", BASIC_NOTE.as_bytes()),
                &test_ctx(),
            )
            .await
            .unwrap();
        let tags = intents[0].payload["tags"].as_array().unwrap();
        let tag_strs: Vec<&str> = tags.iter().map(|v| v.as_str().unwrap()).collect();
        // front-matter: "concept", "ai"; body: "body-tag"
        assert!(tag_strs.contains(&"concept"), "missing front-matter tag");
        assert!(tag_strs.contains(&"ai"), "missing front-matter tag");
        assert!(tag_strs.contains(&"body-tag"), "missing body tag");
        // Sorted order
        assert_eq!(tag_strs, {
            let mut sorted = tag_strs.clone();
            sorted.sort_unstable();
            sorted
        });
        Ok(())
    }

    // -----------------------------------------------------------------------
    // 3. Wikilinks extracted and deduplicated
    // -----------------------------------------------------------------------

    const WIKILINK_NOTE: &str = "\
---
id: wikilink.test
created: 2025-01-01
---
See [[note-a]] and [[note-b|Alias]] and [[note-a]] again and [[note-c#heading]].
";

    #[sinex_test]
    async fn extracts_wikilinks_deduplicated_and_sorted() -> TestResult<()> {
        let mut parser = KnowledgebaseVaultParser;
        let intents = parser
            .parse_record(
                record_for("wikilink.test.md", WIKILINK_NOTE.as_bytes()),
                &test_ctx(),
            )
            .await
            .unwrap();
        let wikilinks = intents[0].payload["wikilinks"].as_array().unwrap();
        let link_strs: Vec<&str> = wikilinks.iter().map(|v| v.as_str().unwrap()).collect();
        assert_eq!(link_strs, vec!["note-a", "note-b", "note-c"]);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // 4. Occurrence key shape
    // -----------------------------------------------------------------------

    #[sinex_test]
    async fn occurrence_key_uses_path_and_body_hash() -> TestResult<()> {
        let mut parser = KnowledgebaseVaultParser;
        let intents = parser
            .parse_record(
                record_for("permanent.concept.test.md", BASIC_NOTE.as_bytes()),
                &test_ctx(),
            )
            .await
            .unwrap();
        let key = intents[0].occurrence_key.as_ref().unwrap();
        assert_eq!(key.source_unit_id.as_str(), SOURCE_UNIT_ID);
        assert_eq!(key.fields[0].0, "path");
        assert_eq!(key.fields[1].0, "body_text_hash");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // 5. BLAKE3 body hash present and stable
    // -----------------------------------------------------------------------

    #[sinex_test]
    async fn body_hash_is_stable_and_present() -> TestResult<()> {
        let mut parser = KnowledgebaseVaultParser;
        let intents = parser
            .parse_record(
                record_for("permanent.concept.test.md", BASIC_NOTE.as_bytes()),
                &test_ctx(),
            )
            .await
            .unwrap();
        let hash1 = intents[0].payload["body_text_hash"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(hash1.len(), 64, "BLAKE3 hex digest should be 64 chars");

        // Same content → same hash.
        let intents2 = parser
            .parse_record(
                record_for("permanent.concept.test.md", BASIC_NOTE.as_bytes()),
                &test_ctx(),
            )
            .await
            .unwrap();
        let hash2 = intents2[0].payload["body_text_hash"].as_str().unwrap();
        assert_eq!(
            hash1, hash2,
            "hash must be stable across parses of same content"
        );
        Ok(())
    }

    // -----------------------------------------------------------------------
    // 6. Note without front-matter still parses
    // -----------------------------------------------------------------------

    const NO_FM_NOTE: &str = "Just a bare note body.\nNo front-matter here.\n";

    #[sinex_test]
    async fn parses_note_without_front_matter() -> TestResult<()> {
        let mut parser = KnowledgebaseVaultParser;
        let intents = parser
            .parse_record(
                record_for("archive.bare-note.md", NO_FM_NOTE.as_bytes()),
                &test_ctx(),
            )
            .await
            .unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].event_source.as_str(), EVENT_SOURCE);
        let tags = intents[0].payload["tags"].as_array().unwrap();
        assert!(tags.is_empty(), "bare note should have no tags");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // 7. Non-.md files are skipped
    // -----------------------------------------------------------------------

    #[sinex_test]
    async fn skips_non_md_files() -> TestResult<()> {
        let mut parser = KnowledgebaseVaultParser;
        let intents = parser
            .parse_record(record_for("assets/image.png", b"\x89PNG\r\n"), &test_ctx())
            .await
            .unwrap();
        assert!(intents.is_empty(), "non-md files must be skipped");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // 8. Markdown heading lines don't produce spurious body tags
    // -----------------------------------------------------------------------

    const HEADING_NOTE: &str = "\
---
id: heading.test
created: 2025-01-01
---
# Top-level heading
## Section heading

Some text with a real #inline-tag here.
";

    #[sinex_test]
    async fn heading_lines_do_not_produce_body_tags() -> TestResult<()> {
        let mut parser = KnowledgebaseVaultParser;
        let intents = parser
            .parse_record(
                record_for("heading.test.md", HEADING_NOTE.as_bytes()),
                &test_ctx(),
            )
            .await
            .unwrap();
        let tags = intents[0].payload["tags"].as_array().unwrap();
        let tag_strs: Vec<&str> = tags.iter().map(|v| v.as_str().unwrap()).collect();
        // "inline-tag" should be present, but "Top-level" and "Section" should not.
        assert!(
            tag_strs.contains(&"inline-tag"),
            "real inline tag must be collected"
        );
        assert!(
            !tag_strs
                .iter()
                .any(|t| *t == "Top-level" || *t == "Section"),
            "heading tokens must not become tags; got: {tag_strs:?}"
        );
        Ok(())
    }

    // -----------------------------------------------------------------------
    // 9. Invalid UTF-8 returns a ParserError
    // -----------------------------------------------------------------------

    #[sinex_test]
    async fn invalid_utf8_returns_parser_error() -> TestResult<()> {
        let mut parser = KnowledgebaseVaultParser;
        let bad_bytes: &[u8] = b"---\nid: test\n---\nHello \xFF world";
        let result = parser
            .parse_record(record_for("bad.md", bad_bytes), &test_ctx())
            .await;
        assert!(result.is_err(), "invalid UTF-8 must surface as ParserError");
        Ok(())
    }
}
