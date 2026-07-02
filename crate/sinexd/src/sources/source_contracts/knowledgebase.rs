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

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult};
use sinex_macros::SourceMeta;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
    ParserManifest, SourceId, SourceRecord, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape,
};
use sinex_primitives::temporal::Timestamp;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SOURCE_ID: &str = "knowledgebase-vault";
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

#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "knowledgebase-vault",
    namespace = "knowledge",
    event_source = "knowledgebase",
    event_type = "note.observed",
    adapter = "DirectoryWalkAdapter",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(path, body_text_hash)"),
    access_scope = AccessScope::StagedExport,
    implementation = "sinexd",
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::OnDemand,
)]
pub struct KnowledgebaseVaultParser;

#[async_trait]
impl MaterialParser for KnowledgebaseVaultParser {
    type Config = KnowledgebaseParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("knowledgebase-vault"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::DirectoryWalk],
            source_id: SourceId::from_static(SOURCE_ID),
            declared_event_types: vec![(
                EventSource::from_static(EVENT_SOURCE),
                EventType::from_static(EVENT_TYPE),
            )],
            privacy_contexts: vec![ProcessingContext::Document],
            sensitivity_hints: Vec::new(),
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
            source_id: SourceId::from_static(SOURCE_ID),
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
            .source_id(ctx.source_id.clone())
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "knowledgebase_test.rs"]
mod tests;
