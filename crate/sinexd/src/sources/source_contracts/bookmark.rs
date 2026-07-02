//! Raindrop bookmark export parser (#1091).
//!
//! Reads Raindrop CSV exports (`raindrop-*.csv`, `raindrop_bookmarks_*.csv`)
//! and emits one `raindrop`/`bookmark.created` event per row. Wired through
//! [`StaticFileAdapter`] (one-shot file read); the parser uses the `csv`
//! crate so quoted fields and embedded commas in `title`/`note`/`excerpt`
//! round-trip cleanly.
//!
//! ## Schema
//!
//! Raindrop's columns: `id,title,note,excerpt,url,folder,tags,created,cover,
//! highlights,favorite`. The parser preserves the semantic fields and drops
//! `cover` (rotting CDN reference) and `highlights` (rarely populated, free
//! text not worth the bytes). `id` becomes `raindrop_id`; `created` is
//! parsed as RFC 3339.
//!
//! ## Occurrence identity
//!
//! `(raindrop_id, url, created)` — the numeric Raindrop id is unique within
//! the user's account, but we include URL + created as a triple-key in case
//! the export ever rewrites ids across snapshots. Idempotent against
//! re-imports of the same export.
//!
//! ## Anchoring
//!
//! Per-row `MaterialAnchor::Line { byte_start: 0, line: <csv_row_index> }`
//! using the CSV row position (1-based, excluding the header). The
//! `StaticFileAdapter` records the whole file as one `ByteRange`; the parser
//! synthesizes per-row line anchors so cascade-archive can target
//! individual bookmarks.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

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
// Raw CSV row
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct RaindropCsvRow {
    id: i64,
    #[serde(default)]
    title: String,
    #[serde(default)]
    note: String,
    #[serde(default)]
    excerpt: String,
    url: String,
    #[serde(default)]
    folder: String,
    #[serde(default)]
    tags: String,
    created: String,
    #[serde(default, rename = "cover")]
    _cover: String,
    #[serde(default, rename = "highlights")]
    _highlights: String,
    favorite: String,
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RaindropParserConfig;

#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "raindrop-bookmarks",
    namespace = "web",
    event_source = "raindrop",
    event_type = "bookmark.created",
    adapter = "StaticFileAdapter",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(raindrop_id, url, created)"),
    access_scope = AccessScope::StagedExport,
    implementation = "sinexd",
    privacy_context = ProcessingContext::Metadata,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::OnDemand,
)]
pub struct RaindropBookmarkParser;

#[async_trait]
impl MaterialParser for RaindropBookmarkParser {
    type Config = RaindropParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("raindrop-bookmarks"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::StaticFile],
            source_id: SourceId::from_static("raindrop-bookmarks"),
            declared_event_types: vec![(
                EventSource::from_static("raindrop"),
                EventType::from_static("bookmark.created"),
            )],
            privacy_contexts: vec![ProcessingContext::Metadata],
            sensitivity_hints: Vec::new(),
            description: "Parses Raindrop CSV bookmark exports into typed \
                bookmark.created events. Drops the cover-image CDN URL and \
                free-text highlights column."
                .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(record.bytes.as_slice());

        let mut intents = Vec::new();

        for (row_index, row_result) in reader.deserialize::<RaindropCsvRow>().enumerate() {
            let row = row_result.map_err(|e| {
                ParserError::Parse(format!("CSV row {} parse error: {e}", row_index + 1))
            })?;
            let line = (row_index + 1) as u64;
            intents.push(parse_row(row, line, ctx)?);
        }

        Ok(intents)
    }

    fn required_input_keys(&self) -> Vec<String> {
        ["id", "url", "created", "favorite"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    }
}

fn parse_row(
    row: RaindropCsvRow,
    line: u64,
    ctx: &ParserContext,
) -> ParserResult<ParsedEventIntent> {
    let created_at = parse_iso8601(&row.created)?;
    let favorite = parse_bool(&row.favorite);

    let occurrence_key = OccurrenceKey {
        source_id: SourceId::from_static("raindrop-bookmarks"),
        fields: vec![
            ("raindrop_id".into(), row.id.to_string()),
            ("url".into(), row.url.clone()),
            ("created".into(), row.created.clone()),
        ],
    };

    let payload = serde_json::json!({
        "raindrop_id": row.id,
        "url": row.url,
        "created_at": created_at,
        "folder": non_empty(&row.folder),
        "title": non_empty(&row.title),
        "note": non_empty(&row.note),
        "excerpt": non_empty(&row.excerpt),
        "tags": non_empty(&row.tags),
        "favorite": favorite,
    });

    Ok(ParsedEventIntent::builder()
        .source_id(ctx.source_id.clone())
        .parser_id(ParserId::from_static("raindrop-bookmarks"))
        .parser_version("1.0.0")
        .event_type(EventType::from_static("bookmark.created"))
        .event_source(EventSource::from_static("raindrop"))
        .payload(payload)
        .ts_orig(created_at)
        .timing(TimingEvidence::Intrinsic {
            field: "created".into(),
            confidence: TimingConfidence::Intrinsic,
        })
        .anchor(MaterialAnchor::Line {
            byte_start: 0,
            line,
        })
        .occurrence_key(occurrence_key)
        .privacy_context(ProcessingContext::Metadata)
        .build())
}

fn parse_iso8601(raw: &str) -> ParserResult<Timestamp> {
    use time::OffsetDateTime;
    use time::format_description::well_known::Rfc3339;
    let dt = OffsetDateTime::parse(raw, &Rfc3339)
        .map_err(|e| ParserError::Parse(format!("invalid Raindrop timestamp '{raw}': {e}")))?;
    Ok(Timestamp::new(dt))
}

fn parse_bool(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "true" | "1" | "yes"
    )
}

fn non_empty(s: &str) -> Option<&str> {
    let trimmed = s.trim();
    (!trimmed.is_empty()).then_some(trimmed)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "bookmark_test.rs"]
mod tests;
