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

use sinex_node_sdk::parser::{MaterialParser, ParserError, ParserResult, StaticFileAdapter};
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

#[derive(Debug, Clone, Default)]
pub struct RaindropBookmarkParser;

#[async_trait]
impl MaterialParser for RaindropBookmarkParser {
    type Config = RaindropParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("raindrop-bookmarks"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::StaticFile],
            source_unit_id: SourceUnitId::from_static("raindrop-bookmarks"),
            declared_event_types: vec![(
                EventSource::from_static("raindrop"),
                EventType::from_static("bookmark.created"),
            )],
            privacy_contexts: vec![ProcessingContext::Metadata],
            proof_obligations: vec![
                "timestamp_intrinsic".into(),
                "anchor_csv_row".into(),
                "occurrence_key_id_url_created".into(),
                "cover_and_highlights_dropped".into(),
            ],
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
}

fn parse_row(
    row: RaindropCsvRow,
    line: u64,
    ctx: &ParserContext,
) -> ParserResult<ParsedEventIntent> {
    let created_at = parse_iso8601(&row.created)?;
    let favorite = parse_bool(&row.favorite);

    let occurrence_key = OccurrenceKey {
        source_unit_id: SourceUnitId::from_static("raindrop-bookmarks"),
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

    Ok(ParsedEventIntent {
        id: sinex_primitives::ids::Id::new(),
        source_unit_id: ctx.source_unit_id.clone(),
        parser_id: ParserId::from_static("raindrop-bookmarks"),
        parser_version: "1.0.0".into(),
        event_type: EventType::from_static("bookmark.created"),
        event_source: EventSource::from_static("raindrop"),
        payload,
        ts_orig: created_at,
        timing: TimingEvidence::Intrinsic {
            field: "created".into(),
            confidence: TimingConfidence::Intrinsic,
        },
        anchor: MaterialAnchor::Line {
            byte_start: 0,
            line,
        },
        occurrence_key: Some(occurrence_key),
        privacy_context: ProcessingContext::Metadata,
        field_privacy_log: None,
        synthesis_parents: None,
    })
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
// Source unit descriptor + binding + registration
// ---------------------------------------------------------------------------

register_source_unit! {
    SourceUnitDescriptor {
        id: "raindrop-bookmarks",
        namespace: "web",
        event_types: &[("raindrop", "bookmark.created")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "timestamp_intrinsic",
            "anchor_csv_row",
            "occurrence_key_id_url_created",
            "cover_and_highlights_dropped",
        ],
        occurrence_identity: OccurrenceIdentity::Uuid5From(
            "(raindrop_id, url, created)",
        ),
        access_policy: "personal_bookmarks",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:raindrop-bookmarks"),
        "raindrop-bookmarks",
        "web",
    )
    .implementation("sinex-source-worker")
    .adapter("StaticFileAdapter")
    .output_event_type("bookmark.created")
    .privacy_context("Metadata")
    .material_policy("static_export_file")
    .checkpoint_policy("static_file_cursor")
    .resource_shape("file_reader")
    .source_unit_id("raindrop-bookmarks")
    .runner_pack("source-worker")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .package_impact("raindrop_bookmarks_source_unit")
    .implementation_mode("rust_in_pack:source-worker")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

crate::register_adapter_ingestor!(
    source_unit_id: "raindrop-bookmarks",
    adapter: StaticFileAdapter,
    parser: RaindropBookmarkParser,
);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::Uuid;
    use sinex_primitives::ids::Id;
    use sinex_primitives::parser::MaterialAnchor;

    use xtask::sandbox::prelude::sinex_test;

    fn test_ctx() -> ParserContext {
        ParserContext {
            source_unit_id: SourceUnitId::from_static("raindrop-bookmarks"),
            source_material_id: Id::new(),
            record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
            operation_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn record_for(bytes: &[u8]) -> SourceRecord {
        SourceRecord {
            material_id: Id::new(),
            anchor: MaterialAnchor::ByteRange {
                start: 0,
                len: bytes.len() as u64,
            },
            bytes: bytes.to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        }
    }

    const SAMPLE_EXPORT: &str = "id,title,note,excerpt,url,folder,tags,created,cover,highlights,favorite\n\
        100,Andy Masley | Substack,note text,short excerpt,https://andymasley.substack.com/,Unsorted,\"tag1,tag2\",2026-01-01T10:53:43.411Z,https://example.com/cover.jpg,,false\n\
        200,Another Page,,,https://example.org/page,Reading,,2026-01-02T12:00:00.000Z,,,true\n";

    #[sinex_test]
    async fn parses_csv_into_two_intents() -> TestResult<()> {
        let mut parser = RaindropBookmarkParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_EXPORT.as_bytes()), &test_ctx())
            .await
            .unwrap();
        assert_eq!(intents.len(), 2);
        for intent in &intents {
            assert_eq!(intent.event_source.as_str(), "raindrop");
            assert_eq!(intent.event_type.as_str(), "bookmark.created");
        }
        Ok(())
    }

    #[sinex_test]
    async fn preserves_url_and_id() -> TestResult<()> {
        let mut parser = RaindropBookmarkParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_EXPORT.as_bytes()), &test_ctx())
            .await
            .unwrap();
        assert_eq!(intents[0].payload["raindrop_id"], 100);
        assert_eq!(
            intents[0].payload["url"],
            "https://andymasley.substack.com/"
        );
        Ok(())
    }

    #[sinex_test]
    async fn line_anchor_starts_at_one() -> TestResult<()> {
        let mut parser = RaindropBookmarkParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_EXPORT.as_bytes()), &test_ctx())
            .await
            .unwrap();
        assert!(matches!(
            intents[0].anchor,
            MaterialAnchor::Line { line: 1, .. }
        ));
        assert!(matches!(
            intents[1].anchor,
            MaterialAnchor::Line { line: 2, .. }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn favorite_parses_true_and_false() -> TestResult<()> {
        let mut parser = RaindropBookmarkParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_EXPORT.as_bytes()), &test_ctx())
            .await
            .unwrap();
        assert_eq!(intents[0].payload["favorite"], false);
        assert_eq!(intents[1].payload["favorite"], true);
        Ok(())
    }

    #[sinex_test]
    async fn occurrence_key_uses_id_url_created_triple() -> TestResult<()> {
        let mut parser = RaindropBookmarkParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_EXPORT.as_bytes()), &test_ctx())
            .await
            .unwrap();
        let key = intents[0].occurrence_key.as_ref().unwrap();
        assert_eq!(
            key.fields,
            vec![
                ("raindrop_id".into(), "100".into()),
                ("url".into(), "https://andymasley.substack.com/".into()),
                ("created".into(), "2026-01-01T10:53:43.411Z".into()),
            ]
        );
        Ok(())
    }

    #[sinex_test]
    async fn cover_and_highlights_dropped() -> TestResult<()> {
        let mut parser = RaindropBookmarkParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_EXPORT.as_bytes()), &test_ctx())
            .await
            .unwrap();
        let payload = &intents[0].payload;
        assert!(payload.get("cover").is_none());
        assert!(payload.get("highlights").is_none());
        Ok(())
    }

    #[sinex_test]
    async fn quoted_fields_with_commas_round_trip() -> TestResult<()> {
        let csv = "id,title,note,excerpt,url,folder,tags,created,cover,highlights,favorite\n\
            42,\"Title, with comma\",,,https://x.com,Folder,\"a,b,c\",2026-01-01T00:00:00.000Z,,,false\n";
        let mut parser = RaindropBookmarkParser;
        let intents = parser
            .parse_record(record_for(csv.as_bytes()), &test_ctx())
            .await
            .unwrap();
        assert_eq!(intents[0].payload["title"], "Title, with comma");
        assert_eq!(intents[0].payload["tags"], "a,b,c");
        Ok(())
    }

    #[sinex_test]
    async fn empty_string_fields_become_none() -> TestResult<()> {
        let mut parser = RaindropBookmarkParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_EXPORT.as_bytes()), &test_ctx())
            .await
            .unwrap();
        // Second row has no note/excerpt/tags — should be absent in payload.
        let payload = &intents[1].payload;
        assert!(payload["note"].is_null());
        assert!(payload["excerpt"].is_null());
        assert!(payload["tags"].is_null());
        Ok(())
    }

    #[sinex_test]
    async fn invalid_timestamp_errors() -> TestResult<()> {
        let bad = "id,title,note,excerpt,url,folder,tags,created,cover,highlights,favorite\n\
            1,,,,https://x.com,,,not-a-timestamp,,,false\n";
        let mut parser = RaindropBookmarkParser;
        let result = parser
            .parse_record(record_for(bad.as_bytes()), &test_ctx())
            .await;
        let err = result.unwrap_err().to_string();
        assert!(err.contains("invalid Raindrop timestamp"), "got: {err}");
        Ok(())
    }
}
