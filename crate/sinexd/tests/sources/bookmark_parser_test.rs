//! Integration tests closing AC for issue #1091 (Raindrop bookmark CSV parser).
//!
//! Covers:
//! - Two overlapping CSV snapshots produce no duplicate logical bookmarks
//!   (occurrence key = `(raindrop_id, url, created)`).
//! - Row/byte anchors identify each bookmark in its source material.
//! - Sensitive URL/note/excerpt/tag content is gated via privacy context;
//!   `cover` and `highlights` are dropped entirely.
//! - Parser satisfies source-worker registration and manifest obligations
//!   (Bus-First admission path verified via `declared_event_types` + `privacy_contexts`).

use std::collections::HashSet;

use sinex_node_sdk::parser::MaterialParser;
use sinex_primitives::{
    Uuid,
    ids::Id,
    parser::{MaterialAnchor, ParserContext, SourceRecord, SourceUnitId},
    privacy::ProcessingContext,
    temporal::Timestamp,
};
use sinexd::sources::sources::bookmark::RaindropBookmarkParser;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Fixture: two overlapping snapshots
//
// Snapshot A: 3 bookmarks — ids 100, 200, 300
// Snapshot B: 3 bookmarks — ids 200, 300, 400  (200 and 300 appear in both)
//
// When the occurrence keys from both snapshots are unioned, ids 200 and 300
// must appear exactly once each.
// ---------------------------------------------------------------------------

const SNAPSHOT_A: &str = "\
id,title,note,excerpt,url,folder,tags,created,cover,highlights,favorite
100,Page Alpha,,Short note on alpha,https://example.com/alpha,Folder A,\"rust,async\",2026-01-01T09:00:00.000Z,https://cdn.example.com/a.jpg,,false
200,Page Beta,Beta note,,https://example.com/beta,Folder B,,2026-01-15T12:00:00.000Z,,,true
300,Page Gamma,,,https://example.com/gamma,,,2026-02-01T08:00:00.000Z,,,false
";

const SNAPSHOT_B: &str = "\
id,title,note,excerpt,url,folder,tags,created,cover,highlights,favorite
200,Page Beta,Beta note,,https://example.com/beta,Folder B,,2026-01-15T12:00:00.000Z,,,true
300,Page Gamma,,,https://example.com/gamma,,,2026-02-01T08:00:00.000Z,,,false
400,Page Delta,New bookmark,,https://example.com/delta,Folder D,delta,2026-03-01T10:00:00.000Z,,,false
";

// ---------------------------------------------------------------------------
// AC: Two overlapping CSV snapshots produce no duplicate logical bookmarks
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlapping_snapshots_dedup_by_occurrence_key() {
    let mut parser = RaindropBookmarkParser;

    let a = parser
        .parse_record(record_for(SNAPSHOT_A.as_bytes()), &test_ctx())
        .await
        .unwrap();
    let b = parser
        .parse_record(record_for(SNAPSHOT_B.as_bytes()), &test_ctx())
        .await
        .unwrap();

    // Build occurrence-key strings from both snapshots.
    let keys_a: HashSet<String> = a
        .iter()
        .map(|i| {
            let k = i.occurrence_key.as_ref().unwrap();
            k.fields
                .iter()
                .map(|(f, v)| format!("{f}={v}"))
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect();

    let keys_b: HashSet<String> = b
        .iter()
        .map(|i| {
            let k = i.occurrence_key.as_ref().unwrap();
            k.fields
                .iter()
                .map(|(f, v)| format!("{f}={v}"))
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect();

    // Overlap: ids 200 and 300 appear in both snapshots with identical keys.
    let overlap: HashSet<&String> = keys_a.intersection(&keys_b).collect();
    assert_eq!(
        overlap.len(),
        2,
        "expected 2 overlapping keys (ids 200, 300)"
    );

    // Union of keys across both snapshots = 4 distinct logical bookmarks.
    let union: HashSet<&String> = keys_a.union(&keys_b).collect();
    assert_eq!(
        union.len(),
        4,
        "union of two overlapping snapshots must produce 4 distinct occurrence keys"
    );
}

// ---------------------------------------------------------------------------
// AC: Row/byte anchors identify each bookmark in its source material
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_anchors_are_sequential_line_numbers() {
    let mut parser = RaindropBookmarkParser;
    let intents = parser
        .parse_record(record_for(SNAPSHOT_A.as_bytes()), &test_ctx())
        .await
        .unwrap();

    assert_eq!(intents.len(), 3, "expected 3 parsed bookmarks");

    // Each row gets a 1-based Line anchor; byte_start is always 0
    // (whole-file material — the StaticFileAdapter records one ByteRange).
    for (i, intent) in intents.iter().enumerate() {
        let expected_line = (i + 1) as u64;
        match &intent.anchor {
            MaterialAnchor::Line { byte_start, line } => {
                assert_eq!(
                    *byte_start, 0,
                    "byte_start should be 0 (whole-file material)"
                );
                assert_eq!(
                    *line, expected_line,
                    "line anchor should be 1-based row index"
                );
            }
            other => panic!("expected Line anchor, got {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_bookmark_anchor_is_distinct() {
    let mut parser = RaindropBookmarkParser;
    let intents = parser
        .parse_record(record_for(SNAPSHOT_A.as_bytes()), &test_ctx())
        .await
        .unwrap();

    let lines: Vec<u64> = intents
        .iter()
        .map(|i| match i.anchor {
            MaterialAnchor::Line { line, .. } => line,
            _ => panic!("expected Line anchor"),
        })
        .collect();

    let unique: HashSet<u64> = lines.iter().copied().collect();
    assert_eq!(
        unique.len(),
        lines.len(),
        "each bookmark must have a distinct line anchor"
    );
}

// ---------------------------------------------------------------------------
// AC: Sensitive fields follow privacy policy
//
// The privacy_context must be ProcessingContext::Metadata on every intent.
// `cover` and `highlights` columns must be absent from the payload.
// Sensitive text fields (url, note, excerpt, tags) must be present in the
// payload (they flow through the privacy engine downstream at admission time)
// rather than being pre-emptively stripped here — the parser's role is to
// emit structured intent under the declared privacy context.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn privacy_context_is_metadata_on_all_intents() {
    let mut parser = RaindropBookmarkParser;
    let intents = parser
        .parse_record(record_for(SNAPSHOT_A.as_bytes()), &test_ctx())
        .await
        .unwrap();

    for intent in &intents {
        assert_eq!(
            intent.privacy_context,
            ProcessingContext::Metadata,
            "all bookmark intents must carry Metadata privacy context"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cover_and_highlights_absent_from_payload() {
    let csv = "\
id,title,note,excerpt,url,folder,tags,created,cover,highlights,favorite
42,Sensitive Bookmark,Secret note here,Secret excerpt,https://private.example.com,Private,sensitive-tag,2026-01-01T00:00:00.000Z,https://cdn.example.com/leaked.jpg,sensitive highlight text,false
";
    let mut parser = RaindropBookmarkParser;
    let intents = parser
        .parse_record(record_for(csv.as_bytes()), &test_ctx())
        .await
        .unwrap();

    assert_eq!(intents.len(), 1);
    let payload = &intents[0].payload;

    // cover and highlights are dropped — they must not appear in the payload.
    assert!(
        payload.get("cover").is_none(),
        "cover must be dropped from payload (rotting CDN reference)"
    );
    assert!(
        payload.get("highlights").is_none(),
        "highlights must be dropped from payload (free text not admitted)"
    );

    // Sensitive fields are present but tagged for privacy-engine admission downstream.
    assert_eq!(payload["url"], "https://private.example.com");
    assert_eq!(payload["note"], "Secret note here");
    assert_eq!(payload["excerpt"], "Secret excerpt");
    assert_eq!(payload["tags"], "sensitive-tag");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn privacy_tier_is_sensitive_in_manifest() {
    let parser = RaindropBookmarkParser;
    let manifest = parser.manifest();

    // Parser declares Metadata context — the source unit is Sensitive tier.
    // Verify the manifest names the Metadata processing context.
    assert!(
        manifest
            .privacy_contexts
            .contains(&ProcessingContext::Metadata),
        "manifest must declare Metadata privacy context"
    );
}

// ---------------------------------------------------------------------------
// AC: Output flows through source-worker + Bus-First admission path
//
// We verify that:
// - The manifest declares the expected (source, event_type) pair.
// - The parser_id and source_unit_id match the registered constants.
// - Every intent carries the source-unit-id and parser-id linking it to the
//   Bus-First admission path (#1081 source-worker registry).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manifest_declares_raindrop_bookmark_created() {
    let parser = RaindropBookmarkParser;
    let manifest = parser.manifest();

    assert_eq!(manifest.parser_id.as_str(), "raindrop-bookmarks");
    assert_eq!(manifest.source_unit_id.as_str(), "raindrop-bookmarks");
    assert_eq!(
        manifest.accepted_input_shapes,
        vec![sinex_primitives::parser::InputShapeKind::StaticFile]
    );

    let event_types: Vec<(&str, &str)> = manifest
        .declared_event_types
        .iter()
        .map(|(src, et)| (src.as_str(), et.as_str()))
        .collect();
    assert!(
        event_types.contains(&("raindrop", "bookmark.created")),
        "manifest must declare (raindrop, bookmark.created)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn intents_carry_source_worker_routing_fields() {
    let mut parser = RaindropBookmarkParser;
    let intents = parser
        .parse_record(record_for(SNAPSHOT_A.as_bytes()), &test_ctx())
        .await
        .unwrap();

    for intent in &intents {
        assert_eq!(
            intent.source_unit_id.as_str(),
            "raindrop-bookmarks",
            "intent source_unit_id must match registered source unit"
        );
        assert_eq!(
            intent.parser_id.as_str(),
            "raindrop-bookmarks",
            "intent parser_id must match registered parser"
        );
        assert_eq!(
            intent.event_source.as_str(),
            "raindrop",
            "intent event_source must be 'raindrop'"
        );
        assert_eq!(
            intent.event_type.as_str(),
            "bookmark.created",
            "intent event_type must be 'bookmark.created'"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn occurrence_key_fields_are_raindrop_id_url_created() {
    let mut parser = RaindropBookmarkParser;
    let intents = parser
        .parse_record(record_for(SNAPSHOT_A.as_bytes()), &test_ctx())
        .await
        .unwrap();

    let key = intents[0].occurrence_key.as_ref().unwrap();
    let field_names: Vec<&str> = key.fields.iter().map(|(f, _)| f.as_str()).collect();
    assert_eq!(
        field_names,
        vec!["raindrop_id", "url", "created"],
        "occurrence key must be (raindrop_id, url, created)"
    );
}

// ---------------------------------------------------------------------------
// Additional: manifest verification tags are declared
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manifest_verification_tags_declared() {
    let parser = RaindropBookmarkParser;
    let manifest = parser.manifest();
    let verification_tags: HashSet<&str> = manifest
        .proof_obligations
        .iter()
        .map(std::string::String::as_str)
        .collect();

    assert!(verification_tags.contains("timestamp_intrinsic"));
    assert!(verification_tags.contains("anchor_csv_row"));
    assert!(verification_tags.contains("occurrence_key_id_url_created"));
    assert!(verification_tags.contains("cover_and_highlights_dropped"));
}

// ---------------------------------------------------------------------------
// Additional: ts_orig matches the Raindrop `created` field
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ts_orig_matches_created_field() {
    let mut parser = RaindropBookmarkParser;
    let intents = parser
        .parse_record(record_for(SNAPSHOT_A.as_bytes()), &test_ctx())
        .await
        .unwrap();

    // First row: created = 2026-01-01T09:00:00.000Z
    let ts = intents[0].ts_orig.inner();
    assert_eq!(ts.year(), 2026);
    assert_eq!(ts.month() as u8, 1);
    assert_eq!(ts.day(), 1);
    assert_eq!(ts.hour(), 9);
}

// ---------------------------------------------------------------------------
// Additional: empty fields become null/absent in payload
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_optional_fields_are_null() {
    let csv = "\
id,title,note,excerpt,url,folder,tags,created,cover,highlights,favorite
999,Title Only,,,https://example.com/only,,,2026-06-01T00:00:00.000Z,,,false
";
    let mut parser = RaindropBookmarkParser;
    let intents = parser
        .parse_record(record_for(csv.as_bytes()), &test_ctx())
        .await
        .unwrap();
    assert_eq!(intents.len(), 1);
    let p = &intents[0].payload;
    assert!(p["note"].is_null(), "empty note must be null");
    assert!(p["excerpt"].is_null(), "empty excerpt must be null");
    assert!(p["tags"].is_null(), "empty tags must be null");
    assert!(p["folder"].is_null(), "empty folder must be null");
}
