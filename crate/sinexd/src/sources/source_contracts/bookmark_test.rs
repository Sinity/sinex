use super::*;
use sinex_primitives::Uuid;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::MaterialAnchor;

use xtask::sandbox::prelude::sinex_test;

fn test_ctx() -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("raindrop-bookmarks"),
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
