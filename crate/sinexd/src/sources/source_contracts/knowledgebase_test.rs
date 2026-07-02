use super::*;
use sinex_primitives::Uuid;
use sinex_primitives::ids::Id;

use xtask::sandbox::prelude::sinex_test;

fn test_ctx() -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static(SOURCE_ID),
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
    assert_eq!(key.source_id.as_str(), SOURCE_ID);
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
