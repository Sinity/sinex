use super::*;
use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{CanonicalCommandPayload, DocumentIngestedPayload};
use sinex_primitives::{Id, Timestamp};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn document_parser_filters_to_document_and_canonical_command_events() -> TestResult<()> {
    let automaton = DocumentParserAutomaton::default();

    assert_eq!(automaton.input_event_type(), "*");
    assert_eq!(
        automaton.input_event_types(),
        vec![
            DocumentIngestedPayload::EVENT_TYPE.as_static_str(),
            CanonicalCommandPayload::EVENT_TYPE.as_static_str(),
        ]
    );
    assert_eq!(automaton.input_provenance_filter(), InputProvenanceFilter::Any);
    Ok(())
}

#[sinex_test]
async fn test_frontmatter_extraction() -> TestResult<()> {
    let input = "---\ntitle: My Note\ntags: rust\n---\n\nBody text here.";
    let (fm, body, body_offset) = extract_frontmatter(input);
    assert_eq!(
        fm.get("title").map(std::string::String::as_str),
        Some("My Note")
    );
    assert_eq!(
        fm.get("tags").map(std::string::String::as_str),
        Some("rust")
    );
    assert!(body.contains("Body text here"));
    // `body` must be the real suffix of `input` at `body_offset` — this is
    // exactly what document.chunked source_anchor computation relies on.
    assert_eq!(&input[body_offset..], body);
    Ok(())
}

#[sinex_test]
async fn test_wikilink_extraction() -> TestResult<()> {
    let text = "See [[design-doc]] and also [[rust/ownership]] for details.";
    let links = extract_wikilinks(text);
    assert!(links.contains(&"design-doc".to_string()));
    assert!(links.contains(&"rust/ownership".to_string()));
    Ok(())
}

#[sinex_test]
async fn test_paragraph_split_basic() -> TestResult<()> {
    let text = "Para one.\n\nPara two.\n\n\nPara three.";
    let chunks = paragraph_split(text);
    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[0], "Para one.");
    assert_eq!(chunks[1], "Para two.");
    assert_eq!(chunks[2], "Para three.");
    Ok(())
}

#[sinex_test]
async fn test_paragraph_split_empty() -> TestResult<()> {
    let chunks = paragraph_split("");
    assert_eq!(chunks.len(), 1);
    assert!(chunks[0].is_empty());
    Ok(())
}

#[sinex_test]
async fn test_document_id_determinism() -> TestResult<()> {
    let id1 = derive_document_id("dendron", "notes/design.md");
    let id2 = derive_document_id("dendron", "notes/design.md");
    assert_eq!(id1, id2);

    let id3 = derive_document_id("dendron", "notes/other.md");
    assert_ne!(id1, id3);
    Ok(())
}

#[sinex_test]
async fn test_frontmatter_no_closing() -> TestResult<()> {
    let input = "---\ntitle: Unclosed\nBody here.";
    let (fm, body, body_offset) = extract_frontmatter(input);
    assert!(fm.is_empty() || body.contains("Body"));
    // No closing delimiter found — the whole input is treated as body at
    // offset 0.
    assert_eq!(body_offset, 0);
    assert_eq!(body, input);
    Ok(())
}

#[sinex_test]
async fn test_overlong_chunk_split() -> TestResult<()> {
    let mut big = String::with_capacity(MAX_CHUNK_BYTES + 1000);
    for _ in 0..((MAX_CHUNK_BYTES / 44) + 10) {
        big.push_str("This is a sentence that takes up some space. ");
    }
    let chunks = paragraph_split(&big);
    assert!(chunks.len() > 1, "overlong paragraph should be split");
    for chunk in &chunks {
        assert!(
            chunk.len() <= MAX_CHUNK_BYTES + 200, // allowance for sentence-boundary fudge
            "chunk {} > cap {}",
            chunk.len(),
            MAX_CHUNK_BYTES
        );
    }
    Ok(())
}

/// sinex-audit-docparser-offsets: paragraph spans must be real byte ranges
/// into the source text, not a reconstruction that drops separator bytes.
/// This is the core anti-vacuity check for the offset bug: reverting the fix
/// (going back to a running sum of returned-chunk lengths) makes this fail
/// because `text[start..end]` would stop lining up with `chunk` once
/// separator bytes between paragraphs are unaccounted for.
#[sinex_test]
async fn test_paragraph_spans_round_trip_with_irregular_separators() -> TestResult<()> {
    // Non-trivial separators: single blank line, then a 3-blank-line run,
    // then a run with trailing whitespace on the "blank" line.
    let text = "Para one.\n\nPara two, a bit\nlonger across two lines.\n\n\n\nPara three.\n   \nPara four.";
    let chunks = paragraph_chunks(text);
    assert_eq!(chunks.len(), 4);

    for chunk in &chunks {
        assert_eq!(
            &text[chunk.start..chunk.end],
            chunk.text,
            "chunk span [{}, {}) must equal the chunk's own text",
            chunk.start,
            chunk.end
        );
    }

    assert_eq!(chunks[0].text, "Para one.");
    assert_eq!(chunks[1].text, "Para two, a bit\nlonger across two lines.");
    assert_eq!(chunks[2].text, "Para three.");
    assert_eq!(chunks[3].text, "Para four.");

    // Spans must be strictly increasing and reflect the real gaps consumed
    // by separators — not a tight running sum of chunk lengths.
    assert!(chunks[0].end < chunks[1].start, "separator bytes must be skipped, not summed away");
    assert!(chunks[1].end < chunks[2].start);
    assert!(chunks[2].end < chunks[3].start);
    Ok(())
}

/// sinex-x6kp: `split_capped_span`'s cut point (`pos + MAX_CHUNK_BYTES`) is
/// raw byte arithmetic with no UTF-8 char-boundary check, unlike every other
/// offset in this file (all `.find()`-derived). A single paragraph over
/// 64 KiB with a multi-byte character straddling the cut point panics the
/// live document-ingestion automaton instead of producing a valid split.
/// This fixture places a 4-byte emoji across bytes [65534, 65538) so the
/// want_end=65536 cut lands on its third byte -- not a char boundary.
#[sinex_test]
async fn test_overlong_chunk_split_respects_utf8_char_boundaries() -> TestResult<()> {
    let mut text = String::with_capacity(65738);
    text.push_str(&"a".repeat(65534));
    text.push('😀'); // 4-byte UTF-8 char occupying bytes [65534, 65538)
    text.push_str(&"a".repeat(200));
    assert!(text.len() > MAX_CHUNK_BYTES, "fixture must exceed the cap to exercise the split");

    let chunks = paragraph_chunks(&text);

    let mut reconstructed = String::with_capacity(text.len());
    for chunk in &chunks {
        assert_eq!(
            &text[chunk.start..chunk.end],
            chunk.text,
            "chunk span [{}, {}) must be a valid, real slice of the source text",
            chunk.start,
            chunk.end
        );
        reconstructed.push_str(chunk.text);
    }
    assert_eq!(
        reconstructed, text,
        "splitting an overlong paragraph must not lose or corrupt any source bytes, \
         including the multi-byte character straddling the chunk-size cap"
    );
    Ok(())
}

/// End-to-end anti-vacuity test: run the real `process_dendron` path over a
/// fixture with YAML frontmatter (so both frontmatter-prefix bytes and
/// paragraph separator bytes are in play) and verify every emitted
/// `document.chunked` event's `byte_offset_start/end` and
/// `source_anchor_start/end` round-trip against the actual source bytes.
/// Reverting the fix reintroduces the drift-by-running-sum bug and this
/// assertion fails starting at the second chunk.
#[sinex_test]
async fn document_chunked_offsets_round_trip_against_source_bytes() -> TestResult<()> {
    let content = "---\ntitle: Fixture Note\ntags: rust\n---\n\nFirst paragraph.\n\nSecond paragraph spans\nmultiple lines here.\n\n\n\nThird paragraph after a wide gap.";

    let dir = std::env::temp_dir();
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let file_path = dir.join(format!("sinex-docparser-offset-test-{unique}.md"));
    std::fs::write(&file_path, content).expect("write fixture file");

    let automaton = DocumentParserAutomaton::default();
    let mut state = DocumentParserState::default();
    let event_id = Id::new();
    let context = AutomatonContext {
        trigger_event_id: event_id,
        source: "dendron".into(),
        event_type: "document.ingested".into(),
        ts_orig: Some(Timestamp::UNIX_EPOCH),
        ts_coided: event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
        trigger_material_id: None,
        trigger_anchor_byte: None,
    };

    let outputs = automaton.process_dendron(
        &mut state,
        serde_json::json!({
            "file_path": file_path.to_string_lossy(),
            "source_material_id": "test-material",
        }),
        &context,
    );
    std::fs::remove_file(&file_path).ok();
    let outputs = outputs?;

    let chunk_events: Vec<_> = outputs
        .iter()
        .filter(|o| o.event_type == Some("document.chunked"))
        .collect();
    assert!(
        chunk_events.len() >= 3,
        "fixture has 3 well-separated paragraphs, got {} chunk events",
        chunk_events.len()
    );

    for event in &chunk_events {
        let text = event.payload["text"].as_str().expect("chunk text");
        let anchor_start = event.payload["source_anchor_start"]
            .as_u64()
            .expect("source_anchor_start") as usize;
        let anchor_end = event.payload["source_anchor_end"]
            .as_u64()
            .expect("source_anchor_end") as usize;

        assert_eq!(
            &content[anchor_start..anchor_end],
            text,
            "source_anchor [{anchor_start}, {anchor_end}) must slice out exactly this chunk's text from the original source bytes"
        );
    }

    Ok(())
}

#[sinex_test]
async fn terminal_chunks_are_not_parser_redacted() -> TestResult<()> {
    let automaton = DocumentParserAutomaton::default();
    let mut state = DocumentParserState::default();
    let event_id = Id::new();
    let context = AutomatonContext {
        trigger_event_id: event_id,
        source: "terminal".into(),
        event_type: "command.canonical".into(),
        ts_orig: Some(Timestamp::UNIX_EPOCH),
        ts_coided: event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
        trigger_material_id: None,
        trigger_anchor_byte: None,
    };
    let token = ["ghp_", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"].concat();

    let outputs = automaton.process_terminal(
        &mut state,
        serde_json::json!({
            "command": "cat token",
            "output": format!("token={token}"),
        }),
        &context,
    )?;

    let chunk = outputs
        .iter()
        .find(|output| output.event_type == Some("document.chunked"));
    assert!(chunk.is_some(), "document.chunked output");
    let text = chunk
        .and_then(|output| output.payload["text"].as_str())
        .unwrap_or("");
    assert!(
        text.contains(token.as_str()),
        "document parser must preserve parsed text; DB/user policy owns redaction"
    );
    Ok(())
}
