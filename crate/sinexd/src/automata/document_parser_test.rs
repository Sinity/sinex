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
    let (fm, body) = extract_frontmatter(input);
    assert_eq!(
        fm.get("title").map(std::string::String::as_str),
        Some("My Note")
    );
    assert_eq!(
        fm.get("tags").map(std::string::String::as_str),
        Some("rust")
    );
    assert!(body.contains("Body text here"));
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
    let (fm, body) = extract_frontmatter(input);
    assert!(fm.is_empty() || body.contains("Body"));
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
