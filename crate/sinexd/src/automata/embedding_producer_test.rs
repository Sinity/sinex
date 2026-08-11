use super::*;
use crate::runtime::Transducer;
use crate::runtime::automaton::AutomatonContext;
use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::Event;
use sinex_primitives::{EventSource, EventType, Id, JsonValue, Timestamp};
use xtask::sandbox::sinex_test;

fn chunked_context() -> AutomatonContext {
    let trigger_event_id: Id<Event<JsonValue>> = Id::new();
    AutomatonContext {
        trigger_event_id,
        source: EventSource::from_static("document-parser"),
        event_type: EventType::from_static("document.chunked"),
        ts_orig: Some(Timestamp::now()),
        ts_coided: trigger_event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
        trigger_material_id: None,
        trigger_anchor_byte: None,
    }
}

// Regression test for sinex-im80: embedding_producer previously constructed
// its DerivedOutput without equivalence_key/semantics_version. See
// entity_extractor_test.rs's identical regression test for the full
// rationale. embedding_producer had no test file at all before this commit.
#[sinex_test]
async fn embedding_producer_stamps_equivalence_key_and_semantics_version() -> TestResult<()> {
    let context = chunked_context();
    let input = serde_json::json!({
        "chunk_id": "chunk-abc123",
        "chunk_hash": "blake3:deadbeef",
        "document_id": "doc-1",
    });

    let output = EmbeddingProducer
        .process(&mut (), input, &context)
        .await?
        .expect("valid chunk input should produce a document.embedded receipt");

    assert_eq!(
        output.semantics_version.as_deref(),
        Some("1.0.0"),
        "semantics_version must match the declared DerivationOutputDeclaration value"
    );
    assert_eq!(
        output.equivalence_key.as_deref(),
        Some("embedding-producer:chunk-abc123"),
        "equivalence_key must be keyed by chunk_id so re-processing the same chunk \
         (e.g. after a restart-during-catchup) dedupes to one receipt, not a duplicate"
    );
    Ok(())
}

// A chunk with no chunk_id falls back to the literal "unknown" per
// embedding_producer.rs's `.unwrap_or("unknown")` — document this as
// intentional current behavior (a real bug on its own, tracked separately,
// not this regression test's concern) rather than let it silently produce
// an untested equivalence_key shape.
#[sinex_test]
async fn embedding_producer_missing_chunk_id_uses_unknown_placeholder() -> TestResult<()> {
    let context = chunked_context();
    let input = serde_json::json!({
        "chunk_hash": "blake3:deadbeef",
        "document_id": "doc-1",
    });

    let output = EmbeddingProducer
        .process(&mut (), input, &context)
        .await?
        .expect("missing chunk_id should still produce a receipt with a placeholder key");

    assert_eq!(
        output.equivalence_key.as_deref(),
        Some("embedding-producer:unknown"),
        "documents current (arguably still-buggy) fallback behavior: chunks missing \
         chunk_id collapse onto a shared equivalence_key and will dedupe against each \
         other incorrectly -- see the wave128 finding on embedding_producer.rs's \
         chunk_id/chunk_hash defaulting for the tracked follow-up"
    );
    Ok(())
}
