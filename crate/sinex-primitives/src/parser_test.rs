use super::*;
use xtask::sandbox::sinex_test;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a minimal material-provenance `ParsedEventIntent` for tests.
fn material_intent() -> ParsedEventIntent {
    use crate::events::EventPayload as _;
    use crate::events::payloads::DocumentIngestedPayload;

    let payload = DocumentIngestedPayload::test_default();
    ParsedEventIntent {
        id: Id::new(),
        source_id: SourceId::from_static("document.staging"),
        parser_id: ParserId::from_static("document-source"),
        parser_version: "1.0.0".into(),
        event_type: payload.event_type(),
        event_source: payload.event_source(),
        payload: serde_json::to_value(&payload).unwrap(),
        ts_orig: Timestamp::now(),
        timing: TimingEvidence::Atemporal,
        anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
        occurrence_key: None,
        privacy_context: crate::privacy::ProcessingContext::Metadata,
        derived_parents: None,
    }
}

/// Build a derived-provenance `ParsedEventIntent` for tests.
fn synthesis_intent() -> ParsedEventIntent {
    let mut intent = material_intent();
    intent.derived_parents = Some(vec![Id::new()]);
    intent
}

// ---------------------------------------------------------------------------
// derive_from_parents tests
// ---------------------------------------------------------------------------

#[sinex_test]
async fn derive_synthesis_from_material_event_succeeds() -> TestResult<()> {
    use crate::events::payloads::KnowledgeTagAppliedPayload;

    let parent = material_intent();
    let tag_payload = KnowledgeTagAppliedPayload::test_default();

    let child = parent.derive_from_parents(&tag_payload)?;

    // derived_parents must be populated with the parent's id
    let parents = child
        .derived_parents
        .as_ref()
        .expect("derived_parents must be Some");
    assert_eq!(parents.len(), 1);
    assert_eq!(parents[0], parent.id);
    assert!(child.is_synthesis());
    assert!(!child.is_material());

    Ok(())
}

#[sinex_test]
async fn derive_synthesis_preserves_parent_acquisition_time() -> TestResult<()> {
    use crate::events::payloads::KnowledgeTagAppliedPayload;

    let parent = material_intent();
    let parent_ts = parent.ts_orig;
    let tag_payload = KnowledgeTagAppliedPayload::test_default();

    let child = parent.derive_from_parents(&tag_payload)?;

    assert_eq!(
        child.ts_orig, parent_ts,
        "child ts_orig must match parent ts_orig (same temporal window)"
    );

    Ok(())
}

#[sinex_test]
async fn derive_synthesis_assigns_fresh_id() -> TestResult<()> {
    use crate::events::payloads::KnowledgeTagAppliedPayload;

    let parent = material_intent();
    let parent_id = parent.id;
    let tag_payload = KnowledgeTagAppliedPayload::test_default();

    let child = parent.derive_from_parents(&tag_payload)?;

    assert_ne!(child.id, parent_id, "child id must differ from parent id");

    Ok(())
}

#[sinex_test]
async fn derive_synthesis_rejects_synthesis_parent() -> TestResult<()> {
    use crate::events::payloads::KnowledgeTagAppliedPayload;

    let parent = synthesis_intent();
    let tag_payload = KnowledgeTagAppliedPayload::test_default();

    let result = parent.derive_from_parents(&tag_payload);

    assert!(
        result.is_err(),
        "derive_from_parents must reject a derived-provenance parent"
    );
    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("material-provenance"),
        "error message should mention material-provenance; got: {msg}"
    );

    Ok(())
}

#[sinex_test]
async fn derive_synthesis_uses_new_event_type() -> TestResult<()> {
    use crate::events::EventPayload as _;
    use crate::events::payloads::KnowledgeTagAppliedPayload;

    let parent = material_intent();
    // Parent is document.ingested; child must be knowledge.tag_applied
    let parent_event_type = parent.event_type.clone();
    let tag_payload = KnowledgeTagAppliedPayload::test_default();

    let child = parent.derive_from_parents(&tag_payload)?;

    assert_ne!(
        child.event_type, parent_event_type,
        "child event_type must come from the new payload, not the parent"
    );
    assert_eq!(
        child.event_type,
        KnowledgeTagAppliedPayload::EVENT_TYPE,
        "child event_type must match KnowledgeTagAppliedPayload::EVENT_TYPE"
    );
    assert_eq!(
        child.event_source,
        KnowledgeTagAppliedPayload::SOURCE,
        "child event_source must match KnowledgeTagAppliedPayload::SOURCE"
    );

    Ok(())
}
