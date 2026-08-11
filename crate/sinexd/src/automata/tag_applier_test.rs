use serde_json::json;
use crate::runtime::automaton::AutomatonContext;
use crate::runtime::{InputProvenanceFilter, Transducer};
use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::Event;
use sinex_primitives::{EventSource, EventType, Id, JsonValue, Timestamp};
use xtask::sandbox::sinex_test;

fn browser_source_context() -> AutomatonContext {
    let trigger_event_id: Id<Event<JsonValue>> = Id::new();
    AutomatonContext {
        trigger_event_id,
        source: EventSource::from_static("browser.history"),
        event_type: EventType::from_static("page.visited"),
        ts_orig: Some(Timestamp::now()),
        ts_coided: trigger_event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
        trigger_material_id: None,
        trigger_anchor_byte: None,
    }
}

// Regression test for sinex-im80: see entity_extractor_test.rs's identical
// regression test for the full rationale (equivalence_key is the sole
// occurrence-dedup mechanism; tag_applier previously left it None on every
// emitted event).
#[sinex_test]
async fn tag_applier_stamps_equivalence_key_and_semantics_version() -> TestResult<()> {
    let context = browser_source_context();
    let trigger_id = context.trigger_uuid();

    let output = super::TagApplier
        .process(&mut (), json!({}), &context)
        .await?
        .expect("browser.history source should match a source-based tagging rule");

    assert_eq!(
        output.semantics_version.as_deref(),
        Some("1.0.0"),
        "semantics_version must match the declared DerivationOutputDeclaration value"
    );
    let key = output
        .equivalence_key
        .as_deref()
        .expect("equivalence_key must be set so a restart-during-catchup reprocess dedupes");
    assert!(
        key.starts_with("tag-applier:") && key.contains(&trigger_id.to_string()),
        "equivalence_key {key:?} should be deterministically derived from the trigger event id"
    );
    Ok(())
}

#[sinex_test]
async fn tag_applier_consumes_material_events_only() -> TestResult<()> {
    let automaton = super::TagApplier;

    assert_eq!(
        automaton.input_provenance_filter(),
        InputProvenanceFilter::MaterialOnly
    );
    assert_eq!(automaton.input_event_type(), "*");
    Ok(())
}

#[sinex_test]
async fn test_source_based_tagging() -> TestResult<()> {
    let input = json!({});
    let _ = input;
    Ok(())
}

#[sinex_test]
async fn test_file_extension_rust() -> TestResult<()> {
    let input = json!({"path": "/home/user/main.rs"});
    let _ = input;
    Ok(())
}

#[sinex_test]
async fn test_file_extension_unknown() -> TestResult<()> {
    let input = json!({"path": "/tmp/file.xyz"});
    let _ = input;
    Ok(())
}
