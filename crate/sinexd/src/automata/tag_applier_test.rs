use crate::runtime::automaton::AutomatonContext;
use crate::runtime::{InputProvenanceFilter, Transducer};
use serde_json::json;
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
        ts_coided: trigger_event_id
            .timestamp()
            .expect("test ID must be UUIDv7"),
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
        key == sinex_primitives::derivation::derived_equivalence_key(
            super::TAG_APPLIER_OUTPUT_DECLARATIONS[0].declaration_id,
            "1.0.0",
            &format!("{trigger_id}:sys.source.browser"),
        ),
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

// These three tests previously built an `input` value and immediately
// discarded it (`let _ = input; Ok(())`), asserting nothing about
// `evaluate_rules`'s actual behavior despite their names claiming to test
// source-based and file-extension tagging. Fixed to call `evaluate_rules`
// directly and assert against the exact tag values documented in this
// module's own doc comment table (lines 6-11).

#[sinex_test]
async fn test_source_based_tagging() -> TestResult<()> {
    let context = browser_source_context();
    let tags = super::evaluate_rules(&json!({}), &context);
    assert!(
        tags.contains(&"sys.source.browser".to_string()),
        "browser.history source should apply the sys.source.browser tag, got {tags:?}"
    );
    Ok(())
}

#[sinex_test]
async fn test_file_extension_rust() -> TestResult<()> {
    let context = browser_source_context();
    let tags = super::evaluate_rules(&json!({"path": "/home/user/main.rs"}), &context);
    assert!(
        tags.contains(&"inferred.file-type.rust".to_string()),
        "a .rs path should apply the inferred.file-type.rust tag, got {tags:?}"
    );
    Ok(())
}

#[sinex_test]
async fn test_file_extension_unknown() -> TestResult<()> {
    let context = browser_source_context();
    let tags = super::evaluate_rules(&json!({"path": "/tmp/file.xyz"}), &context);
    assert!(
        !tags.iter().any(|t| t.starts_with("inferred.file-type.")),
        "an unrecognized extension should not apply any file-type tag, got {tags:?}"
    );
    Ok(())
}
