use super::*;
use crate::runtime::Transducer;
use crate::runtime::automaton::AutomatonContext;
use serde_json::json;
use sinex_primitives::domain::{EventSource, EventType, ProcessingMode, TriggerKind};
use sinex_primitives::events::Event;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{Id, JsonValue};
use xtask::sandbox::sinex_test;

fn material_context() -> AutomatonContext {
    let trigger_event_id: Id<Event<JsonValue>> = Id::new();
    AutomatonContext {
        trigger_event_id,
        source: EventSource::from_static("command.canonical"),
        event_type: EventType::from_static("command.canonical"),
        ts_orig: Some(Timestamp::now()),
        ts_coided: trigger_event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
        trigger_material_id: None,
        trigger_anchor_byte: None,
    }
}

// Regression test for sinex-im80: entity_extractor previously constructed
// DerivedOutput without equivalence_key/semantics_version, leaving both None
// on every persisted event. equivalence_key is the SOLE occurrence-dedup
// mechanism (event_engine admission returns early on None), so a restart
// during catch-up would reprocess and re-emit permanent duplicates with no
// dedup safety net. Mutating either `.with_equivalence_key(...)` or
// `.with_semantics_version(...)` call site in entity_extractor.rs back out
// (or reverting to the pre-fix construction with neither call) makes this
// test fail.
#[sinex_test]
async fn entity_extractor_stamps_equivalence_key_and_semantics_version() -> TestResult<()> {
    let context = material_context();
    let trigger_id = context.trigger_uuid();
    let input = json!({ "text": "Check https://example.com/path for details." });

    let output = EntityExtractor
        .process(&mut (), input, &context)
        .await?
        .expect("URL in input text should produce an entity.extracted output");

    assert_eq!(
        output.semantics_version.as_deref(),
        Some("1.0.0"),
        "semantics_version must match the declared DerivationOutputDeclaration value"
    );
    assert_eq!(
        output.equivalence_key.as_deref(),
        Some(format!("entity-extractor:{trigger_id}").as_str()),
        "equivalence_key must be deterministic per trigger event so a restart-during-catchup \
         reprocess of the same input dedupes instead of minting a permanent duplicate"
    );
    Ok(())
}

#[sinex_test]
async fn test_url_extraction() -> TestResult<()> {
    let text = "Check out https://github.com/Sinity/sinex for more info.";
    let result = find_first_entity(text);
    assert!(result.is_some());
    let entity = result.unwrap();
    assert_eq!(entity.entity_type, EntityTypeName::new("url"));
    assert!(entity.raw_name.contains("github.com"));
    Ok(())
}

#[sinex_test]
async fn test_email_extraction() -> TestResult<()> {
    let text = "Contact user@example.com for support.";
    let result = find_first_entity(text);
    assert!(result.is_some());
    let entity = result.unwrap();
    assert_eq!(entity.entity_type, EntityTypeName::new("person"));
    assert_eq!(entity.raw_name, "user@example.com");
    Ok(())
}

#[sinex_test]
async fn test_file_path_extraction() -> TestResult<()> {
    let text = "Reading from /home/user/.config/nix/nix.conf.";
    let result = find_first_entity(text);
    assert!(result.is_some());
    let entity = result.unwrap();
    assert_eq!(entity.entity_type, EntityTypeName::new("file"));
    Ok(())
}

#[sinex_test]
async fn test_command_extraction() -> TestResult<()> {
    let text = "Run nix build to compile the project.";
    let result = find_first_entity(text);
    assert!(result.is_some());
    let entity = result.unwrap();
    assert_eq!(entity.entity_type, EntityTypeName::new("tool"));
    assert_eq!(entity.raw_name, "nix");
    Ok(())
}

#[sinex_test]
async fn test_url_priority_over_file_path() -> TestResult<()> {
    let text = "See https://example.com/foo/bar for details.";
    let result = find_first_entity(text);
    assert!(result.is_some());
    let entity = result.unwrap();
    // URL should match first, not file path
    assert_eq!(entity.entity_type, EntityTypeName::new("url"));
    Ok(())
}

#[sinex_test]
async fn test_empty_text() -> TestResult<()> {
    let result = find_first_entity("");
    assert!(result.is_none());
    Ok(())
}

#[sinex_test]
async fn test_no_entity() -> TestResult<()> {
    let result = find_first_entity("This is a simple sentence with nothing extractable.");
    assert!(result.is_none());
    Ok(())
}

#[sinex_test]
async fn test_extract_text_fields() -> TestResult<()> {
    let input = json!({
        "text": "Hello https://example.com world",
        "id": "should-be-skipped",
        "byte_offset": 42,
        "nested": {"body": "another text"}
    });
    let text = extract_text_fields(&input);
    assert!(text.contains("Hello"));
    assert!(text.contains("https://example.com"));
    assert!(text.contains("another text"));
    assert!(!text.contains("should-be-skipped"));
    Ok(())
}

/// sinex-g0ve (closed invalid): `context.ts_orig.unwrap_or_else(Timestamp::now)`
/// in `process()` is the deliberate Derived-provenance wall-clock fallback
/// documented in `EventBuilder::build()` ("#1570 Prong B") -- `entity.extracted`
/// has no material to resolve a missing timestamp against, so it synthesizes
/// one at emission time. Positive characterization, not a regression test: it
/// exists so a future audit doesn't re-flag the same `unwrap_or_else` as a
/// doctrine violation against "never falsify provenance clocks" (that doctrine
/// governs Material-provenance events, which resolve ts_orig from the source
/// material's temporal ledger instead of fabricating one).
#[sinex_test]
async fn extraction_falls_back_to_wall_clock_ts_orig_when_context_lacks_one() -> TestResult<()> {
    let mut extractor = EntityExtractor;
    let context = AutomatonContext {
        trigger_event_id: Id::<Event<JsonValue>>::new(),
        source: EventSource::from_static("test.source"),
        event_type: EventType::from_static("test.type"),
        ts_orig: None,
        ts_coided: Timestamp::now(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
        trigger_material_id: None,
        trigger_anchor_byte: None,
    };
    let input = json!({"text": "Check out https://example.com for more info."});
    let before = Timestamp::now();

    let output = extractor
        .process(&mut (), input, &context)
        .await?
        .expect("URL entity should be extracted");

    let after = Timestamp::now();
    assert!(
        output.ts_orig >= before && output.ts_orig <= after,
        "ts_orig should fall back to a wall-clock synthesis time (between {before:?} and \
         {after:?}) when context.ts_orig is None, got {:?}",
        output.ts_orig
    );
    Ok(())
}

/// sinex-im80 (entity_extractor's slice): `entity.extracted` is never stamped
/// with `equivalence_key` or `semantics_version` -- `equivalence_key` is the
/// SOLE occurrence-dedup mechanism (per output.rs docs), so its absence makes
/// every `entity.extracted` structurally un-dedupable, minting duplicates on
/// any restart-during-catchup. Failing by design until entity_extractor.rs
/// calls `.with_equivalence_key(...)` (and ideally `.with_semantics_version`)
/// on its `DerivedOutput::transduced(...)` output.
#[sinex_test]
#[ignore = "sinex-im80 open: entity.extracted carries no equivalence_key -- fails until fixed"]
async fn extraction_output_is_missing_equivalence_key_sinex_im80() -> TestResult<()> {
    let mut extractor = EntityExtractor;
    let context = AutomatonContext {
        trigger_event_id: Id::<Event<JsonValue>>::new(),
        source: EventSource::from_static("test.source"),
        event_type: EventType::from_static("test.type"),
        ts_orig: Some(Timestamp::now()),
        ts_coided: Timestamp::now(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
        trigger_material_id: None,
        trigger_anchor_byte: None,
    };
    let input = json!({"text": "Check out https://example.com for more info."});

    let output = extractor
        .process(&mut (), input, &context)
        .await?
        .expect("URL entity should be extracted");

    assert!(
        output.equivalence_key.is_some(),
        "sinex-im80: entity.extracted must carry an equivalence_key -- it is the sole \
         occurrence-dedup mechanism; without it a restart-during-catchup mints duplicate \
         entity.extracted events for the same source occurrence"
    );
    Ok(())
}
