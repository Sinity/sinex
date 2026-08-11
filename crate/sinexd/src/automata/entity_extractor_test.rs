use super::*;
use crate::runtime::Transducer;
use crate::runtime::automaton::AutomatonContext;
use serde_json::json;
use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::Event;
use sinex_primitives::{EventSource, EventType, Id, JsonValue, Timestamp};
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
