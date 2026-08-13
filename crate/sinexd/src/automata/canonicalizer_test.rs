use super::*;
use crate::runtime::Transducer;
use crate::runtime::automaton::AutomatonContext;
use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::Event;
use sinex_primitives::events::payloads::KittyCommandExecutedPayload;
use sinex_primitives::{EventSource, EventType, Id, JsonValue, Timestamp};
use xtask::sandbox::sinex_test;

fn kitty_context() -> AutomatonContext {
    let trigger_event_id: Id<Event<JsonValue>> = Id::new();
    AutomatonContext {
        trigger_event_id,
        source: EventSource::from_static("shell.kitty"),
        event_type: EventType::from_static("command.executed"),
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

// Regression test for sinex-im80: canonicalizer previously constructed its
// DerivedOutput without equivalence_key/semantics_version. See
// entity_extractor_test.rs's identical regression test for the full
// rationale. canonicalizer had no test file at all before this commit.
#[sinex_test]
async fn canonicalizer_stamps_equivalence_key_and_semantics_version() -> TestResult<()> {
    let context = kitty_context();
    let trigger_id = context.trigger_uuid();
    let payload = KittyCommandExecutedPayload::test_default("echo hello");
    let input = serde_json::to_value(&payload)?;

    let output = TerminalCommandCanonicalizer
        .process(&mut (), input, &context)
        .await?
        .expect("a non-empty kitty command should canonicalize to an output");

    assert_eq!(
        output.semantics_version.as_deref(),
        Some("1.0.0"),
        "semantics_version must match the declared DerivationOutputDeclaration value"
    );
    assert_eq!(
        output.equivalence_key.as_deref(),
        Some(format!("canonicalizer:{trigger_id}").as_str()),
        "equivalence_key must be deterministic per trigger event so a restart-during-catchup \
         reprocess of the same input dedupes instead of minting a permanent duplicate"
    );
    Ok(())
}
