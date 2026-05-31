use serde_json::{Value as JsonValue, json};
use sinex_primitives::Id;
use sinex_primitives::domain::RecordedPath;
use sinex_primitives::events::payloads::{FileCreatedPayload, KittyCommandExecutedPayload};
use sinex_primitives::events::{EventId, SourceMaterial};
use sinex_primitives::{DynamicPayload, Event};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn material_event_builder_sets_fields() -> TestResult<()> {
    let payload = FileCreatedPayload::test_default(
        RecordedPath::from_observed("/test.txt").map_err(|e| color_eyre::eyre::eyre!(e))?,
    );
    let event = Event::builder(payload)
        .from_material(Id::<SourceMaterial>::new(), 42)
        .build()?;

    assert_eq!(event.source.as_str(), "fs-watcher");
    assert_eq!(event.event_type.as_str(), "file.created");
    assert!(event.is_first_order_event());
    assert!(!event.is_synthesized_event());
    assert_eq!(event.get_anchor_byte(), Some(42));
    assert!(event.get_source_event_ids().is_none());
    Ok(())
}

#[sinex_test]
async fn material_event_builder_stamps_anchor_payload_hash() -> TestResult<()> {
    let payload = FileCreatedPayload::test_default(
        RecordedPath::from_observed("/test.txt").map_err(|e| color_eyre::eyre::eyre!(e))?,
    );
    let bytes = b"the captured byte range";
    let event = Event::builder(payload)
        .from_material(Id::<SourceMaterial>::new(), 0)
        .with_anchor_payload_from_bytes(bytes)
        .build()?;

    let expected = *blake3::hash(bytes).as_bytes();
    assert_eq!(
        event.anchor_payload_hash.as_deref(),
        Some(&expected[..]),
        "material-provenance events should carry the BLAKE3 of the captured byte range",
    );
    assert_eq!(
        event.anchor_payload_hash.as_ref().map(std::vec::Vec::len),
        Some(32),
        "anchor_payload_hash must be exactly 32 bytes (matches DB CHECK length=32)",
    );
    Ok(())
}

#[sinex_test]
async fn synthesis_event_builder_drops_anchor_payload_hash() -> TestResult<()> {
    let parent_ids = vec![EventId::new(), EventId::new()];
    let payload = KittyCommandExecutedPayload::test_default("derived");
    // A caller can supply a hash on a derived builder; build() must drop it
    // because derived events derive identity from parent event ids, not from
    // a source-material byte range. The XOR CHECK on core.events would also
    // reject a derived row with a hash anyway, but defending in Rust keeps
    // the on-the-wire shape honest.
    let event = Event::builder(payload)
        .from_parents(parent_ids)?
        .with_anchor_payload_from_bytes(b"not a material payload")
        .build()?;
    assert!(
        event.anchor_payload_hash.is_none(),
        "derived events must never carry anchor_payload_hash",
    );
    Ok(())
}

#[sinex_test]
async fn synthesis_event_builder_tracks_parents() -> TestResult<()> {
    let parent_ids = vec![EventId::new(), EventId::new()];
    let payload = KittyCommandExecutedPayload::test_default("analysis pipeline");
    let event = Event::builder(payload)
        .from_parents(parent_ids.clone())?
        .build()?;

    assert_eq!(event.source.as_str(), "shell.kitty");
    assert_eq!(event.event_type.as_str(), "command.executed");
    assert!(!event.is_first_order_event());
    assert!(event.is_synthesized_event());
    assert_eq!(event.get_anchor_byte(), None);
    assert_eq!(event.get_source_event_ids(), Some(parent_ids.as_slice()));
    Ok(())
}

#[sinex_test]
async fn raw_event_alias_is_equivalent() -> TestResult<()> {
    let event: sinex_primitives::Event<JsonValue> =
        DynamicPayload::new("test", "test.event", json!({"data": "value"}))
            .from_material(Id::<SourceMaterial>::new())
            .build()?;

    let _: sinex_primitives::Event<JsonValue> = event;
    Ok(())
}

#[sinex_test]
async fn json_conversion_round_trips_payload() -> TestResult<()> {
    let original = DynamicPayload::new("test", "test.event", json!({"message": "hello"}))
        .from_material_at(Id::<SourceMaterial>::new(), 10)
        .build()?;

    let raw = original.to_json_event()?;
    let recovered: Event<JsonValue> = raw.to_typed()?;

    assert_eq!(recovered.payload["message"], "hello");
    assert_eq!(recovered.get_anchor_byte(), Some(10));
    Ok(())
}
