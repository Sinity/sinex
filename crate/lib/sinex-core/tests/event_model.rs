use serde_json::{json, Value as JsonValue};
use sinex_core::db::models::event::{Event, EventId, SourceMaterial};
use sinex_core::Id;
use sinex_test_utils::sinex_test;

#[sinex_test]
fn material_event_builder_sets_fields() -> color_eyre::eyre::Result<()> {
    let event = Event::dynamic("fs-watcher", "file.created", json!({"path": "/test.txt"}))
        .from_material(Id::<SourceMaterial>::new(), 42)
        .build();

    assert_eq!(event.source.as_str(), "fs-watcher");
    assert_eq!(event.event_type.as_str(), "file.created");
    assert!(event.is_first_order_event());
    assert!(!event.is_synthesized_event());
    assert_eq!(event.anchor_byte(), Some(42));
    assert!(event.source_event_ids().is_none());
    Ok(())
}

#[sinex_test]
fn synthesis_event_builder_tracks_parents() -> color_eyre::eyre::Result<()> {
    let parent_ids = vec![EventId::new(), EventId::new()];
    let event = Event::dynamic(
        "processor",
        "analysis.completed",
        json!({"result": "success"}),
    )
    .from_parents(parent_ids.clone())
    .build();

    assert_eq!(event.source.as_str(), "processor");
    assert_eq!(event.event_type.as_str(), "analysis.completed");
    assert!(!event.is_first_order_event());
    assert!(event.is_synthesized_event());
    assert_eq!(event.anchor_byte(), None);
    assert_eq!(event.source_event_ids(), Some(parent_ids.as_slice()));
    Ok(())
}

#[sinex_test]
fn raw_event_alias_is_equivalent() -> color_eyre::eyre::Result<()> {
    let event: Event<JsonValue> = Event::dynamic("test", "test.event", json!({"data": "value"}))
        .from_material(Id::<SourceMaterial>::new(), 0)
        .build();

    let _: Event<JsonValue> = event;
    Ok(())
}

#[sinex_test]
fn json_conversion_round_trips_payload() -> color_eyre::eyre::Result<()> {
    let original = Event::dynamic("test", "test.event", json!({"message": "hello"}))
        .from_material(Id::<SourceMaterial>::new(), 10)
        .build();

    let raw = original.to_json_event()?;
    let recovered: Event<JsonValue> = raw.to_typed()?;

    assert_eq!(recovered.payload["message"], "hello");
    assert_eq!(recovered.anchor_byte(), Some(10));
    Ok(())
}
