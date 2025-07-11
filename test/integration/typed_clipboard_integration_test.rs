/// Integration tests for typed clipboard event conversion  
use sinex_events::{
    ClipboardCopiedPayload, ClipboardSelectedPayload, EventEnvelope, TypedClipboardEventBuilder,
    TypedRawEvent,
};
use sinex_ulid::Ulid;
use chrono::Utc;

#[test]
fn test_typed_clipboard_event_builder() {
    let builder = TypedClipboardEventBuilder::new("clipboard");
    
    // Test clipboard copy event
    let copy_event = builder.content_copied(
        "text",
        100,
        Some("Hello world".to_string()),
        Some("hash123".to_string()),
        Some("firefox".to_string()),
    );
    
    // Verify it's the right variant
    match copy_event {
        EventEnvelope::ContentCopied(typed_event) => {
            assert_eq!(typed_event.source, "clipboard");
            assert_eq!(typed_event.event_type, "clipboard.copied");
            assert_eq!(typed_event.payload.content_type, "text");
            assert_eq!(typed_event.payload.content_size, 100);
        }
        _ => panic!("Expected ContentCopied variant"),
    }
}

#[test]
fn test_clipboard_selected_event() {
    let builder = TypedClipboardEventBuilder::new("clipboard");
    
    // Test primary selection event
    let select_event = builder.content_selected(
        "text",
        50,
        Some("Selected text".to_string()),
        "primary",
    );
    
    // Verify it's the right variant
    match select_event {
        EventEnvelope::ContentSelected(typed_event) => {
            assert_eq!(typed_event.source, "clipboard");
            assert_eq!(typed_event.event_type, "clipboard.selected");
            assert_eq!(typed_event.payload.content_type, "text");
            assert_eq!(typed_event.payload.content_size, 50);
            assert_eq!(typed_event.payload.selection_type, "primary");
        }
        _ => panic!("Expected ContentSelected variant"),
    }
}

#[test]
fn test_event_to_json_conversion() {
    // Create a typed clipboard event
    let payload = ClipboardCopiedPayload {
        content_type: "text".to_string(),
        content_size: 100,
        text_preview: Some("Test content".to_string()),
        content_hash: Some("hash123".to_string()),
        source_app: Some("test_app".to_string()),
    };
    
    let typed_event = TypedRawEvent {
        id: Ulid::new(),
        source: "clipboard".to_string(),
        event_type: "clipboard.copied".to_string(),
        payload,
        host: "test-host".to_string(),
        ingestor_version: "0.4.2".to_string(),
        ts_ingest: Utc::now(),
        ts_orig: None,
    };
    
    // Convert to JSON-based RawEvent
    let json_event = typed_event.to_json_event();
    
    // Verify the conversion preserves key information
    assert_eq!(json_event.source, "clipboard");
    assert_eq!(json_event.event_type, "clipboard.copied");
    assert_eq!(json_event.payload["content_type"], "text");
    assert_eq!(json_event.payload["content_size"], 100);
    assert_eq!(json_event.payload["text_preview"], "Test content");
    assert_eq!(json_event.payload["content_hash"], "hash123");
    assert_eq!(json_event.payload["source_app"], "test_app");
}

#[test]
fn test_event_registry_compatibility() {
    // Verify that our clipboard events use the expected event type strings
    // that are registered in the EventRegistry
    
    let builder = TypedClipboardEventBuilder::new("clipboard");
    
    let copy_event = builder.content_copied("text", 100, None, None, None);
    if let EventEnvelope::ContentCopied(typed_event) = copy_event {
        assert_eq!(typed_event.event_type, "clipboard.copied");
    } else {
        panic!("Expected ContentCopied variant");
    }
    
    let select_event = TypedClipboardEventBuilder::new("clipboard")
        .content_selected("text", 50, None, "primary");
    if let EventEnvelope::ContentSelected(typed_event) = select_event {
        assert_eq!(typed_event.event_type, "clipboard.selected");
    } else {
        panic!("Expected ContentSelected variant");
    }
}