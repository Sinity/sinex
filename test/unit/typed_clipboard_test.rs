/// Unit tests for typed clipboard implementation
use serde_json::json;
use sinex_events::{ClipboardCopiedPayload, ClipboardSelectedPayload};

#[test]
fn test_clipboard_copied_payload() {
    let payload = ClipboardCopiedPayload {
        content_type: "text".to_string(),
        content_size: 100,
        text_preview: Some("Hello world".to_string()),
        content_hash: Some("abc123".to_string()),
        source_app: Some("firefox".to_string()),
    };

    // Verify the payload can be serialized to JSON
    let json_value = serde_json::to_value(&payload).unwrap();
    
    // Check the essential fields match the original clipboard implementation
    assert_eq!(json_value["content_type"], "text");
    assert_eq!(json_value["content_size"], 100);
    assert_eq!(json_value["text_preview"], "Hello world");
    assert_eq!(json_value["content_hash"], "abc123");
    assert_eq!(json_value["source_app"], "firefox");
}

#[test]
fn test_clipboard_selected_payload() {
    let payload = ClipboardSelectedPayload {
        content_type: "text".to_string(),
        content_size: 50,
        text_preview: Some("Selected text".to_string()),
        selection_type: "primary".to_string(),
    };

    // Verify the payload can be serialized to JSON
    let json_value = serde_json::to_value(&payload).unwrap();
    
    // Check the essential fields match the original clipboard implementation
    assert_eq!(json_value["content_type"], "text");
    assert_eq!(json_value["content_size"], 50);
    assert_eq!(json_value["text_preview"], "Selected text");
    assert_eq!(json_value["selection_type"], "primary");
}

#[test]
fn test_payload_compatibility() {
    // Test that our simplified payloads produce compatible JSON structures
    let copied_payload = ClipboardCopiedPayload {
        content_type: "text".to_string(),
        content_size: 100,
        text_preview: Some("Test content".to_string()),
        content_hash: Some("hash123".to_string()),
        source_app: Some("test_app".to_string()),
    };
    
    let copied_json = serde_json::to_value(&copied_payload).unwrap();
    
    // Verify essential fields are present (simplified from original but functional)
    assert!(copied_json.get("content_type").is_some());
    assert!(copied_json.get("content_size").is_some());
    assert!(copied_json.get("text_preview").is_some());
    assert!(copied_json.get("content_hash").is_some());
    assert!(copied_json.get("source_app").is_some());
    
    // Original fields that we removed (acceptable simplification)
    assert!(copied_json.get("operation").is_none()); // Always "copy" in our case
    assert!(copied_json.get("file_count").is_none()); // Simplified implementation
    assert!(copied_json.get("annex_key").is_none()); // Removed git-annex integration
    assert!(copied_json.get("blob_id").is_none()); // Removed blob storage
}