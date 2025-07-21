// Typed Clipboard Tests with Snapshot Testing
//
// Demonstrates using snapshot testing for complex clipboard data structures,
// including various content types and cross-application scenarios.

use crate::common::snapshot_testing::{assert_snapshot, assert_inline_snapshot, snapshot, Redaction};
use sinex_events::{ClipboardCopiedPayload, ClipboardSelectedPayload, EventFactory};
use serde_json::{json, Value};
use sha2::{Sha256, Digest};

#[test]
fn test_clipboard_text_operations_snapshot() {
    // Test various text clipboard operations
    let test_scenarios = vec![
        (
            "simple_text_copy",
            ClipboardCopiedPayload {
                content_type: "text".to_string(),
                content_size: 13,
                text_preview: Some("Hello, World!".to_string()),
                content_hash: Some(hash_content("Hello, World!")),
                source_app: Some("vscode".to_string()),
            },
        ),
        (
            "code_snippet_copy",
            ClipboardCopiedPayload {
                content_type: "text".to_string(),
                content_size: 156,
                text_preview: Some("fn main() {\n    println!(\"Hello, Rust!\");\n}".to_string()),
                content_hash: Some(hash_content("fn main() {\n    println!(\"Hello, Rust!\");\n}")),
                source_app: Some("neovim".to_string()),
            },
        ),
        (
            "url_copy",
            ClipboardCopiedPayload {
                content_type: "text".to_string(),
                content_size: 45,
                text_preview: Some("https://github.com/rust-lang/rust".to_string()),
                content_hash: Some(hash_content("https://github.com/rust-lang/rust")),
                source_app: Some("firefox".to_string()),
            },
        ),
        (
            "multiline_text_copy",
            ClipboardCopiedPayload {
                content_type: "text".to_string(),
                content_size: 250,
                text_preview: Some("Line 1: Introduction\nLine 2: Content\nLine 3: More content...".to_string()),
                content_hash: Some(hash_content("Line 1: Introduction\nLine 2: Content\nLine 3: More content\nLine 4: Even more\nLine 5: Conclusion")),
                source_app: Some("libreoffice".to_string()),
            },
        ),
    ];
    
    let snapshot_data = json!({
        "test_type": "clipboard_text_operations",
        "scenarios": test_scenarios.into_iter().map(|(name, payload)| {
            json!({
                "scenario": name,
                "payload": serde_json::to_value(&payload).unwrap(),
                "metadata": {
                    "is_text": payload.content_type == "text",
                    "has_preview": payload.text_preview.is_some(),
                    "preview_truncated": payload.text_preview.as_ref().map(|p| p.len() < payload.content_size).unwrap_or(false),
                    "source_application": payload.source_app.clone().unwrap_or_else(|| "unknown".to_string()),
                }
            })
        }).collect::<Vec<_>>(),
    });
    
    assert_snapshot!(
        snapshot_data,
        "clipboard_text_operations",
        Redaction::regex(r"[a-f0-9]{64}", "HASH_REDACTED")
    );
}

#[test]
fn test_clipboard_selection_types_snapshot() {
    // Test different selection types (primary, clipboard, secondary)
    let selection_scenarios = vec![
        ClipboardSelectedPayload {
            content_type: "text".to_string(),
            content_size: 25,
            text_preview: Some("Selected with mouse".to_string()),
            selection_type: "primary".to_string(),
        },
        ClipboardSelectedPayload {
            content_type: "text".to_string(),
            content_size: 30,
            text_preview: Some("Copied with Ctrl+C".to_string()),
            selection_type: "clipboard".to_string(),
        },
        ClipboardSelectedPayload {
            content_type: "text".to_string(),
            content_size: 40,
            text_preview: Some("Middle-click paste buffer".to_string()),
            selection_type: "secondary".to_string(),
        },
    ];
    
    let selection_data = json!({
        "test_type": "clipboard_selections",
        "selections": selection_scenarios.into_iter().map(|payload| {
            json!({
                "selection_type": payload.selection_type,
                "content": {
                    "type": payload.content_type,
                    "size": payload.content_size,
                    "preview": payload.text_preview,
                },
                "x11_behavior": match payload.selection_type.as_str() {
                    "primary" => "Auto-copied on selection",
                    "clipboard" => "Explicit copy action required",
                    "secondary" => "Legacy, rarely used",
                    _ => "Unknown",
                }
            })
        }).collect::<Vec<_>>(),
    });
    
    assert_snapshot!(selection_data, "clipboard_selection_types");
}

#[test]
fn test_clipboard_rich_content_snapshot() {
    // Test rich content types (HTML, RTF, images references)
    let rich_content_scenarios = json!({
        "test_type": "clipboard_rich_content",
        "scenarios": [
            {
                "name": "html_content",
                "payload": {
                    "content_type": "text/html",
                    "content_size": 512,
                    "text_preview": "<h1>Title</h1><p>Paragraph with <strong>bold</strong> text</p>",
                    "source_app": "chrome",
                    "metadata": {
                        "has_formatting": true,
                        "has_images": false,
                        "has_links": false,
                    }
                }
            },
            {
                "name": "rtf_document",
                "payload": {
                    "content_type": "text/rtf",
                    "content_size": 1024,
                    "text_preview": "{\\rtf1\\ansi\\deff0 {\\fonttbl {\\f0 Times New Roman;}}",
                    "source_app": "word",
                    "metadata": {
                        "has_formatting": true,
                        "has_styles": true,
                        "has_tables": false,
                    }
                }
            },
            {
                "name": "image_reference",
                "payload": {
                    "content_type": "image/png",
                    "content_size": 45678,
                    "text_preview": null,
                    "source_app": "gimp",
                    "metadata": {
                        "dimensions": "1920x1080",
                        "color_depth": 24,
                        "has_transparency": true,
                    }
                }
            },
            {
                "name": "mixed_content",
                "payload": {
                    "content_type": "multipart/mixed",
                    "content_size": 2048,
                    "text_preview": "Plain text version of mixed content",
                    "source_app": "thunderbird",
                    "metadata": {
                        "mime_types": ["text/plain", "text/html", "image/png"],
                        "primary_type": "text/html",
                    }
                }
            }
        ]
    });
    
    assert_snapshot!(
        rich_content_scenarios,
        "clipboard_rich_content_types",
        Redaction::field("scenarios.2.payload.content_size", json!(40000))
    );
}

#[test] 
fn test_clipboard_event_integration_snapshot() {
    // Test how clipboard payloads integrate with the event system
    let factory = EventFactory::new("clipboard");
    
    // Create various clipboard events
    let copy_event = factory.create_event(
        "copied",
        json!({
            "content_type": "text",
            "content_size": 100,
            "text_preview": "Integration test content",
            "content_hash": hash_content("Integration test content"),
            "source_app": "test_app",
        })
    );
    
    let select_event = factory.create_event(
        "selected",
        json!({
            "content_type": "text",
            "content_size": 50,
            "text_preview": "Selected text",
            "selection_type": "primary",
        })
    );
    
    let paste_event = factory.create_event(
        "pasted",
        json!({
            "content_type": "text",
            "destination_app": "terminal",
            "content_hash": hash_content("Integration test content"),
        })
    );
    
    let event_sequence = json!({
        "test_type": "clipboard_event_sequence",
        "events": [
            {
                "event_type": copy_event.event_type,
                "source": copy_event.source,
                "payload": copy_event.payload,
                "metadata": {
                    "event_id": "EVENT_0001",
                    "timestamp": "2024-01-01T00:00:00Z",
                }
            },
            {
                "event_type": select_event.event_type,
                "source": select_event.source,
                "payload": select_event.payload,
                "metadata": {
                    "event_id": "EVENT_0002",
                    "timestamp": "2024-01-01T00:00:01Z",
                }
            },
            {
                "event_type": paste_event.event_type,
                "source": paste_event.source,
                "payload": paste_event.payload,
                "metadata": {
                    "event_id": "EVENT_0003",
                    "timestamp": "2024-01-01T00:00:02Z",
                    "correlation": {
                        "copy_event_id": "EVENT_0001",
                        "content_match": true,
                    }
                }
            }
        ],
        "analysis": {
            "copy_paste_pairs": 1,
            "selection_events": 1,
            "unique_content_hashes": 2,
            "applications_involved": ["test_app", "terminal"],
        }
    });
    
    assert_snapshot!(
        event_sequence,
        "clipboard_event_integration",
        Redaction::ulids(),
        Redaction::timestamps(),
        Redaction::regex(r"[a-f0-9]{64}", "HASH_REDACTED")
    );
}

#[test]
fn test_clipboard_edge_cases_snapshot() {
    // Test edge cases and error scenarios
    let edge_cases = json!({
        "test_type": "clipboard_edge_cases",
        "cases": [
            {
                "name": "empty_clipboard",
                "payload": {
                    "content_type": "text",
                    "content_size": 0,
                    "text_preview": null,
                    "content_hash": hash_content(""),
                    "source_app": "unknown",
                },
                "expected_behavior": "Handle gracefully, store event"
            },
            {
                "name": "huge_content",
                "payload": {
                    "content_type": "text",
                    "content_size": 10_485_760, // 10MB
                    "text_preview": "First 100 chars of huge content...",
                    "content_hash": "HASH_OF_HUGE_CONTENT",
                    "source_app": "database_client",
                },
                "expected_behavior": "Truncate preview, store metadata only"
            },
            {
                "name": "binary_content",
                "payload": {
                    "content_type": "application/octet-stream",
                    "content_size": 2048,
                    "text_preview": null,
                    "content_hash": "BINARY_HASH",
                    "source_app": "hex_editor",
                },
                "expected_behavior": "No text preview, hash for deduplication"
            },
            {
                "name": "unicode_content",
                "payload": {
                    "content_type": "text",
                    "content_size": 50,
                    "text_preview": "Hello 👋 世界 🌍 مرحبا",
                    "content_hash": hash_content("Hello 👋 世界 🌍 مرحبا"),
                    "source_app": "unicode_test",
                },
                "expected_behavior": "Preserve unicode correctly"
            },
            {
                "name": "malformed_content",
                "payload": {
                    "content_type": "text",
                    "content_size": -1, // Invalid size
                    "text_preview": "\\xFF\\xFE Invalid UTF-8",
                    "content_hash": null,
                    "source_app": null,
                },
                "expected_behavior": "Validation should catch and sanitize"
            }
        ]
    });
    
    assert_snapshot!(
        edge_cases,
        "clipboard_edge_cases",
        Redaction::regex(r"[a-f0-9]{64}", "HASH_REDACTED")
    );
}

// Helper function to generate consistent hashes
fn hash_content(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[test]
fn test_inline_snapshot_example() {
    let simple_payload = ClipboardCopiedPayload {
        content_type: "text".to_string(),
        content_size: 20,
        text_preview: Some("Quick test".to_string()),
        content_hash: Some("abc123".to_string()),
        source_app: Some("test".to_string()),
    };
    
    // Example of inline snapshot - useful for simple structures
    assert_inline_snapshot!(
        serde_json::to_value(&simple_payload).unwrap(),
        @r###"
{
  "content_hash": "abc123",
  "content_size": 20,
  "content_type": "text",
  "source_app": "test",
  "text_preview": "Quick test"
}
        "###
    );
}