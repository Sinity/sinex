use sinex_core::{RawEventBuilder, sources, event_type_constants};
use serde_json::json;
use chrono::{DateTime, Utc};

#[test]
fn test_raw_event_builder_basic_creation() {
    let event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        json!({"path": "/test/file.txt", "size": 1024})
    ).build();
    
    assert_eq!(event.source, sources::FILESYSTEM);
    assert_eq!(event.event_type, event_type_constants::filesystem::FILE_CREATED);
    assert_eq!(event.payload["path"], "/test/file.txt");
    assert_eq!(event.payload["size"], 1024);
    
    // Verify timestamps are reasonable
    let now = Utc::now();
    assert!(event.ts_ingest <= now);
    assert!(event.ts_ingest > now - chrono::Duration::seconds(5));
    
    // Verify host is set
    assert!(!event.host.is_empty());
    
    // Verify ULID is valid
    assert!(event.id.to_string().len() == 26);
}

#[test]
fn test_raw_event_builder_with_original_timestamp() {
    let orig_time = DateTime::parse_from_rfc3339("2024-01-01T12:00:00Z").unwrap().with_timezone(&Utc);
    
    let event = RawEventBuilder::new(
        sources::TERMINAL_KITTY,
        event_type_constants::terminal::COMMAND_EXECUTED,
        json!({"command": "ls -la"})
    )
    .with_original_timestamp(orig_time)
    .build();
    
    assert_eq!(event.ts_orig, Some(orig_time));
    assert_eq!(event.source, sources::TERMINAL_KITTY);
    assert_eq!(event.event_type, event_type_constants::terminal::COMMAND_EXECUTED);
}

#[test]
fn test_raw_event_builder_with_schema_id() {
    let schema_id = sinex_ulid::Ulid::new();
    
    let event = RawEventBuilder::new(
        sources::HYPRLAND,
        "window.focus",
        json!({"window_id": 123, "title": "Terminal"})
    )
    .with_schema_id(schema_id)
    .build();
    
    assert_eq!(event.payload_schema_id, Some(schema_id));
    assert_eq!(event.source, sources::HYPRLAND);
    assert_eq!(event.event_type, "window.focus");
}

#[test]
fn test_raw_event_builder_with_version() {
    let event = RawEventBuilder::new(
        sources::SINEX,
        "agent.heartbeat",
        json!({"status": "running"})
    )
    .with_ingestor_version("1.2.3")
    .build();
    
    assert_eq!(event.ingestor_version, Some("1.2.3".to_string()));
}

#[test]
fn test_raw_event_builder_complex_payload() {
    let complex_payload = json!({
        "nested": {
            "array": [1, 2, 3],
            "object": {
                "key": "value",
                "number": 42.5
            }
        },
        "unicode": "测试文件.txt",
        "null_value": null,
        "boolean": true
    });
    
    let event = RawEventBuilder::new(
        sources::FILESYSTEM,
        "file.metadata_changed",
        complex_payload.clone()
    ).build();
    
    assert_eq!(event.payload, complex_payload);
    assert_eq!(event.payload["nested"]["array"][1], 2);
    assert_eq!(event.payload["unicode"], "测试文件.txt");
    assert_eq!(event.payload["nested"]["object"]["number"], 42.5);
}

#[test]
fn test_raw_event_builder_empty_payload() {
    let event = RawEventBuilder::new(
        sources::SINEX,
        "system.startup",
        json!({})
    ).build();
    
    assert_eq!(event.payload, json!({}));
    assert_eq!(event.source, sources::SINEX);
    assert_eq!(event.event_type, "system.startup");
}

#[test]
fn test_raw_event_builder_multiple_builds() {
    let builder = RawEventBuilder::new(
        sources::CLIPBOARD,
        "content.copied",
        json!({"content": "test text"})
    );
    
    let event1 = builder.clone().build();
    let event2 = builder.clone().build();
    
    // Events should have different IDs and timestamps
    assert_ne!(event1.id, event2.id);
    assert!(event2.ts_ingest >= event1.ts_ingest);
    
    // But same content
    assert_eq!(event1.source, event2.source);
    assert_eq!(event1.event_type, event2.event_type);
    assert_eq!(event1.payload, event2.payload);
}

#[test]
fn test_raw_event_builder_ulid_ordering() {
    let mut events = Vec::new();
    
    // Create events in rapid succession
    for i in 0..10 {
        let event = RawEventBuilder::new(
            sources::SINEX,
            "test.sequence",
            json!({"sequence": i})
        ).build();
        events.push(event);
        
        // Small delay to ensure timestamp progression
        std::thread::sleep(std::time::Duration::from_micros(100));
    }
    
    // ULIDs should be in ascending order
    for i in 1..events.len() {
        assert!(events[i].id.to_string() > events[i-1].id.to_string());
        assert!(events[i].ts_ingest >= events[i-1].ts_ingest);
    }
}

#[test]
fn test_raw_event_builder_all_fields_set() {
    let orig_time = DateTime::parse_from_rfc3339("2024-01-01T12:00:00Z").unwrap().with_timezone(&Utc);
    let schema_id = sinex_ulid::Ulid::new();
    
    let event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_MODIFIED,
        json!({"path": "/test/file.txt", "size": 2048})
    )
    .with_original_timestamp(orig_time)
    .with_schema_id(schema_id)
    .with_ingestor_version("test-1.0.0")
    .build();
    
    // Verify all fields are properly set
    assert!(!event.id.to_string().is_empty());
    assert_eq!(event.source, sources::FILESYSTEM);
    assert_eq!(event.event_type, event_type_constants::filesystem::FILE_MODIFIED);
    assert!(event.ts_ingest <= Utc::now());
    assert_eq!(event.ts_orig, Some(orig_time));
    assert!(!event.host.is_empty());
    assert_eq!(event.ingestor_version, Some("test-1.0.0".to_string()));
    assert_eq!(event.payload_schema_id, Some(schema_id));
    assert_eq!(event.payload["path"], "/test/file.txt");
    assert_eq!(event.payload["size"], 2048);
}