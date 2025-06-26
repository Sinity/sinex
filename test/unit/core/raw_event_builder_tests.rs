use crate::common::prelude::*;
use sinex_core::{RawEventBuilder, sources, event_type_constants};
use chrono::{DateTime, Utc};

#[sinex_test]
async fn test_raw_event_builder_basic_creation(_ctx: TestContext) -> TestResult {
    let event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        json!({"path": "/test/file.txt", "size": 1024})
    ).build();
    
    pretty_assertions::assert_eq!(event.source, sources::FILESYSTEM);
    pretty_assertions::assert_eq!(event.event_type, event_type_constants::filesystem::FILE_CREATED);
    pretty_assertions::assert_eq!(event.payload["path"], "/test/file.txt");
    pretty_assertions::assert_eq!(event.payload["size"], 1024);
    
    // Verify timestamps are reasonable
    let now = Utc::now();
    assert!(event.ts_ingest <= now);
    assert!(event.ts_ingest > now - chrono::Duration::seconds(5));
    
    // Verify host is set
    assert!(!event.host.is_empty());
    
    // Verify ULID is valid
    assert!(event.id.to_string().len() == 26);
    Ok(())
}

// Removed trivial setter tests - they just verified that .with_X(value) sets the X field

#[sinex_test]
async fn test_raw_event_builder_complex_payload(_ctx: TestContext) -> TestResult {
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
    
    pretty_assertions::assert_eq!(event.payload, complex_payload);
    pretty_assertions::assert_eq!(event.payload["nested"]["array"][1], 2);
    pretty_assertions::assert_eq!(event.payload["unicode"], "测试文件.txt");
    pretty_assertions::assert_eq!(event.payload["nested"]["object"]["number"], 42.5);
    Ok(())
}

#[sinex_test]
async fn test_raw_event_builder_empty_payload(_ctx: TestContext) -> TestResult {
    let event = RawEventBuilder::new(
        sources::SINEX,
        "system.startup",
        json!({})
    ).build();
    
    pretty_assertions::assert_eq!(event.payload, json!({}));
    pretty_assertions::assert_eq!(event.source, sources::SINEX);
    pretty_assertions::assert_eq!(event.event_type, "system.startup");
    Ok(())
}

#[sinex_test]
async fn test_raw_event_builder_multiple_builds(_ctx: TestContext) -> TestResult {
    let _builder = RawEventBuilder::new(
        sources::CLIPBOARD,
        "content.copied",
        json!({"content": "test text"})
    );
    
    // Create two events with same configuration
    let event1 = RawEventBuilder::new("test", "test.event", json!({"key": "value"})).build();
    let event2 = RawEventBuilder::new("test", "test.event", json!({"key": "value"})).build();
    
    // Events should have different IDs and timestamps
    pretty_assertions::assert_ne!(event1.id, event2.id);
    assert!(event2.ts_ingest >= event1.ts_ingest);
    
    // But same content
    pretty_assertions::assert_eq!(event1.source, event2.source);
    pretty_assertions::assert_eq!(event1.event_type, event2.event_type);
    pretty_assertions::assert_eq!(event1.payload, event2.payload);
    Ok(())
}

#[sinex_test]
async fn test_raw_event_builder_ulid_ordering(_ctx: TestContext) -> TestResult {
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
    Ok(())
}

#[sinex_test]
async fn test_raw_event_builder_all_fields_set(_ctx: TestContext) -> TestResult {
    let orig_time = DateTime::parse_from_rfc3339("2024-01-01T12:00:00Z").unwrap().with_timezone(&Utc);
    let schema_id = sinex_ulid::Ulid::new();
    
    let event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_MODIFIED,
        json!({"path": "/test/file.txt", "size": 2048})
    )
    .with_orig_timestamp(orig_time)
    .with_payload_schema_id(schema_id)
    .with_ingestor_version("test-1.0.0")
    .build();
    
    // Verify all fields are properly set
    assert!(!event.id.to_string().is_empty());
    pretty_assertions::assert_eq!(event.source, sources::FILESYSTEM);
    pretty_assertions::assert_eq!(event.event_type, event_type_constants::filesystem::FILE_MODIFIED);
    assert!(event.ts_ingest <= Utc::now());
    pretty_assertions::assert_eq!(event.ts_orig, Some(orig_time));
    assert!(!event.host.is_empty());
    pretty_assertions::assert_eq!(event.ingestor_version, Some("test-1.0.0".to_string()));
    pretty_assertions::assert_eq!(event.payload_schema_id, Some(schema_id));
    pretty_assertions::assert_eq!(event.payload["path"], "/test/file.txt");
    pretty_assertions::assert_eq!(event.payload["size"], 2048);
    Ok(())
}