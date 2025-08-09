//! Unit Tests for Sinex
//!
//! This module contains comprehensive unit tests for core Sinex functionality.
//! Tests focus on individual components and utilities using the current architecture:
//! - Generic `Id<T>` types and ULID functionality
//! - Domain types (EventSource, EventType, etc.)
//! - Event creation and validation
//! - Error handling with color-eyre
//! - Core utilities and helpers

use color_eyre::eyre::eyre;
use serde_json::json;
use sinex_core::db::models::RawEvent as DbEvent;
use sinex_core::db::repositories::DbPoolExt;
use sinex_core::types::domain::{EventSource, EventType, HostName};
use sinex_core::types::{Id, Ulid};
use sinex_test_utils::prelude::*;
use std::collections::HashSet;
use std::str::FromStr;

// Database unit tests module
mod unit {
    pub mod advisory_lock_test;
    pub mod coordination_primitive_test;
    pub mod database_test;
    pub mod error_paths_test;
    pub mod event_type_system_test;
    pub mod preflight_test;
    pub mod resource_guard_test;
    pub mod typed_clipboard_test;
    pub mod version_system_test;
}

// =============================================================================
// ULID CORE FUNCTIONALITY TESTS - Time-ordered identifiers
// =============================================================================

#[sinex_test]
fn test_ulid_basic_properties() -> color_eyre::eyre::Result<()> {
    let ulid1 = Ulid::new();
    let ulid2 = Ulid::new();

    // ULIDs should be unique
    assert_ne!(ulid1, ulid2);

    // String representation should be 26 characters
    assert_eq!(ulid1.to_string().len(), 26);
    assert_eq!(ulid2.to_string().len(), 26);

    // ULIDs should generally maintain temporal ordering
    assert!(ulid1 <= ulid2);
    Ok(())
}

#[sinex_test]
fn test_ulid_string_conversion() -> color_eyre::eyre::Result<()> {
    let ulid = Ulid::new();
    let ulid_str = ulid.to_string();

    // Round-trip conversion should work
    let parsed = Ulid::from_str(&ulid_str).expect("Should parse valid ULID string");
    assert_eq!(parsed, ulid);
    Ok(())
}

#[sinex_test]
fn test_ulid_ordering_consistency() -> color_eyre::eyre::Result<()> {
    let mut ulids = Vec::new();
    for _ in 0..10 {
        ulids.push(Ulid::new());
        // Small delay to ensure different timestamps
        std::thread::sleep(std::time::Duration::from_millis(1));
    }

    // ULIDs should be in ascending order (generally)
    for window in ulids.windows(2) {
        assert!(
            window[0] <= window[1],
            "ULIDs should maintain temporal ordering"
        );
    }

    // String representations should also be in order
    let ulid_strings: Vec<String> = ulids.iter().map(|u| u.to_string()).collect();
    let mut sorted_strings = ulid_strings.clone();
    sorted_strings.sort();

    assert_eq!(
        ulid_strings, sorted_strings,
        "ULID strings should be naturally sorted"
    );
    Ok(())
}

#[sinex_test]
fn test_ulid_specific_format() -> color_eyre::eyre::Result<()> {
    // Test with a known ULID to ensure format consistency
    let ulid_str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    let ulid = Ulid::from_str(ulid_str).expect("Should parse known valid ULID");

    assert_eq!(ulid.to_string(), ulid_str);
    assert_eq!(ulid.to_string().len(), 26);
    Ok(())
}

#[sinex_test]
fn test_ulid_invalid_strings() -> color_eyre::eyre::Result<()> {
    let invalid_cases = vec![
        "",                                // Empty string
        "invalid",                         // Too short
        "01ARZ3NDEKTSV4RRFFQ69G5FAVEXTRA", // Too long
        "01ARZ3NDEKTSV4RRFFQ69G5FA!",      // Invalid character
    ];

    for invalid_ulid in invalid_cases {
        assert!(
            Ulid::from_str(invalid_ulid).is_err(),
            "Should reject invalid ULID: {}",
            invalid_ulid
        );
    }
    Ok(())
}

// =============================================================================
// GENERIC ID SYSTEM TESTS - Type-safe identifiers
// =============================================================================

#[sinex_test]
fn test_generic_id_creation() -> color_eyre::eyre::Result<()> {
    let event_id = Id::<DbEvent>::new();
    let event_id2 = Id::<DbEvent>::new();

    // IDs should be unique
    assert_ne!(event_id, event_id2);

    // Should be convertible to/from ULID
    let ulid: Ulid = event_id.clone().into();
    let id_from_ulid = Id::<DbEvent>::from(ulid);
    assert_eq!(event_id, id_from_ulid);
    Ok(())
}

#[sinex_test]
fn test_generic_id_type_safety() -> color_eyre::eyre::Result<()> {
    let event_id = Id::<DbEvent>::new();

    // The following should compile - same type
    let _same_type: Id<DbEvent> = event_id.clone();

    // Verify ID properties
    assert_eq!(event_id.to_string().len(), 26);
    assert!(!event_id.to_string().is_empty());
    Ok(())
}

#[sinex_test]
fn test_generic_id_string_conversion() -> color_eyre::eyre::Result<()> {
    let id = Id::<DbEvent>::new();
    let id_str = id.to_string();

    // String should be valid ULID format
    assert_eq!(id_str.len(), 26);
    assert!(id_str.chars().all(|c| c.is_ascii_alphanumeric()));

    // Should be able to create ULID from string
    let ulid = Ulid::from_str(&id_str).expect("Should be valid ULID");
    let new_id = Id::<DbEvent>::from(ulid);
    assert_eq!(id, new_id);
    Ok(())
}

#[sinex_test]
fn test_generic_id_collections() -> color_eyre::eyre::Result<()> {
    // Test IDs work properly in collections
    let mut ids = Vec::new();

    for _ in 0..10 {
        let id = Id::<DbEvent>::new();
        ids.push(id);
    }

    // Verify all IDs are unique by comparing pairwise
    for (i, id1) in ids.iter().enumerate() {
        for id2 in ids.iter().skip(i + 1) {
            assert_ne!(id1, id2, "All IDs should be unique");
        }
    }

    assert_eq!(ids.len(), 10);

    // Test sorting by string representation
    let mut id_strings: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
    id_strings.sort();

    // Verify natural ordering
    for window in id_strings.windows(2) {
        assert!(
            window[0] <= window[1],
            "ID strings should be naturally sorted"
        );
    }
    Ok(())
}

// =============================================================================
// DOMAIN TYPES TESTS - EventSource, EventType, etc.
// =============================================================================

#[sinex_test]
fn test_event_source_creation() -> color_eyre::eyre::Result<()> {
    // Static creation
    let source_static = EventSource::from_static("filesystem");
    assert_eq!(source_static.as_str(), "filesystem");

    // Dynamic creation
    let source_dynamic = EventSource::new("terminal-session");
    assert_eq!(source_dynamic.as_str(), "terminal-session");

    // Should be equal regardless of creation method
    let source1 = EventSource::from_static("test");
    let source2 = EventSource::new("test");
    assert_eq!(source1, source2);
    Ok(())
}

#[sinex_test]
fn test_event_type_creation() -> color_eyre::eyre::Result<()> {
    // Static creation
    let type_static = EventType::from_static("file.created");
    assert_eq!(type_static.as_str(), "file.created");

    // Dynamic creation
    let type_dynamic = EventType::new("command.executed");
    assert_eq!(type_dynamic.as_str(), "command.executed");

    // Should be equal regardless of creation method
    let type1 = EventType::from_static("test.event");
    let type2 = EventType::new("test.event");
    assert_eq!(type1, type2);
    Ok(())
}

#[sinex_test]
fn test_hostname_creation() -> color_eyre::eyre::Result<()> {
    // Test hostname creation
    let hostname = HostName::new("test-host");
    assert_eq!(hostname.as_str(), "test-host");

    // Test current hostname
    let current = HostName::new("localhost"); // Use a static hostname for tests
    assert!(!current.as_str().is_empty());
    Ok(())
}

#[sinex_test]
fn test_domain_type_validation() -> color_eyre::eyre::Result<()> {
    // Test empty string handling
    let empty_source = EventSource::new("");
    assert_eq!(empty_source.as_str(), "");

    // Test various characters
    let special_chars = "source-with_special.chars123";
    let source = EventSource::new(special_chars);
    assert_eq!(source.as_str(), special_chars);

    // Test unicode
    let unicode_source = EventSource::new("unicode-世界");
    assert_eq!(unicode_source.as_str(), "unicode-世界");
    Ok(())
}

#[rstest]
#[case("fs", "file.created")]
#[case("terminal", "command.executed")]
#[case("desktop", "window.focused")]
#[case("system", "service.started")]
#[case("long-source-name-with-hyphens", "deeply.nested.event.type.with.dots")]
fn test_domain_types_with_various_values(#[case] source_name: &str, #[case] type_name: &str) {
    let source = EventSource::new(source_name);
    let event_type = EventType::new(type_name);

    assert_eq!(source.as_str(), source_name);
    assert_eq!(event_type.as_str(), type_name);

    // Test cloning
    let source_clone = source.clone();
    let type_clone = event_type.clone();

    assert_eq!(source, source_clone);
    assert_eq!(event_type, type_clone);
}

// =============================================================================
// EVENT CREATION TESTS - DbEvent::schemaless() builder
// =============================================================================

#[sinex_test]
fn test_event_schemaless_builder() -> color_eyre::eyre::Result<()> {
    let source = EventSource::from_static("test-source");
    let event_type = EventType::from_static("test.event");
    let payload = json!({
        "test": true,
        "value": 42,
        "message": "Unit test event"
    });

    let event = DbEvent::schemaless(source.clone(), event_type.clone(), payload.clone());

    // Verify event structure
    assert_eq!(event.source, source);
    assert_eq!(event.event_type, event_type);
    assert_eq!(event.payload, payload);
    assert!(event.id.is_some());
    assert!(event.ts_ingest > chrono::DateTime::from_timestamp(0, 0).unwrap());
    Ok(())
}

#[sinex_test]
fn test_event_builder_with_optional_fields() -> color_eyre::eyre::Result<()> {
    let mut event = DbEvent::schemaless(
        EventSource::from_static("optional-test"),
        EventType::from_static("optional.event"),
        json!({"basic": true}),
    );
    event.host = HostName::new("custom-host");

    assert_eq!(event.source.as_str(), "optional-test");
    assert_eq!(event.event_type.as_str(), "optional.event");
    assert_eq!(event.host.as_str(), "custom-host");
    assert_eq!(event.payload["basic"], json!(true));
    Ok(())
}

#[sinex_test]
fn test_event_builder_with_timestamps() -> color_eyre::eyre::Result<()> {
    use chrono::{DateTime, Utc};

    let custom_timestamp = Utc::now() - chrono::Duration::hours(1);

    let mut event = DbEvent::schemaless(
        EventSource::from_static("timestamp-test"),
        EventType::from_static("timestamp.event"),
        json!({"timestamp_test": true}),
    );
    event.ts_orig = Some(custom_timestamp);

    assert_eq!(event.ts_orig, Some(custom_timestamp));
    // ts_ingest should be set to current time
    assert!(event.ts_ingest > custom_timestamp);
    Ok(())
}

#[rstest]
#[case(json!(null))]
#[case(json!(true))]
#[case(json!(42))]
#[case(json!("string"))]
#[case(json!({"key": "value"}))]
#[case(json!([1, 2, 3]))]
#[case(json!({"nested": {"deep": {"value": [1, 2, 3]}}}))]
fn test_event_builder_with_various_payloads(#[case] payload: serde_json::Value) {
    let event = DbEvent::schemaless(
        EventSource::from_static("payload-test"),
        EventType::from_static("various.payload"),
        payload.clone(),
    );

    assert_eq!(event.payload, payload);
}

// =============================================================================
// ERROR HANDLING TESTS - color-eyre integration
// =============================================================================

#[sinex_test]
fn test_result_type_compatibility() -> color_eyre::eyre::Result<()> {
    // Test that our Result type works with color-eyre
    fn returns_success() -> color_eyre::eyre::Result<String> {
        Ok("success".to_string())
    }

    fn returns_error() -> color_eyre::eyre::Result<String> {
        Err(color_eyre::eyre::anyhow!("test error"))
    }

    // Test success case
    let success_result = returns_success();
    assert!(success_result.is_ok());
    assert_eq!(success_result.unwrap(), "success");

    // Test error case
    let error_result = returns_error();
    assert!(error_result.is_err());
    assert!(error_result.unwrap_err().to_string().contains("test error"));
    Ok(())
}

#[sinex_test]
async fn test_sinex_error_propagation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test that SinexError works properly with Result

    // This should work fine
    ctx.create_test_event("error-test", "valid.test", json!({"test": true}))
        .await?;

    // Test error handling with invalid data - empty source should work but be empty
    let empty_source = EventSource::new("");
    assert_eq!(empty_source.as_str(), "");

    Ok(())
}

// =============================================================================
// VALIDATION AND EDGE CASES - Robustness testing
// =============================================================================

#[sinex_test]
fn test_edge_case_strings() -> color_eyre::eyre::Result<()> {
    let long_string = "x".repeat(1000);
    let edge_cases = vec![
        ("empty", ""),
        ("whitespace", "   "),
        ("unicode", "Hello 世界 🌍"),
        ("special_chars", "!@#$%^&*()"),
        ("long", long_string.as_str()),
        ("newlines", "line1\nline2\nline3"),
        ("tabs", "col1\tcol2\tcol3"),
        ("quotes", r#"He said "Hello" and 'Goodbye'"#),
    ];

    for (test_name, test_value) in edge_cases {
        let source = EventSource::new(test_value);
        let event_type = EventType::new(&format!("edge.{}", test_name));

        assert_eq!(source.as_str(), test_value);
        assert_eq!(event_type.as_str(), &format!("edge.{}", test_name));

        // Should work in event creation
        let event = DbEvent::schemaless(source, event_type, json!({"test_value": test_value}));

        assert_eq!(event.payload["test_value"], json!(test_value));
    }
    Ok(())
}

#[sinex_test]
fn test_concurrent_ulid_generation() -> color_eyre::eyre::Result<()> {
    use std::sync::{Arc, Mutex};
    use std::thread;

    let ulids = Arc::new(Mutex::new(Vec::new()));
    let mut handles = vec![];

    // Generate ULIDs concurrently
    for _ in 0..10 {
        let ulids_clone = ulids.clone();
        let handle = thread::spawn(move || {
            for _ in 0..100 {
                let ulid = Ulid::new();
                ulids_clone.lock().unwrap().push(ulid);
            }
        });
        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    let final_ulids = ulids.lock().unwrap();

    // Should have 1000 ULIDs
    assert_eq!(final_ulids.len(), 1000);

    // All should be unique by pairwise comparison
    for (i, ulid1) in final_ulids.iter().enumerate() {
        for ulid2 in final_ulids.iter().skip(i + 1) {
            assert_ne!(ulid1, ulid2, "All ULIDs should be unique");
        }
    }
    Ok(())
}

#[sinex_test]
fn test_large_payload_creation() -> color_eyre::eyre::Result<()> {
    // Test creating events with large payloads
    let large_string = "x".repeat(100_000); // 100KB string

    let large_payload = json!({
        "large_data": large_string,
        "metadata": {
            "size": 100000,
            "type": "stress_test",
            "nested": {
                "deep": {
                    "structure": (0..1000).collect::<Vec<i32>>()
                }
            }
        }
    });

    let event = DbEvent::schemaless(
        EventSource::from_static("stress-test"),
        EventType::from_static("large.payload"),
        large_payload.clone(),
    );

    assert_eq!(event.payload, large_payload);
    assert_eq!(event.payload["large_data"].as_str().unwrap().len(), 100_000);
    Ok(())
}

// =============================================================================
// SERIALIZATION AND DESERIALIZATION TESTS - JSON handling
// =============================================================================

#[sinex_test]
fn test_domain_type_serialization() -> color_eyre::eyre::Result<()> {
    // Test that domain types serialize/deserialize correctly
    let source = EventSource::from_static("serialization-test");
    let event_type = EventType::from_static("serialize.test");

    // Test JSON serialization
    let source_json = serde_json::to_string(&source).unwrap();
    let type_json = serde_json::to_string(&event_type).unwrap();

    assert_eq!(source_json, r#""serialization-test""#);
    assert_eq!(type_json, r#""serialize.test""#);

    // Test JSON deserialization
    let deserialized_source: EventSource = serde_json::from_str(&source_json).unwrap();
    let deserialized_type: EventType = serde_json::from_str(&type_json).unwrap();

    assert_eq!(deserialized_source, source);
    assert_eq!(deserialized_type, event_type);
    Ok(())
}

#[sinex_test]
fn test_event_json_roundtrip() -> color_eyre::eyre::Result<()> {
    let original_event = DbEvent::schemaless(
        EventSource::from_static("json-test"),
        EventType::from_static("roundtrip.test"),
        json!({
            "string": "test",
            "number": 42,
            "boolean": true,
            "null": null,
            "array": [1, 2, 3],
            "object": {"nested": "value"}
        }),
    );

    // Serialize to JSON
    let json_str = serde_json::to_string(&original_event).unwrap();

    // Deserialize back
    let deserialized_event: Event = serde_json::from_str(&json_str).unwrap();

    // Should be equal
    assert_eq!(deserialized_event.source, original_event.source);
    assert_eq!(deserialized_event.event_type, original_event.event_type);
    assert_eq!(deserialized_event.payload, original_event.payload);
    assert_eq!(deserialized_event.id, original_event.id);
    Ok(())
}

// =============================================================================
// PERFORMANCE TESTS - Basic performance characteristics
// =============================================================================

#[sinex_test]
fn test_ulid_generation_performance() -> color_eyre::eyre::Result<()> {
    use std::time::Instant;

    let start = Instant::now();
    let count = 10_000;

    let mut ulids = Vec::with_capacity(count);
    for _ in 0..count {
        ulids.push(Ulid::new());
    }

    let duration = start.elapsed();

    println!(
        "Generated {} ULIDs in {:?} ({:.2} ULIDs/ms)",
        count,
        duration,
        count as f64 / duration.as_millis() as f64
    );

    // Basic performance check - should be very fast
    assert!(duration.as_secs() < 1, "Should generate ULIDs quickly");

    // Verify all are unique
    let unique_ulids: HashSet<_> = ulids.into_iter().collect();
    assert_eq!(unique_ulids.len(), count);
    Ok(())
}

#[sinex_test]
fn test_event_creation_performance() -> color_eyre::eyre::Result<()> {
    use std::time::Instant;

    let start = Instant::now();
    let count = 1_000;

    let mut events = Vec::with_capacity(count);
    for i in 0..count {
        let event = DbEvent::schemaless(
            EventSource::from_static("perf-test"),
            EventType::from_static("performance.test"),
            json!({
                "index": i,
                "timestamp": chrono::Utc::now().timestamp(),
                "data": format!("test-data-{}", i)
            }),
        );
        events.push(event);
    }

    let duration = start.elapsed();

    println!(
        "Created {} events in {:?} ({:.2} events/ms)",
        count,
        duration,
        count as f64 / duration.as_millis() as f64
    );

    // Verify all events were created
    assert_eq!(events.len(), count);

    // Verify all have unique IDs by comparing pairwise
    let event_ids: Vec<_> = events.iter().filter_map(|e| e.id.clone()).collect();
    for (i, id1) in event_ids.iter().enumerate() {
        for id2 in event_ids.iter().skip(i + 1) {
            assert_ne!(id1, id2, "All event IDs should be unique");
        }
    }
    assert_eq!(event_ids.len(), count);
    Ok(())
}

// =============================================================================
// REGRESSION TESTS - Preserve important behaviors
// =============================================================================

#[sinex_test]
async fn test_event_ordering_preserved(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Create events with slight delays to ensure ordering
    let mut events = Vec::new();

    for i in 0..5 {
        let event = ctx
            .create_test_event("ordering-test", "sequential.event", json!({"sequence": i}))
            .await?;
        events.push(event);

        // Small delay to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    // Retrieve events and verify ordering is preserved
    let retrieved_events = ctx
        .pool
        .events()
        .get_by_source(&EventSource::from_static("ordering-test"), Some(10), None)
        .await?;

    assert_eq!(retrieved_events.len(), 5);

    // Events should be in insertion order (by timestamp)
    for i in 0..4 {
        assert!(
            retrieved_events[i].ts_ingest <= retrieved_events[i + 1].ts_ingest,
            "Events should be ordered by insertion time"
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_builder_method_chaining_order(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test event creation with different sources
    let event1 = ctx
        .create_test_event("order1", "test", json!({"a": 1}))
        .await?;

    let event2 = ctx
        .create_test_event("order2", "test", json!({"a": 1}))
        .await?;

    // Both should succeed despite different order
    assert_eq!(event1.event_type.as_str(), "test");
    assert_eq!(event2.event_type.as_str(), "test");

    Ok(())
}

#[sinex_test]
async fn test_result_type_alias(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test that Result is properly aliased
    fn returns_test_result() -> color_eyre::eyre::Result<String> {
        Ok("success".to_string())
    }

    let result = returns_test_result();
    assert!(result.is_ok());
    assert_eq!(result?, "success");

    fn returns_error() -> color_eyre::eyre::Result<()> {
        Err(color_eyre::eyre::anyhow!("test error"))
    }

    let error_result = returns_error();
    assert!(error_result.is_err());

    Ok(())
}
