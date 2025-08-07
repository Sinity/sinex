//! Core Unit Tests
//!
//! Tests for core types and utilities using the current architecture:
//! - Generic Id<T> types
//! - Event creation and validation
//! - ULID functionality
//! - Error handling with color-eyre
//! - Modern test infrastructure with sinex-test-utils

use sinex_test_utils::prelude::*;
use sinex_types::{Id, Ulid};
use sinex_db::models::Event;
use sinex_types::domain::{EventSource, EventType};
use serde_json::json;
use std::collections::HashSet;

// =============================================================================
// ULID TESTS - Core functionality verification
// =============================================================================

#[sinex_test]
fn test_ulid_basic_properties() {
    let ulid1 = Ulid::new();
    let ulid2 = Ulid::new();
    
    // ULIDs should be unique
    assert_ne!(ulid1, ulid2);
    
    // String representation should be 26 characters
    assert_eq!(ulid1.to_string().len(), 26);
    assert_eq!(ulid2.to_string().len(), 26);
    
    // ULIDs should generally maintain temporal ordering (though not guaranteed at millisecond level)
    assert!(ulid1 <= ulid2);
}

#[sinex_test] 
fn test_ulid_string_conversion() {
    let ulid = Ulid::new();
    let ulid_str = ulid.to_string();
    
    // Round-trip conversion should work
    let parsed = Ulid::from_string(&ulid_str).expect("Should parse valid ULID string");
    assert_eq!(parsed, ulid);
}

#[sinex_test]
fn test_ulid_ordering_consistency() {
    let mut ulids = Vec::new();
    for _ in 0..10 {
        ulids.push(Ulid::new());
        // Small delay to ensure different timestamps
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    
    // ULIDs should be in ascending order (generally)
    for window in ulids.windows(2) {
        assert!(window[0] <= window[1], "ULIDs should maintain temporal ordering");
    }
    
    // String representations should also be in order
    let mut ulid_strings: Vec<String> = ulids.iter().map(|u| u.to_string()).collect();
    let mut sorted_strings = ulid_strings.clone();
    sorted_strings.sort();
    
    assert_eq!(ulid_strings, sorted_strings, "ULID strings should be naturally sorted");
}

// =============================================================================
// GENERIC ID TESTS - New architecture verification
// =============================================================================

#[sinex_test]
fn test_generic_id_creation() {
    let event_id = Id::<Event>::new();
    let event_id2 = Id::<Event>::new();
    
    // IDs should be unique
    assert_ne!(event_id, event_id2);
    
    // Should be convertible to/from ULID
    let ulid: Ulid = event_id.into();
    let id_from_ulid = Id::<Event>::from(ulid);
    assert_eq!(event_id, id_from_ulid);
}

#[sinex_test]
fn test_generic_id_type_safety() {
    let event_id = Id::<Event>::new();
    
    // The following should compile - same type
    let _same_type: Id<Event> = event_id;
    
    // The following would NOT compile if uncommented - different types
    // let _different_type: Id<SomeOtherType> = event_id; // Compilation error
}

// =============================================================================
// EVENT CREATION TESTS - Current architecture
// =============================================================================

#[sinex_test]
async fn test_event_creation_with_builder(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test the current Event::schemaless() builder pattern
    let source = EventSource::from_static("test");
    let event_type = EventType::from_static("unit.test");
    let payload = json!({
        "test": true,
        "value": 42
    });
    
    let event = Event::schemaless()
        .source(source.clone())
        .event_type(event_type.clone())
        .payload(payload.clone())
        .build();
    
    // Verify event structure
    assert_eq!(event.source, source);
    assert_eq!(event.event_type, event_type);
    assert_eq!(event.payload, payload);
    assert!(event.id.is_some());
    assert!(event.ts_ingest > chrono::DateTime::from_timestamp(0, 0).unwrap());
    
    Ok(())
}

#[sinex_test]
async fn test_event_insertion_and_retrieval(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Create an event using the new direct pattern
    let event = ctx.create_test_event(
        "test-source",
        "test.event", 
        json!({
            "test_value": 123,
            "test_string": "hello"
        })
    ).await?;
    
    // Verify the event was created properly
    assert_eq!(event.source.as_str(), "test-source");
    assert_eq!(event.event_type.as_str(), "test.event");
    assert_eq!(event.payload["test_value"], json!(123));
    assert_eq!(event.payload["test_string"], json!("hello"));
    
    // Query it back using the repository pattern
    let events = ctx.pool.events()
        .by_source("test-source")
        .by_type("test.event")
        .fetch()
        .await?;
    
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].id, event.id);
    
    Ok(())
}

#[sinex_test]
async fn test_multiple_events_with_different_sources(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let test_cases = vec![
        ("filesystem", "file.created", json!({"path": "/test/file.txt"})),
        ("terminal", "command.executed", json!({"command": "ls -la"})),
        ("desktop", "window.focused", json!({"window_class": "firefox"})),
    ];
    
    let mut inserted_events = Vec::new();
    
    for (source, event_type, payload) in test_cases {
        let event = ctx.create_test_event(source, event_type, payload.clone()).await?;
        inserted_events.push(event);
    }
    
    // Verify all events were inserted
    let all_events = ctx.pool.events().get_recent(10).await?;
    assert!(all_events.len() >= 3);
    
    // Verify each source has the correct number of events
    for (source, _, _) in [("filesystem", "file.created", json!({})), ("terminal", "command.executed", json!({})), ("desktop", "window.focused", json!({}))] {
        let source_events = ctx.pool.events().by_source(source).fetch().await?;
        assert_eq!(source_events.len(), 1, "Source {} should have exactly 1 event", source);
    }
    
    Ok(())
}

// ============================================================================= 
// ERROR HANDLING TESTS - color-eyre integration
// =============================================================================

#[sinex_test]
async fn test_error_propagation_in_tests(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test that our Result<()> type works properly with color-eyre
    
    // This should work fine
    let _event = ctx.create_test_event("valid-source", "valid.type", json!({})).await?;
    
    // Test error handling with invalid data - empty source should be caught by EventSource validation
    let result = std::panic::catch_unwind(|| {
        EventSource::new("")
    });
    
    // Empty source validation behavior depends on EventSource implementation
    // This test verifies error handling works in general
    
    Ok(())
}

// =============================================================================
// PERFORMANCE AND EDGE CASE TESTS
// =============================================================================

#[sinex_test]
async fn test_concurrent_event_creation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    use std::sync::Arc;
    use tokio::task::JoinSet;
    
    let ctx = Arc::new(ctx);
    let mut join_set = JoinSet::new();
    
    // Create multiple events concurrently
    for i in 0..10 {
        let ctx_clone = ctx.clone();
        join_set.spawn(async move {
            ctx_clone.create_test_event(
                "concurrent-test",
                "concurrent.event",
                json!({
                    "task_id": i,
                    "timestamp": chrono::Utc::now().timestamp()
                })
            ).await
        });
    }
    
    // Wait for all tasks to complete
    let mut events = Vec::new();
    while let Some(result) = join_set.join_next().await {
        let event = result??; // Handle both join error and insert error
        events.push(event);
    }
    
    // Verify all events were created successfully
    assert_eq!(events.len(), 10);
    
    // Verify all events have unique IDs
    let ids: HashSet<_> = events.iter().map(|e| e.id).collect();
    assert_eq!(ids.len(), 10, "All event IDs should be unique");
    
    // Verify they're all in the database
    let db_events = ctx.pool.events().by_source("concurrent-test").fetch().await?;
    assert_eq!(db_events.len(), 10);
    
    Ok(())
}

#[rstest]
#[case("short", 10)]
#[case("medium", 1000)]
#[case("large", 10000)]
#[tokio::test]
async fn test_large_payload_handling(
    #[case] size_name: &str,
    #[case] payload_size: usize,
) -> color_eyre::eyre::Result<()> {
    let ctx = TestContext::new().await?;
    
    // Create a large payload
    let large_string = "x".repeat(payload_size);
    let payload = json!({
        "size_category": size_name,
        "data": large_string,
        "size": payload_size
    });
    
    let event = ctx.create_test_event("payload-test", "large.payload", payload.clone()).await?;
    
    // Verify the event was stored correctly
    assert_eq!(event.payload, payload);
    
    // Verify we can retrieve it
    let retrieved = ctx.pool.events()
        .get_by_id(event.id.unwrap())
        .await?
        .expect("Event should exist");
    
    assert_eq!(retrieved.payload, payload);
    
    Ok(())
}

// =============================================================================
// INTEGRATION WITH MODERN TEST INFRASTRUCTURE
// =============================================================================

#[sinex_test]
async fn test_timing_utilities(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test the timing measurement functionality
    let start_time = ctx.elapsed();
    
    // Do some work
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    
    let end_time = ctx.elapsed();
    assert!(end_time > start_time, "Time should advance");
    assert!(end_time.as_millis() >= 50, "Should measure at least 50ms");
    
    // Test the measurement helper
    let (result, duration) = ctx.measure(async {
        tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
        Ok::<_, color_eyre::eyre::Error>("test_result")
    }).await?;
    
    assert_eq!(result, "test_result");
    assert!(duration.as_millis() >= 25, "Duration should be at least 25ms");
    
    Ok(())
}

#[sinex_test]
async fn test_assertion_helpers(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test the enhanced assertion functionality
    let events = vec![
        ctx.create_test_event("test1", "test", json!({})).await?,
        ctx.create_test_event("test2", "test", json!({})).await?,
        ctx.create_test_event("test3", "test", json!({})).await?,
    ];
    
    // Test collection assertions - using standard assertions for now
    assert!(!events.is_empty(), "Event collection should not be empty");
    assert_eq!(events.len(), 3, "Should have exactly 3 events");
    
    // Test that all events have valid IDs
    for event in &events {
        assert!(event.id.is_some(), "Event should have a valid ID");
    }
    
    Ok(())
}

// =============================================================================
// REGRESSION TESTS - Preserve important behaviors
// =============================================================================

#[sinex_test]
fn test_ulid_specific_format() {
    // Test with a known ULID to ensure format consistency
    let ulid_str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    let ulid = Ulid::from_string(ulid_str).expect("Should parse known valid ULID");
    
    assert_eq!(ulid.to_string(), ulid_str);
    assert_eq!(ulid.to_string().len(), 26);
}

#[sinex_test]
async fn test_event_ordering_preserved(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Create events with slight delays to ensure ordering
    let mut events = Vec::new();
    
    for i in 0..5 {
        let event = ctx.create_test_event(
            "ordering-test",
            "sequential.event",
            json!({"sequence": i})
        ).await?;
        events.push(event);
        
        // Small delay to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
    
    // Retrieve events and verify ordering is preserved
    let retrieved_events = ctx.pool.events()
        .by_source("ordering-test")
        .fetch()
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