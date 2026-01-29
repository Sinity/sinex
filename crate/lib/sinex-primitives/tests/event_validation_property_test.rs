#![allow(unexpected_cfgs)]

//! Event Validation Property Tests
//!
//! Migrated from test/property/event_validation_property_test.rs to modern infrastructure.
//! This module contains property-based tests for event validation using the modern
//! RawEvent::schemaless() builder pattern and updated validation architecture.

use sinex_db::validation::EventValidator;
use sinex_primitives::{Event, EventSource, EventType, HostName, Id, JsonValue, Ulid};
use time::{Duration, OffsetDateTime};
use xtask::sandbox::prelude::*;
type RawEvent = Event<JsonValue>;
// =============================================================================
// Property Test Helpers
// =============================================================================

/// Strategy for generating arbitrary JSON payloads for testing
fn event_payloads() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(json!({})),
        Just(json!({"key": "value"})),
        Just(json!({"data": "test", "size": 1024})),
        Just(json!({"path": "/tmp/test.txt", "size_bytes": 4096})),
        Just(json!({"command": "ls -la", "exit_code": 0})),
        Just(json!({"window_title": "Terminal", "app": "kitty"})),
        Just(json!({"nested": {"deep": {"value": "test"}}})),
        prop::collection::hash_map("[a-zA-Z_][a-zA-Z0-9_]*", any::<String>(), 0..10)
            .prop_map(|map| json!(map)),
    ]
}

/// Strategy for generating arbitrary valid events
fn arbitrary_event() -> impl Strategy<Value = RawEvent> {
    let source = "[a-z][a-z0-9_]{2,49}".prop_map(|raw| format!("prop_{raw}"));
    (
        source,                  // source
        "[a-z][a-z0-9_.]{2,99}", // event_type
        "[a-zA-Z0-9_.-]{1,255}", // host
        event_payloads(),        // payload
        prop::bool::ANY,         // random bool for ts_orig
    )
        .prop_map(|(source, event_type, host, payload, has_ts_orig)| {
            let mut event = test_event(
                EventSource::new(source),
                EventType::new(event_type),
                payload,
            );
            event.host = HostName::new(host);

            // Simulate ingest by assigning an ID
            event.id = Some(Id::from_ulid(Ulid::new()));

            // Conditionally set ts_orig
            if has_ts_orig {
                let ingest_ts = event
                    .id
                    .as_ref()
                    .map(|id| id.as_ulid().timestamp())
                    .unwrap_or_else(OffsetDateTime::now_utc);
                event.ts_orig = Some(ingest_ts - Duration::seconds(60));
            }

            event
        })
}

/// Strategy for generating events with empty source
fn empty_source_event() -> impl Strategy<Value = RawEvent> {
    (
        Just("".to_string()),    // empty source
        "[a-z][a-z0-9_.]{2,99}", // event_type
        event_payloads(),        // payload
    )
        .prop_map(|(source, event_type, payload)| {
            let mut event = test_event(
                EventSource::new(source),
                EventType::new(event_type),
                payload,
            );
            event.id = Some(Id::from_ulid(Ulid::new()));
            event
        })
}

/// Strategy for generating events with metadata
fn metadata_rich_events() -> impl Strategy<Value = RawEvent> {
    (
        "[a-z][a-z0-9_]{2,49}",  // source
        "[a-z][a-z0-9_.]{2,99}", // event_type
    )
        .prop_map(|(source, event_type)| {
            let payload = json!({
                "data": "test",
                "_metadata": {
                    "source": source,
                    "timestamp": OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339).unwrap()
                }
            });

            let mut event = test_event(
                EventSource::new(source),
                EventType::new(event_type),
                payload,
            );
            event.id = Some(Id::from_ulid(Ulid::new()));

            event
        })
}

/// Strategy for generating boundary condition events
fn boundary_condition_events() -> impl Strategy<Value = RawEvent> {
    let edge_cases = vec![
        // Very short fields
        ("a".to_string(), "b.c".to_string(), json!(null)),
        // Very long fields (but still valid)
        (
            "a".repeat(50),
            "event.type".repeat(10),
            json!({"data": "x".repeat(1000)}),
        ),
        // Numeric edge cases
        (
            "source".to_string(),
            "numeric.test".to_string(),
            json!({"value": i64::MAX}),
        ),
        (
            "source".to_string(),
            "numeric.test".to_string(),
            json!({"value": i64::MIN}),
        ),
        (
            "source".to_string(),
            "numeric.test".to_string(),
            json!({"value": 0}),
        ),
        // Array edge cases
        ("source".to_string(), "array.test".to_string(), json!([])),
        (
            "source".to_string(),
            "array.test".to_string(),
            json!((0..100).collect::<Vec<i32>>()),
        ),
    ];

    proptest::sample::select(edge_cases).prop_map(|(source, event_type, payload)| {
        let mut event = test_event(
            EventSource::new(source),
            EventType::new(event_type),
            payload,
        );
        event.id = Some(Id::from_ulid(Ulid::new()));
        event
    })
}

/// Strategy for generating concurrent operation events
#[cfg(feature = "concurrent_tests")]
fn concurrent_operation_events() -> impl Strategy<Value = Vec<RawEvent>> {
    prop::collection::vec(
        (0usize..10, 0u64..1000).prop_map(|(worker_id, operation_id)| {
            let payload = json!({
                "worker_id": worker_id,
                "operation_id": operation_id,
                "timestamp": OffsetDateTime::now_utc().timestamp_millis()
            });

            let mut event = test_event(
                EventSource::new("concurrent_test"),
                EventType::new("worker.operation"),
                payload,
            );
            event.id = Some(Id::from_ulid(Ulid::new()));
            event
        }),
        10..100,
    )
}

/// Strategy for performance characteristic events
#[cfg(feature = "performance_tests")]
fn performance_characteristic_events() -> impl Strategy<Value = Vec<RawEvent>> {
    prop::collection::vec(arbitrary_event(), 10..1000)
}

/// Production validation wrapper for events (avoid mock-only checks).
fn validate_event(event: &RawEvent) -> std::result::Result<(), String> {
    let validator = EventValidator::with_validation_enabled(false);
    validator.validate(event).map_err(|err| err.to_string())
}

// =============================================================================
// Core Property Tests
// =============================================================================

sinex_proptest! {
    #![cases(64)]
    fn test_valid_events_pass_validation(
        event in arbitrary_event()
    ) -> TestResult<()> {
        let result = validate_event(&event);
        prop_assert!(result.is_ok(), "Generated event should pass validation: {:?}", result);
        Ok(())
    }

    fn test_empty_source_fails_validation(
        event in empty_source_event()
    ) -> TestResult<()> {
        let result = validate_event(&event);
        prop_assert!(result.is_err(), "Event with empty source should fail validation");
        if let Err(e) = result {
            prop_assert!(
                e.to_string().contains("source") || e.to_string().contains("empty"),
                "Error should mention source issue: {}",
                e
            );
        }
        Ok(())
    }

    fn test_event_field_constraints(
        source in "[a-z][a-z0-9_]{0,49}",
        event_type in "[a-z][a-z0-9_.]{0,99}",
        host in "[a-zA-Z0-9_.-]{1,255}",
        payload in event_payloads()
    ) -> TestResult<()> {
        let mut event = test_event(
            EventSource::new(source.clone()),
            EventType::new(event_type.clone()),
            payload,
        );
        event.host = HostName::new(host.clone());
        event.id = Some(Id::from_ulid(Ulid::new()));

        prop_assert!(!event.source.is_empty());
        prop_assert!(!event.event_type.is_empty());
        prop_assert!(event.source.len() <= 50);
        prop_assert!(event.event_type.len() <= 100);
        prop_assert!(event.host.len() <= 255);
        Ok(())
    }

    fn test_payload_size_validation(
        size_kb in 1usize..=256usize
    ) -> TestResult<()> {
        let large_data = "x".repeat(size_kb * 1024);
        let payload = json!({
            "data": large_data,
            "size_kb": size_kb
        });

        let mut event = test_event(
            EventSource::new("test"),
            EventType::new("payload.size.test"),
            payload,
        );
        event.id = Some(Id::from_ulid(Ulid::new()));

        let serialized = serde_json::to_string(&event);
        prop_assert!(serialized.is_ok(), "Should serialize large payload");

        if let Ok(json_str) = serialized {
            prop_assert!(json_str.len() > size_kb * 1024,
                "Serialized size should exceed payload size");
        }
        Ok(())
    }

    fn test_event_timestamp_consistency(
        event in arbitrary_event()
    ) -> TestResult<()> {
        if let (Some(id), Some(ts_orig)) = (event.id.clone(), event.ts_orig) {
            let ingest_ts = id.timestamp();
            prop_assert!(
                ingest_ts + Duration::hours(1) >= ts_orig,
                "ULID timestamp should not significantly precede origin time"
            );
        }

        if let Some(ts_orig) = event.ts_orig {
            let now = OffsetDateTime::now_utc();
            let diff = (now - ts_orig).whole_days().abs();
            prop_assert!(diff < 365 * 100,
                "Timestamp should be within 100 years of now");
        }
        Ok(())
    }

    fn test_event_uniqueness_properties(
        events in proptest::collection::vec(arbitrary_event(), 2..32)
    ) -> TestResult<()> {
        let mut ids: Vec<Ulid> = events
            .iter()
            .filter_map(|e| e.id.clone().map(|id| id.into()))
            .collect();

        let unique_ids: std::collections::HashSet<_> = ids.iter().cloned().collect();
        prop_assert_eq!(ids.len(), unique_ids.len(),
            "All event IDs should be unique");

        ids.sort();
        for window in ids.windows(2) {
            prop_assert!(window[0] <= window[1], "IDs should maintain sort order");
        }
        Ok(())
    }

    fn test_event_metadata_fields(
        event in metadata_rich_events()
    ) -> TestResult<()> {
        if let Value::Object(ref map) = event.payload {
            if let Some(metadata) = map.get("_metadata") {
                prop_assert!(metadata.is_object(), "Metadata should be an object");
            }
        }
        Ok(())
    }

    fn test_boundary_condition_handling(
        event in boundary_condition_events()
    ) -> TestResult<()> {
        prop_assert!(!event.source.is_empty(), "Source should not be empty");
        prop_assert!(!event.event_type.is_empty(), "Event type should not be empty");
        if let Some(id) = event.id {
            prop_assert_ne!(Into::<Ulid>::into(id), Ulid::nil(), "ID should not be nil");
        }

        serde_json::to_string(&event.payload)
            .expect("Boundary payload should be serializable");
        Ok(())
    }

    fn test_validation_preserves_error_hierarchy(
        event in arbitrary_event()
    ) -> TestResult<()> {
        let result = validate_event(&event);

        if let Err(error) = result {
            let error_string = error.to_string();
            prop_assert!(!error_string.is_empty(), "Error message should not be empty");
            if event.source.is_empty() {
                prop_assert!(error_string.contains("source"));
            }
            if event.event_type.is_empty() {
                prop_assert!(error_string.contains("event_type") || error_string.contains("type"));
            }
        }
        Ok(())
    }
}

sinex_proptest! {
    #![cases(64)]
    fn property_payload_size_validation(
        size_kb in 1usize..=256usize
    ) -> TestResult<()> {
        // Property: Events should handle various payload sizes gracefully
        let large_data = "x".repeat(size_kb * 1024);
        let payload = json!({
            "data": large_data,
            "size_kb": size_kb
        });

        let mut event = test_event(
            EventSource::new("test"),
            EventType::new("payload.size.test"),
            payload,
        );
        event.id = Some(Id::from_ulid(Ulid::new()));

        // Check that large payloads are handled
        let serialized = serde_json::to_string(&event);
        prop_assert!(serialized.is_ok(), "Should serialize large payload");

        // Verify size is roughly what we expect (with JSON overhead)
        if let Ok(json_str) = serialized {
            prop_assert!(
                json_str.len() > size_kb * 1024,
                "Serialized size should exceed payload size"
            );
        }
        Ok(())
    }

    fn property_event_timestamp_consistency(
        event in arbitrary_event()
    ) -> TestResult<()> {
        // Property: ULID timestamp (ingest) should be close to ts_orig when both exist
        if let (Some(id), Some(ts_orig)) = (event.id.clone(), event.ts_orig) {
            let ingest_ts = id.timestamp();
            prop_assert!(
                ingest_ts + Duration::hours(1) >= ts_orig,
                "ULID timestamp should not significantly precede origin time"
            );
        }

        // If ts_orig exists, it should be reasonable
        if let Some(ts_orig) = event.ts_orig {
            let now = OffsetDateTime::now_utc();
            let diff = (now - ts_orig).whole_days().abs();
            prop_assert!(diff < 365 * 100, "Timestamp should be within 100 years of now");
        }
        Ok(())
    }

    fn property_event_uniqueness(
        events in proptest::collection::vec(arbitrary_event(), 2..32)
    ) -> TestResult<()> {
        // Property: Events should have unique IDs and maintain ordering
        let mut ids: Vec<Ulid> = events
            .iter()
            .filter_map(|e| e.id.clone().map(|id| id.into()))
            .collect();

        // Check uniqueness (though IDs are generated and should be unique)
        let unique_ids: std::collections::HashSet<_> = ids.iter().cloned().collect();
        prop_assert_eq!(ids.len(), unique_ids.len(), "All event IDs should be unique");

        // Check that sorting by ID gives consistent order
        ids.sort();
        for window in ids.windows(2) {
            prop_assert!(window[0] <= window[1], "IDs should maintain sort order");
        }
        Ok(())
    }

    fn property_source_event_id_validation(
        parent_events in proptest::collection::vec(Just(()).prop_map(|_| Ulid::new()), 0..10),
        _event in arbitrary_event()
    ) -> TestResult<()> {
        // Property: Source event IDs should be valid ULIDs
        // Note: source_event_ids field is not available in the current Event type
        // This property test validates the ULID values themselves
        for id in parent_events {
            // ULIDs don't have version numbers like UUIDs, but should not be nil
            prop_assert!(!id.is_nil(), "ULID should not be nil");
        }
        Ok(())
    }

    fn property_json_schema_compatibility(
        event in arbitrary_event()
    ) -> TestResult<()> {
        // Property: Event payloads should be valid JSON that can be schema-validated
        let payload_str = serde_json::to_string(&event.payload);
        prop_assert!(payload_str.is_ok(), "Payload should serialize to JSON");

        if let Ok(json_str) = payload_str {
            let parsed: std::result::Result<Value, _> = serde_json::from_str(&json_str);
            prop_assert!(parsed.is_ok(), "Payload should round-trip through JSON");
        }
        Ok(())
    }

    fn property_event_metadata_fields(
        event in metadata_rich_events()
    ) -> TestResult<()> {
        // Property: Metadata-rich events should have expected optional fields
        // Note: source_material fields are not available in Event, focusing on payload metadata

        // Check payload has metadata if it's an object
        if let Value::Object(ref map) = event.payload {
            if let Some(metadata) = map.get("_metadata") {
                prop_assert!(metadata.is_object(), "Metadata should be an object");
            }
        }
        Ok(())
    }

    fn property_boundary_condition_handling(
        event in boundary_condition_events()
    ) -> TestResult<()> {
        // Property: Boundary condition events should be processable
        // Even boundary cases should have valid structure
        prop_assert!(!event.source.is_empty(), "Source should not be empty");
        prop_assert!(!event.event_type.is_empty(), "Event type should not be empty");
        if let Some(id) = event.id {
            prop_assert_ne!(Into::<Ulid>::into(id), Ulid::nil(), "ID should not be nil");
        }

        // Payload should be valid JSON
        let _ = serde_json::to_string(&event.payload)
            .expect("Boundary payload should be serializable");
        Ok(())
    }
}

// =============================================================================
// Concurrent Tests (Feature-Gated)
// =============================================================================

#[cfg(feature = "concurrent_tests")]
mod concurrent_tests {
    use super::*;
    use std::sync::Arc;
    use xtask::sandbox::sinex_proptest;

    sinex_proptest! {
        fn property_concurrent_event_ordering(
            events in concurrent_operation_events()
        ) -> TestResult<()> {
            // Property: Concurrent events should maintain per-worker ordering
            let mut by_worker: std::collections::HashMap<usize, Vec<_>> =
                std::collections::HashMap::new();

            for event in events {
                if let Value::Object(ref map) = event.payload {
                    if let (Some(Value::Number(worker_id)), Some(Value::Number(op_id))) =
                        (map.get("worker_id"), map.get("operation_id"))
                    {
                        let worker = worker_id.as_u64().unwrap() as usize;
                        let op = op_id.as_u64().unwrap();
                        by_worker.entry(worker).or_default().push(op);
                    }
                }
            }

            // Each worker's operations should be in order
            for (_, ops) in by_worker {
                for window in ops.windows(2) {
                    prop_assert!(
                        window[0] <= window[1],
                        "Worker operations should be ordered"
                    );
                }
            }
            Ok(())
        }
    }
}

// =============================================================================
// Performance Tests (Feature-Gated)
// =============================================================================

#[cfg(feature = "performance_tests")]
mod performance_tests {
    use super::*;
    use std::time::Instant;

    use xtask::sandbox::sinex_proptest;

    sinex_proptest! {
        fn property_event_creation_performance(
            events in performance_characteristic_events()
        ) -> TestResult<()> {
            // Property: Event creation should complete in reasonable time
            let start = Instant::now();

            let serialized = serde_json::to_string(&events);

            let elapsed = start.elapsed();

            prop_assert!(
                serialized.is_ok(),
                "Should serialize performance test event"
            );
            prop_assert!(
                elapsed.as_millis() < 1000, // Increased from 100ms to 1s for large batches
                "Serialization should complete within 1000ms, took {}ms",
                elapsed.as_millis()
            );
            Ok(())
        }

        fn property_validation_errors_deterministic(
            source in "[a-z]*", // May be empty
            event_type in "[a-z]*", // May be empty
            payload in event_payloads()
        ) -> TestResult<()> {
            // Property: Same invalid input should always produce same error
            if source.is_empty() || event_type.is_empty() {
                let mut event1 = test_event(
                    EventSource::new(source.clone()),
                    EventType::new(event_type.clone()),
                    payload.clone(),
                );
                event1.id = Some(Id::from_ulid(Ulid::new()));

                let mut event2 = test_event(
                    EventSource::new(source),
                    EventType::new(event_type),
                    payload,
                );
                event2.id = Some(Id::from_ulid(Ulid::new()));

                let result1 = validate_event(&event1);
                let result2 = validate_event(&event2);

                // Both should fail with similar errors
                prop_assert!(result1.is_err() && result2.is_err());

                // Error messages should be consistent
                if let (Err(e1), Err(e2)) = (result1, result2) {
                    let msg1 = e1.to_string();
                    let msg2 = e2.to_string();
                    prop_assert_eq!(msg1, msg2, "Validation errors should be deterministic");
                }
            }
            Ok(())
        }

        fn property_validation_error_hierarchy(
            event in arbitrary_event()
        ) -> TestResult<()> {
            // Property: Validation errors should preserve proper error hierarchy
            let result = validate_event(&event);

            if let Err(error) = result {
                let error_string = error.to_string();

                // Error should contain contextual information
                prop_assert!(!error_string.is_empty(), "Error message should not be empty");

                // Error should be structured (contain field information if validation failed)
                if event.source.is_empty() {
                    prop_assert!(
                        error_string.contains("source"),
                        "Empty source error should mention 'source': {}",
                        error_string
                    );
                }
                if event.event_type.is_empty() {
                    prop_assert!(
                        error_string.contains("event_type")
                            || error_string.contains("type"),
                        "Empty event_type error should mention type: {}",
                        error_string
                    );
                }
            }
            Ok(())
        }
    }
}
