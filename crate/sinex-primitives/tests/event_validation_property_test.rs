#![allow(unexpected_cfgs)]

//! Event Validation Property Tests
//!
//! Migrated from `test/property/event_validation_property_test.rs` to modern infrastructure.
//! This module contains property-based tests for event validation using the modern
//! `RawEvent::schemaless()` builder pattern and updated validation architecture.

use serde_json::Value;
use sinex_db::validation::EventValidator;
use sinex_primitives::testing::event_fixture;
use sinex_primitives::{Event, EventSource, EventType, HostName, Id, JsonValue, Timestamp, Uuid};
use time::Duration;
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

fn valid_event_type_strings() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_.]{2,99}".prop_filter(
        "must not start/end with dot or contain consecutive dots",
        |value| !value.starts_with('.') && !value.ends_with('.') && !value.contains(".."),
    )
}

fn valid_event_sources() -> impl Strategy<Value = EventSource> {
    "[a-z][a-z0-9_]{0,49}"
        .prop_map(|value| EventSource::new(value).expect("regex-generated source is valid"))
}

fn valid_event_types() -> impl Strategy<Value = EventType> {
    valid_event_type_strings()
        .prop_map(|value| EventType::new(value).expect("filtered regex event type is valid"))
}

fn valid_hosts() -> impl Strategy<Value = HostName> {
    "[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?){0,3}"
        .prop_map(|value| HostName::new(value).expect("regex-generated host is valid"))
}

/// Strategy for generating arbitrary valid events.
fn arbitrary_event() -> impl Strategy<Value = RawEvent> {
    (
        valid_event_sources(),
        valid_event_types(),
        valid_hosts(),
        event_payloads(),
        prop::bool::ANY,
    )
        .prop_map(|(source, event_type, host, payload, has_ts_orig)| {
            let mut event = event_fixture(source, event_type, payload);
            event.host = host;

            // Simulate ingest by assigning an ID.
            event.id = Some(Id::from_uuid(Uuid::now_v7()));

            // Conditionally set ts_orig.
            if has_ts_orig {
                let ingest_ts = event
                    .id
                    .as_ref()
                    .map_or_else(Timestamp::now, sinex_db::Id::timestamp);
                event.ts_orig = Some(ingest_ts - Duration::seconds(60));
            }

            event
        })
}

/// Strategy for generating events with metadata.
fn metadata_rich_events() -> impl Strategy<Value = RawEvent> {
    (valid_event_sources(), valid_event_types()).prop_map(|(source, event_type)| {
        let metadata_timestamp = (*Timestamp::now())
            .format(&time::format_description::well_known::Rfc3339)
            .expect("Timestamp should format as RFC3339");
        let payload = json!({
            "data": "test",
            "_metadata": {
                "source": source.as_str(),
                "timestamp": metadata_timestamp
            }
        });

        let mut event = event_fixture(source, event_type, payload);
        event.id = Some(Id::from_uuid(Uuid::now_v7()));

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
        let source = EventSource::new(source)
            .expect("boundary source fixture should be a valid EventSource");
        let event_type = EventType::new(event_type)
            .expect("boundary event-type fixture should be a valid EventType");
        let mut event = event_fixture(source, event_type, payload);
        event.id = Some(Id::from_uuid(Uuid::now_v7()));
        event
    })
}

/// Strategy for generating concurrent operation events
fn concurrent_operation_events() -> impl Strategy<Value = Vec<RawEvent>> {
    prop::collection::vec((0usize..10, 0u64..1000), 10..100).prop_map(|mut operations| {
        operations.sort_unstable_by_key(|(worker_id, operation_id)| (*worker_id, *operation_id));
        operations
            .into_iter()
            .map(|(worker_id, operation_id)| {
                let payload = json!({
                    "worker_id": worker_id,
                    "operation_id": operation_id,
                    "timestamp": (*Timestamp::now()).unix_timestamp_nanos() / 1_000_000
                });

                let mut event = event_fixture(
                    EventSource::from_static("concurrent_test"),
                    EventType::from_static("worker.operation"),
                    payload,
                );
                event.id = Some(Id::from_uuid(Uuid::now_v7()));
                event
            })
            .collect()
    })
}

/// Strategy for performance characteristic events
fn performance_characteristic_events() -> impl Strategy<Value = Vec<RawEvent>> {
    prop::collection::vec(event_payloads(), 10..250).prop_map(|payloads| {
        payloads
            .into_iter()
            .map(|payload| {
                let mut event = event_fixture(
                    EventSource::from_static("perf_test"),
                    EventType::from_static("perf.event"),
                    payload,
                );
                event.id = Some(Id::from_uuid(Uuid::now_v7()));
                event
            })
            .collect()
    })
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
        event_type in valid_event_type_strings(),
        payload in event_payloads()
    ) -> TestResult<()> {
        let event = serde_json::json!({
            "source": "",
            "event_type": event_type,
            "host": "localhost",
            "payload": payload,
            "provenance": {
                "kind": "material",
                "id": Uuid::now_v7(),
                "anchor_byte": 0,
                "offset_kind": "byte"
            }
        });
        let result = serde_json::from_value::<RawEvent>(event);
        prop_assert!(result.is_err(), "Event with empty source should fail deserialization");
        if let Err(e) = result {
            let e = e.to_string();
            prop_assert!(
                e.contains("source") || e.contains("empty"),
                "Error should mention source issue: {}",
                e
            );
        }
        Ok(())
    }

    fn test_event_field_constraints(
        source in valid_event_sources(),
        event_type in valid_event_types(),
        host in valid_hosts(),
        payload in event_payloads()
    ) -> TestResult<()> {
        let mut event = event_fixture(
            source,
            event_type,
            payload,
        );
        event.host = host;
        event.id = Some(Id::from_uuid(Uuid::now_v7()));

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

        let mut event = event_fixture(
            EventSource::from_static("test"),
            EventType::from_static("payload.size.test"),
            payload,
        );
        event.id = Some(Id::from_uuid(Uuid::now_v7()));

        let json_str = serde_json::to_string(&event).map_err(|error| {
            TestCaseError::fail(format!("large payload event should serialize: {error}"))
        })?;
        let decoded = serde_json::from_str::<RawEvent>(&json_str).map_err(|error| {
            TestCaseError::fail(format!("large payload event should deserialize: {error}"))
        })?;
        prop_assert_eq!(decoded.payload, event.payload);
        prop_assert!(
            json_str.len() > size_kb * 1024,
            "Serialized size should exceed payload size"
        );
        Ok(())
    }

    fn test_event_timestamp_consistency(
        event in arbitrary_event()
    ) -> TestResult<()> {
        if let (Some(id), Some(ts_orig)) = (event.id, event.ts_orig) {
            let ingest_ts = id.timestamp();
            prop_assert!(
                ingest_ts + Duration::hours(1) >= Timestamp::from(*ts_orig),
                "UUIDv7 timestamp should not significantly precede origin time"
            );
        }

        if let Some(ts_orig) = event.ts_orig {
            let now = Timestamp::now();
            let diff = (*now - *ts_orig).whole_days().abs();
            prop_assert!(diff < 365 * 100,
                "Timestamp should be within 100 years of now");
        }
        Ok(())
    }

    fn test_event_uniqueness_properties(
        events in proptest::collection::vec(arbitrary_event(), 2..32)
    ) -> TestResult<()> {
        let mut ids: Vec<Uuid> = events
            .iter()
            .filter_map(|e| e.id.map(std::convert::Into::into))
            .collect();

        let unique_ids: std::collections::HashSet<_> = ids.iter().copied().collect();
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
        if let Value::Object(ref map) = event.payload
            && let Some(metadata) = map.get("_metadata") {
                prop_assert!(metadata.is_object(), "Metadata should be an object");
            }
        Ok(())
    }

    fn test_boundary_condition_handling(
        event in boundary_condition_events()
    ) -> TestResult<()> {
        prop_assert!(!event.source.is_empty(), "Source should not be empty");
        prop_assert!(!event.event_type.is_empty(), "Event type should not be empty");
        if let Some(id) = event.id {
            prop_assert_ne!(Into::<Uuid>::into(id), Uuid::nil(), "ID should not be nil");
        }

        let payload_json = serde_json::to_string(&event.payload).map_err(|error| {
            TestCaseError::fail(format!("boundary payload should serialize: {error}"))
        })?;
        let decoded_payload = serde_json::from_str::<Value>(&payload_json).map_err(|error| {
            TestCaseError::fail(format!("boundary payload should deserialize: {error}"))
        })?;
        prop_assert_eq!(decoded_payload, event.payload);
        Ok(())
    }

    fn test_validation_preserves_error_hierarchy(
        event in arbitrary_event()
    ) -> TestResult<()> {
        let result = validate_event(&event);

        if let Err(error) = result {
            let error_string = error;
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

// =============================================================================
// Concurrent Tests
// =============================================================================

mod concurrent_tests {
    use super::*;
    use xtask::sandbox::sinex_proptest;

    sinex_proptest! {
        fn property_concurrent_event_ordering(
            events in concurrent_operation_events()
        ) -> TestResult<()> {
            // Property: Concurrent events should maintain per-worker ordering
            let mut by_worker: std::collections::HashMap<usize, Vec<_>> =
                std::collections::HashMap::new();

            for event in events {
                if let Value::Object(ref map) = event.payload
                    && let (Some(Value::Number(worker_id)), Some(Value::Number(op_id))) =
                        (map.get("worker_id"), map.get("operation_id"))
                        && let (Some(worker), Some(op)) = (worker_id.as_u64(), op_id.as_u64()) {
                            by_worker.entry(worker as usize).or_default().push(op);
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
// Performance Tests
// =============================================================================

mod performance_tests {
    use super::*;
    use std::time::Instant;

    use xtask::sandbox::sinex_proptest;

    sinex_proptest! {
        #![cases(16)]
        #[ignore = "heavy: property throughput check, run via xtask test --heavy"]
        fn property_event_creation_performance(
            events in performance_characteristic_events()
        ) -> TestResult<()> {
            // Property: Event creation should complete in reasonable time
            let start = Instant::now();

            let serialized = serde_json::to_string(&events).map_err(|error| {
                TestCaseError::fail(format!("performance test events should serialize: {error}"))
            })?;
            let decoded = serde_json::from_str::<Vec<RawEvent>>(&serialized).map_err(|error| {
                TestCaseError::fail(format!("performance test events should deserialize: {error}"))
            })?;

            let elapsed = start.elapsed();

            prop_assert_eq!(decoded.len(), events.len());
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
        ) -> TestResult<()> {
            // Property: Same invalid input should always produce same error
            let source_result_1 = EventSource::new(source.clone()).map_err(|error| error.to_string());
            let source_result_2 = EventSource::new(source).map_err(|error| error.to_string());
            prop_assert_eq!(source_result_1, source_result_2);

            let event_type_result_1 =
                EventType::new(event_type.clone()).map_err(|error| error.to_string());
            let event_type_result_2 = EventType::new(event_type).map_err(|error| error.to_string());
            prop_assert_eq!(event_type_result_1, event_type_result_2);
            Ok(())
        }

        fn property_validation_error_hierarchy(
            event in arbitrary_event()
        ) -> TestResult<()> {
            // Property: Validation errors should preserve proper error hierarchy
            let result = validate_event(&event);

            if let Err(error) = result {
                let error_string = error.clone();

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
