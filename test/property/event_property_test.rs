use crate::common::prelude::*;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use serde_json::{json, Value};
use std::sync::Barrier;
use std::thread;

/// Property tests for event-related functionality
///
/// This module consolidates property tests from:
/// - raw_event_property_tests.rs (RawEvent serialization and validation)
/// - event_registry_property_tests.rs (EventRegistry thread-safety and lookups)
/// - Additional event-related property tests

// =============================================================================
// RawEvent Property Tests
// =============================================================================

/// Helper function to compare JSON values with tolerance for floating-point precision
fn json_values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(n1), Value::Number(n2)) => {
            // If both are integers, compare exactly
            if let (Some(i1), Some(i2)) = (n1.as_i64(), n2.as_i64()) {
                i1 == i2
            } else if let (Some(u1), Some(u2)) = (n1.as_u64(), n2.as_u64()) {
                u1 == u2
            } else if let (Some(f1), Some(f2)) = (n1.as_f64(), n2.as_f64()) {
                // For floats, check if they're very close (accounting for precision loss)
                // Use a more generous epsilon for JSON roundtrip precision loss
                let epsilon = 1e-10 * f1.abs().max(f2.abs()).max(1.0);
                (f1 - f2).abs() < epsilon
            } else {
                false
            }
        }
        (Value::Array(arr1), Value::Array(arr2)) => {
            arr1.len() == arr2.len()
                && arr1
                    .iter()
                    .zip(arr2.iter())
                    .all(|(a, b)| json_values_equal(a, b))
        }
        (Value::Object(obj1), Value::Object(obj2)) => {
            obj1.len() == obj2.len()
                && obj1
                    .iter()
                    .all(|(k, v)| obj2.get(k).is_some_and(|v2| json_values_equal(v, v2)))
        }
        _ => a == b,
    }
}

/// Generate arbitrary JSON values for payloads
fn arb_json_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|n| Value::Number(n.into())),
        any::<f64>()
            .prop_filter("must be finite", |f| f.is_finite())
            .prop_map(|f| json!(f)),
        "[a-zA-Z0-9_-]{1,50}".prop_map(Value::String),
    ];

    leaf.prop_recursive(
        8,   // 8 levels deep
        256, // Shoot for maximum size of 256 nodes
        10,  // Each collection is up to 10 elements
        |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..10).prop_map(Value::Array),
                prop::collection::hash_map("[a-zA-Z_][a-zA-Z0-9_-]{0,20}", inner, 0..10)
                    .prop_map(|map| Value::Object(map.into_iter().collect())),
            ]
        },
    )
}

/// Generate arbitrary valid source names
fn arb_source_name() -> impl Strategy<Value = String> {
    "[a-zA-Z][a-zA-Z0-9_.-]{2,50}"
}

/// Generate arbitrary valid event type names
fn arb_event_type_name() -> impl Strategy<Value = String> {
    prop_oneof![
        // Filesystem events
        Just("file.created".to_string()),
        Just("file.modified".to_string()),
        Just("file.deleted".to_string()),
        // Terminal events
        Just("command.executed".to_string()),
        Just("session.started".to_string()),
        // Window events
        Just("window.focused".to_string()),
        Just("window.opened".to_string()),
        Just("window.closed".to_string()),
        // Custom format
        "[a-zA-Z][a-zA-Z0-9_-]{1,30}\\.[a-zA-Z][a-zA-Z0-9_-]{1,30}"
    ]
}

/// Generate arbitrary hostnames
fn arb_hostname() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9][a-zA-Z0-9-]{1,62}(\\.[a-zA-Z0-9][a-zA-Z0-9-]{1,62}){0,3}"
}

/// Generate arbitrary version strings
fn arb_version() -> impl Strategy<Value = String> {
    "[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}(-[a-zA-Z0-9-]+)?"
}

/// Generate arbitrary timestamps within a reasonable range
fn arb_timestamp() -> impl Strategy<Value = DateTime<Utc>> {
    // Generate timestamps from 1 year ago to 1 hour in the future
    let now = Utc::now();
    let start = now - ChronoDuration::days(365);
    let end = now + ChronoDuration::hours(1);

    (start.timestamp_millis()..=end.timestamp_millis())
        .prop_map(move |ts| DateTime::from_timestamp_millis(ts).unwrap_or(now))
}

/// Strategy for generating complete RawEvent instances
fn arb_raw_event() -> impl Strategy<Value = RawEvent> {
    (
        arb_source_name(),
        arb_event_type_name(),
        arb_json_value(),
        prop::option::of(arb_timestamp()),
        prop::option::of(arb_hostname()),
        prop::option::of(arb_version()),
        prop::option::of(any::<Ulid>()),
    )
        .prop_map(
            |(source, event_type, payload, ts_orig, host, version, schema_id)| {
                let mut builder = RawEventBuilder::new(source, event_type, payload);

                if let Some(ts) = ts_orig {
                    builder = builder.with_orig_timestamp(ts);
                }
                if let Some(h) = host {
                    builder = builder.with_host(h);
                }
                if let Some(v) = version {
                    builder = builder.with_ingestor_version(v);
                }
                if let Some(s) = schema_id {
                    builder = builder.with_payload_schema_id(s);
                }

                builder.build()
            },
        )
}

#[test]
fn test_raw_event_serde_roundtrip() {
    proptest!(|(event in arb_raw_event())| {
        // Serialize to JSON
        let json_str = serde_json::to_string(&event).unwrap();

        // Deserialize back
        let deserialized: RawEvent = serde_json::from_str(&json_str).unwrap();

        // Should be identical
        prop_assert_eq!(event.id, deserialized.id);
        prop_assert_eq!(event.source, deserialized.source);
        prop_assert_eq!(event.event_type, deserialized.event_type);
        prop_assert_eq!(event.ts_ingest, deserialized.ts_ingest);
        prop_assert_eq!(event.ts_orig, deserialized.ts_orig);
        prop_assert_eq!(event.host, deserialized.host);
        prop_assert_eq!(event.ingestor_version, deserialized.ingestor_version);
        prop_assert_eq!(event.payload_schema_id, deserialized.payload_schema_id);

        // For payload, use a custom comparison that handles floating-point precision
        prop_assert!(json_values_equal(&event.payload, &deserialized.payload));
    });
}

#[test]
fn test_raw_event_id_properties() {
    proptest!(|(
        source in arb_source_name(),
        event_type in arb_event_type_name(),
        payload in arb_json_value()
    )| {
        let event1 = RawEventBuilder::new(&source, &event_type, payload.clone()).build();
        let event2 = RawEventBuilder::new(&source, &event_type, payload).build();

        // ULID IDs should be unique
        prop_assert_ne!(event1.id, event2.id);

        // ULID timestamps should be extractable and recent
        let ts1 = event1.id.timestamp();
        let ts2 = event2.id.timestamp();
        let now = Utc::now();

        prop_assert!(ts1 <= now);
        prop_assert!(ts2 <= now);
        prop_assert!(now - ts1 < ChronoDuration::seconds(10));
        prop_assert!(now - ts2 < ChronoDuration::seconds(10));

        // ts_ingest_from_ulid should match the ULID timestamp
        prop_assert_eq!(event1.ts_ingest_from_ulid(), ts1);
        prop_assert_eq!(event2.ts_ingest_from_ulid(), ts2);
    });
}

#[test]
fn test_raw_event_field_constraints() {
    proptest!(|(event in arb_raw_event())| {
        // Source should not be empty
        prop_assert!(!event.source.is_empty());
        prop_assert!(event.source.len() <= 255); // Reasonable database limit

        // Event type should not be empty
        prop_assert!(!event.event_type.is_empty());
        prop_assert!(event.event_type.len() <= 255);

        // Host should not be empty
        prop_assert!(!event.host.is_empty());

        // ts_ingest should be recent (within last hour)
        let now = Utc::now();
        prop_assert!(event.ts_ingest <= now);
        prop_assert!(now - event.ts_ingest < ChronoDuration::hours(1));

        // If ts_orig is present, it should be reasonable
        if let Some(ts_orig) = event.ts_orig {
            // Original timestamp should not be in the far future
            prop_assert!(ts_orig <= now + ChronoDuration::hours(1));
            // Original timestamp should not be too old (1 year)
            prop_assert!(ts_orig >= now - ChronoDuration::days(365));
        }

        // Payload should be valid JSON
        prop_assert!(serde_json::to_string(&event.payload).is_ok());
    });
}

#[test]
fn test_raw_event_builder_preserves_values() {
    proptest!(|(
        source in arb_source_name(),
        event_type in arb_event_type_name(),
        payload in arb_json_value(),
        ts_orig in arb_timestamp(),
        host in arb_hostname(),
        version in arb_version(),
        schema_id in any::<Ulid>()
    )| {
        let event = RawEventBuilder::new(&source, &event_type, payload.clone())
            .with_orig_timestamp(ts_orig)
            .with_host(&host)
            .with_ingestor_version(&version)
            .with_payload_schema_id(schema_id)
            .build();

        prop_assert_eq!(event.source, source);
        prop_assert_eq!(event.event_type, event_type);
        prop_assert_eq!(event.payload, payload);
        prop_assert_eq!(event.ts_orig, Some(ts_orig));
        prop_assert_eq!(event.host, host);
        prop_assert_eq!(event.ingestor_version, Some(version));
        prop_assert_eq!(event.payload_schema_id, Some(schema_id));
    });
}

#[test]
fn test_multiple_events_created_in_sequence_should_have_ordered_ulids() {
    proptest!(|(
        source in arb_source_name(),
        event_type in arb_event_type_name(),
        payloads in prop::collection::vec(arb_json_value(), 2..20)
    )| {
        let mut events = Vec::new();

        for payload in payloads {
            events.push(RawEventBuilder::new(&source, &event_type, payload).build());
            // Small delay to ensure ULID ordering
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        // ULIDs should be in ascending order
        for window in events.windows(2) {
            prop_assert!(window[0].id < window[1].id);
            prop_assert!(window[0].ts_ingest <= window[1].ts_ingest);
        }
    });
}

#[test]
fn test_raw_event_edge_case_payloads() {
    proptest!(|(
        source in arb_source_name(),
        event_type in arb_event_type_name()
    )| {
        let edge_cases = vec![
            json!(null),
            json!({}),
            json!([]),
            json!(""),
            json!(0),
            json!(false),
            json!({"nested": {"deep": {"very": {"deeply": {"nested": "value"}}}}}),
            json!((0..100).collect::<Vec<i32>>()), // Large array
            json!({"key": "x".repeat(1000)}), // Large string
        ];

        for payload in edge_cases {
            let event = RawEventBuilder::new(&source, &event_type, payload.clone()).build();

            // Should serialize and deserialize correctly
            let json_str = serde_json::to_string(&event).unwrap();
            let deserialized: RawEvent = serde_json::from_str(&json_str).unwrap();
            prop_assert_eq!(event.payload, deserialized.payload);
        }
    });
}

// =============================================================================
// EventRegistry Property Tests
// =============================================================================

/// Generate arbitrary event type names that match registry patterns
fn arb_event_type() -> impl Strategy<Value = String> {
    prop_oneof![
        // Known event types from registry
        Just("file.created".to_string()),
        Just("file.modified".to_string()),
        Just("file.deleted".to_string()),
        Just("command.executed".to_string()),
        Just("window.focused".to_string()),
        Just("window.opened".to_string()),
        Just("workspace.changed".to_string()),
        Just("monitor.focused".to_string()),
        Just("shell.history.command".to_string()),
        Just("terminal.asciinema.session_started".to_string()),
        Just("dbus.signal".to_string()),
        Just("system.notification".to_string()),
        // Unknown event types (should not be found)
        Just("unknown.event".to_string()),
        Just("nonexistent.type".to_string()),
        Just("invalid.name".to_string()),
        // Randomly generated event types
        "[a-zA-Z][a-zA-Z0-9_-]{1,20}\\.[a-zA-Z][a-zA-Z0-9_-]{1,20}"
    ]
}

/// Generate arbitrary source names
fn arb_registry_source_name() -> impl Strategy<Value = String> {
    prop_oneof![
        // Known source names from registry
        Just("fs".to_string()),
        Just("shell.kitty".to_string()),
        Just("wm.hyprland".to_string()),
        Just("shell_history".to_string()),
        Just("dbus".to_string()),
        // Unknown source names
        Just("unknown_source".to_string()),
        Just("nonexistent".to_string()),
        // Random source names
        "[a-zA-Z][a-zA-Z0-9_-]{1,30}"
    ]
}

/// Test concurrent access to EventRegistry methods
fn test_concurrent_registry_access<F>(
    num_threads: usize,
    operations_per_thread: usize,
    operation: F,
) where
    F: Fn(&EventRegistry, usize, usize) + Send + Sync + 'static,
{
    let builder = sinex_core::unified_collector::EventRegistryBuilder::new();
    let registry = Arc::new(builder.build());
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = Vec::new();

    let operation = Arc::new(operation);

    for thread_id in 0..num_threads {
        let registry = Arc::clone(&registry);
        let barrier = Arc::clone(&barrier);
        let operation = Arc::clone(&operation);

        let handle = thread::spawn(move || {
            barrier.wait();

            for op_id in 0..operations_per_thread {
                operation(&registry, thread_id, op_id);
            }
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().expect("Thread should complete successfully");
    }
}

#[test]
fn test_event_registry_concurrent_reads() {
    proptest!(|(
        num_threads in 2usize..=10,
        operations_per_thread in 10usize..=50,
        event_types in prop::collection::vec(arb_event_type(), 5..=20)
    )| {
        test_concurrent_registry_access(
            num_threads,
            operations_per_thread,
            move |registry, _thread_id, op_id| {
                let event_type = &event_types[op_id % event_types.len()];

                // These operations should be thread-safe
                let _ = registry.source_for_event(event_type);
                let _ = registry.has_event(event_type);
                let _ = registry.all_sources();
                let _ = registry.event_types.len();

                // Verify consistency across calls
                let has_event = registry.has_event(event_type);
                let source_option = registry.source_for_event(event_type);

                // If has_event is true, source_for_event should return Some
                if has_event {
                    assert!(source_option.is_some(),
                        "Event {} should have a source if it exists", event_type);
                }
            }
        );
    });
}

#[test]
fn test_event_registry_concurrent_schema_access() {
    proptest!(|(
        num_threads in 2usize..=8,
        operations_per_thread in 5usize..=20
    )| {
        let known_events = [
            "file.created",
            "file.modified",
            "command.executed",
            "window.focused",
            "unknown.event", // This should return None
        ];

        test_concurrent_registry_access(
            num_threads,
            operations_per_thread,
            move |registry, _thread_id, op_id| {
                let event_type = known_events[op_id % known_events.len()];

                // Schema access should be thread-safe
                let schema_result = registry.schema_for_event(event_type);

                // Verify consistency
                let has_event = registry.has_event(event_type);

                // Known events should have schemas, unknown should not
                if event_type == "unknown.event" {
                    assert!(!has_event);
                    assert!(schema_result.is_none());
                } else {
                    // For now, not all known events have schema generators
                    // but the call should still be thread-safe
                    let _ = schema_result;
                }
            }
        );
    });
}

#[test]
fn test_event_registry_lookup_consistency() {
    proptest!(|(
        num_threads in 3usize..=8,
        _lookups_per_thread in 20usize..=100
    )| {
        let builder = sinex_core::unified_collector::EventRegistryBuilder::new();
    let registry = Arc::new(builder.build());
        let barrier = Arc::new(Barrier::new(num_threads));
        let mut handles = Vec::new();

        // Collect results from each thread
        let results = Arc::new(std::sync::Mutex::new(Vec::new()));

        for thread_id in 0..num_threads {
            let registry = Arc::clone(&registry);
            let barrier = Arc::clone(&barrier);
            let results = Arc::clone(&results);

            let handle = thread::spawn(move || {
                barrier.wait();
                let mut thread_results = HashMap::new();

                // Test all known event types
                for &event_type in registry.event_types {
                    let source = registry.source_for_event(event_type);
                    let has_event = registry.has_event(event_type);
                    let events_for_source = if let Some(src) = source {
                        registry.events_for_source(src)
                    } else {
                        Vec::new()
                    };

                    thread_results.insert(event_type, (source, has_event, events_for_source));
                }

                results.lock().unwrap().push((thread_id, thread_results));
            });
            handles.push(handle);
        }

        // Wait for completion
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all threads got identical results
        let all_results = results.lock().unwrap();
        let first_results = &all_results[0].1;

        for (thread_id, thread_results) in all_results.iter().skip(1) {
            for event_type in first_results.keys() {
                let first_result = &first_results[event_type];
                let current_result = &thread_results[event_type];

                prop_assert_eq!(first_result.0, current_result.0,
                    "Thread {} got different source for {}", thread_id, event_type);
                prop_assert_eq!(first_result.1, current_result.1,
                    "Thread {} got different has_event for {}", thread_id, event_type);
                prop_assert_eq!(&first_result.2, &current_result.2,
                    "Thread {} got different events_for_source for {}", thread_id, event_type);
            }
        }
    });
}

#[test]
fn test_event_registry_bidirectional_mappings() {
    proptest!(|(
        source_names in prop::collection::vec(arb_registry_source_name(), 3..=10)
    )| {
        let builder = sinex_core::unified_collector::EventRegistryBuilder::new();
        let registry = builder.build();

        // Test bidirectional consistency for all known mappings
        for &event_type in registry.event_types {
            if let Some(source) = registry.source_for_event(event_type) {
                let events_for_source = registry.events_for_source(source);
                prop_assert!(events_for_source.contains(&event_type),
                    "Event {} maps to source {} but source doesn't map back to event",
                    event_type, source);
            }
        }

        // Test with unknown sources
        for source_name in &source_names {
            let events = registry.events_for_source(source_name);

            // All events returned should actually map back to this source
            for event in events {
                let mapped_source = registry.source_for_event(event).unwrap();
                prop_assert_eq!(mapped_source, source_name,
                    "Event {} returned for source {} but maps to different source {}",
                    event, source_name, mapped_source);
            }
        }
    });
}

#[test]
fn test_event_registry_edge_cases() {
    proptest!(|(
        edge_case_inputs in prop::collection::vec(".*", 0..=10)
    )| {
        let builder = sinex_core::unified_collector::EventRegistryBuilder::new();
        let registry = builder.build();

        let edge_cases = vec![
            "",
            " ",
            "  \t\n  ",
            "event.",
            ".type",
            "event..type",
            "UPPERCASE.EVENT",
            "event.type.with.many.dots",
            "event-with-dashes",
            "event_with_underscores",
            "123.numeric.start",
            "event.123",
            "special.chars!@#$",
            "very.long.event.name.that.might.cause.issues.with.storage.or.processing",
        ];

        // Combine generated and fixed edge cases
        let mut all_cases: Vec<String> = edge_cases.into_iter().map(|s| s.to_string()).collect();
        all_cases.extend(edge_case_inputs);

        for test_input in all_cases {
            // These calls should never panic, even with invalid inputs
            let source_result = registry.source_for_event(&test_input);
            let has_event_result = registry.has_event(&test_input);
            let events_for_source_result = registry.events_for_source(&test_input);

            // Results should be consistent
            if has_event_result {
                prop_assert!(source_result.is_some(),
                    "has_event returned true for {} but source_for_event returned None", test_input);
            }

            // events_for_source should always return a Vec (possibly empty)
            // This verifies the method doesn't panic on invalid input
            let _ = events_for_source_result.len();
        }
    });
}

// =============================================================================
// Stress Tests
// =============================================================================

#[cfg(test)]
mod stress_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    #[test]
    fn test_registry_high_concurrency_stress() {
        const NUM_THREADS: usize = 50;
        const OPERATIONS_PER_THREAD: usize = 1000;
        const TOTAL_OPERATIONS: usize = NUM_THREADS * OPERATIONS_PER_THREAD;

        let builder = sinex_core::unified_collector::EventRegistryBuilder::new();
        let _registry = Arc::new(builder.build());
        let operation_counter = Arc::new(AtomicUsize::new(0));
        let start_time = Instant::now();

        let counter_clone = Arc::clone(&operation_counter);
        test_concurrent_registry_access(
            NUM_THREADS,
            OPERATIONS_PER_THREAD,
            move |registry, _thread_id, op_id| {
                // Cycle through different operations
                match op_id % 5 {
                    0 => {
                        let _ = registry.source_for_event("file.created");
                    }
                    1 => {
                        let _ = registry.has_event("window.focused");
                    }
                    2 => {
                        let _ = registry.events_for_source("fs");
                    }
                    3 => {
                        let _ = registry.all_sources();
                    }
                    4 => {
                        let _ = registry.schema_for_event("command.executed");
                    }
                    _ => unreachable!(),
                }

                counter_clone.fetch_add(1, Ordering::Relaxed);
            },
        );

        let elapsed = start_time.elapsed();
        let final_count = operation_counter.load(Ordering::Relaxed);

        pretty_assertions::assert_eq!(final_count, TOTAL_OPERATIONS);
        println!(
            "Completed {} operations in {:?} ({:.2} ops/sec)",
            final_count,
            elapsed,
            final_count as f64 / elapsed.as_secs_f64()
        );
    }

    #[test]
    fn test_registry_memory_safety_under_stress() {
        const STRESS_DURATION_SECS: u64 = 2;
        const NUM_THREADS: usize = 20;

        let builder = sinex_core::unified_collector::EventRegistryBuilder::new();
        let registry = Arc::new(builder.build());
        let should_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut handles = Vec::new();

        // Start threads that continuously access the registry
        for _thread_id in 0..NUM_THREADS {
            let registry = Arc::clone(&registry);
            let should_stop = Arc::clone(&should_stop);

            let handle = thread::spawn(move || {
                let mut operation_count = 0;

                while !should_stop.load(Ordering::Relaxed) {
                    // Rapidly cycle through all registry operations
                    for &event_type in registry.event_types {
                        if should_stop.load(Ordering::Relaxed) {
                            break;
                        }

                        let _ = registry.source_for_event(event_type);
                        let _ = registry.has_event(event_type);

                        if let Some(source) = registry.source_for_event(event_type) {
                            let _ = registry.events_for_source(source);
                        }

                        operation_count += 1;
                    }
                }

                operation_count
            });
            handles.push(handle);
        }

        // Let them run for a while
        thread::sleep(Duration::from_secs(STRESS_DURATION_SECS));
        should_stop.store(true, Ordering::Relaxed);

        // Collect results
        let mut total_operations = 0;
        for handle in handles {
            total_operations += handle.join().expect("Thread should complete");
        }

        println!(
            "Memory safety stress test completed {} operations",
            total_operations
        );
        assert!(total_operations > 0);
    }
}

// =============================================================================
// Unit Tests
// =============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_raw_event_builder_defaults() {
        let event =
            RawEventBuilder::new("test_source", "test.event", json!({"key": "value"})).build();

        pretty_assertions::assert_eq!(event.source, "test_source");
        pretty_assertions::assert_eq!(event.event_type, "test.event");
        pretty_assertions::assert_eq!(event.payload, json!({"key": "value"}));
        assert!(event.ts_orig.is_none());
        assert!(!event.host.is_empty()); // Should get hostname
        assert!(event.ingestor_version.is_none());
        assert!(event.payload_schema_id.is_none());
    }

    #[test]
    fn test_raw_event_ulid_timestamp_extraction() {
        let event = RawEventBuilder::new("source", "type", json!({})).build();

        // The ULID timestamp should be close to ts_ingest
        let ulid_ts = event.ts_ingest_from_ulid();
        let diff = (event.ts_ingest - ulid_ts).abs();

        // Should be within 1 second (ULID has millisecond precision)
        assert!(diff < ChronoDuration::seconds(1));
    }

    #[test]
    fn test_json_values_equal_function() {
        // Test exact equality
        assert!(json_values_equal(&json!(42), &json!(42)));
        assert!(json_values_equal(&json!("test"), &json!("test")));
        assert!(json_values_equal(&json!(true), &json!(true)));
        assert!(json_values_equal(&json!(null), &json!(null)));

        // Test floating point tolerance
        assert!(json_values_equal(&json!(1.0), &json!(1.0000000001)));
        assert!(!json_values_equal(&json!(1.0), &json!(2.0)));

        // Test nested objects
        let obj1 = json!({"key": "value", "num": 42});
        let obj2 = json!({"key": "value", "num": 42});
        assert!(json_values_equal(&obj1, &obj2));

        // Test arrays
        let arr1 = json!([1, 2, 3]);
        let arr2 = json!([1, 2, 3]);
        assert!(json_values_equal(&arr1, &arr2));
    }

    #[test]
    fn test_arb_generators_produce_valid_values() {
        let mut runner = proptest::test_runner::TestRunner::deterministic();

        // Test source name generator
        let source = arb_source_name().new_tree(&mut runner).unwrap().current();
        assert!(!source.is_empty());
        assert!(source.len() <= 52); // 50 + 2 minimum

        // Test event type generator
        let event_type = arb_event_type_name()
            .new_tree(&mut runner)
            .unwrap()
            .current();
        assert!(!event_type.is_empty());

        // Test hostname generator
        let hostname = arb_hostname().new_tree(&mut runner).unwrap().current();
        assert!(!hostname.is_empty());

        // Test version generator
        let version = arb_version().new_tree(&mut runner).unwrap().current();
        assert!(!version.is_empty());
        assert!(version.matches('.').count() >= 2); // At least major.minor.patch

        // Test timestamp generator
        let timestamp = arb_timestamp().new_tree(&mut runner).unwrap().current();
        let now = Utc::now();
        assert!(timestamp >= now - ChronoDuration::days(366));
        assert!(timestamp <= now + ChronoDuration::hours(2));
    }
}
