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
