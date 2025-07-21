// Property-based tests using proptest
//
// These tests use proptest to verify properties that should hold across
// a wide range of inputs, providing more comprehensive testing than
// example-based tests.

use crate::common::prelude::*;

// Consolidated property tests
// Disabled - references obsolete sinex_collector module
// pub mod event_model_fuzzing_test;
// Disabled - references obsolete EventRegistry
// pub mod event_property_test;
// Disabled - references missing ConnectionManager
// pub mod redis_streams_property_test;
pub mod automation_property_test;
pub mod checkpoint_property_test;
pub mod satellite_property_test;
pub mod schema_property_test;
pub mod ulid_property_test;
// Disabled - file not yet created
// pub mod example_property_builders_test;

// Re-export commonly used proptest utilities
pub use proptest::prelude::*;

// Property test strategies for common Sinex types
#[allow(dead_code)]
pub mod strategies {
    use super::*;
    
    use chrono::{DateTime, Utc};

    /// Strategy for generating valid ULID timestamps
    pub fn valid_timestamps() -> impl Strategy<Value = DateTime<Utc>> {
        (0u64..2_000_000_000u64) // Valid Unix timestamp range
            .prop_map(|ts| DateTime::from_timestamp(ts as i64, 0).unwrap_or(Utc::now()))
    }

    /// Strategy for generating realistic event payloads
    pub fn event_payloads() -> impl Strategy<Value = serde_json::Value> {
        prop_oneof![
            // Small payload
            Just(serde_json::json!({"type": "simple", "data": "test"})),
            // Medium payload
            Just(serde_json::json!({
                "type": "medium",
                "data": vec![1, 2, 3, 4, 5],
                "metadata": {"created": "2024-01-01"}
            })),
            // Large payload
            Just(serde_json::json!({
                "type": "large",
                "content": "x".repeat(1000),
                "fields": (0..20).map(|i| (format!("field_{}", i), i)).collect::<std::collections::HashMap<_, _>>()
            })),
            // Deeply nested payload
            create_nested_payload_strategy(5),
            // Array payload
            Just(serde_json::json!({
                "type": "array",
                "items": (0..100).collect::<Vec<_>>()
            })),
            // Unicode payload
            Just(serde_json::json!({
                "type": "unicode",
                "content": "🦀 Rust 中文 العربية 🚀"
            })),
            // Mixed types payload
            Just(serde_json::json!({
                "string": "test",
                "number": 42,
                "boolean": true,
                "null": null,
                "array": [1, "two", 3.0, true],
                "object": {"nested": "value"}
            }))
        ]
    }

    /// Strategy for generating realistic file paths
    pub fn file_paths() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("/home/user/document.txt".to_string()),
            Just("/tmp/cache/file.json".to_string()),
            Just("/var/log/system.log".to_string()),
            Just("/home/user/code/project/src/main.rs".to_string()),
            Just("/home/user/.config/app/settings.toml".to_string()),
            // Edge case paths
            Just("/".to_string()),
            Just("/tmp".to_string()),
            Just("/home/user/.hidden_file".to_string()),
            Just("/home/user/very/deep/nested/path/file.txt".to_string()),
            // Problematic paths
            Just("/home/user/file with spaces.txt".to_string()),
            Just("/home/user/file-with-dashes.txt".to_string()),
            Just("/home/user/file_with_underscores.txt".to_string()),
            Just("/home/user/file.multiple.dots.txt".to_string()),
        ]
    }

    /// Strategy for generating event source names
    pub fn event_sources() -> impl Strategy<Value = &'static str> {
        prop_oneof![
            Just("fs"),
            Just("shell.kitty"),
            Just("wm.hyprland"),
            Just("clipboard"),
            Just("shell_history"),
            Just("dbus"),
            Just("journal"),
            Just("sinex"),
            Just("terminal"),
            Just("desktop"),
            Just("system"),
        ]
    }

    /// Strategy for generating event type names
    pub fn event_types() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("file.created".to_string()),
            Just("file.modified".to_string()),
            Just("file.deleted".to_string()),
            Just("command.executed".to_string()),
            Just("window.opened".to_string()),
            Just("window.closed".to_string()),
            Just("content.copied".to_string()),
            Just("session.started".to_string()),
            Just("automaton.heartbeat".to_string()),
            // Edge case types
            Just("test.event".to_string()),
            Just("boundary.test".to_string()),
            Just("performance.test".to_string()),
        ]
    }

    /// Strategy for generating valid ULIDs
    pub fn ulids() -> impl Strategy<Value = sinex_ulid::Ulid> {
        any::<[u8; 16]>().prop_map(|bytes| {
            sinex_ulid::Ulid::from_bytes(bytes).unwrap_or_else(|_| sinex_ulid::Ulid::new())
        })
    }

    /// Strategy for generating Redis stream keys
    pub fn redis_stream_keys() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("sinex:events".to_string()),
            Just("test:events".to_string()),
            Just("automaton:command-canonicalizer".to_string()),
            Just("automaton:health-aggregator".to_string()),
            Just("api:command:analytics".to_string()),
            Just("api:response:analytics".to_string()),
        ]
    }

    /// Strategy for generating consumer group names
    pub fn consumer_groups() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("command-canonicalizer-group".to_string()),
            Just("health-aggregator-group".to_string()),
            Just("test-group".to_string()),
            Just("analytics-group".to_string()),
            Just("pkm-group".to_string()),
        ]
    }

    /// Strategy for generating consumer names
    pub fn consumer_names() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("consumer-1".to_string()),
            Just("consumer-2".to_string()),
            Just("test-consumer".to_string()),
            Just("primary-consumer".to_string()),
            Just("backup-consumer".to_string()),
        ]
    }

    /// Strategy for generating checkpoint data
    pub fn checkpoint_data() -> impl Strategy<Value = serde_json::Value> {
        prop_oneof![
            Just(serde_json::json!(null)),
            Just(serde_json::json!({"cursor": "12345"})),
            Just(serde_json::json!({"processed": 100, "skipped": 5})),
            Just(serde_json::json!({"state": "active", "last_seen": "2024-01-01T00:00:00Z"})),
        ]
    }

    /// Strategy for generating automaton names
    pub fn automaton_names() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("command-canonicalizer".to_string()),
            Just("health-aggregator".to_string()),
            Just("pkm-automaton".to_string()),
            Just("analytics-automaton".to_string()),
            Just("content-automaton".to_string()),
            Just("search-automaton".to_string()),
            Just("test-automaton".to_string()),
        ]
    }

    /// Strategy for generating payload sizes for boundary testing
    pub fn payload_sizes() -> impl Strategy<Value = usize> {
        prop_oneof![
            Just(0),                // Empty
            Just(1),                // Minimal
            Just(1024),             // 1KB
            Just(64 * 1024),        // 64KB
            Just(1024 * 1024),      // 1MB
            Just(10 * 1024 * 1024), // 10MB (large)
        ]
    }

    /// Strategy for generating batch sizes
    pub fn batch_sizes() -> impl Strategy<Value = usize> {
        prop_oneof![Just(1), Just(10), Just(100), Just(1000), Just(10000),]
    }

    /// Strategy for generating time intervals
    pub fn time_intervals() -> impl Strategy<Value = chrono::Duration> {
        prop_oneof![
            Just(chrono::Duration::milliseconds(1)),
            Just(chrono::Duration::milliseconds(100)),
            Just(chrono::Duration::seconds(1)),
            Just(chrono::Duration::seconds(10)),
            Just(chrono::Duration::minutes(1)),
            Just(chrono::Duration::hours(1)),
        ]
    }

    /// Create nested payload strategy
    fn create_nested_payload_strategy(depth: usize) -> BoxedStrategy<serde_json::Value> {
        if depth == 0 {
            any::<String>().prop_map(|s| serde_json::json!(s)).boxed()
        } else {
            any::<String>().prop_map(move |s| {
                let mut obj = serde_json::Map::new();
                obj.insert("level".to_string(), serde_json::json!(depth));
                obj.insert("data".to_string(), serde_json::json!(s));
                if depth > 1 {
                    obj.insert(
                        "nested".to_string(),
                        serde_json::json!({"level": depth - 1}),
                    );
                }
                serde_json::Value::Object(obj)
            }).boxed()
        }
    }

    /// Strategy for generating malformed/adversarial payloads
    pub fn adversarial_payloads() -> impl Strategy<Value = serde_json::Value> {
        prop_oneof![
            // Empty objects and arrays
            Just(serde_json::json!({})),
            Just(serde_json::json!([])),
            // Null values
            Just(serde_json::json!(null)),
            // Very large strings
            Just(serde_json::json!({"large": "x".repeat(1000000)})),
            // Deeply nested structures
            create_deeply_nested_json(50),
            // Unicode edge cases
            Just(serde_json::json!({"unicode": "\u{0000}\u{FEFF}\u{FFFF}"})),
            // Mixed numeric types
            Just(serde_json::json!({"numbers": [i64::MAX, i64::MIN, 0, -1, 1]})),
            // Special float values
            Just(serde_json::json!({"floats": [0.0, -0.0, 1.0, -1.0]})),
        ]
    }

    /// Create deeply nested JSON for testing
    fn create_deeply_nested_json(depth: usize) -> BoxedStrategy<serde_json::Value> {
        if depth == 0 {
            any::<String>().prop_map(|s| serde_json::json!(s)).boxed()
        } else {
            any::<String>().prop_map(move |s| {
                let mut current = serde_json::json!(s);
                for level in 0..depth {
                    current = serde_json::json!({
                        "level": level,
                        "nested": current
                    });
                }
                current
            }).boxed()
        }
    }

    /// Strategy for generating realistic event sequences
    pub fn event_sequences() -> impl Strategy<Value = Vec<RawEvent>> {
        (1usize..=100).prop_flat_map(|size| {
            proptest::collection::vec(
                (event_sources(), event_types(), event_payloads()).prop_map(
                    |(source, event_type, payload)| {
                        crate::common::events::create_raw_event(
                            source,
                            &event_type,
                            payload,
                            chrono::Utc::now(),
                        )
                    },
                ),
                size,
            )
        })
    }

    /// Strategy for generating concurrent operations
    pub fn concurrent_operations() -> impl Strategy<Value = Vec<String>> {
        proptest::collection::vec(
            prop_oneof![
                Just("insert_event".to_string()),
                Just("query_events".to_string()),
                Just("update_checkpoint".to_string()),
                Just("publish_stream".to_string()),
                Just("consume_stream".to_string()),
            ],
            1..=20,
        )
    }
}
