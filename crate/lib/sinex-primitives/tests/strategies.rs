//! Shared proptest strategies for sinex-primitives domain types
//!
//! This module provides reusable strategies for generating valid instances
//! of core domain types for property-based testing.

use time::{Duration, OffsetDateTime};
use proptest::prelude::*;
use serde_json::{json, Value};
use sinex_primitives::{EventSource, EventType, Timestamp, Ulid};

// =============================================================================
// Domain Type Strategies
// =============================================================================

/// Strategy for generating valid EventSource values
///
/// EventSource must be lowercase alphanumeric with dots and underscores,
/// starting with a letter, length 1-255.
pub fn arb_event_source() -> impl Strategy<Value = EventSource> {
    prop_oneof![
        Just(EventSource::new("filesystem")),
        Just(EventSource::new("shell.bash")),
        Just(EventSource::new("clipboard")),
        Just(EventSource::new("wm.hyprland")),
        Just(EventSource::new("test.source")),
        Just(EventSource::new("sinex.system")),
        "[a-z][a-z0-9._]{0,49}".prop_map(EventSource::new),
    ]
}

/// Strategy for generating valid EventType values
///
/// EventType must be lowercase alphanumeric with dots and underscores,
/// starting with a letter, length 1-255.
pub fn arb_event_type() -> impl Strategy<Value = EventType> {
    prop_oneof![
        Just(EventType::new("file.created")),
        Just(EventType::new("file.modified")),
        Just(EventType::new("file.deleted")),
        Just(EventType::new("command.executed")),
        Just(EventType::new("window.focused")),
        Just(EventType::new("test.event")),
        "[a-z][a-z0-9._]{0,99}".prop_map(EventType::new),
    ]
}

/// Strategy for generating valid ULID values
///
/// Uses the actual ULID generator to ensure validity.
pub fn arb_ulid() -> impl Strategy<Value = Ulid> {
    // Generate ULIDs from random timestamps within reasonable range
    // (2020-01-01 to 2030-01-01)
    (1577836800i64..1893456000i64).prop_map(|ts| {
        let dt = OffsetDateTime::from_unix_timestamp(ts).unwrap_or_else(|_| OffsetDateTime::now_utc());
        Ulid::from_datetime(dt.into())
    })
}

/// Strategy for generating valid JSON payloads
///
/// Generates diverse JSON structures suitable for event payloads,
/// including edge cases like deep nesting, Unicode, special values.
pub fn arb_json_payload() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        (0.0f64..1000.0f64).prop_map(Value::from),
        "[a-zA-Z0-9_./: -]{0,100}".prop_map(Value::from),
        // Edge cases
        Just(Value::from(i64::MIN)),
        Just(Value::from(i64::MAX)),
        Just(Value::from(0)),
        Just(Value::from("")),
    ];

    leaf.prop_recursive(
        4,   // max depth
        64,  // max nodes
        10,  // max items per collection
        |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..5).prop_map(Value::from),
                prop::collection::hash_map("[a-zA-Z_][a-zA-Z0-9_]{0,20}", inner, 0..5)
                    .prop_map(|map| {
                        Value::from(map.into_iter().collect::<serde_json::Map<_, _>>())
                    }),
            ]
        },
    )
}

/// Strategy for generating compact JSON payloads
///
/// Similar to arb_json_payload but with smaller depth and size,
/// suitable for tests that need many instances.
pub fn arb_json_payload_compact() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(json!(null)),
        Just(json!({})),
        Just(json!({"key": "value"})),
        Just(json!({"count": 42})),
        Just(json!({"path": "/tmp/file.txt"})),
        any::<u32>().prop_map(|n| json!({"id": n})),
    ]
}

/// Strategy for generating processor names
///
/// Used for checkpoint and automation testing.
pub fn arb_processor_name() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("command-canonicalizer".to_string()),
        Just("health-aggregator".to_string()),
        Just("pkm-automaton".to_string()),
        Just("analytics-automaton".to_string()),
        Just("content-automaton".to_string()),
        Just("search-automaton".to_string()),
        Just("test-automaton".to_string()),
        "[a-z][a-z0-9-]{4,30}".prop_map(|s| format!("{}-automaton", s)),
    ]
}

/// Strategy for generating valid timestamps
///
/// Generates timestamps within a reasonable range (2020-2030).
pub fn arb_timestamp() -> impl Strategy<Value = Timestamp> {
    (1577836800i64..1893456000i64).prop_map(|ts| {
        let dt = OffsetDateTime::from_unix_timestamp(ts).unwrap_or_else(|_| OffsetDateTime::now_utc());
        Timestamp::new(dt)
    })
}

/// Strategy for generating timestamp ranges
///
/// Ensures start < end with reasonable duration between them.
pub fn arb_timestamp_range() -> impl Strategy<Value = (Timestamp, Timestamp)> {
    (arb_timestamp(), 1i64..86400i64).prop_map(|(start, duration_secs)| {
        let start_dt = start.inner();
        let end_dt = start_dt + Duration::seconds(duration_secs);
        (start, Timestamp::new(end_dt))
    })
}

/// Strategy for generating file paths
///
/// Generates diverse file paths including edge cases.
pub fn arb_file_path() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("/tmp/test.txt".to_string()),
        Just("/home/user/document.pdf".to_string()),
        Just("/var/log/system.log".to_string()),
        Just("/.hidden".to_string()),
        Just("/tmp/file with spaces.txt".to_string()),
        "/[a-z0-9/._-]{1,100}\\.[a-z]{1,5}".prop_map(|s| s.to_string()),
    ]
}

// =============================================================================
// Event-specific Strategies
// =============================================================================

/// Strategy for generating filesystem event payloads
pub fn arb_filesystem_event_payload() -> impl Strategy<Value = Value> {
    (arb_file_path(), any::<u64>(), arb_timestamp()).prop_map(|(path, size, modified)| {
        json!({
            "path": path,
            "size": size,
            "modified_time": modified.format(&time::format_description::well_known::Rfc3339).expect("RFC3339 format")
        })
    })
}

/// Strategy for generating shell command payloads
pub fn arb_shell_command_payload() -> impl Strategy<Value = Value> {
    (
        "[a-z]{2,10}",
        prop::collection::vec("[a-z0-9-]{1,20}", 0..5),
        any::<i32>(),
    )
        .prop_map(|(command, args, exit_code)| {
            json!({
                "command": command,
                "args": args,
                "exit_code": exit_code
            })
        })
}

/// Strategy for generating window event payloads
pub fn arb_window_event_payload() -> impl Strategy<Value = Value> {
    ("[a-zA-Z0-9 ]{1,50}", "[a-z]{3,20}").prop_map(|(title, app)| {
        json!({
            "window_title": title,
            "app": app
        })
    })
}

// =============================================================================
// Checkpoint Strategies
// =============================================================================

/// Strategy for generating checkpoint data
pub fn arb_checkpoint_data() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(json!(null)),
        Just(json!({"cursor": "12345"})),
        Just(json!({"processed": 100, "skipped": 5})),
        Just(json!({"state": "active"})),
        any::<u64>().prop_map(|n| json!({"offset": n})),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::test_runner::TestRunner;
    use proptest::strategy::ValueTree;

    #[test]
    fn test_arb_event_source_generates_valid_sources() {
        let mut runner = TestRunner::deterministic();
        for _ in 0..100 {
            let source = arb_event_source()
                .new_tree(&mut runner)
                .unwrap()
                .current();
            let s = source.as_str();
            assert!(!s.is_empty());
            assert!(s.len() <= 255);
            assert!(s.chars().next().unwrap().is_ascii_lowercase());
            assert!(s
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_'));
        }
    }

    #[test]
    fn test_arb_event_type_generates_valid_types() {
        let mut runner = TestRunner::deterministic();
        for _ in 0..100 {
            let event_type = arb_event_type()
                .new_tree(&mut runner)
                .unwrap()
                .current();
            let s = event_type.as_str();
            assert!(!s.is_empty());
            assert!(s.len() <= 255);
            assert!(s.chars().next().unwrap().is_ascii_lowercase());
        }
    }

    #[test]
    fn test_arb_ulid_generates_valid_ulids() {
        let mut runner = TestRunner::deterministic();
        for _ in 0..100 {
            let ulid = arb_ulid().new_tree(&mut runner).unwrap().current();
            let s = ulid.to_string();
            assert_eq!(s.len(), 26);
            assert!(s
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()));
        }
    }

    #[test]
    fn test_arb_timestamp_range_has_valid_order() {
        let mut runner = TestRunner::deterministic();
        for _ in 0..100 {
            let (start, end) = arb_timestamp_range()
                .new_tree(&mut runner)
                .unwrap()
                .current();
            assert!(start < end, "Start should be before end");
            let duration = end - start;
            assert!(
                duration.whole_seconds() > 0,
                "Duration should be positive"
            );
        }
    }

    #[test]
    fn test_arb_json_payload_generates_valid_json() {
        let mut runner = TestRunner::deterministic();
        for _ in 0..50 {
            let payload = arb_json_payload()
                .new_tree(&mut runner)
                .unwrap()
                .current();
            // Should be serializable
            let serialized = serde_json::to_string(&payload);
            assert!(serialized.is_ok());
            // Should be deserializable
            let deserialized: Result<Value, _> = serde_json::from_str(&serialized.unwrap());
            assert!(deserialized.is_ok());
        }
    }
}
