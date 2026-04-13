//! Shared proptest strategies for sinex-primitives domain types
//!
//! This module provides reusable strategies for generating valid instances
//! of core domain types for property-based testing.

use proptest::prelude::*;
use serde_json::{Value, json};
use sinex_primitives::{EventSource, EventType, Timestamp, Uuid};
use time::Duration;

// =============================================================================
// Domain Type Strategies
// =============================================================================

/// Strategy for generating valid `EventSource` values
///
/// `EventSource` must be lowercase alphanumeric with dots and underscores,
/// starting with a letter, length 1-255.
pub fn arb_event_source() -> impl Strategy<Value = EventSource> {
    prop_oneof![
        Just(EventSource::from_static("filesystem")),
        Just(EventSource::from_static("shell.bash")),
        Just(EventSource::from_static("clipboard")),
        Just(EventSource::from_static("wm.hyprland")),
        Just(EventSource::from_static("test.source")),
        Just(EventSource::from_static("sinex.system")),
        "[a-z][a-z0-9._]{0,49}".prop_map(
            |s| EventSource::new(s).unwrap_or_else(|_| EventSource::from_static("test.source"))
        ),
    ]
}

/// Strategy for generating valid `EventType` values
///
/// `EventType` must be lowercase alphanumeric with dots and underscores,
/// starting with a letter, length 1-255.
pub fn arb_event_type() -> impl Strategy<Value = EventType> {
    prop_oneof![
        Just(EventType::from_static("file.created")),
        Just(EventType::from_static("file.modified")),
        Just(EventType::from_static("file.deleted")),
        Just(EventType::from_static("command.executed")),
        Just(EventType::from_static("window.focused")),
        Just(EventType::from_static("test.event")),
        "[a-z][a-z0-9._]{0,99}".prop_map(
            |s| EventType::new(s).unwrap_or_else(|_| EventType::from_static("test.event"))
        ),
    ]
}

/// Strategy for generating valid `UUIDv7` values
///
/// Uses the actual `UUIDv7` generator to ensure validity.
pub fn arb_uuid() -> impl Strategy<Value = Uuid> {
    // Generate UUIDv7 IDs from random timestamps within reasonable range
    // (2020-01-01 to 2030-01-01)
    (1577836800i64..1893456000i64)
        .prop_map(|ts| Uuid::new_v7(uuid::Timestamp::from_unix(uuid::NoContext, ts as u64, 0)))
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
        4,  // max depth
        64, // max nodes
        10, // max items per collection
        |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..5).prop_map(Value::from),
                prop::collection::hash_map("[a-zA-Z_][a-zA-Z0-9_]{0,20}", inner, 0..5).prop_map(
                    |map| { Value::from(map.into_iter().collect::<serde_json::Map<_, _>>()) }
                ),
            ]
        },
    )
}

/// Strategy for generating compact JSON payloads
///
/// Similar to `arb_json_payload` but with smaller depth and size,
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

/// Strategy for generating node names
///
/// Used for checkpoint and automation testing.
pub fn arb_node_name() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("command-canonicalizer".to_string()),
        Just("health-aggregator".to_string()),
        Just("pkm-automaton".to_string()),
        Just("analytics-automaton".to_string()),
        Just("content-automaton".to_string()),
        Just("search-automaton".to_string()),
        Just("test-automaton".to_string()),
        "[a-z][a-z0-9-]{4,30}".prop_map(|s| format!("{s}-automaton")),
    ]
}

/// Strategy for generating valid timestamps
///
/// Generates timestamps within a reasonable range (2020-2030).
pub fn arb_timestamp() -> impl Strategy<Value = Timestamp> {
    (1577836800i64..1893456000i64)
        .prop_map(|ts| Timestamp::from_unix_timestamp(ts).unwrap_or_else(Timestamp::now))
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
        "/[a-z0-9/._-]{1,100}\\.[a-z]{1,5}".prop_map(|s| s),
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
            "modified_time": (*modified)
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default()
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
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn test_arb_event_source_generates_valid_sources() -> TestResult<()> {
        let mut runner = TestRunner::deterministic();
        for _ in 0..100 {
            let source = arb_event_source()
                .new_tree(&mut runner)
                .map_err(|e| color_eyre::eyre::eyre!("{e}"))?
                .current();
            let s = source.as_str();
            assert!(!s.is_empty());
            assert!(s.len() <= 255);
            assert!(s.chars().next().is_some_and(|c| c.is_ascii_lowercase()));
            assert!(
                s.chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_')
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_arb_event_type_generates_valid_types() -> TestResult<()> {
        let mut runner = TestRunner::deterministic();
        for _ in 0..100 {
            let event_type = arb_event_type()
                .new_tree(&mut runner)
                .map_err(|e| color_eyre::eyre::eyre!("{e}"))?
                .current();
            let s = event_type.as_str();
            assert!(!s.is_empty());
            assert!(s.len() <= 255);
            assert!(s.chars().next().is_some_and(|c| c.is_ascii_lowercase()));
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_arb_uuid_generates_valid_uuids() -> TestResult<()> {
        let mut runner = TestRunner::deterministic();
        for _ in 0..100 {
            let uuid = arb_uuid()
                .new_tree(&mut runner)
                .map_err(|e| color_eyre::eyre::eyre!("{e}"))?
                .current();
            assert_eq!(uuid.get_version_num(), 7);
            let parsed = Uuid::parse_str(&uuid.to_string())
                .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
            assert_eq!(parsed, uuid);
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_arb_timestamp_range_has_valid_order() -> TestResult<()> {
        let mut runner = TestRunner::deterministic();
        for _ in 0..100 {
            let (start, end) = arb_timestamp_range()
                .new_tree(&mut runner)
                .map_err(|e| color_eyre::eyre::eyre!("{e}"))?
                .current();
            assert!(start < end, "Start should be before end");
            let duration = end - start;
            assert!(duration.whole_seconds() > 0, "Duration should be positive");
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_arb_json_payload_generates_valid_json() -> TestResult<()> {
        let mut runner = TestRunner::deterministic();
        for _ in 0..50 {
            let payload = arb_json_payload()
                .new_tree(&mut runner)
                .map_err(|e| color_eyre::eyre::eyre!("{e}"))?
                .current();
            // Should be serializable
            let serialized =
                serde_json::to_string(&payload).map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
            // Should be deserializable
            let deserialized: Result<Value, _> = serde_json::from_str(&serialized);
            assert!(deserialized.is_ok());
        }
        Ok(())
    }
}
