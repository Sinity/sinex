//! Property-based tests using proptest
//!
//! These tests use proptest to verify properties that should hold across
//! a wide range of inputs, providing more comprehensive testing than
//! example-based tests.

// Track 1 - Property-Based Testing Expansion (Agent Beta)
pub mod raw_event_property_tests;
pub mod ulid_concurrent_property_tests;
pub mod event_registry_property_tests;
pub mod json_schema_property_tests;
pub mod ulid_ordering_property_tests;
pub mod work_queue_property_tests;

// Agent Alpha - VM Infrastructure  
pub mod ulid_properties;

// Re-export commonly used proptest utilities
pub use proptest::prelude::*;

// Property test strategies for common Sinex types
pub mod strategies {
    use super::*;
    use chrono::{DateTime, Utc};

    /// Strategy for generating valid ULID timestamps
    pub fn valid_timestamps() -> impl Strategy<Value = DateTime<Utc>> {
        (0u64..2_000_000_000u64)  // Valid Unix timestamp range
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
        ]
    }

    /// Strategy for generating event source names
    pub fn event_sources() -> impl Strategy<Value = &'static str> {
        prop_oneof![
            Just("filesystem"),
            Just("terminal.kitty"),
            Just("hyprland"),
            Just("clipboard"),
            Just("shell_history"),
        ]
    }
}