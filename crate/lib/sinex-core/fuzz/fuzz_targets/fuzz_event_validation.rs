//! Fuzz target for event validation logic.
//!
//! Tests the EventValidator with arbitrary event-like data to ensure
//! it handles malformed inputs gracefully without panics.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use serde_json::{json, Value};

/// Arbitrary input mimicking event structure.
#[derive(Debug, Arbitrary)]
struct EventInput {
    /// Event source identifier
    source: String,
    /// Event type string
    event_type: String,
    /// Arbitrary payload keys
    payload_keys: Vec<String>,
    /// Arbitrary payload values (as strings)
    payload_values: Vec<String>,
    /// ULID-like timestamp (random bytes)
    ulid_bytes: [u8; 16],
    /// Whether to include provenance
    include_provenance: bool,
    /// Parent event ID bytes
    parent_id_bytes: Option<[u8; 16]>,
}

impl EventInput {
    /// Convert to JSON payload.
    fn to_payload(&self) -> Value {
        let mut obj = serde_json::Map::new();
        for (k, v) in self.payload_keys.iter().zip(self.payload_values.iter()) {
            obj.insert(k.clone(), Value::String(v.clone()));
        }
        Value::Object(obj)
    }

    /// Create a mock event JSON for validation.
    fn to_event_json(&self) -> Value {
        let mut event = json!({
            "id": hex::encode(&self.ulid_bytes),
            "source": self.source,
            "event_type": self.event_type,
            "payload": self.to_payload(),
        });

        if self.include_provenance {
            if let Some(parent_bytes) = &self.parent_id_bytes {
                event["provenance"] = json!({
                    "parent_id": hex::encode(parent_bytes),
                    "depth": 1
                });
            }
        }

        event
    }
}

fuzz_target!(|input: EventInput| {
    let payload = input.to_payload();

    // Test JSON validation on the payload
    let _ = sinex_core::types::validation::validate_json_value(&payload);

    // Test check_json_expansion on payload
    let _ = sinex_core::types::validation::check_json_expansion(&payload);

    // Test normalize_unicode on source and event_type
    let _ = sinex_core::types::validation::normalize_unicode(&input.source);
    let _ = sinex_core::types::validation::normalize_unicode(&input.event_type);

    // Test contains_shell_metacharacters
    let _ = sinex_core::types::validation::contains_shell_metacharacters(&input.source);
    let _ = sinex_core::types::validation::contains_shell_metacharacters(&input.event_type);

    // Validate config content patterns in source/event_type
    let _ = sinex_core::db::security::SecurityValidator::validate_config_content(&input.source);
    let _ =
        sinex_core::db::security::SecurityValidator::validate_config_content(&input.event_type);

    // Test serialization round-trip
    let event_json = input.to_event_json();
    if let Ok(serialized) = serde_json::to_string(&event_json) {
        let _ = sinex_core::types::validation::validate_json(&serialized);
    }
});
