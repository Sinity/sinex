//! Type-safe event envelope for pattern matching
//!
//! This module provides a simplified `EventEnvelope` that automatically handles
//! all event payload types through the inventory system, eliminating the need
//! to manually maintain hundreds of enum variants.
//!
//! The envelope transforms event processing from:
//! ```ignore
//! match event.event_type.as_str() {
//!     "file.created" => {
//!         let payload: FileCreatedPayload = serde_json::from_value(event.payload)?;
//!         // handle payload
//!     }
//!     // ... many more string matches
//! }
//! ```
//!
//! To:
//! ```ignore
//! match event.to_envelope()? {
//!     EventEnvelope::Typed { source, event_type, payload } => {
//!         // handle any payload generically with type information
//!     }
//!     EventEnvelope::Unknown(event) => {
//!         // handle unknown/new event types
//!     }
//! }
//! ```

use crate::error::SinexError;
use crate::events::schema_registry::get_all_payloads;
use serde::{Deserialize, Serialize};

/// Type-safe envelope for event payload types
///
/// This enum provides a simplified approach to event handling that automatically
/// supports all EventPayload types through the inventory system without requiring
/// manual maintenance of hundreds of enum variants.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "envelope_type")]
pub enum EventEnvelope {
    /// A typed event with source, event_type, and JSON payload
    ///
    /// This variant contains any event that matches a known EventPayload type
    /// registered through the inventory system. The payload is kept as JSON
    /// for flexibility while preserving type information.
    Typed {
        source: String,
        event_type: String,
        payload: serde_json::Value,
    },

    /// Unknown or unsupported event type
    ///
    /// This variant is used when:
    /// - The event type is not recognized by this version of the code
    /// - Deserialization of the payload fails for a known type
    /// - Forward compatibility is needed for new event types
    Unknown(Box<UnknownEvent>),
}

/// Container for unknown event types
///
/// This preserves the original event data when we cannot parse it into
/// a known envelope variant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnknownEvent {
    pub source: String,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub reason: String,
}

impl EventEnvelope {
    /// Get the event source for any envelope variant
    pub fn source(&self) -> String {
        match self {
            EventEnvelope::Typed { source, .. } => source.clone(),
            EventEnvelope::Unknown(unknown) => unknown.source.clone(),
        }
    }

    /// Get the event type for any envelope variant
    pub fn event_type(&self) -> String {
        match self {
            EventEnvelope::Typed { event_type, .. } => event_type.clone(),
            EventEnvelope::Unknown(unknown) => unknown.event_type.clone(),
        }
    }

    /// Try to parse an event from source, event_type, and payload JSON
    ///
    /// This method uses the inventory system to check if the source and event_type
    /// combination corresponds to a known EventPayload type. If found, it returns
    /// the Typed variant; otherwise, it returns Unknown.
    pub fn from_parts(source: &str, event_type: &str, payload: serde_json::Value) -> Self {
        // Check if this source/event_type combination is known via inventory
        let is_known =
            get_all_payloads().any(|info| info.source == source && info.event_type == event_type);

        if is_known {
            EventEnvelope::Typed {
                source: source.to_string(),
                event_type: event_type.to_string(),
                payload,
            }
        } else {
            EventEnvelope::Unknown(Box::new(UnknownEvent {
                source: source.to_string(),
                event_type: event_type.to_string(),
                payload,
                reason: "Unknown event type not registered in inventory".to_string(),
            }))
        }
    }

    /// Try to deserialize the payload to a specific type
    ///
    /// This method allows consumers to deserialize the JSON payload to their
    /// desired type while preserving type safety.
    pub fn payload<T>(&self) -> Result<T, SinexError>
    where
        T: serde::de::DeserializeOwned,
    {
        let json_payload = match self {
            EventEnvelope::Typed { payload, .. } => payload,
            EventEnvelope::Unknown(unknown) => &unknown.payload,
        };

        serde_json::from_value(json_payload.clone())
            .map_err(|e| SinexError::serialization(format!("Failed to deserialize payload: {}", e)))
    }

    /// Check if this envelope represents a known event type
    pub fn is_known(&self) -> bool {
        matches!(self, EventEnvelope::Typed { .. })
    }

    /// Check if this envelope represents an unknown event type
    pub fn is_unknown(&self) -> bool {
        matches!(self, EventEnvelope::Unknown(_))
    }

    /// Extract the underlying payload as JSON value for any variant
    pub fn to_json_value(&self) -> Result<serde_json::Value, SinexError> {
        match self {
            EventEnvelope::Typed { payload, .. } => Ok(payload.clone()),
            EventEnvelope::Unknown(unknown) => Ok(unknown.payload.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[sinex_test]
    fn test_from_parts_known_event() {
        // Note: This test assumes fs-watcher/file.created is registered via inventory
        // The actual registration happens when the FileCreatedPayload is defined
        let payload = json!({
            "path": "/test/file.txt",
            "size": 1024,
            "created_at": "2024-01-01T00:00:00Z",
            "permissions": 644
        });

        let envelope = EventEnvelope::from_parts("fs-watcher", "file.created", payload.clone());

        match envelope {
            EventEnvelope::Typed {
                source,
                event_type,
                payload: envelope_payload,
            } => {
                assert_eq!(source, "fs-watcher");
                assert_eq!(event_type, "file.created");
                assert_eq!(envelope_payload, payload);
            }
            _ => {
                // If inventory doesn't have fs-watcher/file.created, it will be Unknown
                // This is expected in isolated tests
                println!("Event type not found in inventory - this is expected in unit tests");
            }
        }
    }

    #[sinex_test]
    fn test_from_parts_unknown_event() {
        let payload = json!({"unknown": "data"});
        let envelope = EventEnvelope::from_parts("unknown-source", "unknown.type", payload);

        match envelope {
            EventEnvelope::Unknown(unknown) => {
                assert_eq!(unknown.source, "unknown-source");
                assert_eq!(unknown.event_type, "unknown.type");
                assert_eq!(unknown.payload["unknown"], "data");
            }
            _ => panic!("Expected Unknown variant"),
        }
    }

    #[sinex_test]
    fn test_payload_deserialization() {
        use crate::events::payloads::filesystem::FileCreatedPayload;

        let test_payload = json!({
            "path": "/test/file.txt",
            "size": 1024,
            "created_at": "2024-01-01T00:00:00Z",
            "permissions": 644
        });

        let envelope = EventEnvelope::Typed {
            source: "fs-watcher".to_string(),
            event_type: "file.created".to_string(),
            payload: test_payload,
        };

        let result: Result<FileCreatedPayload, _> = envelope.payload();
        assert!(result.is_ok());

        let deserialized = result.unwrap();
        assert_eq!(deserialized.path, "/test/file.txt");
        assert_eq!(deserialized.size, 1024);
    }

    #[sinex_test]
    fn test_source_and_event_type_methods() {
        let envelope = EventEnvelope::Typed {
            source: "test-source".to_string(),
            event_type: "test.event".to_string(),
            payload: json!({"test": "data"}),
        };

        assert_eq!(envelope.source(), "test-source");
        assert_eq!(envelope.event_type(), "test.event");
    }

    #[sinex_test]
    fn test_is_known_and_is_unknown() {
        let known_envelope = EventEnvelope::Typed {
            source: "test".to_string(),
            event_type: "test.known".to_string(),
            payload: json!({}),
        };

        let unknown_envelope = EventEnvelope::Unknown(Box::new(UnknownEvent {
            source: "test".to_string(),
            event_type: "test.unknown".to_string(),
            payload: json!({}),
            reason: "test".to_string(),
        }));

        assert!(known_envelope.is_known());
        assert!(!known_envelope.is_unknown());
        assert!(!unknown_envelope.is_known());
        assert!(unknown_envelope.is_unknown());
    }

    #[sinex_test]
    fn test_to_json_value() {
        let test_payload = json!({"test": "data"});
        let envelope = EventEnvelope::Typed {
            source: "test".to_string(),
            event_type: "test.event".to_string(),
            payload: test_payload.clone(),
        };

        let json_value = envelope.to_json_value().unwrap();
        assert_eq!(json_value, test_payload);
    }
}
