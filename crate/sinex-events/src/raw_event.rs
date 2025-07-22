//! Raw Event Types and Builders
//!
//! This module contains the core RawEvent struct, which is
//! the fundamental building blocks for all events in the Sinex system.

use serde::{Deserialize, Serialize};
use sinex_ulid::Ulid;
use crate::{event_types, sources};

// Type aliases for timestamp and JSON handling
pub type Timestamp = chrono::DateTime<chrono::Utc>;
pub type OptionalTimestamp = Option<chrono::DateTime<chrono::Utc>>;
pub type JsonValue = serde_json::Value;

/// Raw event structure
///
/// This is the canonical event structure used throughout the system for both
/// raw observations and synthesized events. The distinction is made via the
/// source_event_ids field:
/// - Raw Event: source_event_ids is None
/// - Synthesis Event: source_event_ids is Some(Vec<Ulid>)
///
/// NOTE: This struct uses ULID directly. When using with SQLX queries,
/// use type overrides like: `id::uuid as "id: _"` for proper type inference
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct RawEvent {
    pub id: Ulid,
    pub source: String,
    pub event_type: String,
    pub ts_ingest: Timestamp,
    pub ts_orig: OptionalTimestamp,
    pub host: String,
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<Ulid>,
    pub payload: JsonValue,

    /// Provenance field for event synthesis
    /// - None: This is a raw event from an ingestor
    /// - Some(Vec<Ulid>): This is a synthesis event derived from the listed events
    pub source_event_ids: Option<Vec<Ulid>>,

    /// External source material reference
    pub source_material_id: Option<Ulid>,
    pub source_material_offset_start: Option<i64>,
    pub source_material_offset_end: Option<i64>,
    /// Immutable anchor byte offset within source material
    /// Unlike source_material_offset_start, this value never changes
    pub anchor_byte: Option<i64>,

    /// Array of associated blob IDs (screenshots, recordings, etc.)
    pub associated_blob_ids: Option<Vec<Ulid>>,
}

impl RawEvent {
    /// Extract ingestion timestamp from ULID (convenience method)
    pub fn ts_ingest_from_ulid(&self) -> Timestamp {
        self.id.timestamp()
    }

    /// Check if this is a raw event (no source events)
    pub fn is_raw_event(&self) -> bool {
        self.source_event_ids.is_none()
    }

    /// Check if this is a synthesis event (has source events)
    pub fn is_synthesis_event(&self) -> bool {
        self.source_event_ids.is_some()
    }

    /// Get the source event IDs if this is a synthesis event
    pub fn get_source_event_ids(&self) -> Option<&[Ulid]> {
        self.source_event_ids.as_deref()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::EventFactory;
    use serde_json::json;

    #[test]
    fn test_event_factory_basic() {
        let payload = json!({"key": "value"});
        let factory = EventFactory::new("test-source");
        let event = factory.create_event("test.event", payload.clone());

        assert_eq!(event.source, "test-source");
        assert_eq!(event.event_type, "test.event");
        assert_eq!(event.payload, payload);
        assert!(event.ts_orig.is_none());
        assert!(event.ingestor_version.is_some());
        assert!(event.payload_schema_id.is_none());
    }

    #[test]
    fn test_event_factory_with_optional_fields() {
        let payload = json!({"data": 42});
        let test_id = Ulid::new();
        let test_schema_id = Ulid::new();
        let test_timestamp = chrono::Utc::now();

        let factory = EventFactory::new(sources::FS);
        let mut event = factory.create_event(event_types::filesystem::FILE_CREATED, payload.clone());
        event.id = test_id;
        event.host = "test-host".to_string();
        event.ingestor_version = Some("1.0.0".to_string());
        event.payload_schema_id = Some(test_schema_id);
        event.ts_orig = Some(test_timestamp);

        assert_eq!(event.id, test_id);
        assert_eq!(event.source, sources::FS);
        assert_eq!(event.event_type, event_types::filesystem::FILE_CREATED);
        assert_eq!(event.host, "test-host");
        assert_eq!(event.ingestor_version, Some("1.0.0".to_string()));
        assert_eq!(event.payload_schema_id, Some(test_schema_id));
        assert_eq!(event.ts_orig, Some(test_timestamp));
        assert_eq!(event.payload, payload);
    }

    #[test]
    fn test_event_factory_timestamp_from_ulid() {
        let test_id = Ulid::new();
        let payload = json!({"test": true});
        let factory = EventFactory::new("test");
        let mut event = factory.create_event("test.event", payload);
        event.id = test_id;

        // The ts_ingest_from_ulid should return the same timestamp as the ULID
        let ulid_timestamp = event.ts_ingest_from_ulid();
        let expected_timestamp = test_id.timestamp();
        assert_eq!(ulid_timestamp, expected_timestamp);
    }

    #[test]
    fn test_event_factory_serialization() {
        let payload = json!({"serialization": "test"});
        let factory = EventFactory::new("serialization");
        let event = factory.create_event("test.serialize", payload);

        // Test serialization
        let serialized = serde_json::to_string(&event).expect("Should serialize");
        assert!(serialized.contains("serialization"));
        assert!(serialized.contains("test.serialize"));

        // Test deserialization
        let deserialized: RawEvent = serde_json::from_str(&serialized).expect("Should deserialize");
        assert_eq!(deserialized.source, event.source);
        assert_eq!(deserialized.event_type, event.event_type);
        assert_eq!(deserialized.payload, event.payload);
    }
}
