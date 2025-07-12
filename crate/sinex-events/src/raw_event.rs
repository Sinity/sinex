//! Raw Event Types and Builders
//!
//! This module contains the core RawEvent struct and RawEventBuilder, which are
//! the fundamental building blocks for all events in the Sinex system.

use serde::{Deserialize, Serialize};
use sinex_ulid::Ulid;

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

/// Builder for creating RawEvent instances
pub struct RawEventBuilder {
    id: Option<Ulid>,
    source: String,
    event_type: String,
    payload: JsonValue,
    ts_orig: OptionalTimestamp,
    host: Option<String>,
    ingestor_version: Option<String>,
    payload_schema_id: Option<Ulid>,
    source_event_ids: Option<Vec<Ulid>>,
}

impl RawEventBuilder {
    pub fn new(
        source: impl Into<String>,
        event_type: impl Into<String>,
        payload: JsonValue,
    ) -> Self {
        Self {
            id: None,
            source: source.into(),
            event_type: event_type.into(),
            payload,
            ts_orig: None,
            host: None,
            ingestor_version: None,
            payload_schema_id: None,
            source_event_ids: None,
        }
    }

    pub fn with_orig_timestamp(mut self, ts: Timestamp) -> Self {
        self.ts_orig = Some(ts);
        self
    }

    /// Alias for with_orig_timestamp for compatibility
    pub fn with_timestamp(self, ts: Timestamp) -> Self {
        self.with_orig_timestamp(ts)
    }

    /// Set a specific ID for the event (useful for testing)
    pub fn with_id(mut self, id: Ulid) -> Self {
        self.id = Some(id);
        self
    }

    pub fn with_host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    pub fn with_ingestor_version(mut self, version: impl Into<String>) -> Self {
        self.ingestor_version = Some(version.into());
        self
    }

    pub fn with_payload_schema_id(mut self, id: Ulid) -> Self {
        self.payload_schema_id = Some(id);
        self
    }

    /// Mark this event as a synthesis event derived from the given events
    pub fn with_source_events(mut self, source_event_ids: Vec<Ulid>) -> Self {
        self.source_event_ids = Some(source_event_ids);
        self
    }

    /// Convenience method for synthesis events - mark as derived from a single event
    pub fn derived_from(self, source_event_id: Ulid) -> Self {
        self.with_source_events(vec![source_event_id])
    }

    pub fn build(self) -> RawEvent {
        let id = self.id.unwrap_or_default();
        let hostname = self
            .host
            .unwrap_or_else(|| gethostname::gethostname().to_string_lossy().to_string());

        RawEvent {
            id,
            source: self.source,
            event_type: self.event_type,
            ts_ingest: chrono::Utc::now(),
            ts_orig: self.ts_orig,
            host: hostname,
            ingestor_version: self.ingestor_version,
            payload_schema_id: self.payload_schema_id,
            payload: self.payload,
            source_event_ids: self.source_event_ids,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_raw_event_builder_basic() {
        let payload = json!({"key": "value"});
        let event = RawEventBuilder::new("test-source", "test.event", payload.clone()).build();

        assert_eq!(event.source, "test-source");
        assert_eq!(event.event_type, "test.event");
        assert_eq!(event.payload, payload);
        assert!(event.ts_orig.is_none());
        assert!(event.ingestor_version.is_none());
        assert!(event.payload_schema_id.is_none());
    }

    #[test]
    fn test_raw_event_builder_with_optional_fields() {
        let payload = json!({"data": 42});
        let test_id = Ulid::new();
        let test_schema_id = Ulid::new();
        let test_timestamp = chrono::Utc::now();

        let event = RawEventBuilder::new("fs", "file.created", payload.clone())
            .with_id(test_id)
            .with_host("test-host")
            .with_ingestor_version("1.0.0")
            .with_payload_schema_id(test_schema_id)
            .with_orig_timestamp(test_timestamp)
            .build();

        assert_eq!(event.id, test_id);
        assert_eq!(event.source, "fs");
        assert_eq!(event.event_type, "file.created");
        assert_eq!(event.host, "test-host");
        assert_eq!(event.ingestor_version, Some("1.0.0".to_string()));
        assert_eq!(event.payload_schema_id, Some(test_schema_id));
        assert_eq!(event.ts_orig, Some(test_timestamp));
        assert_eq!(event.payload, payload);
    }

    #[test]
    fn test_raw_event_timestamp_from_ulid() {
        let test_id = Ulid::new();
        let payload = json!({"test": true});
        let event = RawEventBuilder::new("test", "test.event", payload)
            .with_id(test_id)
            .build();

        // The ts_ingest_from_ulid should return the same timestamp as the ULID
        let ulid_timestamp = event.ts_ingest_from_ulid();
        let expected_timestamp = test_id.timestamp();
        assert_eq!(ulid_timestamp, expected_timestamp);
    }

    #[test]
    fn test_raw_event_serialization() {
        let payload = json!({"serialization": "test"});
        let event = RawEventBuilder::new("serialization", "test.serialize", payload).build();

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
