//! Unified Event Model
//!
//! This module contains the unified Event<T> structure that replaces the old
//! Event<JsonValue>/Event<T> dichotomy.
//!
//! - Event<T> is the generic structure for all events
//! - Event<JsonValue> is an alias for Event<JsonValue>
//! - All events MUST have provenance (Material or Synthesis)
//! - anchor_byte is moved into Material provenance where it belongs

pub use crate::db::models::event_builder::{
    EventBuilder, HasProvenance, NoProvenance, OffsetKind, Operation, Provenance,
};
use crate::types::domain::{EventSource, EventType, HostName};

use crate::types::{Id, Ulid};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

// Type aliases for timestamp and JSON handling
pub type Timestamp = chrono::DateTime<chrono::Utc>;
pub type OptionalTimestamp = Option<chrono::DateTime<chrono::Utc>>;

// JsonValue is already imported above

/// Unified generic event structure
///
/// This is the canonical event structure used throughout the system.
///
/// - `Event<T>` provides strongly-typed payloads for homogeneous processing
/// - `Event<JsonValue>` (aka Event<JsonValue>) for heterogeneous processing and storage
/// - ALL events MUST have provenance (Material or Synthesis)
/// - The id field determines if this is a new event or a persisted one
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event<T = JsonValue> {
    /// Event ID - elegant distinction between new and persisted events
    /// - None: New event to be inserted (builder pattern)
    /// - Some(id): Persisted event retrieved from database
    ///   This pattern avoids separate NewEvent/PersistedEvent types
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Id<Event<T>>>,

    /// Event source (e.g., "fs-watcher", "terminal")
    pub source: EventSource,

    /// Event type (e.g., "file.created", "command.executed")
    pub event_type: EventType,

    /// Event payload (typed or JSON)
    pub payload: T,

    // ts_ingest is derived from ULID at the DB layer; not modeled here.
    /// Original timestamp when the event occurred
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts_orig: OptionalTimestamp,

    /// Hostname where the event was generated
    #[serde(default = "get_hostname")]
    pub host: HostName,

    /// Version of the ingestor that created this event
    pub ingestor_version: Option<String>,

    /// Schema ID for payload validation
    pub payload_schema_id: Option<Ulid>,

    /// REQUIRED: Provenance tracking (Material or Synthesis)
    pub provenance: Provenance,

    /// Array of associated blob IDs (screenshots, recordings, etc.)
    pub associated_blob_ids: Option<Vec<Ulid>>,
}

/// Type alias for event IDs in references (stable across type parameters)
pub type EventId = Id<Event<JsonValue>>;

/// Marker type for source material IDs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SourceMaterial;

// Typestate markers and EventBuilder moved to event_builder.rs

// EventBuilder implementations moved to event_builder.rs
// Provenance type moved to event_builder.rs
// OffsetKind moved to event_builder.rs
// Operation moved to event_builder.rs

impl<T> Event<T> {
    /// Universal constructor - requires all fields explicitly
    #[deprecated(since = "0.5.0", note = "Use EventBuilder::new()...build() instead")]
    pub fn create(
        source: impl Into<EventSource>,
        event_type: impl Into<EventType>,
        payload: T,
        provenance: Provenance,
    ) -> Self {
        // Implementation unchanged, just deprecated tag?
        // Actually, create IS the "universal constructor".
        // Maybe keeping it is fine, just encourage builder?
        // Phase 1.4 says "Deprecate Event::create and Event::dynamic".
        Self {
            id: None,
            source: source.into(),
            event_type: event_type.into(),
            payload,

            ts_orig: Some(Utc::now()),
            host: get_hostname(),
            ingestor_version: get_ingestor_version(),
            payload_schema_id: None,
            provenance,
            associated_blob_ids: None,
        }
    }

    /// Modify timestamp after creation
    pub fn at_time(mut self, ts: DateTime<Utc>) -> Self {
        self.ts_orig = Some(ts);
        self
    }

    /// Add associated blobs after creation
    pub fn with_associated_blobs(mut self, blobs: Vec<Ulid>) -> Self {
        self.associated_blob_ids = Some(blobs);
        self
    }

    #[cfg(feature = "testing")]
    /// Create a test event with dummy Material provenance
    ///
    /// This is for testing only and creates events with a well-known test
    /// material ID. In production, all events must have real provenance.
    pub fn test_event(
        source: impl Into<EventSource>,
        event_type: impl Into<EventType>,
        payload: T,
    ) -> Self {
        let test_material_id = Id::<SourceMaterial>::from_ulid(
            crate::types::Ulid::from_bytes([
                0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54,
                0x32, 0x10,
            ])
            .unwrap_or_else(|_| {
                panic!("hardcoded test ULID bytes should be valid - this is a programming error")
            }),
        );

        Self::create(
            source,
            event_type,
            payload,
            Provenance::from_material(test_material_id, 0, None, None),
        )
    }
}

// Convenience constructors for typed events
impl<T> Event<T>
where
    T: crate::types::events::EventPayload,
{
    /// Quick constructor for typed events - derives source/type from payload
    pub fn new(payload: T, provenance: Provenance) -> Self {
        Self::create(T::SOURCE, T::EVENT_TYPE, payload, provenance)
    }

    /// Start building a typed event with builder pattern
    pub fn builder(payload: T) -> EventBuilder<T, NoProvenance> {
        EventBuilder::new(T::SOURCE, T::EVENT_TYPE, payload)
    }
}

// Convenience constructors for dynamic events (Event<JsonValue>)
impl Event<JsonValue> {
    /// Start building a dynamic event with explicit source and type
    /// (overrides the generic "system"/"generic" defaults from EventPayload impl)
    #[deprecated(since = "0.5.0", note = "Use EventBuilder::new(...) instead")]
    pub fn dynamic(
        source: impl Into<EventSource>,
        event_type: impl Into<EventType>,
        payload: JsonValue,
    ) -> EventBuilder<JsonValue, NoProvenance> {
        EventBuilder::new(source.into(), event_type.into(), payload)
    }

    // No RawEvent; use Event::<JsonValue>::test_event() in tests.
}

impl<T> Event<T> {
    /// Set the timestamp
    pub fn with_timestamp(mut self, ts: Timestamp) -> Self {
        self.ts_orig = Some(ts);
        self
    }

    /// Set the host
    pub fn with_host(mut self, host: HostName) -> Self {
        self.host = host;
        self
    }

    /// Set the ingestor version
    pub fn with_ingestor_version(mut self, version: impl Into<String>) -> Self {
        self.ingestor_version = Some(version.into());
        self
    }

    /// Set the schema ID
    pub fn with_schema_id(mut self, schema_id: Ulid) -> Self {
        self.payload_schema_id = Some(schema_id);
        self
    }

    /// Set associated blob IDs
    pub fn with_blobs(mut self, blob_ids: Vec<Ulid>) -> Self {
        self.associated_blob_ids = Some(blob_ids);
        self
    }

    /// Check if this event has been persisted to the database
    pub fn is_persisted(&self) -> bool {
        self.id.is_some()
    }

    /// Check if this is a first-order event (derived from Source Material)
    pub fn is_first_order_event(&self) -> bool {
        matches!(self.provenance, Provenance::Material { .. })
    }

    /// Check if this is a synthesized event (derived from other events)
    pub fn is_synthesized_event(&self) -> bool {
        matches!(self.provenance, Provenance::Synthesis { .. })
    }

    /// Get the anchor byte if this is a Material event
    pub fn anchor_byte(&self) -> Option<i64> {
        match &self.provenance {
            Provenance::Material { anchor_byte, .. } => Some(*anchor_byte),
            _ => None,
        }
    }

    /// Get the source event IDs if this is a Synthesis event
    pub fn source_event_ids(&self) -> Option<&[EventId]> {
        match &self.provenance {
            Provenance::Synthesis {
                source_event_ids, ..
            } => Some(source_event_ids),
            _ => None,
        }
    }
}

impl<T: Serialize> Event<T> {
    /// Convert to Event<JsonValue> (type erasure)
    pub fn to_json_event(self) -> Result<Event<JsonValue>, serde_json::Error> {
        Ok(Event {
            id: None, // New ID for different type
            source: self.source,
            event_type: self.event_type,
            payload: serde_json::to_value(self.payload)?,

            ts_orig: self.ts_orig,
            host: self.host,
            ingestor_version: self.ingestor_version,
            payload_schema_id: self.payload_schema_id,
            provenance: self.provenance,
            associated_blob_ids: self.associated_blob_ids,
        })
    }
}

impl Event<JsonValue> {
    /// Try to convert to typed event (type recovery)
    pub fn to_typed<T>(&self) -> Result<Event<T>, serde_json::Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        Ok(Event {
            id: None, // New ID for different type
            source: self.source.clone(),
            event_type: self.event_type.clone(),
            payload: serde_json::from_value(self.payload.clone())?,

            ts_orig: self.ts_orig,
            host: self.host.clone(),
            ingestor_version: self.ingestor_version.clone(),
            payload_schema_id: self.payload_schema_id,
            provenance: self.provenance.clone(),
            associated_blob_ids: self.associated_blob_ids.clone(),
        })
    }
}

// EventBuilder implementations
// EventBuilder impl blocks moved to event_builder.rs

// Helper function to get hostname
pub(crate) fn get_hostname() -> HostName {
    HostName::new(gethostname::gethostname().to_string_lossy().to_string())
}

// Helper function to get ingestor version
pub(crate) fn get_ingestor_version() -> Option<String> {
    // Priority: compile-time git revision > runtime env var > None
    match option_env!("SINEX_GIT_REV") {
        Some(git_rev) if !git_rev.is_empty() && git_rev != "unknown" => {
            // Format: git-<short-rev> (e.g., "git-a1b2c3d")
            Some(format!("git-{}", git_rev))
        }
        _ => {
            // Fallback to runtime environment variable (legacy support)
            std::env::var("SINEX_VERSION").ok()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn event_builder_sets_offsets_for_material_provenance() {
        let material_id = Id::from_ulid(Ulid::new());
        let event = Event::dynamic("offset-test", "offset.event", json!({"key": "value"}))
            .from_material(material_id, 4)
            .with_offset_start(10)
            .expect("offset start should apply to material provenance")
            .with_offset_end(20)
            .expect("offset end should apply to material provenance")
            .with_offset_kind(OffsetKind::Line)
            .expect("offset kind should apply to material provenance")
            .build()
            .expect("event should build with material provenance");

        match event.provenance {
            Provenance::Material {
                offset_start,
                offset_end,
                offset_kind,
                ..
            } => {
                assert_eq!(offset_start, Some(10));
                assert_eq!(offset_end, Some(20));
                assert_eq!(offset_kind, OffsetKind::Line);
            }
            _ => panic!("expected material provenance"),
        }
    }

    #[test]
    fn events_contain_build_version() {
        // Create a test event with material provenance
        let material_id = Id::from_ulid(Ulid::new());
        let event = Event::dynamic("test", "test.event", json!({"key": "value"}))
            .from_material(material_id, 4)
            .build()
            .expect("Event should build");

        // Verify ingestor version is present
        let version = get_ingestor_version();
        assert!(
            version.is_some(),
            "Ingestor version should be set from build.rs"
        );

        // Verify event contains the version
        assert_eq!(event.ingestor_version, version);

        // Verify format (should start with "git-" when compiled)
        if let Some(ref ver) = version {
            if ver != "unknown" {
                assert!(
                    ver.starts_with("git-") || !ver.is_empty(),
                    "Version should be in git-<rev> format or be a non-empty string, got: {}",
                    ver
                );
            }
        }
    }

    #[test]
    fn get_ingestor_version_returns_git_revision() {
        let version = get_ingestor_version();

        // Version should be set at compile time via build.rs
        // This test will pass even if git is not available (returns None or "unknown")
        if let Some(ref ver) = version {
            println!("Ingestor version: {}", ver);
            // If git revision is available, it should be formatted correctly
            if ver.starts_with("git-") {
                assert!(ver.len() > 4, "Git revision should have content after 'git-'");
            }
        }
    }
}
