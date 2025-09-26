//! Unified Event Model
//!
//! This module contains the unified Event<T> structure that replaces the old
//! Event<JsonValue>/Event<T> dichotomy.
//!
//! - Event<T> is the generic structure for all events
//! - Event<JsonValue> is an alias for Event<JsonValue>
//! - All events MUST have provenance (Material or Synthesis)
//! - anchor_byte is moved into Material provenance where it belongs

use crate::types::domain::{EventSource, EventType, HostName};
use crate::types::non_empty::NonEmptyVec;
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
    /// TODO: Consider removing - might be redundant for local-only capture
    /// Could move to payload for specific event types that need it
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

// Typestate markers for EventBuilder
pub struct NoProvenance;
pub struct HasProvenance;

/// Event builder with typestate pattern for compile-time safety
pub struct EventBuilder<T, P = NoProvenance> {
    payload: T,
    source: EventSource,
    event_type: EventType,
    provenance: Option<Provenance>,
    ts_orig: Option<Timestamp>,
    associated_blob_ids: Option<Vec<Ulid>>,
    _phantom: std::marker::PhantomData<P>,
}

/// Provenance type for tracking event lineage
///
/// This enum enforces the XOR constraint: every event must have exactly one type of provenance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Provenance {
    /// Event derived from source material (first-order event)
    Material {
        id: Id<SourceMaterial>,
        anchor_byte: i64, // MOVED HERE: where it semantically belongs!
        offset_start: Option<i64>,
        offset_end: Option<i64>,
        offset_kind: OffsetKind,
    },
    /// Event derived from other events (synthesized event)  
    Synthesis {
        source_event_ids: NonEmptyVec<EventId>, // Enforces non-empty at type level!
        operation_id: Option<Id<Operation>>,
    },
}

/// Type of offset measurement
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OffsetKind {
    Byte,
    Line,
    Record,
    Character,
}

impl Default for OffsetKind {
    fn default() -> Self {
        Self::Byte
    }
}

/// Marker type for operation IDs  
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Operation;

impl Provenance {
    /// Create material provenance from source material
    ///
    /// anchor_byte is REQUIRED - this ensures Material events always have valid anchor
    pub fn from_material(
        id: impl Into<Id<SourceMaterial>>,
        anchor_byte: i64, // Non-optional - enforces invariant at type level
        offset_start: Option<i64>,
        offset_end: Option<i64>,
    ) -> Self {
        Provenance::Material {
            id: id.into(),
            anchor_byte,
            offset_start,
            offset_end,
            offset_kind: OffsetKind::default(),
        }
    }

    /// Create synthesis provenance from parent event IDs
    /// Returns None if the iterator is empty (enforces non-empty invariant)
    pub fn from_synthesis<I: IntoIterator<Item = EventId>>(ids: I) -> Option<Self> {
        let vec: Vec<EventId> = ids.into_iter().collect();
        NonEmptyVec::from_vec(vec).map(|source_event_ids| Provenance::Synthesis {
            source_event_ids,
            operation_id: None,
        })
    }

    /// Create synthesis provenance with at least one parent ID
    pub fn from_synthesis_safe(first: EventId, rest: Vec<EventId>) -> Self {
        Provenance::Synthesis {
            source_event_ids: NonEmptyVec::from_head_tail(first, rest),
            operation_id: None,
        }
    }
}

impl<T> Event<T> {
    /// Universal constructor - requires all fields explicitly
    pub fn create(
        source: impl Into<EventSource>,
        event_type: impl Into<EventType>,
        payload: T,
        provenance: Provenance,
    ) -> Self {
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

    // Deprecated constructors for backwards compatibility
    #[deprecated(since = "0.5.0", note = "Use Event::create() or Event::new() instead")]
    pub fn from_material(
        source: impl Into<EventSource>,
        event_type: impl Into<EventType>,
        payload: T,
        material_id: impl Into<Id<SourceMaterial>>,
        anchor_byte: i64,
    ) -> Self {
        Self::create(
            source,
            event_type,
            payload,
            Provenance::from_material(material_id, anchor_byte, None, None),
        )
    }

    #[deprecated(since = "0.5.0", note = "Use Event::create() or Event::new() instead")]
    pub fn from_synthesis<I>(
        source: impl Into<EventSource>,
        event_type: impl Into<EventType>,
        payload: T,
        parent_ids: I,
    ) -> Self
    where
        I: IntoIterator<Item = EventId>,
    {
        Self::create(
            source,
            event_type,
            payload,
            Provenance::from_synthesis(parent_ids)
                .unwrap_or_else(|| panic!("from_synthesis requires at least one parent ID")),
        )
    }

    #[deprecated(since = "0.5.0", note = "Events should have real provenance")]
    pub fn system_event(
        source: impl Into<EventSource>,
        event_type: impl Into<EventType>,
        payload: T,
    ) -> Self {
        let system_bootstrap_id = EventId::from_ulid(
            crate::types::Ulid::from_bytes([
                0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00,
            ])
            .unwrap_or_else(|_| panic!("hardcoded ULID bytes should be valid")),
        );

        Self::create(
            source,
            event_type,
            payload,
            Provenance::from_synthesis_safe(system_bootstrap_id, vec![]),
        )
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

        #[allow(deprecated)]
        {
            Self::from_material(source, event_type, payload, test_material_id, 0)
        }
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

    /// Temporary constructor for telemetry events that haven't gone through sensd yet
    /// TODO: Telemetry should go through sensd and get proper source material IDs
    #[deprecated(
        since = "0.5.0",
        note = "Telemetry events should go through sensd for proper provenance"
    )]
    pub fn new_telemetry(payload: T) -> Self {
        // Using a well-known telemetry bootstrap event ID
        // This indicates the telemetry hasn't been properly ingested yet
        let telemetry_bootstrap_id = EventId::from_ulid(
            crate::types::Ulid::from_bytes([
                0x01, 0x90, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x01,
            ])
            .unwrap_or_else(|_| panic!("hardcoded telemetry ULID bytes should be valid")),
        );

        Self::create(
            T::SOURCE,
            T::EVENT_TYPE,
            payload,
            Provenance::from_synthesis_safe(telemetry_bootstrap_id, vec![]),
        )
    }

    /// Start building a typed event with builder pattern
    pub fn builder(payload: T) -> EventBuilder<T, NoProvenance> {
        EventBuilder {
            payload,
            source: T::SOURCE,
            event_type: T::EVENT_TYPE,
            provenance: None,
            ts_orig: None,
            associated_blob_ids: None,
            _phantom: std::marker::PhantomData,
        }
    }
}

// Convenience constructors for dynamic events (Event<JsonValue>)
impl Event<JsonValue> {
    /// Start building a dynamic event with explicit source and type
    /// (overrides the generic "system"/"generic" defaults from EventPayload impl)
    pub fn dynamic(
        source: impl Into<EventSource>,
        event_type: impl Into<EventType>,
        payload: JsonValue,
    ) -> EventBuilder<JsonValue, NoProvenance> {
        EventBuilder {
            payload,
            source: source.into(),
            event_type: event_type.into(),
            provenance: None,
            ts_orig: None,
            associated_blob_ids: None,
            _phantom: std::marker::PhantomData,
        }
    }

    // No RawEvent; use Event::<JsonValue>::test_event() in tests.

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
impl<T> EventBuilder<T, NoProvenance> {
    /// Set provenance and transition to HasProvenance state
    pub fn with_provenance(mut self, provenance: Provenance) -> EventBuilder<T, HasProvenance> {
        self.provenance = Some(provenance);
        EventBuilder {
            payload: self.payload,
            source: self.source,
            event_type: self.event_type,
            provenance: self.provenance,
            ts_orig: self.ts_orig,
            associated_blob_ids: self.associated_blob_ids,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Convenience: create from material
    pub fn from_material(
        self,
        material_id: impl Into<Id<SourceMaterial>>,
        anchor: i64,
    ) -> EventBuilder<T, HasProvenance> {
        self.with_provenance(Provenance::from_material(material_id, anchor, None, None))
    }

    /// Convenience: create from parent events
    pub fn from_parents<I>(self, parents: I) -> EventBuilder<T, HasProvenance>
    where
        I: IntoIterator<Item = EventId>,
    {
        let ids: Vec<EventId> = parents.into_iter().collect();
        let provenance = Provenance::from_synthesis(ids)
            .unwrap_or_else(|| panic!("from_parents requires at least one parent ID"));
        self.with_provenance(provenance)
    }
}

impl<T> EventBuilder<T, HasProvenance> {
    /// Set timestamp (optional)
    pub fn at_time(mut self, ts: Timestamp) -> Self {
        self.ts_orig = Some(ts);
        self
    }

    /// Add associated blobs (optional)
    pub fn with_associated_blobs(mut self, blobs: Vec<Ulid>) -> Self {
        self.associated_blob_ids = Some(blobs);
        self
    }

    /// Build the event (only available after provenance is set)
    pub fn build(self) -> Event<T> {
        Event {
            id: None,
            source: self.source,
            event_type: self.event_type,
            payload: self.payload,

            provenance: self.provenance.expect("guaranteed by typestate"),
            ts_orig: self.ts_orig.or_else(|| Some(Utc::now())),
            host: get_hostname(),
            ingestor_version: get_ingestor_version(),
            payload_schema_id: None,
            associated_blob_ids: self.associated_blob_ids,
        }
    }
}

// Helper function to get hostname
fn get_hostname() -> HostName {
    HostName::new(gethostname::gethostname().to_string_lossy().to_string())
}

// Helper function to get ingestor version
fn get_ingestor_version() -> Option<String> {
    std::env::var("SINEX_VERSION").ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_material_event_creation() {
        let event = Event::dynamic("fs-watcher", "file.created", json!({"path": "/test.txt"}))
            .from_material(Id::<SourceMaterial>::new(), 42)
            .build();

        assert_eq!(event.source.as_str(), "fs-watcher");
        assert_eq!(event.event_type.as_str(), "file.created");
        assert!(event.is_first_order_event());
        assert!(!event.is_synthesized_event());
        assert_eq!(event.anchor_byte(), Some(42));
        assert!(event.source_event_ids().is_none());
    }

    #[test]
    fn test_synthesis_event_creation() {
        let parent_ids = vec![EventId::new(), EventId::new()];
        let event = Event::dynamic(
            "processor",
            "analysis.completed",
            json!({"result": "success"}),
        )
        .from_parents(parent_ids.clone())
        .build();

        assert_eq!(event.source.as_str(), "processor");
        assert_eq!(event.event_type.as_str(), "analysis.completed");
        assert!(!event.is_first_order_event());
        assert!(event.is_synthesized_event());
        assert_eq!(event.anchor_byte(), None);
        assert_eq!(event.source_event_ids(), Some(parent_ids.as_slice()));
    }

    #[test]
    fn test_raw_event_alias() {
        let event: Event<JsonValue> =
            Event::dynamic("test", "test.event", json!({"data": "value"}))
                .from_material(Id::<SourceMaterial>::new(), 0)
                .build();

        // Verify it's the same type
        let _: Event<JsonValue> = event;
    }

    #[test]
    fn test_type_conversions() {
        let original = Event::dynamic("test", "test.event", json!({"message": "hello"}))
            .from_material(Id::<SourceMaterial>::new(), 10)
            .build();

        // Convert to raw
        let raw = original.to_json_event().unwrap();

        // Convert back to typed
        let recovered: Event<JsonValue> = raw.to_typed().unwrap();

        assert_eq!(recovered.payload["message"], "hello");
        assert_eq!(recovered.anchor_byte(), Some(10));
    }
}
