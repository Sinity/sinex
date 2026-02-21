//! Unified Event Model
//!
//! This module contains the unified Event<T> structure.
//!
//! - Event<T> is the generic structure for all events
//! - Event<JsonValue> (aka `RawEvent`) for heterogeneous processing
//! - ALL events MUST have provenance (Material or Synthesis)

pub mod builder;
pub mod enums;
mod markers;
pub mod payload;
pub mod payloads;
pub mod schema_registry;

pub use builder::*;
pub use payload::*;
pub use payloads::*;

use crate::domain::{EventSource, EventType, HostName};
use crate::ids::Id;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sinex_schema::primitives::Ulid;

// Re-export Timestamp for use by other modules
pub use sinex_schema::primitives::Timestamp;

/// Unified generic event structure
///
/// This is the canonical event structure used throughout the system.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event<T = JsonValue> {
    /// Event ID - elegant distinction between new and persisted events
    /// - None: New event to be inserted (builder pattern)
    /// - Some(id): Persisted event retrieved from database
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Id<Event<T>>>,

    /// Event source (e.g., "fs-watcher", "terminal")
    pub source: EventSource,

    /// Event type (e.g., "file.created", "command.executed")
    pub event_type: EventType,

    /// Event payload (typed or JSON)
    pub payload: T,

    /// Original timestamp when the event occurred
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts_orig: Option<Timestamp>,

    /// Hostname where the event was generated
    #[serde(default = "get_hostname_default")]
    pub host: HostName,

    /// Version of the node that created this event
    pub node_version: Option<String>,

    /// Schema ID for payload validation
    pub payload_schema_id: Option<Ulid>,

    /// Provenance tracking the origin of this event
    /// Serializes flatly for wire format compatibility
    #[serde(flatten)]
    pub provenance: Provenance,

    /// Array of associated blob IDs (screenshots, recordings, etc.)
    pub associated_blob_ids: Option<Vec<Ulid>>,
}

/// Marker type for source material IDs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SourceMaterial;

// Convenience constructors for typed events
impl<T> Event<T>
where
    T: EventPayload,
{
    /// Quick constructor for typed events - derives source/type from payload
    pub fn new(payload: T, provenance: Provenance) -> Self {
        Self {
            id: None,
            source: T::SOURCE,
            event_type: T::EVENT_TYPE,
            payload,
            ts_orig: Some(Timestamp::now()),
            host: builder::get_hostname(),
            node_version: builder::get_node_version(),
            payload_schema_id: None,
            provenance,
            associated_blob_ids: None,
        }
    }

    /// Start building a typed event with builder pattern
    pub fn builder(payload: T) -> EventBuilder<T, NoProvenance> {
        EventBuilder::new_internal(T::SOURCE, T::EVENT_TYPE, payload)
    }
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

    /// Set the node version
    pub fn with_node_version(mut self, version: impl Into<String>) -> Self {
        self.node_version = Some(version.into());
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

    /// Get a reference to provenance
    pub fn provenance(&self) -> &Provenance {
        &self.provenance
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
    pub fn get_anchor_byte(&self) -> Option<i64> {
        match &self.provenance {
            Provenance::Material { anchor_byte, .. } => Some(*anchor_byte),
            _ => None,
        }
    }

    /// Get the source event IDs if this is a Synthesis event
    pub fn get_source_event_ids(&self) -> Option<&[EventId]> {
        match &self.provenance {
            Provenance::Synthesis {
                source_event_ids, ..
            } => Some(source_event_ids.as_slice()),
            _ => None,
        }
    }
}

impl<T: Serialize> Event<T> {
    /// Convert to Event<JsonValue> (type erasure)
    /// Preserves event ID.
    pub fn to_json_event(self) -> Result<Event<JsonValue>, serde_json::Error> {
        Ok(Event {
            id: self.id.map(|id| Id::from_ulid(*id.as_ulid())),
            source: self.source,
            event_type: self.event_type,
            payload: serde_json::to_value(self.payload)?,
            ts_orig: self.ts_orig,
            host: self.host,
            node_version: self.node_version,
            payload_schema_id: self.payload_schema_id,
            provenance: self.provenance,
            associated_blob_ids: self.associated_blob_ids,
        })
    }
}

impl Event<JsonValue> {
    /// Try to convert to typed event (type recovery)
    /// Preserves event ID.
    pub fn to_typed<T>(&self) -> Result<Event<T>, serde_json::Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        Ok(Event {
            id: self.id.as_ref().map(|id| Id::from_ulid(*id.as_ulid())),
            source: self.source.clone(),
            event_type: self.event_type.clone(),
            payload: serde_json::from_value(self.payload.clone())?,
            ts_orig: self.ts_orig,
            host: self.host.clone(),
            node_version: self.node_version.clone(),
            payload_schema_id: self.payload_schema_id,
            provenance: self.provenance.clone(),
            associated_blob_ids: self.associated_blob_ids.clone(),
        })
    }

    /// Create a dynamic JSON event with explicit provenance.
    pub fn new_json(
        source: impl Into<EventSource>,
        event_type: impl Into<EventType>,
        payload: JsonValue,
        provenance: Provenance,
    ) -> Self {
        Self {
            id: None,
            source: source.into(),
            event_type: event_type.into(),
            payload,
            ts_orig: Some(Timestamp::now()),
            host: builder::get_hostname(),
            node_version: builder::get_node_version(),
            payload_schema_id: None,
            provenance,
            associated_blob_ids: None,
        }
    }
}

// Helper for serde default
fn get_hostname_default() -> HostName {
    builder::get_hostname()
}
