//! Unified RawEvent Type
//!
//! This module contains the unified RawEvent struct that replaces the old
//! RawEvent/NewEvent dichotomy. A RawEvent with id: None is a new event
//! to be inserted, while a RawEvent with id: Some(...) is a persisted event.

use crate::types::domain::{EventSource, EventType, HostName};
use crate::types::{Id, Ulid};
use serde::{Deserialize, Serialize};

// Type aliases for timestamp and JSON handling
pub type Timestamp = chrono::DateTime<chrono::Utc>;
pub type OptionalTimestamp = Option<chrono::DateTime<chrono::Utc>>;
pub type JsonValue = serde_json::Value;

/// Unified event structure for both creation and retrieval
///
/// This is the canonical event structure used throughout the system for both
/// raw observations and synthesized events. The distinction is made via the
/// provenance field:
/// - Raw Event: provenance is None
/// - Synthesis Event: provenance contains either Events or Material source
///
/// The id field determines if this is a new event or a persisted one:
/// - id: None => New event to be created
/// - id: Some(id) => RawEvent retrieved from database
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, bon::Builder)]
#[builder(on(String, into))] // Convert &str to String automatically
pub struct RawEvent {
    /// Event ID - None when creating, Some when from DB
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(skip)]
    pub id: Option<Id<RawEvent>>,

    /// Event source (e.g., "fs-watcher", "terminal")
    pub source: EventSource,

    /// Event type (e.g., "file.created", "command.executed")
    pub event_type: EventType,

    /// Event payload as JSON
    pub payload: JsonValue,

    /// Ingestion timestamp - set by database
    #[builder(skip)]
    pub ts_ingest: Timestamp,

    /// Original timestamp when the event occurred
    #[builder(default)]
    pub ts_orig: OptionalTimestamp,

    /// Hostname where the event was generated
    #[builder(default = get_hostname())]
    pub host: HostName,

    /// Version of the ingestor that created this event
    pub ingestor_version: Option<String>,

    /// Schema ID for payload validation
    pub payload_schema_id: Option<Ulid>,

    /// Provenance tracking: either from events or source material
    pub provenance: Option<Provenance>,

    /// Immutable anchor byte offset within source material
    pub anchor_byte: Option<i64>,

    /// Array of associated blob IDs (screenshots, recordings, etc.)
    pub associated_blob_ids: Option<Vec<Ulid>>,
}

/// Marker type for source material IDs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SourceMaterial;

/// Provenance type for tracking event lineage
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Provenance {
    /// Event derived from other events
    Events(Vec<Id<RawEvent>>),
    /// Event derived from source material
    Material {
        id: Id<SourceMaterial>,
        offset_start: Option<i64>,
        offset_end: Option<i64>,
    },
}

impl From<Vec<Id<RawEvent>>> for Provenance {
    fn from(ids: Vec<Id<RawEvent>>) -> Self {
        Provenance::Events(ids)
    }
}

impl From<&[Id<RawEvent>]> for Provenance {
    fn from(ids: &[Id<RawEvent>]) -> Self {
        Provenance::Events(ids.to_vec())
    }
}

impl<const N: usize> From<[Id<RawEvent>; N]> for Provenance {
    fn from(ids: [Id<RawEvent>; N]) -> Self {
        Provenance::Events(ids.to_vec())
    }
}

impl Provenance {
    /// Create event provenance from a list of event IDs
    pub fn from_events<I: IntoIterator<Item = Id<RawEvent>>>(ids: I) -> Self {
        Provenance::Events(ids.into_iter().collect())
    }

    /// Create material provenance
    pub fn from_material(
        id: impl Into<Id<SourceMaterial>>,
        offset_start: Option<i64>,
        offset_end: Option<i64>,
    ) -> Self {
        Provenance::Material {
            id: id.into(),
            offset_start,
            offset_end,
        }
    }
}

impl RawEvent {
    /// Create a schemaless/external event with minimal required fields
    ///
    /// This creates a RawEvent that can be chained with `with_*` methods:
    /// ```ignore
    /// let event = RawEvent::schemaless(source, event_type, payload)
    ///     .with_ts_orig(Some(timestamp))
    ///     .with_provenance(provenance);
    /// ```
    pub fn schemaless(
        source: impl Into<EventSource>,
        event_type: impl Into<EventType>,
        payload: JsonValue,
    ) -> Self {
        RawEvent {
            id: None,
            source: source.into(),
            event_type: event_type.into(),
            payload,
            ts_ingest: chrono::Utc::now(),
            ts_orig: None,
            host: get_hostname(),
            ingestor_version: None,
            payload_schema_id: None,
            provenance: None,
            anchor_byte: None,
            associated_blob_ids: None,
        }
    }

    /// Fluent method to set timestamp origin
    pub fn with_ts_orig(mut self, ts: Option<Timestamp>) -> Self {
        self.ts_orig = ts;
        self
    }

    /// Fluent method to set provenance
    pub fn with_provenance(mut self, provenance: impl Into<Provenance>) -> Self {
        self.provenance = Some(provenance.into());
        self
    }

    /// Fluent method to set anchor byte
    pub fn with_anchor_byte(mut self, byte: Option<i64>) -> Self {
        self.anchor_byte = byte;
        self
    }

    /// Fluent method to set associated blob IDs
    pub fn with_associated_blobs(mut self, blob_ids: Option<Vec<Ulid>>) -> Self {
        self.associated_blob_ids = blob_ids;
        self
    }

    /// Check if this event has been persisted to the database
    pub fn is_persisted(&self) -> bool {
        self.id.is_some()
    }

    /// Check if this is a raw event (no provenance)
    pub fn is_raw_event(&self) -> bool {
        self.provenance.is_none()
    }

    /// Check if this is a synthesis event (has event provenance)
    pub fn is_synthesis_event(&self) -> bool {
        matches!(self.provenance, Some(Provenance::Events(_)))
    }

    /// Get the source event IDs if this is a synthesis event
    pub fn get_source_event_ids(&self) -> Option<&[Id<RawEvent>]> {
        match &self.provenance {
            Some(Provenance::Events(ids)) => Some(ids),
            _ => None,
        }
    }

    /// Extract ingestion timestamp from ULID if persisted
    pub fn ts_ingest_from_ulid(&self) -> Option<Timestamp> {
        self.id.as_ref().map(|id| id.timestamp())
    }

    /// Simple constructor for the most common use case
    pub fn simple(source: EventSource, event_type: EventType, payload: JsonValue) -> Self {
        RawEvent::builder()
            .source(source)
            .event_type(event_type)
            .payload(payload)
            .build()
    }
}

// Helper function to get hostname
fn get_hostname() -> HostName {
    HostName::new(gethostname::gethostname().to_string_lossy().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[sinex_test]
    fn test_schemaless_event_builder() {
        let mut event = RawEvent::schemaless(
            EventSource::new("test"),
            EventType::new("test.created"),
            json!({"message": "hello"}),
        );
        event.host = HostName::new("test-host");

        assert_eq!(event.source.as_str(), "test");
        assert_eq!(event.event_type.as_str(), "test.created");
        assert!(event.id.is_none());
        assert!(event.is_raw_event());
        assert!(!event.is_persisted());
    }

    #[sinex_test]
    fn test_simple_constructor() {
        let event = RawEvent::simple(
            EventSource::new("test"),
            EventType::new("test.created"),
            json!({"message": "hello"}),
        );

        assert_eq!(event.source.as_str(), "test");
        assert_eq!(event.event_type.as_str(), "test.created");
        assert!(event.id.is_none());
    }

    #[sinex_test]
    fn test_synthesis_event() {
        let source_ids = vec![Id::<RawEvent>::new(), Id::<RawEvent>::new()];
        let mut event = RawEvent::schemaless(
            EventSource::new("processor"),
            EventType::new("analysis.completed"),
            json!({"result": "success"}),
        );
        event.host = HostName::new("test-host");
        let event = event.with_provenance(Provenance::Events(source_ids.clone()));

        assert!(event.is_synthesis_event());
        assert!(!event.is_raw_event());
        assert_eq!(event.get_source_event_ids().unwrap(), &source_ids);
    }
}
