//! Unified Event Type
//!
//! This module contains the unified Event struct that replaces the old
//! RawEvent/NewEvent dichotomy. An Event with id: None is a new event
//! to be inserted, while an Event with id: Some(...) is a persisted event.

use serde::{Deserialize, Serialize};
use sinex_types::domain::{EventSource, EventType, HostName};
use sinex_types::{Id, Ulid};

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
/// - id: Some(id) => Event retrieved from database
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, bon::Builder)]
#[builder(on(String, into))] // Convert &str to String automatically
pub struct Event {
    /// Event ID - None when creating, Some when from DB
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(skip)]
    pub id: Option<Id<Event>>,

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
    Events(Vec<Id<Event>>),
    /// Event derived from source material
    Material {
        id: Id<SourceMaterial>,
        offset_start: Option<i64>,
        offset_end: Option<i64>,
    },
}

impl From<Vec<Id<Event>>> for Provenance {
    fn from(ids: Vec<Id<Event>>) -> Self {
        Provenance::Events(ids)
    }
}

impl From<&[Id<Event>]> for Provenance {
    fn from(ids: &[Id<Event>]) -> Self {
        Provenance::Events(ids.to_vec())
    }
}

impl<const N: usize> From<[Id<Event>; N]> for Provenance {
    fn from(ids: [Id<Event>; N]) -> Self {
        Provenance::Events(ids.to_vec())
    }
}

impl Provenance {
    /// Create event provenance from a list of event IDs
    pub fn from_events<I: IntoIterator<Item = Id<Event>>>(ids: I) -> Self {
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

impl Event {
    /// Create a builder for schemaless/external events
    pub fn schemaless() -> EventBuilder {
        Event::builder()
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
    pub fn get_source_event_ids(&self) -> Option<&[Id<Event>]> {
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
        Event::builder()
            .source(source)
            .event_type(event_type)
            .payload(payload)
            .build()
    }

    /// Create an event from a strongly-typed payload
    ///
    /// This is a convenience method to avoid the orphan rule issue.
    /// Since Event is in sinex-db and EventPayload is in sinex-types,
    /// we can't implement From<T> for Event where T: EventPayload.
    ///
    /// ## History and differences from the original Event::from
    ///
    /// When Event lived in sinex-events, we had:
    /// ```ignore
    /// impl Event {
    ///     pub fn from<P: EventPayload>(payload: P) -> Self {
    ///         // Look up schema ID from cache
    ///         let schema_id = crate::schema_registry::get_schema_id(
    ///             P::SOURCE.as_str(),
    ///             P::EVENT_TYPE.as_str()
    ///         );
    ///         
    ///         Event {
    ///             id: None,
    ///             source: P::SOURCE,
    ///             event_type: P::EVENT_TYPE,
    ///             payload: serde_json::to_value(payload).expect("EventPayload must serialize"),
    ///             ts_ingest: chrono::Utc::now(),
    ///             ts_orig: None,
    ///             host: get_hostname(),
    ///             ingestor_version: Some(env!("CARGO_PKG_VERSION").to_string()),
    ///             payload_schema_id: schema_id,
    ///             provenance: None,
    ///             anchor_byte: None,
    ///             associated_blob_ids: None,
    ///         }
    ///     }
    /// }
    /// ```
    ///
    /// ### Current differences:
    /// 1. **Method name**: `from` → `from_payload` (but could still be named `from`)
    /// 2. **Return type**: `Self` → `Result<Self, SinexError>` (but could still return `Self`)
    /// 3. **Schema ID**: Was looked up from registry → Now None (LOST FUNCTIONALITY)
    /// 4. **Ingestor version**: Was set to CARGO_PKG_VERSION → Now None (LOST FUNCTIONALITY)
    /// 5. **Timestamp**: Was explicitly set → Now relies on builder default (POTENTIAL BUG)
    /// 6. **Error handling**: Used `.expect()` → Now returns Result
    ///
    /// ### Why not a From trait implementation?
    ///
    /// If we could implement the From trait (blocked by orphan rule):
    /// ```ignore
    /// impl<T: EventPayload> From<T> for Event { ... }
    /// ```
    ///
    /// Then users could call it THREE ways:
    /// - `Event::from(payload)` - method syntax
    /// - `payload.into()` - Into trait (automatic)
    /// - `Into::<Event>::into(payload)` - explicit Into
    ///
    /// With our regular method, only ONE way works:
    /// - `Event::from_payload(payload)?` - just a method call
    /// - `payload.into()` - ❌ DOESN'T WORK
    ///
    /// The method call looks identical to From trait, but we lose:
    /// - `.into()` conversions
    /// - Implicit conversions in function arguments expecting `impl Into<Event>`
    /// - Integration with Rust's conversion trait ecosystem
    ///
    /// ### TODO when fixing:
    /// 1. Consider renaming back to `from` for compatibility
    /// 2. Consider returning `Self` with `.expect()` for same ergonomics
    /// 3. Restore schema_id lookup functionality
    /// 4. Restore ingestor_version tracking
    /// 5. Ensure ts_ingest is properly initialized
    pub fn from_payload<P: sinex_types::events::EventPayload>(payload: P) -> Self {
        Event::builder()
            .source(P::SOURCE)
            .event_type(P::EVENT_TYPE)
            .payload(serde_json::to_value(&payload).expect("EventPayload must serialize"))
            .build()
    }

    /// Convert this Event to a typed EventEnvelope
    ///
    /// This method attempts to deserialize the event's payload into the appropriate
    /// strongly-typed payload based on the source and event_type combination.
    /// If deserialization fails or the event type is unknown, returns the
    /// Unknown variant with the original event data.
    pub fn to_envelope(&self) -> sinex_types::events::EventEnvelope {
        sinex_types::events::EventEnvelope::from_parts(
            self.source.as_str(),
            self.event_type.as_str(),
            self.payload.clone(),
        )
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

    #[test]
    fn test_schemaless_event_builder() {
        let event = Event::schemaless()
            .source(EventSource::new("test"))
            .event_type(EventType::new("test.created"))
            .payload(json!({"message": "hello"}))
            .host(HostName::new("test-host"))
            .build();

        assert_eq!(event.source.as_str(), "test");
        assert_eq!(event.event_type.as_str(), "test.created");
        assert!(event.id.is_none());
        assert!(event.is_raw_event());
        assert!(!event.is_persisted());
    }

    #[test]
    fn test_simple_constructor() {
        let event = Event::simple(
            EventSource::new("test"),
            EventType::new("test.created"),
            json!({"message": "hello"}),
        );

        assert_eq!(event.source.as_str(), "test");
        assert_eq!(event.event_type.as_str(), "test.created");
        assert!(event.id.is_none());
    }

    #[test]
    fn test_synthesis_event() {
        let source_ids = vec![Id::<Event>::new(), Id::<Event>::new()];
        let event = Event::schemaless()
            .source(EventSource::new("processor"))
            .event_type(EventType::new("analysis.completed"))
            .payload(json!({"result": "success"}))
            .host(HostName::new("test-host"))
            .build()
            .with_provenance(Provenance::Events(source_ids.clone()));

        assert!(event.is_synthesis_event());
        assert!(!event.is_raw_event());
        assert_eq!(event.get_source_event_ids().unwrap(), &source_ids);
    }

    #[test]
    fn test_to_envelope() {
        use sinex_types::events::{EventEnvelope, FileCreatedPayload};
        use serde_json::json;

        // Create an event with a known payload type
        let event = Event::simple(
            sinex_types::domain::EventSource::new("fs-watcher"),
            sinex_types::domain::EventType::new("file.created"),
            json!({
                "path": "/test/file.txt",
                "size": 1024,
                "created_at": "2024-01-01T00:00:00Z",
                "permissions": 644
            }),
        );

        let envelope = event.to_envelope();

        match envelope {
            EventEnvelope::FileCreated(payload) => {
                assert_eq!(payload.path, "/test/file.txt");
                assert_eq!(payload.size, 1024);
            }
            _ => panic!("Expected FileCreated envelope variant"),
        }
    }

    #[test]
    fn test_to_envelope_unknown() {
        use sinex_types::events::EventEnvelope;
        use serde_json::json;

        // Create an event with an unknown payload type
        let event = Event::simple(
            sinex_types::domain::EventSource::new("unknown-source"),
            sinex_types::domain::EventType::new("unknown.type"),
            json!({"unknown": "data"}),
        );

        let envelope = event.to_envelope();

        match envelope {
            EventEnvelope::Unknown(unknown) => {
                assert_eq!(unknown.source, "unknown-source");
                assert_eq!(unknown.event_type, "unknown.type");
                assert_eq!(unknown.payload["unknown"], "data");
            }
            _ => panic!("Expected Unknown envelope variant"),
        }
    }
}
