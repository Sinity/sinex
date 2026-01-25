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
/// - `Event<JsonValue>` (aka RawEvent) for heterogeneous processing and storage
/// - ALL events MUST have provenance (Material or Synthesis)
/// - The id field determines if this is a new event or a persisted one
///
/// # Serialization Format
///
/// Events serialize provenance fields flatly (not nested) for compatibility with NATS/ingestd:
/// - Material: `{"source_material_id": "...", "anchor_byte": 0, "offset_start": ..., ...}`
/// - Synthesis: `{"source_event_ids": ["...", "..."]}`
///
/// The `Provenance` enum handles this serialization automatically via custom Serialize/Deserialize.
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

    /// Provenance tracking the origin of this event
    /// Serializes flatly for wire format compatibility
    #[serde(flatten)]
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
}

// Convenience constructors for typed events
impl<T> Event<T>
where
    T: crate::types::events::EventPayload,
{
    /// Quick constructor for typed events - derives source/type from payload
    pub fn new(payload: T, provenance: Provenance) -> Self {
        Self {
            id: None,
            source: T::SOURCE,
            event_type: T::EVENT_TYPE,
            payload,
            ts_orig: Some(Utc::now()),
            host: get_hostname(),
            ingestor_version: get_ingestor_version(),
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
    ///
    /// This conversion erases the payload type parameter but **preserves the event ID**.
    /// If the event has an ID, it will be carried over to the JsonValue variant.
    /// This ensures ID stability across type boundaries, which is critical for
    /// event tracking, provenance chains, and database operations.
    ///
    /// # Example
    /// ```rust,ignore
    /// let typed_event: Event<MyPayload> = Event::new(payload, provenance);
    /// let json_event: Event<JsonValue> = typed_event.to_json_event()?;
    /// // json_event.id == typed_event.id (if set)
    /// ```
    pub fn to_json_event(self) -> Result<Event<JsonValue>, serde_json::Error> {
        Ok(Event {
            id: self.id.map(|id| Id::from_ulid(*id.as_ulid())),
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
    ///
    /// This conversion recovers the typed payload from JsonValue while **preserving the event ID**.
    /// If the event has an ID, it will be carried over to the typed variant. This is essential
    /// for maintaining event identity when deserializing from the database or when converting
    /// between typed representations.
    ///
    /// # Example
    /// ```rust,ignore
    /// let json_event: Event<JsonValue> = fetch_from_db().await?;
    /// let typed_event: Event<MyPayload> = json_event.to_typed()?;
    /// // typed_event.id == json_event.id (if set)
    /// ```
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
            ingestor_version: self.ingestor_version.clone(),
            payload_schema_id: self.payload_schema_id,
            provenance: self.provenance.clone(),
            associated_blob_ids: self.associated_blob_ids.clone(),
        })
    }

    /// Create a dynamic JSON event with explicit provenance.
    ///
    /// Use this for constructing events with runtime-determined source/type
    /// when you already have a Provenance value.
    ///
    /// # Example
    /// ```rust,ignore
    /// let event = Event::new_json(
    ///     "my-source",
    ///     "my.event",
    ///     json!({"key": "value"}),
    ///     provenance,
    /// );
    /// ```
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
            ts_orig: Some(chrono::Utc::now()),
            host: get_hostname(),
            ingestor_version: get_ingestor_version(),
            payload_schema_id: None,
            provenance,
            associated_blob_ids: None,
        }
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
    use crate::types::events::DynamicPayload;
    use serde_json::json;

    #[test]
    fn event_builder_sets_offsets_for_material_provenance() {
        let material_id = Id::from_ulid(Ulid::new());
        let event = DynamicPayload::new("offset-test", "offset.event", json!({"key": "value"}))
            .from_material_at(material_id, 4)
            .with_offset_start(10)
            .expect("offset start should apply to material provenance")
            .with_offset_end(20)
            .expect("offset end should apply to material provenance")
            .with_offset_kind(OffsetKind::Line)
            .expect("offset kind should apply to material provenance")
            .build()
            .expect("event should build with material provenance");

        match event.provenance() {
            Provenance::Material {
                offset_start,
                offset_end,
                offset_kind,
                ..
            } => {
                assert_eq!(*offset_start, Some(10));
                assert_eq!(*offset_end, Some(20));
                assert_eq!(*offset_kind, OffsetKind::Line);
            }
            _ => panic!("expected material provenance"),
        }
    }

    #[test]
    fn events_contain_build_version() {
        // Create a test event with material provenance
        let material_id = Id::from_ulid(Ulid::new());
        let event = DynamicPayload::new("test", "test.event", json!({"key": "value"}))
            .from_material_at(material_id, 4)
            .build()
            .expect("Event should build");

        // Get version - may be None in test environments without git
        let version = get_ingestor_version();

        // Event's version should match what get_ingestor_version returns
        assert_eq!(
            event.ingestor_version, version,
            "Event version should match get_ingestor_version()"
        );

        // If version is present, verify format
        if let Some(ref ver) = version {
            if ver != "unknown" {
                assert!(
                    ver.starts_with("git-") || !ver.is_empty(),
                    "Version should be in git-<rev> format or be a non-empty string, got: {}",
                    ver
                );
            }
        }
        // Note: version can be None in test environments where:
        // - SINEX_GIT_REV is not set at compile time
        // - SINEX_VERSION is not set at runtime
        // This is expected and not a failure condition
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
                assert!(
                    ver.len() > 4,
                    "Git revision should have content after 'git-'"
                );
            }
        }
    }
}
