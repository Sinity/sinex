//! Unified Event Model
//!
//! This module contains the unified Event<T> structure.
//!
//! - Event<T> is the generic structure for all events
//! - Event<JsonValue> (aka `RawEvent`) for heterogeneous processing
//! - ALL events MUST have provenance (Material or Derived)

pub mod admission;
pub mod builder;
pub mod enums;
pub mod occurrence;
pub mod payload;
pub mod payloads;
pub mod schema_registry;

pub use admission::*;
pub use builder::*;
pub use occurrence::MaterialOccurrenceKey;
pub use payload::*;
pub use payloads::*;

use crate::domain::{
    AutomatonModel, EventSource, EventType, HostName, SyntheticTemporalPolicy, TemporalSourceType,
};
use crate::ids::Id;
use crate::primitives::Uuid;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

// Re-export Timestamp for use by other modules
pub use crate::primitives::Timestamp;

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

    /// Original timestamp when the event occurred.
    ///
    /// `None` is the "derive me at persistence" signal for material-provenance
    /// events: the event_engine admission stage resolves it from the source-material
    /// timing tier (#1570 Prong B). Derived events and any caller that set an
    /// explicit time carry `Some`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts_orig: Option<Timestamp>,

    /// Quality rung of `ts_orig` on the temporal ladder.
    ///
    /// Set by the parser to `IntrinsicContent` when a `#[timestamp]` field
    /// resolves; left `None` for material-provenance events that defer
    /// resolution to the persistence stage, which fills in the resolved rung.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts_quality: Option<TemporalSourceType>,

    /// Hostname where the event was generated
    #[serde(default = "get_hostname_default")]
    pub host: HostName,

    /// UUID of the node run (session) that created this event.
    /// References `core.runs.id`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub module_run_id: Option<Uuid>,

    /// Schema ID for payload validation
    pub payload_schema_id: Option<Uuid>,

    /// Provenance tracking the origin of this event
    /// Serializes as a flat wire-format shape
    #[serde(flatten)]
    pub provenance: Provenance,

    /// BLAKE3 hash of source-material byte range (material events only).
    /// NULL for derived. 32 bytes. Verified on replay — mismatch → DLQ.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub anchor_payload_hash: Option<Vec<u8>>,

    /// Array of associated blob IDs (screenshots, recordings, etc.)
    pub associated_blob_ids: Option<Vec<Uuid>>,

    // Synthetic event metadata (nullable — only populated for derived/synthesized events)
    /// Which temporal policy governed `ts_orig` derivation for this synthesized event
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temporal_policy: Option<SyntheticTemporalPolicy>,

    /// Version of the node logic that produced this event (for deterministic replay)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub semantics_version: Option<String>,

    /// Scope identifier for scope-reconciler replacement patterns
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope_key: Option<String>,

    /// Identifies which output "slot" this event occupies (for targeted replacement)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub equivalence_key: Option<String>,

    /// Which replay/operation created this event, if any
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by_operation_id: Option<Uuid>,

    /// Which automaton model produced this event
    #[serde(skip_serializing_if = "Option::is_none")]
    pub automaton_model: Option<AutomatonModel>,
}

/// Marker type for source material IDs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SourceMaterial;

// Convenience constructors for typed events
impl<T> Event<T>
where
    T: EventPayload,
{
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

    /// Set the node run ID (references `core.runs`)
    pub fn with_module_run_id(mut self, run_id: Uuid) -> Self {
        self.module_run_id = Some(run_id);
        self
    }

    /// Set the schema ID
    pub fn with_schema_id(mut self, schema_id: Uuid) -> Self {
        self.payload_schema_id = Some(schema_id);
        self
    }

    /// Set associated blob IDs
    pub fn with_blobs(mut self, blob_ids: Vec<Uuid>) -> Self {
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
        matches!(self.provenance, Provenance::Derived { .. })
    }

    /// Get the anchor byte if this is a Material event
    pub fn get_anchor_byte(&self) -> Option<i64> {
        match &self.provenance {
            Provenance::Material { anchor_byte, .. } => Some(*anchor_byte),
            _ => None,
        }
    }

    /// Get the source event IDs if this is a Derived event
    pub fn get_source_event_ids(&self) -> Option<&[EventId]> {
        match &self.provenance {
            Provenance::Derived {
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
            id: self.id.map(|id| Id::from_uuid(*id.as_uuid())),
            source: self.source,
            event_type: self.event_type,
            payload: serde_json::to_value(self.payload)?,
            ts_orig: self.ts_orig,
            ts_quality: self.ts_quality,
            host: self.host,
            module_run_id: self.module_run_id,
            payload_schema_id: self.payload_schema_id,
            provenance: self.provenance,
            anchor_payload_hash: self.anchor_payload_hash.clone(),
            associated_blob_ids: self.associated_blob_ids,
            temporal_policy: self.temporal_policy,
            semantics_version: self.semantics_version,
            scope_key: self.scope_key,
            equivalence_key: self.equivalence_key,
            created_by_operation_id: self.created_by_operation_id,
            automaton_model: self.automaton_model,
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
            id: self.id.as_ref().map(|id| Id::from_uuid(*id.as_uuid())),
            source: self.source.clone(),
            event_type: self.event_type.clone(),
            payload: serde_json::from_value(self.payload.clone())?,
            ts_orig: self.ts_orig,
            ts_quality: self.ts_quality,
            host: self.host.clone(),
            module_run_id: self.module_run_id,
            payload_schema_id: self.payload_schema_id,
            provenance: self.provenance.clone(),
            anchor_payload_hash: self.anchor_payload_hash.clone(),
            associated_blob_ids: self.associated_blob_ids.clone(),
            temporal_policy: self.temporal_policy,
            semantics_version: self.semantics_version.clone(),
            scope_key: self.scope_key.clone(),
            equivalence_key: self.equivalence_key.clone(),
            created_by_operation_id: self.created_by_operation_id,
            automaton_model: self.automaton_model,
        })
    }

    /// Create a dynamic JSON event with explicit provenance.
    pub fn new_json(
        source: impl Into<EventSource>,
        event_type: impl Into<EventType>,
        payload: JsonValue,
        provenance: Provenance,
    ) -> Self {
        let provenance = provenance.into_canonical();
        let created_by_operation_id = provenance.operation_uuid();

        Self {
            id: None,
            source: source.into(),
            event_type: event_type.into(),
            payload,
            ts_orig: Some(Timestamp::now()),
            ts_quality: None,
            host: builder::get_hostname(),
            module_run_id: None,
            payload_schema_id: None,
            provenance,
            anchor_payload_hash: None,
            associated_blob_ids: None,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id,
            automaton_model: None,
        }
    }
}

// Helper for serde default
fn get_hostname_default() -> HostName {
    builder::get_hostname()
}
