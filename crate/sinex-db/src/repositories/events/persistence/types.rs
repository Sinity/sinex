use crate::JsonValue;
use crate::models::Event;
use serde::{Deserialize, Serialize};
use sinex_primitives::domain::{EventSource, EventType, HostName, SchemaVersion};
use sinex_primitives::events::{EquivalenceKey, EventId, ScopeKey, SourceMaterial};
use sinex_primitives::{Id, Timestamp};
use sqlx::FromRow;
use uuid::Uuid;

/// Minimum batch size that routes to the COPY-based insert path.
///
/// Below this threshold the `QueryBuilder` (VALUES) approach has lower latency
/// because it avoids the staging-table round-trips.  Above it, COPY's lack of
/// a 65 535-parameter limit and lower per-row protocol overhead dominate.
/// Minimum batch size to use COPY protocol instead of `QueryBuilder` (VALUES).
/// Below this threshold the `QueryBuilder` approach has lower latency because it
/// avoids the staging-table round-trips. Above it, COPY's lack of a 65 535-parameter
/// limit and lower per-row protocol overhead dominate.
pub const COPY_BATCH_THRESHOLD: usize = 50;

/// Lightweight DTO for stream batch inserts from event_engine.
///
/// This struct provides a minimal representation of event data for high-throughput
/// batch inserts, avoiding the overhead of the full `Event<T>` type tree.
/// All fields are pre-validated and pre-parsed by the caller.
#[derive(Debug, Clone)]
pub struct StreamBatchRow {
    /// Pre-parsed `UUIDv7` for the event
    pub id: Uuid,
    /// Event source identifier
    pub source: EventSource,
    /// Event type identifier
    pub event_type: EventType,
    /// Pre-parsed timestamp
    pub ts_orig: Timestamp,
    /// Resolved `ts_orig` quality rung (`TemporalSourceType` display string).
    /// `None` for derived events and legacy callers that do not track quality.
    pub ts_quality: Option<String>,
    /// Hostname where event originated
    pub host: HostName,
    /// Event payload as JSON
    pub payload: JsonValue,
    /// Source material ID (for material provenance)
    pub source_material_id: Option<Id<SourceMaterial>>,
    /// Anchor byte offset into source material
    pub anchor_byte: Option<i64>,
    /// Start offset within source material
    pub offset_start: Option<i64>,
    /// End offset within source material
    pub offset_end: Option<i64>,
    /// Offset kind (e.g., "byte", "line")
    pub offset_kind: Option<String>,
    /// Parent event IDs (for derived provenance)
    pub source_event_ids: Option<Vec<EventId>>,
    /// Schema ID for payload validation
    pub payload_schema_id: Option<Uuid>,
    /// UUID of the module run session that produced this event
    pub module_run_id: Option<Uuid>,
    /// Associated blob IDs
    pub associated_blob_ids: Option<Vec<Uuid>>,
    /// BLAKE3 hash of source-material byte range (material events only)
    pub anchor_payload_hash: Option<Vec<u8>>,

    // Synthetic event metadata (nullable — only set for derived/synthesized events)
    /// Temporal policy used for `ts_orig` derivation
    pub temporal_policy: Option<String>,
    /// Version of the producer logic that produced this event
    pub semantics_version: Option<String>,
    /// Scope identifier for scope-reconciler replacement
    pub scope_key: Option<ScopeKey>,
    /// Output slot identifier for targeted replacement
    pub equivalence_key: Option<EquivalenceKey>,
    /// Which replay/operation created this event
    pub created_by_operation_id: Option<Uuid>,
    /// Which automaton model produced this event
    pub automaton_model: Option<String>,
}

/// Result of a stream batch insert operation.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreamBatchInsertResult {
    /// Number of rows successfully inserted
    pub inserted_count: usize,
    /// IDs of events that were actually inserted (excludes conflicts).
    /// Only populated when using ON CONFLICT DO NOTHING.
    pub inserted_ids: Option<Vec<Uuid>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamBatchInsertStrategy {
    QueryBuilder,
    Copy,
    Derived,
}

/// Event payload schema record from the database.
///
/// Represents a JSON schema definition for validating event payloads from a specific `source/event_type` combination.
/// Schemas are versioned and can be marked inactive when superseded.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, FromRow)]
pub struct EventPayloadSchema {
    /// Unique schema identifier
    pub id: Id<EventPayloadSchema>,
    /// Event source (e.g., "fs-watcher")
    pub source: EventSource,
    /// Event type (e.g., "file.created")
    pub event_type: EventType,
    /// Semantic version of this schema
    pub schema_version: SchemaVersion,
    /// JSON Schema content for validation
    pub schema_content: JsonValue,
    /// Blake3 hash of the schema content for deduplication
    pub content_hash: String,
    /// Whether this schema is currently active for new events
    pub is_active: bool,
    /// Timestamp of the last update
    pub updated_at: Timestamp,
}

/// User annotation or note attached to an event.
///
/// Allows attaching arbitrary metadata, comments, or tags to events for analytical or investigative purposes.
#[derive(Debug, FromRow)]
pub struct EventAnnotation {
    /// Unique annotation identifier
    pub id: Id<EventAnnotation>,
    /// ID of the event being annotated
    pub event_id: Id<Event<JsonValue>>,
    /// Type/category of the annotation (e.g., "comment", "tag", "flag")
    pub annotation_type: String,
    /// Annotation content or text
    pub content: String,
    /// Additional structured metadata for the annotation
    pub metadata: JsonValue,
    /// User or system that created this annotation
    pub created_by: String,
    /// Timestamp when the annotation was created
    pub created_at: Timestamp,
    /// Timestamp of the last update to this annotation
    pub updated_at: Timestamp,
}

/// Record of an event with a payload that failed validation against its schema.
#[derive(Debug)]
pub struct InvalidPayloadEvent {
    /// ID of the event with invalid payload
    pub event_id: Id<Event<JsonValue>>,
    /// Event source
    pub source: EventSource,
    /// Event type
    pub event_type: EventType,
    /// Ingestion timestamp
    pub ts_coided: Timestamp,
    /// The invalid JSON payload
    pub payload: JsonValue,
}

/// Record indicating a violation of event ordering constraints within a batch.
///
/// Used to detect temporal anomalies where events from the same source arrive out of order.
#[derive(Debug, FromRow)]
pub struct BatchViolation {
    /// ID of the event with the constraint violation
    pub event_id: Option<Id<Event<JsonValue>>>,
    /// ID of the previous event in the sequence
    pub prev_event_id: Option<Id<Event<JsonValue>>>,
    /// Original timestamp of the current event
    pub ts_orig: Option<Timestamp>,
    /// Original timestamp of the previous event
    pub prev_ts_orig: Option<Timestamp>,
    /// Event source
    pub source: EventSource,
    /// Row number in the batch where violation occurred
    pub row_num: Option<i64>,
}

/// Record of an event flagged as suspicious based on anomaly detection.
///
/// Used to identify unusual events that may indicate malicious activity or data quality issues.
#[derive(Debug, FromRow)]
pub struct SuspiciousEvent {
    /// ID of the suspicious event
    pub event_id: Id<Event<JsonValue>>,
    /// Event source
    pub source: EventSource,
    /// Event type
    pub event_type: EventType,
    /// Event payload
    pub payload: JsonValue,
    /// Detected payload type (if analyzable)
    pub payload_type: Option<String>,
    /// Size of the payload in bytes
    pub payload_size: Option<i32>,
}

/// Record of an event with a timestamp that violates business rules or constraints.
#[derive(Debug)]
pub struct InvalidTimestamp {
    /// ID of the event with invalid timestamp
    pub event_id: Id<Event<JsonValue>>,
    /// Original event timestamp (may be None or invalid)
    pub ts_orig: Option<Timestamp>,
    /// Ingestion timestamp (typically valid)
    pub ts_coided: Timestamp,
}

/// Source table for cascade graph traversal operations.
///
/// The cascade graph can be expanded from either the live event store
/// (`core.events`) or the archive (`audit.archived_events`). This enum
/// makes callers explicit and allows the pair of populate/expand methods
/// to be unified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CascadeSource {
    /// Traverse from live events in `core.events`.
    Live,
    /// Traverse from archived events in `audit.archived_events`.
    Archive,
}

impl CascadeSource {
    pub(super) fn table_name(self) -> &'static str {
        match self {
            CascadeSource::Live => "core.events",
            CascadeSource::Archive => "audit.archived_events",
        }
    }
}
