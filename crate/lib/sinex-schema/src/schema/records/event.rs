//! Event record types for database operations

use crate::ids::Id;
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// Forward declare types for Id<T>
pub struct Event;
pub struct SourceMaterial;

/// Record type representing an event row in the database
#[derive(Debug, Clone, FromRow)]
pub struct EventRecord {
    pub id: uuid::Uuid,
    pub ts_ingest: DateTime<Utc>, // Generated column from ULID
    pub ts_orig: Option<DateTime<Utc>>,
    pub source: String,
    pub event_type: String,
    pub host: String,
    pub payload: JsonValue,
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<uuid::Uuid>,
    pub payload_schema_name: Option<String>,
    pub payload_schema_version: Option<String>,

    // Provenance fields
    pub source_event_ids: Option<Vec<uuid::Uuid>>,
    pub source_material_id: Option<uuid::Uuid>,
    pub source_material_offset_start: Option<i64>,
    pub source_material_offset_end: Option<i64>,
    pub anchor_byte: Option<i64>,

    // Associated data
    pub associated_blob_ids: Option<Vec<uuid::Uuid>>,
}

impl EventRecord {
    /// Get the ID as a strongly-typed Id<Event>
    pub fn typed_id(&self) -> Id<Event> {
        Id::from_uuid(self.id)
    }

    /// Get the source material ID as strongly-typed
    pub fn typed_source_material_id(&self) -> Option<Id<SourceMaterial>> {
        self.source_material_id.map(Id::from_uuid)
    }

    /// Get the source event IDs as strongly-typed
    pub fn typed_source_event_ids(&self) -> Option<Vec<Id<Event>>> {
        self.source_event_ids
            .as_ref()
            .map(|ids| ids.iter().map(|&id| Id::from_uuid(id)).collect())
    }
}
