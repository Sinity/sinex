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
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub ts_orig: Option<DateTime<Utc>>,
    pub source: String,
    pub event_type: String,
    pub host: String, // Missing field
    pub payload: JsonValue,
    #[sqlx(rename = "payload_schema_id")]
    pub payload_schema_id: Option<uuid::Uuid>,
    pub processed_at: Option<DateTime<Utc>>,

    // Provenance fields
    pub source_event_ids: Option<Vec<uuid::Uuid>>,
    pub source_material_id: Option<uuid::Uuid>,
    pub source_material_offset_start: Option<i64>, // Missing field
    pub source_material_offset_end: Option<i64>,   // Missing field
    pub anchor_byte: Option<i64>,                  // Missing field

    pub ingestor_version: Option<String>, // Missing field
    pub processor_name: Option<String>,
    pub processor_version: Option<String>,
    pub associated_blob_ids: Option<Vec<uuid::Uuid>>,
    pub event_cluster_id: Option<uuid::Uuid>,
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
