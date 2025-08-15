//! Event record types for database operations

use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sinex_macros::ValidateRecord;
use sqlx::FromRow;
use uuid::Uuid;

/// Record type representing an event row in the database
///
/// This type uses UUID for database compatibility with PostgreSQL.
/// Convert to domain types using EventRecordExt trait in repositories.
#[derive(Debug, Clone, FromRow, ValidateRecord)]
#[validate_against(crate::schema::core_events::Events)]
pub struct EventRecord {
    pub id: Uuid,
    pub ts_ingest: DateTime<Utc>, // Generated column from ULID
    pub ts_orig: Option<DateTime<Utc>>,
    pub source: String,
    pub event_type: String,
    pub host: String,
    pub payload: JsonValue,
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<Uuid>,
    pub payload_schema_name: Option<String>,
    pub payload_schema_version: Option<String>,

    // Provenance fields
    pub source_event_ids: Option<Vec<Uuid>>,
    pub source_material_id: Option<Uuid>,
    pub source_material_offset_start: Option<i64>,
    pub source_material_offset_end: Option<i64>,
    pub anchor_byte: Option<i64>,

    // Associated data
    pub associated_blob_ids: Option<Vec<Uuid>>,
}

impl EventRecord {
    /// Get the event ID as UUID (raw database value)
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Get the source material ID as UUID
    pub fn source_material_id(&self) -> Option<Uuid> {
        self.source_material_id
    }

    /// Get the source event IDs as UUIDs
    pub fn source_event_ids(&self) -> Option<&[Uuid]> {
        self.source_event_ids.as_deref()
    }

    /// Get the associated blob IDs as UUIDs
    pub fn associated_blob_ids(&self) -> Option<&[Uuid]> {
        self.associated_blob_ids.as_deref()
    }
}
