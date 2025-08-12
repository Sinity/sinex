//! Event record types for database operations

use crate::ulid::Ulid;
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

/// Record type representing an event row in the database
#[derive(Debug, Clone, FromRow)]
pub struct EventRecord {
    pub id: Ulid,
    pub ts_ingest: DateTime<Utc>, // Generated column from ULID
    pub ts_orig: Option<DateTime<Utc>>,
    pub source: String,
    pub event_type: String,
    pub host: String,
    pub payload: JsonValue,
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<Ulid>,
    pub payload_schema_name: Option<String>,
    pub payload_schema_version: Option<String>,

    // Provenance fields
    pub source_event_ids: Option<Vec<Ulid>>,
    pub source_material_id: Option<Ulid>,
    pub source_material_offset_start: Option<i64>,
    pub source_material_offset_end: Option<i64>,
    pub anchor_byte: Option<i64>,

    // Associated data
    pub associated_blob_ids: Option<Vec<Ulid>>,
}

impl EventRecord {
    /// Get the event ID as raw ULID
    pub fn id(&self) -> Ulid {
        self.id
    }

    /// Get the source material ID
    pub fn source_material_id(&self) -> Option<Ulid> {
        self.source_material_id
    }

    /// Get the source event IDs
    pub fn source_event_ids(&self) -> Option<&[Ulid]> {
        self.source_event_ids.as_deref()
    }

    /// Get the associated blob IDs
    pub fn associated_blob_ids(&self) -> Option<&[Ulid]> {
        self.associated_blob_ids.as_deref()
    }
}
