//! Event record types for database operations

use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

/// Record type representing an event row in the database
#[derive(Debug, Clone, FromRow)]
pub struct EventRecord {
    pub id: uuid::Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub ts_orig: Option<DateTime<Utc>>,
    pub source: String,
    pub event_type: String,
    pub payload: JsonValue,
    #[sqlx(rename = "payload_schema_id")]
    pub payload_schema_id: Option<uuid::Uuid>,
    pub processed_at: Option<DateTime<Utc>>,
    pub source_event_ids: Option<Vec<uuid::Uuid>>,
    pub source_material_id: Option<uuid::Uuid>,
    pub processor_name: Option<String>,
    pub processor_version: Option<String>,
    pub associated_blob_ids: Option<Vec<uuid::Uuid>>,
    pub event_cluster_id: Option<uuid::Uuid>,
}