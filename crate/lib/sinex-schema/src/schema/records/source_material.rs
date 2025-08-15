//! Source material record types for database operations

use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::FromRow;
use uuid::Uuid;

/// Record type representing a source material row in the database
///
/// This type uses UUID for database compatibility with PostgreSQL.
/// Convert to domain types using conversion functions in repositories.
#[derive(Debug, Clone, FromRow)]
pub struct SourceMaterialRecord {
    pub id: Uuid,
    pub checksum: Option<String>,
    pub source_identifier: String,
    pub source_type: String,
    pub source_path: Option<String>,
    pub content_type: Option<String>,
    pub status: String,
    pub total_bytes: Option<i64>,
    pub created_at: DateTime<Utc>,
    pub finalized_at: Option<DateTime<Utc>>,
    pub staged_at: Option<DateTime<Utc>>,
    pub metadata: Option<JsonValue>,
    pub data: Option<Vec<u8>>,
    pub optional_blob_id: Option<Uuid>,
    pub material_type: String,
    pub content_preview: Option<String>,
    pub source_uri: String,
    pub encoding: String,
    pub is_archived: bool,
    pub retention_policy: Option<String>,
    pub ingestion_time: Option<DateTime<Utc>>,
    pub archive_time: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

impl SourceMaterialRecord {
    /// Get the source material ID as UUID (raw database value)
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Get the blob ID as UUID
    pub fn blob_id(&self) -> Option<Uuid> {
        self.optional_blob_id
    }
}
