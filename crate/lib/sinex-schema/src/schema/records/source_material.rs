//! Source material record types for database operations

use crate::ids::Id;
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// Forward declare types for Id<T>
pub struct SourceMaterial;
pub struct Blob;

/// Record type representing a source material row in the database
#[derive(Debug, Clone, FromRow)]
pub struct SourceMaterialRecord {
    pub id: uuid::Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub material_type: String,
    pub source_path: Option<String>,
    pub content_hash: Option<String>,
    pub size_bytes: Option<i64>,
    pub metadata: Option<JsonValue>,
    #[sqlx(rename = "optional_blob_id")]
    pub blob_id: Option<uuid::Uuid>,
}

impl SourceMaterialRecord {
    /// Get the ID as a strongly-typed Id<SourceMaterial>
    pub fn typed_id(&self) -> Id<SourceMaterial> {
        Id::from_uuid(self.id)
    }

    /// Get the blob ID as strongly-typed
    pub fn typed_blob_id(&self) -> Option<Id<Blob>> {
        self.blob_id.map(Id::from_uuid)
    }
}
