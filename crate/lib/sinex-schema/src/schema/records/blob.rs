//! Blob record types for database operations

use crate::ids::Id;
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// Forward declare Blob type for Id<T>
pub struct Blob;

/// Record type representing a blob row in the database
#[derive(Debug, Clone, FromRow)]
pub struct BlobRecord {
    pub id: uuid::Uuid, // Still UUID at boundary for SQLx compatibility
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub content_hash: String,
    pub size_bytes: i64,
    pub stored_at: Option<DateTime<Utc>>,
    pub content_type: Option<String>,
    pub metadata: Option<JsonValue>,
}

impl BlobRecord {
    /// Get the ID as a strongly-typed Id<Blob>
    pub fn typed_id(&self) -> Id<Blob> {
        Id::from_uuid(self.id)
    }
}
