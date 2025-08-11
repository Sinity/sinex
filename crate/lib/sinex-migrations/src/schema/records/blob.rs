//! Blob record types for database operations

use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

/// Record type representing a blob row in the database
#[derive(Debug, Clone, FromRow)]
pub struct BlobRecord {
    pub id: uuid::Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub content_hash: String,
    pub size_bytes: i64,
    pub stored_at: Option<DateTime<Utc>>,
    pub content_type: Option<String>,
    pub metadata: Option<JsonValue>,
}