//! Blob record types for database operations

use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::FromRow;
use uuid::Uuid;

/// Record type representing a blob row in the database
///
/// This type uses UUID for database compatibility with PostgreSQL.
/// Convert to domain types using conversion functions in repositories.
#[derive(Debug, Clone, FromRow)]
pub struct BlobRecord {
    pub id: Uuid,
    pub annex_key: String,
    pub original_filename: Option<String>,
    pub size_bytes: i64,
    pub mime_type: Option<String>,
    pub checksum_sha256: String,
    pub checksum_blake3: String,
    pub storage_backend: String,
    pub metadata: Option<JsonValue>,
    pub created_at: DateTime<Utc>,
    pub last_verified_at: Option<DateTime<Utc>>,
    pub verification_status: String,
    // Legacy fields for compatibility
    pub updated_at: Option<DateTime<Utc>>,
    pub content_hash: Option<String>,
    pub stored_at: Option<DateTime<Utc>>,
    pub content_type: Option<String>,
}

impl BlobRecord {
    /// Get the blob ID as UUID (raw database value)
    pub fn id(&self) -> Uuid {
        self.id
    }
}
