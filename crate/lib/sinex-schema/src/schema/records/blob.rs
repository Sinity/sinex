//! Blob record types for database operations

use crate::ulid::Ulid;
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

/// Record type representing a blob row in the database
#[derive(Debug, Clone, FromRow)]
pub struct BlobRecord {
    pub id: Ulid,
    pub annex_key: String,
    pub original_filename: Option<String>,
    pub size_bytes: i64,
    pub mime_type: Option<String>,
    pub checksum_sha256: Option<String>,
    pub checksum_blake3: Option<String>,
    pub storage_backend: String,
    pub metadata: Option<JsonValue>,
    pub created_at: DateTime<Utc>,
    pub last_verified_at: Option<DateTime<Utc>>,
    pub verification_status: Option<String>,
}

impl BlobRecord {
    /// Get the blob ID as raw ULID
    pub fn id(&self) -> Ulid {
        self.id
    }
}
