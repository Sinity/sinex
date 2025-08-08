//! Blob model for core.blobs table
//!
//! Represents large binary objects stored in git-annex with metadata in PostgreSQL.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sinex_core::types::{ulid::Ulid, Id};

/// Blob metadata stored in core.blobs table
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, bon::Builder)]
pub struct Blob {
    /// Unique identifier for the blob
    #[builder(skip)]
    pub id: Option<Id<Blob>>,

    /// Git-annex key for the blob content
    pub annex_key: String,

    /// Original filename when the blob was created
    pub original_filename: String,

    /// Size of the blob in bytes
    pub size_bytes: i64,

    /// MIME type of the blob content
    pub mime_type: Option<String>,

    /// SHA256 checksum of the blob content
    pub checksum_sha256: String,

    /// BLAKE3 checksum of the blob content (for deduplication)
    pub checksum_blake3: Option<String>,

    /// Storage backend (default: 'git-annex')
    #[builder(default = "git-annex".to_string())]
    pub storage_backend: String,

    /// Additional metadata as JSON
    #[builder(default = serde_json::Value::Object(serde_json::Map::new()))]
    pub metadata: JsonValue,

    /// When the blob was created
    #[builder(skip)]
    pub created_at: DateTime<Utc>,

    /// When the blob was last verified
    pub last_verified_at: Option<DateTime<Utc>>,

    /// Verification status
    pub verification_status: Option<String>,
}

impl Blob {
    /// Create a new blob with the given ID
    pub fn with_id(mut self, id: Id<Blob>) -> Self {
        self.id = Some(id);
        self
    }

    /// Get the ID as a ULID, panicking if not set
    pub fn ulid(&self) -> Ulid {
        *self.id.as_ref().expect("Blob ID not set").as_ulid()
    }
}

/// Database representation of a blob record
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct BlobRecord {
    pub id: Ulid,
    pub annex_key: String,
    pub original_filename: String,
    pub size_bytes: i64,
    pub mime_type: Option<String>,
    pub checksum_sha256: String,
    pub checksum_blake3: Option<String>,
    pub storage_backend: String,
    pub metadata: JsonValue,
    pub created_at: DateTime<Utc>,
    pub last_verified_at: Option<DateTime<Utc>>,
    pub verification_status: Option<String>,
}

impl From<BlobRecord> for Blob {
    fn from(record: BlobRecord) -> Self {
        Blob {
            id: Some(Id::from_ulid(record.id)),
            annex_key: record.annex_key,
            original_filename: record.original_filename,
            size_bytes: record.size_bytes,
            mime_type: record.mime_type,
            checksum_sha256: record.checksum_sha256,
            checksum_blake3: record.checksum_blake3,
            storage_backend: record.storage_backend,
            metadata: record.metadata,
            created_at: record.created_at,
            last_verified_at: record.last_verified_at,
            verification_status: record.verification_status,
        }
    }
}

impl From<Blob> for BlobRecord {
    fn from(blob: Blob) -> Self {
        BlobRecord {
            id: blob.id.map(|id| *id.as_ulid()).unwrap_or_else(Ulid::new),
            annex_key: blob.annex_key,
            original_filename: blob.original_filename,
            size_bytes: blob.size_bytes,
            mime_type: blob.mime_type,
            checksum_sha256: blob.checksum_sha256,
            checksum_blake3: blob.checksum_blake3,
            storage_backend: blob.storage_backend,
            metadata: blob.metadata,
            created_at: blob.created_at,
            last_verified_at: blob.last_verified_at,
            verification_status: blob.verification_status,
        }
    }
}
