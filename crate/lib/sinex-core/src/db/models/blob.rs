//! Blob model for binary large object storage

use crate::{BlobRecord, Id, Ulid};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Blob represents a binary large object stored in git-annex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Blob {
    pub id: Id<Blob>,
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

impl Blob {
    /// Create a new blob builder
    pub fn builder() -> BlobBuilder {
        BlobBuilder::default()
    }
}

/// Builder for creating new Blob instances
#[derive(Default)]
pub struct BlobBuilder {
    annex_key: Option<String>,
    original_filename: Option<String>,
    size_bytes: Option<i64>,
    mime_type: Option<String>,
    checksum_sha256: Option<String>,
    checksum_blake3: Option<String>,
    storage_backend: Option<String>,
    metadata: Option<JsonValue>,
}

impl BlobBuilder {
    pub fn annex_key(mut self, key: String) -> Self {
        self.annex_key = Some(key);
        self
    }

    pub fn original_filename(mut self, filename: String) -> Self {
        self.original_filename = Some(filename);
        self
    }

    pub fn size_bytes(mut self, size: i64) -> Self {
        self.size_bytes = Some(size);
        self
    }

    pub fn mime_type(mut self, mime: String) -> Self {
        self.mime_type = Some(mime);
        self
    }

    pub fn checksum_sha256(mut self, checksum: String) -> Self {
        self.checksum_sha256 = Some(checksum);
        self
    }

    pub fn checksum_blake3(mut self, checksum: String) -> Self {
        self.checksum_blake3 = Some(checksum);
        self
    }

    pub fn storage_backend(mut self, backend: String) -> Self {
        self.storage_backend = Some(backend);
        self
    }

    pub fn metadata(mut self, metadata: JsonValue) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn build(self) -> Blob {
        Blob {
            id: Id::new(),
            annex_key: self.annex_key.unwrap_or_default(),
            original_filename: self.original_filename,
            size_bytes: self.size_bytes.unwrap_or(0),
            mime_type: self.mime_type,
            checksum_sha256: self.checksum_sha256,
            checksum_blake3: self.checksum_blake3,
            storage_backend: self
                .storage_backend
                .unwrap_or_else(|| "git-annex".to_string()),
            metadata: self.metadata,
            created_at: Utc::now(),
            last_verified_at: None,
            verification_status: None,
        }
    }
}

/// Convert from Blob to BlobRecord for database operations
impl From<Blob> for BlobRecord {
    fn from(blob: Blob) -> Self {
        BlobRecord {
            id: blob.id.to_uuid(),
            annex_key: blob.annex_key,
            original_filename: blob.original_filename,
            size_bytes: blob.size_bytes,
            mime_type: blob.mime_type.clone(),
            checksum_sha256: blob.checksum_sha256.clone().unwrap_or_default(),
            checksum_blake3: blob.checksum_blake3.clone().unwrap_or_default(),
            storage_backend: blob.storage_backend,
            metadata: blob.metadata,
            created_at: blob.created_at,
            last_verified_at: blob.last_verified_at,
            verification_status: blob
                .verification_status
                .clone()
                .unwrap_or_else(|| "unverified".to_string()),
            // Legacy fields
            updated_at: Some(blob.created_at),
            content_hash: blob.checksum_sha256.clone(),
            stored_at: blob.last_verified_at,
            content_type: blob.mime_type,
        }
    }
}

/// Convert from BlobRecord to Blob for domain operations
impl From<BlobRecord> for Blob {
    fn from(record: BlobRecord) -> Self {
        Blob {
            id: Id::from_uuid(record.id),
            annex_key: record.annex_key,
            original_filename: record.original_filename,
            size_bytes: record.size_bytes,
            mime_type: record.mime_type,
            checksum_sha256: if record.checksum_sha256.is_empty() {
                None
            } else {
                Some(record.checksum_sha256)
            },
            checksum_blake3: if record.checksum_blake3.is_empty() {
                None
            } else {
                Some(record.checksum_blake3)
            },
            storage_backend: record.storage_backend,
            metadata: record.metadata,
            created_at: record.created_at,
            last_verified_at: record.last_verified_at,
            verification_status: Some(record.verification_status),
        }
    }
}
