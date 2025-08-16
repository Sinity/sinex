//! Blob model for binary large object storage

use crate::types::Id;
use crate::BlobRecord;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Blob represents a binary large object stored in git-annex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Blob {
    pub id: Id<Blob>,
    pub annex_backend: String, // e.g., "SHA256E"
    pub content_hash: String,  // The hash value
    pub original_filename: Option<String>,
    pub size_bytes: i64,
    pub mime_type: Option<String>,
    pub checksum_blake3: Option<String>,
    pub metadata: Option<JsonValue>,
    pub created_at: DateTime<Utc>,
    pub last_verified_at: Option<DateTime<Utc>>,
    pub verification_status: Option<String>,
}

impl Blob {
    /// Construct the git-annex key from components
    /// Format: BACKEND-sSize--filename (e.g., SHA256E-s12345--filename.ext)
    pub fn annex_key(&self) -> String {
        let filename = self.original_filename.as_deref().unwrap_or("file");
        format!("{}-s{}--{}", self.annex_backend, self.size_bytes, filename)
    }

    /// Parse an annex key into its components
    pub fn parse_annex_key(key: &str) -> Option<(String, i64, String)> {
        let parts: Vec<&str> = key.split("--").collect();
        if parts.len() != 2 {
            return None;
        }

        let prefix_parts: Vec<&str> = parts[0].split("-s").collect();
        if prefix_parts.len() != 2 {
            return None;
        }

        let backend = prefix_parts[0].to_string();
        let size = prefix_parts[1].parse::<i64>().ok()?;
        let filename = parts[1].to_string();

        Some((backend, size, filename))
    }
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
    annex_backend: Option<String>,
    content_hash: Option<String>,
    original_filename: Option<String>,
    size_bytes: Option<i64>,
    mime_type: Option<String>,
    checksum_blake3: Option<String>,
    metadata: Option<JsonValue>,
}

impl BlobBuilder {
    pub fn annex_backend(mut self, backend: String) -> Self {
        self.annex_backend = Some(backend);
        self
    }

    pub fn content_hash(mut self, hash: String) -> Self {
        self.content_hash = Some(hash);
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

    pub fn checksum_blake3(mut self, checksum: String) -> Self {
        self.checksum_blake3 = Some(checksum);
        self
    }

    pub fn metadata(mut self, metadata: JsonValue) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn build(self) -> Blob {
        Blob {
            id: Id::new(),
            annex_backend: self.annex_backend.unwrap_or_else(|| "SHA256E".to_string()),
            content_hash: self.content_hash.unwrap_or_default(),
            original_filename: self.original_filename,
            size_bytes: self.size_bytes.unwrap_or(0),
            mime_type: self.mime_type,
            checksum_blake3: self.checksum_blake3,
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
            id: blob.id.into(), // Convert Id<Blob> to Ulid
            annex_backend: blob.annex_backend,
            content_hash: blob.content_hash,
            original_filename: blob.original_filename.unwrap_or_default(),
            size_bytes: blob.size_bytes,
            mime_type: blob.mime_type,
            checksum_blake3: blob.checksum_blake3,
            metadata: blob.metadata,
            created_at: blob.created_at,
            last_verified_at: blob.last_verified_at,
            verification_status: blob.verification_status,
        }
    }
}

/// Convert from BlobRecord to Blob for domain operations
impl From<BlobRecord> for Blob {
    fn from(record: BlobRecord) -> Self {
        Blob {
            id: Id::from_ulid(record.id),
            annex_backend: record.annex_backend,
            content_hash: record.content_hash,
            original_filename: if record.original_filename.is_empty() {
                None
            } else {
                Some(record.original_filename)
            },
            size_bytes: record.size_bytes,
            mime_type: record.mime_type,
            checksum_blake3: record.checksum_blake3,
            metadata: record.metadata,
            created_at: record.created_at,
            last_verified_at: record.last_verified_at,
            verification_status: record.verification_status,
        }
    }
}
