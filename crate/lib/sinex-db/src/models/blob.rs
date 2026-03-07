//! Blob model for binary large object storage

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sinex_primitives::Id;
use sinex_primitives::Timestamp;
use sinex_primitives::domain::BlobVerificationStatus;
use sinex_schema::schema::BlobRecord;

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
    pub created_at: Timestamp,
    pub last_verified_at: Option<Timestamp>,
    pub verification_status: Option<BlobVerificationStatus>,
}

impl Blob {
    /// Construct the git-annex key from components
    /// Format: BACKEND-sSize--hash_fragment (e.g., SHA256E-s12345--abcdef123)
    #[must_use]
    pub fn annex_key(&self) -> String {
        let hash_fragment = if self.content_hash.is_empty() {
            self.original_filename
                .as_deref()
                .unwrap_or("content")
                .to_string()
        } else {
            self.content_hash.clone()
        };

        format!(
            "{}-s{}--{}",
            self.annex_backend, self.size_bytes, hash_fragment
        )
    }

    /// Parse an annex key into its components
    #[must_use]
    pub fn parse_annex_key(key: &str) -> Option<(String, i64, String)> {
        let mut segments = key.splitn(2, "--");
        let prefix = segments.next()?;
        let hash_fragment = segments.next()?.to_string();

        let mut prefix_parts = prefix.splitn(2, "-s");
        let backend = prefix_parts.next()?.to_string();
        let size_str = prefix_parts.next()?;
        let size = size_str.parse::<i64>().ok()?;

        Some((backend, size, hash_fragment))
    }
}

impl Blob {
    /// Create a new blob builder
    #[must_use]
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
    #[must_use]
    pub fn annex_backend(mut self, backend: String) -> Self {
        self.annex_backend = Some(backend);
        self
    }

    #[must_use]
    pub fn content_hash(mut self, hash: String) -> Self {
        self.content_hash = Some(hash);
        self
    }

    #[must_use]
    pub fn original_filename(mut self, filename: String) -> Self {
        self.original_filename = Some(filename);
        self
    }

    #[must_use]
    pub fn size_bytes(mut self, size: i64) -> Self {
        self.size_bytes = Some(size);
        self
    }

    #[must_use]
    pub fn mime_type(mut self, mime: String) -> Self {
        self.mime_type = Some(mime);
        self
    }

    #[must_use]
    pub fn checksum_blake3(mut self, checksum: String) -> Self {
        self.checksum_blake3 = Some(checksum);
        self
    }

    #[must_use]
    pub fn metadata(mut self, metadata: JsonValue) -> Self {
        self.metadata = Some(metadata);
        self
    }

    #[must_use]
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
            created_at: Timestamp::now(),
            last_verified_at: None,
            verification_status: None,
        }
    }
}

/// Convert from Blob to `BlobRecord` for database operations
impl From<Blob> for BlobRecord {
    fn from(blob: Blob) -> Self {
        BlobRecord {
            id: blob.id.into(), // Convert Id<Blob> to Uuid
            annex_backend: blob.annex_backend,
            content_hash: blob.content_hash,
            original_filename: blob.original_filename.unwrap_or_default(),
            size_bytes: blob.size_bytes,
            mime_type: blob.mime_type,
            checksum_blake3: blob.checksum_blake3,
            metadata: blob
                .metadata
                .unwrap_or(serde_json::Value::Object(serde_json::Map::default())),
            created_at: blob.created_at,
            last_verified_at: blob.last_verified_at,
            verification_status: blob.verification_status.map(|s| s.to_string()),
        }
    }
}

/// Convert from `BlobRecord` to Blob for domain operations
impl From<BlobRecord> for Blob {
    fn from(record: BlobRecord) -> Self {
        use std::str::FromStr;
        Blob {
            id: Id::from_uuid(record.id),
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
            metadata: Some(record.metadata),
            created_at: record.created_at,
            last_verified_at: record.last_verified_at,
            verification_status: record
                .verification_status
                .as_deref()
                .and_then(|s| BlobVerificationStatus::from_str(s).ok()),
        }
    }
}
