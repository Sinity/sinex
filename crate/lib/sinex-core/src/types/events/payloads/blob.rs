//! Blob storage event payloads

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "blob_storage", event_type = "blob.stored")]
pub struct BlobStoredPayload {
    pub blob_id: String,
    pub content_type: String,
    pub size_bytes: u64,
    pub hash_sha256: String,
    pub stored_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "blob_storage", event_type = "blob.retrieved")]
pub struct BlobRetrievedPayload {
    pub blob_id: String,
    pub retrieval_time_ms: u64,
    pub cache_hit: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "blob_storage", event_type = "blob.deleted")]
pub struct BlobDeletedPayload {
    pub blob_id: String,
    pub deletion_reason: String,
    pub freed_bytes: u64,
}

// Operation events with blob context

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "blob_storage", event_type = "blob.ingested")]
pub struct BlobIngestedPayload {
    pub blob_id: String,
    pub size_bytes: i64,
    pub mime_type: Option<String>,
    pub checksum_blake3: String,
    pub deduplicated: bool, // true if this was a duplicate
    pub original_filename: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "blob_storage", event_type = "blob.verified")]
pub struct BlobVerifiedPayload {
    pub blob_id: String,
    pub verification_status: String, // "verified", "corrupted"
    pub checksum_matched: bool,
}

// Aggregate statistics (no specific blob)

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "blob_storage", event_type = "storage.statistics")]
pub struct StorageStatisticsPayload {
    pub total_blobs: i64,
    pub total_size_bytes: i64,
    pub failed_verifications: i64,
    pub storage_backend: String, // "git-annex"
}

impl BlobStoredPayload {
    /// Builder-style method for content type
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }

    /// Builder-style method for size
    pub fn with_size_bytes(mut self, size: u64) -> Self {
        self.size_bytes = size;
        self
    }

    /// Builder-style method for stored timestamp
    pub fn with_stored_at(mut self, timestamp: DateTime<Utc>) -> Self {
        self.stored_at = timestamp;
        self
    }
}

impl BlobRetrievedPayload {
    /// Builder-style method for retrieval time
    pub fn with_retrieval_time_ms(mut self, time_ms: u64) -> Self {
        self.retrieval_time_ms = time_ms;
        self
    }

    /// Builder-style method for cache hit
    pub fn with_cache_hit(mut self, cache_hit: bool) -> Self {
        self.cache_hit = cache_hit;
        self
    }
}

impl BlobDeletedPayload {
    /// Builder-style method for deletion reason
    pub fn with_deletion_reason(mut self, reason: impl Into<String>) -> Self {
        self.deletion_reason = reason.into();
        self
    }

    /// Builder-style method for freed bytes
    pub fn with_freed_bytes(mut self, bytes: u64) -> Self {
        self.freed_bytes = bytes;
        self
    }
}

impl BlobIngestedPayload {
    /// Builder-style method for size
    pub fn with_size_bytes(mut self, size: i64) -> Self {
        self.size_bytes = size;
        self
    }

    /// Builder-style method for MIME type
    pub fn with_mime_type(mut self, mime_type: impl Into<String>) -> Self {
        self.mime_type = Some(mime_type.into());
        self
    }

    /// Builder-style method for deduplicated flag
    pub fn with_deduplicated(mut self, deduplicated: bool) -> Self {
        self.deduplicated = deduplicated;
        self
    }
}

impl BlobVerifiedPayload {
    /// Builder-style method for verification status
    pub fn with_verification_status(mut self, status: impl Into<String>) -> Self {
        self.verification_status = status.into();
        self
    }

    /// Builder-style method for checksum matched
    pub fn with_checksum_matched(mut self, matched: bool) -> Self {
        self.checksum_matched = matched;
        self
    }
}

impl StorageStatisticsPayload {
    /// Builder-style method for total blobs
    pub fn with_total_blobs(mut self, count: i64) -> Self {
        self.total_blobs = count;
        self
    }

    /// Builder-style method for total size
    pub fn with_total_size_bytes(mut self, size: i64) -> Self {
        self.total_size_bytes = size;
        self
    }

    /// Builder-style method for failed verifications
    pub fn with_failed_verifications(mut self, count: i64) -> Self {
        self.failed_verifications = count;
        self
    }

    /// Builder-style method for storage backend
    pub fn with_storage_backend(mut self, backend: impl Into<String>) -> Self {
        self.storage_backend = backend.into();
        self
    }
}
