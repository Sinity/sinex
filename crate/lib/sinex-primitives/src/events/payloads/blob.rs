//! Blob storage event payloads

use crate::Timestamp;
use crate::domain::BlobVerificationStatus;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "blob_storage", event_type = "blob.retrieved")]
pub struct BlobRetrievedPayload {
    pub blob_id: String,
    pub retrieval_time_ms: u64,
    pub cache_hit: bool,
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
    pub verification_status: BlobVerificationStatus,
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

// Test helpers for external tests
#[cfg(any(test, feature = "testing"))]
impl BlobIngestedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            blob_id: "test-blob-id".into(),
            size_bytes: 0,
            mime_type: None,
            checksum_blake3: "test-checksum".into(),
            deduplicated: false,
            original_filename: "test-file".into(),
        }
    }
}
