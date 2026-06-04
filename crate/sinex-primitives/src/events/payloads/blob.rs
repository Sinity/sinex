//! Blob storage event payloads

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

// ─────────────────────────────────────────────────────────────────────────────
// Source descriptor for blob storage infra events.
//
// `blob_storage` is not a normal ingestor source — it is sinex's
// content-addressable BLOB store, which emits operational events as it
// retrieves, ingests, verifies blobs and reports aggregate statistics. We
// register a descriptor so the (source, event_type) pairs declared via
// `#[event_payload(...)]` are claimed by the descriptor inventory used by
// `sinexctl verify --sources`. The descriptor has no `SourceRuntimeBinding`
// because there is no per-host service named "blob storage"; these events are
// produced from inside long-running sinex processes.
// ─────────────────────────────────────────────────────────────────────────────

use crate::proof::{
    CheckpointFamily as SuCheckpointFamily, Horizon as SuHorizon,
    OccurrenceIdentity as SuOccurrenceIdentity, PrivacyTier as SuPrivacyTier,
    RetentionPolicy as SuRetentionPolicy, RuntimeShape as SuRuntimeShape, SourceRuntimeBinding,
    SourceBuildImpact, SourceContract, SubjectRef,
};
use crate::{register_source_contract, register_source_runtime_binding};

register_source_contract! {
    SourceContract {
        id: "blob-storage",
        namespace: "infra",
        event_types: &[
            ("blob_storage", "blob.retrieved"),
            ("blob_storage", "blob.ingested"),
            ("blob_storage", "blob.verified"),
            ("blob_storage", "storage.statistics"),
        ],
        privacy_tier: SuPrivacyTier::Sensitive,
        horizons: &[SuHorizon::Continuous],
        retention: SuRetentionPolicy::Forever,
        occurrence_identity: SuOccurrenceIdentity::Natural,
        access_policy: "embedded_in_pipeline_processes",
    }
}

// Infra source: descriptor-only by design — events emitted from inside
// other binaries (event_engine, gateway, node SDK). The binding records the
// embedded shape; `proposed: true` flags it as not a host-level adapter.
register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:blob-storage"),
        "blob-storage",
        "infra",
    )
    .implementation("sinex-primitives::blob")
    .adapter("EmbeddedEmitter")
    .output_event_type("blob.retrieved")
    .privacy_context("blob_metadata")
    .material_policy("none")
    .checkpoint_policy("live_observation")
    .resource_shape("embedded_emitter")
    .source_id("blob-storage")
    .proposed(true)
    .runner_pack("infra")
    .checkpoint_family(SuCheckpointFamily::LiveObservation)
    .runtime_shape(SuRuntimeShape::Continuous)
    .package_impact("no_new_output")
    .implementation_mode("rust_in_pipeline_processes")
    .build_impact(SourceBuildImpact::ZERO)
    .build()
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
