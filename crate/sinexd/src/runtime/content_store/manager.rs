//! Content-store management utilities.
//!
//! The manager deduplicates incoming content, registers metadata in `core.blobs`,
//! wires provenance through `source_material` records, and emits ingestion/health
//! events that downstream services can rely on.
//!
//! See `README.md#architecture` (blob storage) and the
//! `sinex-schema` declarative schema definitions for canonical table constraints.

use crate::runtime::{RuntimeResult, SinexError};
use camino::{Utf8Path, Utf8PathBuf};
use serde_json::json;
use sinex_db::DbPool;
use sinex_db::DbPoolExt;
use sinex_db::SourceMaterialRecord;
use sinex_db::models::{Blob, SourceMaterial};
use sinex_db::repositories::source_materials::SourceMaterial as SourceMaterialRegistration;
use sinex_primitives::DynamicPayload;
use sinex_primitives::domain::BlobVerificationStatus;
use sinex_primitives::events::{
    BlobIngestedPayload, BlobRetrievedPayload, BlobVerifiedPayload, StorageStatisticsPayload,
};
use sinex_primitives::{
    ByteRange, DecodedMaterialManifest, Event, Id, JsonValue, MaterialManifestV1, Uuid,
};
use std::time::Instant;
use tracing::{debug, info, warn};

use super::{
    ContentStoreConfig, ContentStoreKey, LOCAL_BLAKE3_CAS_BACKEND, MaterialContentStore,
    path_validator::{VerifiedPath, create_secure_temp_path, validate_path_exists},
    run_command_async,
};
use tokio::io::AsyncWriteExt;
use tokio::process::Command as AsyncCommand;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

// Re-export Blob type for content-store consumers.
pub use sinex_db::models::Blob as BlobMetadata;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaterialReplayAuthority {
    ManifestV1,
    LegacyBlobFallback,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MaterialReplayContent {
    pub authority: MaterialReplayAuthority,
    pub bytes: Vec<u8>,
    manifest: Option<MaterialManifestV1>,
}

impl MaterialReplayContent {
    pub fn slice_range(&self, material_id: Uuid, range: ByteRange) -> RuntimeResult<Vec<u8>> {
        let selected = if let Some(manifest) = &self.manifest {
            manifest.exact_range(range).map_err(|error| {
                SinexError::validation(format!(
                    "manifest replay range validation failed for source material {material_id}: {error}"
                ))
            })?
        } else {
            let start = usize::try_from(range.start).map_err(|error| {
                SinexError::validation("legacy replay range start does not fit host index")
                    .with_source(error)
            })?;
            let end = usize::try_from(range.end).map_err(|error| {
                SinexError::validation("legacy replay range end does not fit host index")
                    .with_source(error)
            })?;
            if start >= end || end > self.bytes.len() {
                return Err(SinexError::validation(format!(
                    "legacy replay range is outside source material {material_id}"
                )));
            }
            start..end
        };
        Ok(self.bytes[selected].to_vec())
    }
}

/// Default capacity for content-store-manager event channels to prevent unbounded buffering.
pub const BLOB_EVENT_CHANNEL_CAPACITY: usize = 1024;

/// Owns the transient source path used while copying an in-memory upload into
/// the durable CAS.  The CAS copy is not the ownership transfer: every error
/// after this path is created must still remove the source file, including
/// post-write verification and metadata-registration failures.
struct TemporaryBlobPath(Utf8PathBuf);

impl TemporaryBlobPath {
    fn new(path: Utf8PathBuf) -> Self {
        Self(path)
    }

    fn as_path(&self) -> &Utf8Path {
        &self.0
    }
}

impl Drop for TemporaryBlobPath {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_file(self.0.as_std_path())
            && error.kind() != std::io::ErrorKind::NotFound
        {
            warn!(
                error = %error,
                path = %self.0,
                "Failed to remove temporary blob file after ingest"
            );
        }
    }
}

fn verification_status_persist_error(
    content_key: &str,
    status: BlobVerificationStatus,
    error: &SinexError,
) -> SinexError {
    SinexError::processing(format!(
        "failed to persist blob verification status for {content_key}"
    ))
    .with_context("verification_status", status)
    .with_source(error.to_string())
}

fn attach_verification_status_update_error(
    error: SinexError,
    status_error: &SinexError,
) -> SinexError {
    error.with_context("verification_status_update_error", status_error.to_string())
}

fn material_name_for_blob(blob: &Blob) -> String {
    blob.original_filename
        .as_deref()
        .filter(|filename| !filename.trim().is_empty())
        .map_or_else(|| blob.content_key(), ToOwned::to_owned)
}

fn content_hash_is_backend_digest(blob: &Blob) -> bool {
    blob.storage_backend != LOCAL_BLAKE3_CAS_BACKEND && !blob.content_hash.is_empty()
}

fn require_ingest_filename<'a>(
    validated_path: &'a Utf8Path,
    original_filename: Option<&'a str>,
) -> RuntimeResult<&'a str> {
    if let Some(filename) = original_filename.filter(|filename| !filename.trim().is_empty()) {
        return Ok(filename);
    }

    validated_path.file_name().ok_or_else(|| {
        SinexError::validation(format!(
            "Blob ingestion requires a file name, but path has no final component: {validated_path}"
        ))
    })
}

#[derive(Debug)]
pub struct ContentStoreManager {
    content_store: MaterialContentStore,
    db_pool: DbPool,
    event_sender: Option<mpsc::Sender<Event<JsonValue>>>,
}

impl ContentStoreManager {
    pub fn new(
        content_store_config: ContentStoreConfig,
        db_pool: DbPool,
        event_sender: Option<mpsc::Sender<Event<JsonValue>>>,
    ) -> RuntimeResult<Self> {
        let content_store = MaterialContentStore::new(content_store_config)
            .map_err(|e| SinexError::blob_storage(e).with_operation("initialize"))?;
        Ok(ContentStoreManager {
            content_store,
            db_pool,
            event_sender,
        })
    }

    async fn persist_verification_status(
        &self,
        content_key: &str,
        status: BlobVerificationStatus,
    ) -> RuntimeResult<()> {
        self.update_verification_status(content_key, status)
            .await
            .map_err(|error| verification_status_persist_error(content_key, status, &error))
    }

    /// Builds an event tied to the supplied source material.
    fn create_blob_event<T: serde::Serialize>(
        event_type: &str,
        payload: T,
        material_id: Id<SourceMaterial>,
    ) -> RuntimeResult<Event<JsonValue>> {
        let payload_value = serde_json::to_value(payload).map_err(SinexError::serialization)?;
        DynamicPayload::new("blob_storage", event_type, payload_value)
            .from_material(material_id)
            .build()
            .map_err(|err| {
                SinexError::processing(format!(
                    "Failed to build blob event: {err}\n  event_type: {event_type}\n  material_id: {material_id}"
                ))
            })
    }

    async fn ensure_material_for_blob(&self, blob: &Blob) -> RuntimeResult<Id<SourceMaterial>> {
        let repo = self.db_pool.source_materials();

        if let Some(existing) = repo.find_by_blob_id(blob.id).await? {
            return Ok(Id::<SourceMaterial>::from_uuid(existing.id));
        }

        let filename = material_name_for_blob(blob);

        let mut material = if blob
            .mime_type
            .as_deref()
            .is_some_and(|mime| mime.starts_with("text/"))
        {
            SourceMaterialRegistration::blob_text(filename.clone())
        } else {
            SourceMaterialRegistration::blob_binary(filename.clone())
        };

        if let Some(metadata) = &blob.metadata {
            material = material.with_metadata(metadata.clone());
        }

        if let Some(mime) = &blob.mime_type {
            material = material.with_metadata(json!({ "mime_type": mime }));
        }

        if let Some(checksum) = &blob.checksum_blake3 {
            material = material.with_metadata(json!({ "checksum_blake3": checksum }));
        }

        material = material.with_blob_id(blob.id).with_metadata(json!({
            "storage_backend": blob.storage_backend,
            "content_hash": blob.content_hash,
            "content_key": blob.content_key(),
            "size_bytes": blob.size_bytes,
        }));

        let record = repo.register_material(material).await.map_err(|e| {
            SinexError::processing("Failed to register source material for blob").with_source(e)
        })?;

        Ok(Id::<SourceMaterial>::from_uuid(record.id))
    }

    async fn publish_blob_event<T: serde::Serialize>(
        &self,
        event_type: &str,
        payload: T,
        blob: &Blob,
    ) -> RuntimeResult<()> {
        if let Some(sender) = &self.event_sender {
            let material_id = self.ensure_material_for_blob(blob).await?;
            let event = Self::create_blob_event(event_type, payload, material_id)?;

            match sender.try_send(event) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    warn!(
                        channel_capacity = BLOB_EVENT_CHANNEL_CAPACITY,
                        "ContentStoreManager event channel full; dropping {} event", event_type
                    );
                }
                Err(TrySendError::Closed(_)) => {
                    return Err(SinexError::processing(format!(
                        "Failed to emit {event_type} event: event channel closed"
                    )));
                }
            }
        } else {
            debug!(
                "ContentStoreManager event emission disabled; skipping {} notification",
                event_type
            );
        }
        Ok(())
    }

    /// Check for existing blob by BLAKE3 hash. If found, update the filename
    /// registry, emit a deduplicated event, and return the existing metadata.
    async fn check_dedup(
        &self,
        blake3_hash: &str,
        filename: &str,
    ) -> RuntimeResult<Option<BlobMetadata>> {
        let Some(existing) = self.find_blob_by_blake3(blake3_hash).await? else {
            return Ok(None);
        };

        let existing_key = existing.content_key().clone();

        // Defense-in-depth against zombie core.blobs rows
        // (sinex-audit-cas-zombie-blob-rows): a row can outlive its CAS file
        // (e.g. dropped by delete-on-tombstone once dereferenced). Trusting
        // such a row as a dedup hit would silently skip re-writing content
        // that is actually absent from disk -- the caller would believe the
        // blob is present when it permanently is not. Verify the local file
        // still exists before short-circuiting; if it's gone, fall through
        // so the caller re-stores the content and self-heals the row via the
        // ON CONFLICT (checksum_blake3) upsert in `register_new_blob`.
        if let Ok(Some(local_path)) = self.content_store.path_if_local(&existing_key)
            && !local_path.exists()
        {
            warn!(
                content_key = %existing_key,
                path = %local_path,
                "core.blobs row references a missing CAS file (zombie row); \
                 re-writing content instead of trusting the stale dedup hit"
            );
            return Ok(None);
        }

        info!("Content already exists in blob store with key: {existing_key}");

        self.add_original_filename(&existing_key, filename).await?;

        if let Err(e) = self
            .publish_blob_event(
                "blob.ingested",
                BlobIngestedPayload {
                    blob_id: existing_key,
                    size_bytes: existing.size_bytes,
                    mime_type: existing.mime_type.clone(),
                    checksum_blake3: blake3_hash.to_string(),
                    deduplicated: true,
                    original_filename: filename.to_string(),
                },
                &existing,
            )
            .await
        {
            warn!(error = %e, "Failed to emit blob.ingested event for deduplicated blob");
        }

        Ok(Some(existing))
    }

    /// Register a new blob after content has been added to the content store.
    /// Parses the content-store key, inserts the DB record, and emits the ingested event.
    async fn register_new_blob(
        &self,
        content_key: &ContentStoreKey,
        filename: &str,
        size_bytes: i64,
        mime_type: String,
        blake3_hash: String,
    ) -> RuntimeResult<BlobMetadata> {
        let (backend, _, _) =
            Blob::parse_content_store_key(&content_key.key).map_err(SinexError::processing)?;

        let blob = Blob::builder()
            .storage_backend(backend)
            .content_hash(content_key.digest.clone())
            .original_filename(filename.to_string())
            .size_bytes(size_bytes)
            .mime_type(mime_type.clone())
            .checksum_blake3(blake3_hash.clone())
            .build();

        let blob_metadata = self.insert_blob(&blob).await?;
        let blob_key = blob_metadata.content_key().clone();
        info!("Successfully ingested blob: {blob_key}");

        if let Err(e) = self
            .publish_blob_event(
                "blob.ingested",
                BlobIngestedPayload {
                    blob_id: blob_key,
                    size_bytes,
                    mime_type: Some(mime_type),
                    checksum_blake3: blake3_hash,
                    deduplicated: false,
                    original_filename: filename.to_string(),
                },
                &blob_metadata,
            )
            .await
        {
            warn!(error = %e, "Failed to emit blob.ingested event for new blob");
        }

        Ok(blob_metadata)
    }

    /// Ingest a file into the content store
    pub async fn ingest_file(
        &self,
        file_path: &VerifiedPath,
        original_filename: Option<&str>,
    ) -> RuntimeResult<BlobMetadata> {
        validate_path_exists(file_path.as_path())
            .map_err(|e| SinexError::blob_storage(e).with_operation("validate_path"))?;
        let validated_path = file_path.as_path();

        info!("Ingesting file: {:?}", validated_path);

        // Check max blob size before computing hash
        let file_metadata = tokio::fs::metadata(validated_path)
            .await
            .map_err(SinexError::io)?;
        self.content_store
            .ensure_file_size_allowed(file_metadata.len())?;
        let size_bytes = i64::try_from(file_metadata.len()).map_err(|error| {
            SinexError::blob_storage("blob size does not fit database metadata").with_source(error)
        })?;

        let blake3_hash = MaterialContentStore::compute_blake3_hash_with_limit(
            validated_path,
            self.content_store.config.max_blob_size,
        )
        .await
        .map_err(|e| SinexError::blob_storage(e).with_operation("compute_hash"))?;

        let effective_filename = require_ingest_filename(validated_path, original_filename)?;

        if let Some(existing) = self.check_dedup(&blake3_hash, effective_filename).await? {
            return Ok(existing);
        }

        let mime_type = Self::detect_mime_type(validated_path)
            .map_err(|e| SinexError::blob_storage(e).with_operation("detect_mime_type"))?;

        let content_key = self
            .content_store
            .store_file(validated_path)
            .await
            .map_err(|e| {
                SinexError::processing("Failed to add file to content store").with_source(e)
            })?;
        info!("Added to content store with key: {}", content_key.key);

        self.verify_post_write(&content_key.key, &blake3_hash)
            .await?;

        self.register_new_blob(
            &content_key,
            effective_filename,
            size_bytes,
            mime_type,
            blake3_hash,
        )
        .await
    }

    /// Ingest content from bytes (for in-memory content like clipboard)
    pub async fn ingest_from_bytes(
        &self,
        content: &[u8],
        filename: &str,
        content_type: &str,
    ) -> RuntimeResult<BlobMetadata> {
        info!("Ingesting {} bytes as {}", content.len(), filename);

        // Check max blob size
        let max_size = self.content_store.config.max_blob_size;
        if max_size > 0 && content.len() > max_size {
            return Err(SinexError::blob_storage(format!(
                "blob size {} exceeds limit {max_size} for {filename}",
                content.len(),
            )));
        }

        let blake3_hash = blake3::hash(content).to_hex().to_string();

        if let Some(existing) = self.check_dedup(&blake3_hash, filename).await? {
            return Ok(existing);
        }

        // Write to secure temp file for content-store ingestion
        let temp_file_path = TemporaryBlobPath::new(
            create_secure_temp_path("sinex_blob", "tmp")
                .map_err(|e| SinexError::io(std::io::Error::other(e)))?,
        );

        let mut temp_file = tokio::fs::File::create(temp_file_path.as_path())
            .await
            .map_err(SinexError::io)?;
        // sinex-vyi3: this transient file (holding the full content bytes
        // about to be staged into CAS) lives in the SYSTEM temp directory,
        // not under any of the ingest-spool paths the NixOS directoryRules
        // fix tightens -- restrict it explicitly.
        super::restrict_permissions(
            temp_file_path.as_path().as_std_path(),
            super::CONTENT_STORE_FILE_MODE,
        );
        temp_file.write_all(content).await.map_err(SinexError::io)?;
        temp_file.sync_all().await.map_err(SinexError::io)?;
        drop(temp_file);

        let content_key = self
            .content_store
            .store_owned_temp_file(temp_file_path.as_path())
            .await
            .map_err(|e| {
                SinexError::processing("Failed to add buffered upload to content store")
                    .with_source(e)
            })?;
        info!("Added to content store with key: {}", content_key.key);

        self.verify_post_write(&content_key.key, &blake3_hash)
            .await?;

        self.register_new_blob(
            &content_key,
            filename,
            i64::try_from(content.len()).map_err(|error| {
                SinexError::blob_storage("blob size does not fit database metadata")
                    .with_source(error)
            })?,
            content_type.to_string(),
            blake3_hash,
        )
        .await
    }

    /// Retrieve blob content as bytes
    pub async fn retrieve_content(&self, content_key: &str) -> RuntimeResult<Vec<u8>> {
        let start = Instant::now();
        let blob = self.get_blob_metadata(content_key).await?;
        if blob.size_bytes < 0 {
            return Err(SinexError::processing(format!(
                "Blob metadata reported negative size for {content_key}: {}",
                blob.size_bytes
            )));
        }
        let metadata_size = u64::try_from(blob.size_bytes).map_err(|error| {
            SinexError::processing("Blob metadata size does not fit an unsigned size")
                .with_source(error)
        })?;
        self.content_store.ensure_file_size_allowed(metadata_size)?;
        let canonical_key = blob.content_key();

        // Ensure content is available locally
        self.content_store
            .ensure_content_local(&canonical_key)
            .await
            .map_err(|e| SinexError::blob_storage(e).with_operation("retrieve"))?;

        // Find the actual file path
        let path = self.find_symlink_path(&canonical_key).await?;

        let file_len = tokio::fs::metadata(&path)
            .await
            .map_err(SinexError::io)?
            .len();
        self.content_store.ensure_file_size_allowed(file_len)?;

        // Read the content
        let content = MaterialContentStore::read_file_with_limit(
            &path,
            self.content_store.config.max_blob_size,
        )
        .await?;

        // Verify integrity against the stored hashes if available. Prefer the
        // canonical content hash (git-annex SHA256), but fall back to the
        // BLAKE3 checksum we always store during ingestion so tampering is
        // detected even when the backend digest is missing.
        let mut verified = false;
        if content_hash_is_backend_digest(&blob) {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(&content);
            let computed = format!("{:x}", hasher.finalize()); // Note: hasher.finalize() is not a simple variable, keeping as is

            let mut expected = blob
                .content_hash
                .trim_start_matches("sha256:")
                .trim_start_matches("SHA256:")
                .trim_start_matches("SHA256E-")
                .to_string();

            if let Some((hash, _ext)) = expected.split_once('.') {
                expected = hash.to_string();
            }

            if !expected.is_empty() && computed != expected {
                let mismatch_error = SinexError::processing(format!(
                    "Blob content hash mismatch for {canonical_key} (expected {expected}, got {computed})"
                ));
                if let Err(status_error) = self
                    .persist_verification_status(&canonical_key, BlobVerificationStatus::Corrupted)
                    .await
                {
                    return Err(attach_verification_status_update_error(
                        mismatch_error,
                        &status_error,
                    ));
                }
                return Err(mismatch_error);
            } else if !expected.is_empty() {
                self.persist_verification_status(&canonical_key, BlobVerificationStatus::Verified)
                    .await?;
                verified = true;
            }
        }

        if !verified && let Some(expected_blake3) = &blob.checksum_blake3 {
            let computed = blake3::hash(&content).to_hex();
            if computed.as_str() != expected_blake3 {
                let mismatch_error = SinexError::processing(format!(
                    "Blob BLAKE3 hash mismatch for {canonical_key} (expected {expected_blake3}, got {computed})"
                ));
                if let Err(status_error) = self
                    .persist_verification_status(&canonical_key, BlobVerificationStatus::Corrupted)
                    .await
                {
                    return Err(attach_verification_status_update_error(
                        mismatch_error,
                        &status_error,
                    ));
                }
                return Err(mismatch_error);
            }
            self.persist_verification_status(&canonical_key, BlobVerificationStatus::Verified)
                .await?;
        }

        self.publish_blob_event(
            "blob.retrieved",
            BlobRetrievedPayload {
                blob_id: canonical_key,
                retrieval_time_ms: start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                cache_hit: true,
            },
            &blob,
        )
        .await?;

        Ok(content)
    }

    /// Retrieve a local CAS object that is not represented by a `core.blobs`
    /// row, such as a `MaterialManifestV1` object.  The key is still parsed,
    /// size-checked, path-confined, and BLAKE3-verified before bytes are
    /// returned; this is intentionally narrower than the user-facing blob
    /// retrieval route.
    pub async fn retrieve_cas_object(&self, content_key: &str) -> RuntimeResult<Vec<u8>> {
        let parsed = ContentStoreKey::parse(content_key).map_err(SinexError::validation)?;
        if !parsed.is_local_blake3_cas() {
            return Err(SinexError::validation(
                "unregistered CAS object retrieval requires a local BLAKE3 key",
            ));
        }
        self.content_store.ensure_file_size_allowed(parsed.size)?;
        self.content_store
            .ensure_content_local(content_key)
            .await
            .map_err(|error| {
                SinexError::blob_storage(error).with_operation("retrieve_cas_object")
            })?;
        let path = self
            .content_store
            .path_if_local(content_key)?
            .ok_or_else(|| SinexError::processing("local CAS path was not resolved"))?;
        let path = self
            .content_store
            .canonicalize_local_cas_path(&path)
            .await?;
        let content = MaterialContentStore::read_file_with_limit(
            &path,
            self.content_store.config.max_blob_size,
        )
        .await?;
        if content.len() as u64 != parsed.size {
            return Err(
                SinexError::processing("CAS object size does not match its key")
                    .with_context("expected_size", parsed.size.to_string())
                    .with_context("actual_size", content.len().to_string()),
            );
        }
        let actual_digest = blake3::hash(&content).to_hex().to_string();
        if actual_digest != parsed.digest {
            return Err(
                SinexError::processing("CAS object digest does not match its key")
                    .with_context("expected_digest", parsed.digest)
                    .with_context("actual_digest", actual_digest),
            );
        }
        Ok(content)
    }

    /// Load and fully validate one material's durable replay authority.
    ///
    /// V1 materials are resolved through the canonical manifest object and its
    /// encoded BLAKE3 CAS identity. Materials without a manifest retain the
    /// explicit legacy registry-blob fallback. The returned bytes are bounded
    /// by the content-store max object size and can be sliced repeatedly for
    /// all occurrences of this material without another full CAS read/hash.
    pub async fn retrieve_material_replay_content(
        &self,
        material_id: Uuid,
    ) -> RuntimeResult<MaterialReplayContent> {
        let material = self
            .db_pool
            .source_materials()
            .get_by_id(Id::<SourceMaterialRecord>::from_uuid(material_id))
            .await?
            .ok_or_else(|| {
                SinexError::processing(format!(
                    "source material {material_id} not found in registry"
                ))
            })?;

        if let Some(manifest_reference) = material.metadata.get("material_manifest") {
            let manifest_object = manifest_reference.as_object().ok_or_else(|| {
                SinexError::validation(format!(
                    "source material {material_id} has malformed manifest reference"
                ))
            })?;
            let manifest_key = manifest_object
                .get("content_key")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| {
                    SinexError::validation(format!(
                        "source material {material_id} has malformed manifest reference"
                    ))
                })?;
            let manifest_bytes = self.retrieve_cas_object(manifest_key).await?;
            let manifest = match MaterialManifestV1::decode(&manifest_bytes).map_err(|error| {
                SinexError::serialization(format!(
                    "failed to decode manifest for source material {material_id}"
                ))
                .with_context("reason", error)
            })? {
                DecodedMaterialManifest::V1(manifest) => manifest,
                DecodedMaterialManifest::Legacy(legacy) => {
                    return Err(SinexError::validation(format!(
                        "source material {material_id} has unsupported legacy manifest {}",
                        legacy.manifest_type.as_str()
                    )));
                }
                DecodedMaterialManifest::Unknown(unknown) => {
                    return Err(SinexError::validation(format!(
                        "source material {material_id} has unsupported manifest {}",
                        unknown.manifest_type.as_str()
                    )));
                }
            };
            let canonical_manifest_bytes = manifest.canonical_bytes().map_err(|error| {
                SinexError::serialization(format!(
                    "failed to canonicalize manifest for source material {material_id}"
                ))
                .with_source(error)
            })?;
            if manifest_bytes != canonical_manifest_bytes {
                return Err(SinexError::validation(format!(
                    "manifest for source material {material_id} is not in canonical encoding"
                )));
            }
            manifest.validate().map_err(|error| {
                SinexError::validation(format!(
                    "manifest validation failed for source material {material_id}: {error}"
                ))
            })?;
            if manifest.source_material_id != material_id {
                return Err(SinexError::validation(format!(
                    "manifest source material mismatch: expected {material_id}, got {}",
                    manifest.source_material_id
                )));
            }

            let encoded_key = manifest.encoded_content_store_key().map_err(|error| {
                SinexError::validation(format!(
                    "manifest for source material {material_id} has invalid encoded CAS identity: {error}"
                ))
            })?;
            let encoded_bytes = self.retrieve_cas_object(&encoded_key).await?;
            if encoded_bytes.len() as u64 != manifest.bytes.encoded_size
                || blake3::hash(&encoded_bytes).to_hex().to_string()
                    != manifest.bytes.encoded.value_hex
            {
                return Err(SinexError::processing(format!(
                    "manifest-authoritative CAS bytes failed digest or size validation for source material {material_id}"
                )));
            }
            if let Some(blob_id) = material.optional_blob_id
                && let Some(blob) = self
                    .db_pool
                    .blobs()
                    .get_by_id(Id::<Blob>::from_uuid(blob_id))
                    .await?
            {
                let blob_matches = blob.size_bytes >= 0
                    && blob.size_bytes as u64 == manifest.bytes.encoded_size
                    && blob
                        .checksum_blake3
                        .as_deref()
                        .is_none_or(|checksum| checksum == manifest.bytes.encoded.value_hex);
                if !blob_matches {
                    return Err(SinexError::processing(format!(
                        "manifest byte identity mismatch for source material {material_id}"
                    )));
                }
            }

            return Ok(MaterialReplayContent {
                authority: MaterialReplayAuthority::ManifestV1,
                bytes: encoded_bytes,
                manifest: Some(manifest),
            });
        }

        let blob_id = material.optional_blob_id.ok_or_else(|| {
            SinexError::validation(format!(
                "legacy source material {material_id} has no associated blob for replay fallback"
            ))
        })?;
        let blob = self
            .db_pool
            .blobs()
            .get_by_id(Id::<Blob>::from_uuid(blob_id))
            .await?
            .ok_or_else(|| {
                SinexError::processing(format!(
                    "legacy replay blob {blob_id} for source material {material_id} not found"
                ))
            })?;
        let bytes = self.retrieve_content(&blob.content_key()).await?;
        Ok(MaterialReplayContent {
            authority: MaterialReplayAuthority::LegacyBlobFallback,
            bytes,
            manifest: None,
        })
    }

    /// Resolve one replay occurrence from a previously fully validated
    /// material authority. This convenience route preserves the range API for
    /// callers that need one occurrence, while generic replay preflight uses
    /// [`Self::retrieve_material_replay_content`] once per material.
    pub async fn retrieve_material_replay_range(
        &self,
        material_id: Uuid,
        range: ByteRange,
    ) -> RuntimeResult<MaterialReplayContent> {
        let resolved = self.retrieve_material_replay_content(material_id).await?;
        let authority = resolved.authority;
        let bytes = resolved.slice_range(material_id, range)?;
        Ok(MaterialReplayContent {
            authority,
            bytes,
            manifest: None,
        })
    }

    /// Retrieve a blob's content path
    pub async fn get_blob_path(&self, content_key: &str) -> RuntimeResult<Utf8PathBuf> {
        let start = Instant::now();
        let blob = self.get_blob_metadata(content_key).await?;

        // Ensure content is available locally
        self.content_store
            .ensure_content_local(&blob.content_key())
            .await?;

        self.publish_blob_event(
            "blob.retrieved",
            BlobRetrievedPayload {
                blob_id: content_key.to_string(),
                retrieval_time_ms: start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                cache_hit: true,
            },
            &blob,
        )
        .await?;

        // Find the symlink path in the repository
        self.find_symlink_path(&blob.content_key()).await
    }

    /// Verify blob integrity
    pub async fn verify_blob(&self, content_key: &str) -> RuntimeResult<bool> {
        let _start = Instant::now();
        let blob = self.get_blob_metadata(content_key).await?;

        let verification = self
            .content_store
            .verify_key(false, false, Some(content_key))
            .await?;
        let is_verified = verification.success;

        // Update verification status in database
        let status = if is_verified {
            BlobVerificationStatus::Verified
        } else {
            BlobVerificationStatus::Corrupted
        };
        self.update_verification_status(content_key, status).await?;

        self.publish_blob_event(
            "blob.verified",
            BlobVerifiedPayload {
                blob_id: content_key.to_string(),
                verification_status: status,
                checksum_matched: is_verified,
            },
            &blob,
        )
        .await?;

        Ok(is_verified)
    }

    /// Find blob by BLAKE3 hash for deduplication
    async fn find_blob_by_blake3(&self, blake3_hash: &str) -> RuntimeResult<Option<Blob>> {
        self.db_pool.blobs().find_by_blake3(blake3_hash).await
    }

    /// Insert new blob metadata into database
    pub async fn insert_blob(&self, blob: &Blob) -> RuntimeResult<Blob> {
        self.db_pool.blobs().insert(blob.clone()).await
    }

    /// Drop content from the underlying CAS by key. Idempotent: tolerates
    /// missing files. `force = true` skips the local-CAS safety check
    /// (required for actual deletion as opposed to dropping replicas).
    ///
    /// Used by the gateway's tombstone-approve handler to delete-on-tombstone
    /// for source materials whose only references were the tombstoned events
    /// (#987).
    pub async fn drop_content(&self, content_key: &str, force: bool) -> RuntimeResult<()> {
        self.content_store.drop_content(content_key, force).await
    }

    /// Get blob metadata by content-store key
    pub async fn get_blob_metadata(&self, content_key: &str) -> RuntimeResult<Blob> {
        let (backend, size, hash_fragment) =
            Blob::parse_content_store_key(content_key).map_err(SinexError::processing)?;

        self.db_pool
            .blobs()
            .get_by_content(&backend, &hash_fragment, size)
            .await?
            .ok_or_else(|| {
                SinexError::processing(format!("Blob not found in database for key: {content_key}"))
            })
    }

    /// Update verification status
    async fn update_verification_status(
        &self,
        content_key: &str,
        status: BlobVerificationStatus,
    ) -> RuntimeResult<()> {
        // First get the blob to get its ID
        let blob = self.get_blob_metadata(content_key).await?;
        self.db_pool
            .blobs()
            .update_verification_status(blob.id, status)
            .await
    }

    /// Add original filename to existing blob
    async fn add_original_filename(&self, content_key: &str, filename: &str) -> RuntimeResult<()> {
        // First get the blob to get its ID
        let blob = self.get_blob_metadata(content_key).await?;
        self.db_pool
            .blobs()
            .add_original_filename(blob.id, filename)
            .await
    }

    /// Verify that stored blob content matches the expected BLAKE3 hash.
    ///
    /// Called after content-store write to detect silent corruption during write.
    /// Re-reads the content from the storage backend and compares the hash
    /// against the one computed from the original input.
    async fn verify_post_write(
        &self,
        content_key: &str,
        expected_blake3: &str,
    ) -> RuntimeResult<()> {
        let path = self.find_symlink_path(content_key).await?;
        let stored_content = MaterialContentStore::read_file_with_limit(
            &path,
            self.content_store.config.max_blob_size,
        )
        .await
        .map_err(|e| {
            SinexError::blob_storage(format!(
                "Post-write verification: failed to re-read {content_key}: {e}"
            ))
        })?;
        let computed = blake3::hash(&stored_content).to_hex();
        if computed.as_str() != expected_blake3 {
            return Err(SinexError::blob_storage(format!(
                "Post-write verification failed for {content_key}: expected BLAKE3 {expected_blake3}, got {computed}"
            )));
        }
        debug!(content_key, "Post-write BLAKE3 verification passed");
        Ok(())
    }

    /// Find content path in repository for content-store key
    async fn find_symlink_path(&self, content_key: &str) -> RuntimeResult<Utf8PathBuf> {
        if let Some(path) = self.content_store.path_if_local(content_key)? {
            return self.content_store.canonicalize_local_cas_path(&path).await;
        }

        if !self.content_store.config.legacy_annex_enabled {
            return Err(SinexError::processing(format!(
                "legacy annex disabled; cannot locate non-local-CAS key: {content_key}"
            )));
        }

        let mut cmd = AsyncCommand::new("git-annex");
        cmd.arg("contentlocation")
            .arg(content_key)
            .current_dir(self.content_store.root_path());
        let output = run_command_async(cmd, "Failed to run git-annex contentlocation").await?;

        if !output.status.success() {
            return Err(SinexError::processing(format!(
                "git-annex contentlocation failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        self.content_store
            .resolve_command_path(
                &output.stdout,
                "Failed to resolve git-annex contentlocation path",
            )
            .await
    }

    /// Simple MIME type detection
    pub fn detect_mime_type(file_path: &Utf8Path) -> RuntimeResult<String> {
        let extension = file_path.extension().unwrap_or("");

        let mime_type = match extension.to_lowercase().as_str() {
            "txt" => "text/plain",
            "md" => "text/markdown",
            "json" => "application/json",
            "pdf" => "application/pdf",
            "jpg" | "jpeg" => "image/jpeg",
            "png" => "image/png",
            "mp4" => "video/mp4",
            "mp3" => "audio/mpeg",
            _ => "application/octet-stream",
        };

        Ok(mime_type.to_string())
    }

    /// Emit storage statistics (called periodically by background task)
    pub async fn emit_storage_stats(&self) -> RuntimeResult<()> {
        // Get storage statistics from blob repository
        let stats = self.db_pool.blobs().get_storage_stats().await?;

        let blob_count = stats.total_blobs;
        let total_size = stats.total_size_bytes;
        let failed_count = stats.failed_verifications;

        let metrics_material = self
            .db_pool
            .source_materials()
            .register_material(SourceMaterialRegistration::blob().with_metadata(json!({
                "component": "content-store-manager",
                "purpose": "storage_statistics",
            })))
            .await?;

        let material_id = Id::<SourceMaterial>::from_uuid(metrics_material.id);

        let backend_label = if self.content_store.config.legacy_annex_enabled {
            "hybrid-local-cas-git-annex"
        } else {
            "local-cas"
        };

        let new_event = Self::create_blob_event(
            "storage.statistics",
            StorageStatisticsPayload {
                total_blobs: blob_count,
                total_size_bytes: total_size,
                failed_verifications: failed_count,
                storage_backend: backend_label.to_string(),
            },
            material_id,
        )?;

        if let Some(sender) = &self.event_sender {
            match sender.try_send(new_event) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    warn!(
                        channel_capacity = BLOB_EVENT_CHANNEL_CAPACITY,
                        "ContentStoreManager event channel full; dropping storage.statistics event"
                    );
                    return Ok(());
                }
                Err(TrySendError::Closed(_)) => {
                    return Err(SinexError::processing(
                        "Failed to emit blob storage statistics: event channel closed".to_string(),
                    ));
                }
            }
        } else {
            debug!(
                "ContentStoreManager event emission disabled; skipping storage.statistics event"
            );
        }

        Ok(())
    }
}

#[cfg(test)]
#[path = "manager_test.rs"]
mod tests;
