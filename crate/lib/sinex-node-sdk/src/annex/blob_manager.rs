//! Git-annex blob management utilities.
//!
//! The manager deduplicates incoming content, registers metadata in `core.blobs`,
//! wires provenance through source_material records, and emits ingestion/health
//! events that downstream services can rely on.
//!
//! See `docs/current/architecture/Core_Architecture.md` (blob storage) and the
//! `m20241028_000001_create_canonical_schema` migration for the canonical design
//! and schema definition.

use crate::{NodeResult, SinexError};
use camino::{Utf8Path, Utf8PathBuf};
use serde_json::json;
use sinex_db::models::{Blob, SourceMaterial};
use sinex_db::repositories::source_materials::SourceMaterial as SourceMaterialRegistration;
use sinex_db::DbPool;
use sinex_db::DbPoolExt;
use sinex_primitives::events::{
    BlobIngestedPayload, BlobRetrievedPayload, BlobVerifiedPayload, StorageStatisticsPayload,
};
use sinex_primitives::DynamicPayload;
use sinex_primitives::{Event, Id, JsonValue};
use std::time::Instant;
use tracing::{debug, info, warn};

use super::{
    path_validator::{create_secure_temp_path, validate_path_exists, VerifiedPath},
    AnnexConfig, AnnexKey, GitAnnex,
};
use tokio::io::AsyncWriteExt;
use tokio::process::Command as AsyncCommand;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

// Re-export Blob type for annex consumers.
pub use sinex_db::models::Blob as BlobMetadata;

/// Default capacity for blob-manager event channels to prevent unbounded buffering.
pub const BLOB_EVENT_CHANNEL_CAPACITY: usize = 1024;

#[derive(Debug)]
pub struct BlobManager {
    annex: GitAnnex,
    db_pool: DbPool,
    event_sender: Option<mpsc::Sender<Event<JsonValue>>>,
}

impl BlobManager {
    pub fn new(
        annex_config: AnnexConfig,
        db_pool: DbPool,
        event_sender: Option<mpsc::Sender<Event<JsonValue>>>,
    ) -> NodeResult<Self> {
        let annex = GitAnnex::new(annex_config)
            .map_err(|e| SinexError::blob_storage(e).with_operation("initialize"))?;
        Ok(BlobManager {
            annex,
            db_pool,
            event_sender,
        })
    }

    /// Builds an event tied to the supplied source material.
    fn create_blob_event<T: serde::Serialize>(
        event_type: &str,
        payload: T,
        material_id: Id<SourceMaterial>,
    ) -> NodeResult<Event<JsonValue>> {
        let payload_value = serde_json::to_value(payload).map_err(SinexError::serialization)?;
        DynamicPayload::new("blob-manager", event_type, payload_value)
            .from_material(material_id)
            .build()
            .map_err(|err| {
                SinexError::processing(format!(
                    "Failed to build blob event: {err}\n  event_type: {event_type}\n  material_id: {material_id}"
                ))
            })
    }

    async fn ensure_material_for_blob(&self, blob: &Blob) -> NodeResult<Id<SourceMaterial>> {
        let repo = self.db_pool.source_materials();

        if let Some(existing) = repo.find_by_blob_id(blob.id).await? {
            return Ok(Id::<SourceMaterial>::from_ulid(existing.id));
        }

        let filename = blob
            .original_filename
            .clone()
            .unwrap_or_else(|| "unknown".to_string());

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
            "annex_backend": blob.annex_backend,
            "content_hash": blob.content_hash,
            "annex_key": blob.annex_key(),
            "size_bytes": blob.size_bytes,
        }));

        let record = repo.register_material(material).await.map_err(|e| {
            SinexError::processing(format!("Failed to register source material for blob: {e}"))
        })?;

        Ok(Id::<SourceMaterial>::from_ulid(record.id))
    }

    async fn publish_blob_event<T: serde::Serialize>(
        &self,
        event_type: &str,
        payload: T,
        blob: &Blob,
    ) -> NodeResult<()> {
        if let Some(sender) = &self.event_sender {
            let material_id = self.ensure_material_for_blob(blob).await?;
            let event = Self::create_blob_event(event_type, payload, material_id)?;

            match sender.try_send(event) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    warn!(
                        channel_capacity = BLOB_EVENT_CHANNEL_CAPACITY,
                        "BlobManager event channel full; dropping {} event", event_type
                    );
                }
                Err(TrySendError::Closed(_)) => {
                    return Err(SinexError::processing(format!(
                        "Failed to emit {event_type} event: event channel closed"
                    )))
                }
            }
        } else {
            debug!(
                "BlobManager event emission disabled; skipping {} notification",
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
    ) -> NodeResult<Option<BlobMetadata>> {
        let existing = match self.find_blob_by_blake3(blake3_hash).await? {
            Some(blob) => blob,
            None => return Ok(None),
        };

        let existing_key = existing.annex_key().clone();
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

    /// Register a new blob after content has been added to git-annex.
    /// Parses the annex key, inserts the DB record, and emits the ingested event.
    async fn register_new_blob(
        &self,
        annex_key: &AnnexKey,
        filename: &str,
        size_bytes: i64,
        mime_type: String,
        blake3_hash: String,
    ) -> NodeResult<BlobMetadata> {
        let (backend, _, _) = Blob::parse_annex_key(&annex_key.key)
            .ok_or_else(|| SinexError::processing("Invalid annex key format".to_string()))?;

        let blob = Blob::builder()
            .annex_backend(backend)
            .content_hash(annex_key.hash.clone())
            .original_filename(filename.to_string())
            .size_bytes(size_bytes)
            .mime_type(mime_type.clone())
            .checksum_blake3(blake3_hash.clone())
            .build();

        let blob_metadata = self.insert_blob(&blob).await?;
        let blob_key = blob_metadata.annex_key().clone();
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

    /// Ingest a file into the blob management system
    pub async fn ingest_file(
        &self,
        file_path: &VerifiedPath,
        original_filename: Option<&str>,
    ) -> NodeResult<BlobMetadata> {
        validate_path_exists(file_path.as_path())
            .map_err(|e| SinexError::blob_storage(e).with_operation("validate_path"))?;
        let validated_path = file_path.as_path();

        info!("Ingesting file: {:?}", validated_path);

        let blake3_hash = GitAnnex::compute_blake3_hash(validated_path)
            .await
            .map_err(|e| SinexError::blob_storage(e).with_operation("compute_hash"))?;

        let effective_filename =
            original_filename.unwrap_or_else(|| validated_path.file_name().unwrap_or("unknown"));

        if let Some(existing) = self.check_dedup(&blake3_hash, effective_filename).await? {
            return Ok(existing);
        }

        let file_metadata = tokio::fs::metadata(validated_path)
            .await
            .map_err(SinexError::io)?;
        let size_bytes = file_metadata.len() as i64;

        let mime_type = Self::detect_mime_type(validated_path)
            .map_err(|e| SinexError::blob_storage(e).with_operation("detect_mime_type"))?;

        let annex_key = self.annex.add_file(validated_path).await.map_err(|e| {
            SinexError::processing(format!("Failed to add file to git-annex: {e}"))
        })?;
        info!("Added to git-annex with key: {}", annex_key.key);

        self.verify_post_write(&annex_key.key, &blake3_hash).await?;

        self.register_new_blob(&annex_key, effective_filename, size_bytes, mime_type, blake3_hash)
            .await
    }

    /// Ingest content from bytes (for in-memory content like clipboard)
    pub async fn ingest_from_bytes(
        &self,
        content: &[u8],
        filename: &str,
        content_type: &str,
    ) -> NodeResult<BlobMetadata> {
        info!("Ingesting {} bytes as {}", content.len(), filename);

        let blake3_hash = blake3::hash(content).to_hex().to_string();

        if let Some(existing) = self.check_dedup(&blake3_hash, filename).await? {
            return Ok(existing);
        }

        // Write to secure temp file for git-annex ingestion
        let temp_file_path = create_secure_temp_path("sinex_blob", "tmp")
            .map_err(|e| SinexError::io(std::io::Error::other(e)))?;

        let mut temp_file = tokio::fs::File::create(&temp_file_path)
            .await
            .map_err(SinexError::io)?;
        temp_file.write_all(content).await.map_err(SinexError::io)?;
        temp_file.sync_all().await.map_err(SinexError::io)?;
        drop(temp_file);

        let annex_key = self
            .annex
            .add_file(temp_file_path.as_path())
            .await
            .map_err(|e| {
                SinexError::processing(format!("Failed to add buffered upload to git-annex: {e}"))
            })?;
        info!("Added to git-annex with key: {}", annex_key.key);

        self.verify_post_write(&annex_key.key, &blake3_hash).await?;

        if let Err(e) = tokio::fs::remove_file(&temp_file_path).await {
            warn!(
                error = %e,
                path = %temp_file_path,
                "Failed to remove temporary blob file after ingest"
            );
        }

        self.register_new_blob(
            &annex_key,
            filename,
            content.len() as i64,
            content_type.to_string(),
            blake3_hash,
        )
        .await
    }

    /// Retrieve blob content as bytes
    pub async fn retrieve_content(&self, annex_key: &str) -> NodeResult<Vec<u8>> {
        let start = Instant::now();

        // Ensure content is available locally
        self.annex
            .get_content(annex_key)
            .await
            .map_err(|e| SinexError::blob_storage(e).with_operation("retrieve"))?;

        // Find the actual file path
        let path = self.find_symlink_path(annex_key).await?;

        // Read the content
        let content = tokio::fs::read(&path).await.map_err(SinexError::io)?;

        let blob = self.get_blob_metadata(annex_key).await?;

        // Verify integrity against the stored hashes if available. Prefer the
        // canonical content hash (git-annex SHA256), but fall back to the
        // BLAKE3 checksum we always store during ingestion so tampering is
        // detected even when the annex hash is missing.
        let mut verified = false;
        if !blob.content_hash.is_empty() {
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
                let _ = self
                    .update_verification_status(annex_key, "corrupted")
                    .await;
                return Err(SinexError::processing(format!(
                    "Blob content hash mismatch for {annex_key} (expected {expected}, got {computed})"
                )));
            } else if !expected.is_empty() {
                let _ = self.update_verification_status(annex_key, "verified").await;
                verified = true;
            }
        }

        if !verified {
            if let Some(expected_blake3) = &blob.checksum_blake3 {
                let computed = blake3::hash(&content).to_hex();
                if computed.as_str() != expected_blake3 {
                    let _ = self
                        .update_verification_status(annex_key, "corrupted")
                        .await;
                    return Err(SinexError::processing(format!(
                        "Blob BLAKE3 hash mismatch for {annex_key} (expected {expected_blake3}, got {computed})"
                    )));
                }
                let _ = self.update_verification_status(annex_key, "verified").await;
            }
        }

        self.publish_blob_event(
            "blob.retrieved",
            BlobRetrievedPayload {
                blob_id: annex_key.to_string(),
                retrieval_time_ms: start.elapsed().as_millis().min(u64::MAX as u128) as u64,
                cache_hit: true,
            },
            &blob,
        )
        .await?;

        Ok(content)
    }

    /// Retrieve a blob's content path
    pub async fn get_blob_path(&self, annex_key: &str) -> NodeResult<Utf8PathBuf> {
        let start = Instant::now();
        let blob = self.get_blob_metadata(annex_key).await?;

        // Ensure content is available locally
        self.annex.get_content(&blob.annex_key()).await?;

        self.publish_blob_event(
            "blob.retrieved",
            BlobRetrievedPayload {
                blob_id: annex_key.to_string(),
                retrieval_time_ms: start.elapsed().as_millis().min(u64::MAX as u128) as u64,
                cache_hit: true,
            },
            &blob,
        )
        .await?;

        // Find the symlink path in the repository
        self.find_symlink_path(&blob.annex_key()).await
    }

    /// Verify blob integrity
    pub async fn verify_blob(&self, annex_key: &str) -> NodeResult<bool> {
        let _start = Instant::now();
        let blob = self.get_blob_metadata(annex_key).await?;

        // Run git-annex fsck on specific key
        let fsck_output = self.annex.fsck(false, false, Some(annex_key)).await?;

        // Parse fsck output to determine if this specific blob is ok
        let is_verified = !fsck_output.contains("failed") && !fsck_output.contains("error");

        // Update verification status in database
        let status = if is_verified { "verified" } else { "corrupted" };
        self.update_verification_status(annex_key, status).await?;

        self.publish_blob_event(
            "blob.verified",
            BlobVerifiedPayload {
                blob_id: annex_key.to_string(),
                verification_status: status.to_string(),
                checksum_matched: is_verified,
            },
            &blob,
        )
        .await?;

        Ok(is_verified)
    }

    /// Find blob by BLAKE3 hash for deduplication
    async fn find_blob_by_blake3(&self, blake3_hash: &str) -> NodeResult<Option<Blob>> {
        self.db_pool.blobs().find_by_blake3(blake3_hash).await
    }

    /// Insert new blob metadata into database
    pub async fn insert_blob(&self, blob: &Blob) -> NodeResult<Blob> {
        self.db_pool.blobs().insert(blob.clone()).await
    }

    /// Get blob metadata by annex key
    pub fn get_blob_metadata_sync(&self, annex_key: &str) -> NodeResult<Blob> {
        let (backend, size, hash_fragment) = Blob::parse_annex_key(annex_key).ok_or_else(|| {
            SinexError::processing(format!("Invalid annex key format: {annex_key}"))
        })?;

        futures::executor::block_on(self.db_pool.blobs().get_by_content(
            &backend,
            &hash_fragment,
            size,
        ))?
        .ok_or_else(|| {
            SinexError::processing(format!("Blob not found in database for key: {annex_key}"))
        })
    }

    /// Get blob metadata by annex key
    pub async fn get_blob_metadata(&self, annex_key: &str) -> NodeResult<Blob> {
        let (backend, size, hash_fragment) = Blob::parse_annex_key(annex_key).ok_or_else(|| {
            SinexError::processing(format!("Invalid annex key format: {annex_key}"))
        })?;

        self.db_pool
            .blobs()
            .get_by_content(&backend, &hash_fragment, size)
            .await?
            .ok_or_else(|| {
                SinexError::processing(format!("Blob not found in database for key: {annex_key}"))
            })
    }

    /// Update verification status
    async fn update_verification_status(&self, annex_key: &str, status: &str) -> NodeResult<()> {
        // First get the blob to get its ID
        let blob = self.get_blob_metadata(annex_key).await?;
        self.db_pool
            .blobs()
            .update_verification_status(blob.id, status)
            .await
    }

    /// Add original filename to existing blob
    async fn add_original_filename(&self, annex_key: &str, filename: &str) -> NodeResult<()> {
        // First get the blob to get its ID
        let blob = self.get_blob_metadata(annex_key).await?;
        self.db_pool
            .blobs()
            .add_original_filename(blob.id, filename)
            .await
    }

    /// Verify that stored blob content matches the expected BLAKE3 hash.
    ///
    /// Called after `git-annex add` to detect silent corruption during write.
    /// Re-reads the content from the annex backend and compares the hash
    /// against the one computed from the original input.
    async fn verify_post_write(&self, annex_key: &str, expected_blake3: &str) -> NodeResult<()> {
        let path = self.find_symlink_path(annex_key).await?;
        let stored_content = tokio::fs::read(&path).await.map_err(|e| {
            SinexError::blob_storage(format!(
                "Post-write verification: failed to re-read {annex_key}: {e}"
            ))
        })?;
        let computed = blake3::hash(&stored_content).to_hex();
        if computed.as_str() != expected_blake3 {
            return Err(SinexError::blob_storage(format!(
                "Post-write verification failed for {annex_key}: expected BLAKE3 {expected_blake3}, got {computed}"
            )));
        }
        debug!(annex_key, "Post-write BLAKE3 verification passed");
        Ok(())
    }

    /// Find content path in repository for annex key
    async fn find_symlink_path(&self, annex_key: &str) -> NodeResult<Utf8PathBuf> {
        let output = AsyncCommand::new("git-annex")
            .arg("contentlocation")
            .arg(annex_key)
            .current_dir(self.annex.repo_path())
            .output()
            .await
            .map_err(SinexError::io)?;

        if !output.status.success() {
            return Err(SinexError::processing(format!(
                "git-annex contentlocation failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        let relative = String::from_utf8(output.stdout).map_err(|e| {
            SinexError::processing(format!("Invalid UTF-8 from git-annex contentlocation: {e}"))
        })?;
        let trimmed = relative.trim();
        if trimmed.is_empty() {
            return Err(SinexError::processing(format!(
                "git-annex contentlocation returned empty path for {annex_key}"
            )));
        }

        let path = self.annex.repo_path().join(trimmed);
        Ok(path)
    }

    /// Simple MIME type detection
    pub fn detect_mime_type(file_path: &Utf8Path) -> NodeResult<String> {
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
    pub async fn emit_storage_stats(&self) -> NodeResult<()> {
        // Get storage statistics from blob repository
        let stats = self.db_pool.blobs().get_storage_stats().await?;

        let blob_count = stats.total_blobs;
        let total_size = stats.total_size_bytes;
        let failed_count = stats.failed_verifications;

        let metrics_material = self
            .db_pool
            .source_materials()
            .register_material(SourceMaterialRegistration::blob().with_metadata(json!({
                "component": "blob-manager",
                "purpose": "storage_statistics",
            })))
            .await?;

        let material_id = Id::<SourceMaterial>::from_ulid(metrics_material.id);

        let new_event = Self::create_blob_event(
            "storage.statistics",
            StorageStatisticsPayload {
                total_blobs: blob_count,
                total_size_bytes: total_size,
                failed_verifications: failed_count,
                storage_backend: "git-annex".to_string(),
            },
            material_id,
        )?;

        if let Some(sender) = &self.event_sender {
            match sender.try_send(new_event) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    warn!(
                        channel_capacity = BLOB_EVENT_CHANNEL_CAPACITY,
                        "BlobManager event channel full; dropping storage.statistics event"
                    );
                    return Ok(());
                }
                Err(TrySendError::Closed(_)) => {
                    return Err(SinexError::processing(
                        "Failed to emit blob storage statistics: event channel closed".to_string(),
                    ))
                }
            }
        } else {
            debug!("BlobManager event emission disabled; skipping storage.statistics event");
        }

        Ok(())
    }
}
