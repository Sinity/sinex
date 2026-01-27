//! Git-annex blob management utilities.
//!
//! The manager deduplicates incoming content, registers metadata in `core.blobs`,
//! wires provenance through source_material records, and emits ingestion/health
//! events that downstream services can rely on.
//!
//! See `docs/current/architecture/Core_Architecture.md` (blob storage) and the
//! `m20241028_000001_create_canonical_schema` migration for the canonical design
//! and schema definition.

use camino::{Utf8Path, Utf8PathBuf};
use color_eyre::eyre::{bail, eyre, Context, Result};
use serde_json::json;
use sinex_core::db::repositories::source_materials::SourceMaterial as SourceMaterialRegistration;
use sinex_core::db::DbPool;
use sinex_core::types::events::{
    BlobIngestedPayload, BlobRetrievedPayload, BlobVerifiedPayload, StorageStatisticsPayload,
};
use sinex_core::DbPoolExt;
use sinex_core::{Blob, DynamicPayload, Event, Id, JsonValue, SourceMaterial};
use std::time::Instant;
use tracing::{debug, info, warn};

use super::{
    path_validator::{create_secure_temp_path, validate_path_exists, VerifiedPath},
    AnnexConfig, GitAnnex,
};
use tokio::io::AsyncWriteExt;
use tokio::process::Command as AsyncCommand;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

// Re-export Blob type for annex consumers.
pub use sinex_core::Blob as BlobMetadata;

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
    ) -> Result<Self> {
        let annex = GitAnnex::new(annex_config)?;
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
    ) -> Result<Event<JsonValue>> {
        let payload_value = serde_json::to_value(payload)
            .map_err(|e| eyre!("Failed to serialize blob event payload: {}", e))?;
        DynamicPayload::new("blob-manager", event_type, payload_value)
            .from_material(material_id)
            .build()
            .map_err(|err| {
                eyre!(
                    "Failed to build blob event: {}\n  event_type: {}\n  material_id: {}",
                    err,
                    event_type,
                    material_id
                )
            })
    }

    async fn ensure_material_for_blob(&self, blob: &Blob) -> Result<Id<SourceMaterial>> {
        let repo = self.db_pool.source_materials();

        if let Some(existing) = repo
            .find_by_blob_id(blob.id.clone())
            .await
            .wrap_err("Failed to query source material by blob id")?
        {
            return Ok(Id::<SourceMaterial>::from_ulid(existing.id));
        }

        let filename = blob
            .original_filename
            .clone()
            .unwrap_or_else(|| "unknown".to_string());

        let mut material = if blob
            .mime_type
            .as_deref()
            .map(|mime| mime.starts_with("text/"))
            .unwrap_or(false)
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

        material = material.with_blob_id(blob.id.clone()).with_metadata(json!({
            "annex_backend": blob.annex_backend,
            "content_hash": blob.content_hash,
            "annex_key": blob.annex_key(),
            "size_bytes": blob.size_bytes,
        }));

        let record = repo
            .register_material(material)
            .await
            .wrap_err("Failed to register source material for blob")?;

        Ok(Id::<SourceMaterial>::from_ulid(record.id))
    }

    async fn publish_blob_event<T: serde::Serialize>(
        &self,
        event_type: &str,
        payload: T,
        blob: &Blob,
    ) -> Result<()> {
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
                    return Err(eyre!(
                        "Failed to emit {event_type} event: event channel closed"
                    ))
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

    /// Ingest a file into the blob management system
    pub async fn ingest_file(
        &self,
        file_path: &VerifiedPath,
        original_filename: Option<&str>,
    ) -> Result<BlobMetadata> {
        // Validate file path before processing to prevent path traversal attacks
        validate_path_exists(file_path.as_path())?;
        let validated_path = file_path.as_path();

        info!("Ingesting file: {:?}", validated_path);
        let _start = Instant::now();

        // Compute BLAKE3 hash for deduplication
        let blake3_hash = GitAnnex::compute_blake3_hash(&validated_path).await?;
        debug!("Computed BLAKE3 hash: {}", blake3_hash);

        // Check if blob already exists
        if let Some(existing) = self.find_blob_by_blake3(&blake3_hash).await? {
            let existing_key = existing.annex_key().clone();
            info!(
                "File already exists in blob store with key: {}",
                existing_key
            );

            // Update original_filenames array if this is a new filename
            if let Some(filename) = original_filename {
                self.add_original_filename(&existing_key, filename).await?;
            }

            if let Err(e) = self
                .publish_blob_event(
                    "blob.ingested",
                    BlobIngestedPayload {
                        blob_id: existing_key.clone(),
                        size_bytes: existing.size_bytes,
                        mime_type: existing.mime_type.clone(),
                        checksum_blake3: blake3_hash,
                        deduplicated: true,
                        original_filename: original_filename
                            .or(existing.original_filename.as_deref())
                            .unwrap_or("unknown")
                            .to_string(),
                    },
                    &existing,
                )
                .await
            {
                warn!(
                    error = %e,
                    "Failed to emit blob.ingested event for deduplicated annex file"
                );
            }

            return Ok(existing);
        }

        // Get file metadata
        let file_metadata = tokio::fs::metadata(&validated_path)
            .await
            .wrap_err("Failed to get file metadata")?;
        let size_bytes = file_metadata.len() as i64;

        // Detect MIME type
        let mime_type = Self::detect_mime_type(&validated_path)?;

        // Add to git-annex
        let annex_key = self
            .annex
            .add_file(&validated_path)
            .await
            .wrap_err("Failed to add file to git-annex")?;
        info!("Added to git-annex with key: {}", annex_key.key);

        // Create blob record in database
        let filename =
            original_filename.unwrap_or_else(|| validated_path.file_name().unwrap_or("unknown"));

        // Parse the annex key to get backend and hash
        let (backend, _, _) = Blob::parse_annex_key(&annex_key.key)
            .ok_or_else(|| eyre!("Invalid annex key format"))?;

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
        info!("Successfully ingested blob: {}", blob_key);

        self.publish_blob_event(
            "blob.ingested",
            BlobIngestedPayload {
                blob_id: blob_key.clone(),
                size_bytes,
                mime_type: Some(mime_type),
                checksum_blake3: blake3_hash,
                deduplicated: false,
                original_filename: filename.to_string(),
            },
            &blob_metadata,
        )
        .await?;

        Ok(blob_metadata)
    }

    /// Ingest content from bytes (for in-memory content like clipboard)
    pub async fn ingest_from_bytes(
        &self,
        content: &[u8],
        filename: &str,
        content_type: &str,
    ) -> Result<BlobMetadata> {
        info!("Ingesting {} bytes as {}", content.len(), filename);
        let _start = Instant::now();

        // Compute BLAKE3 hash for deduplication
        let blake3_hash = blake3::hash(content).to_hex().to_string();
        debug!("Computed BLAKE3 hash: {}", blake3_hash);

        // Check if blob already exists
        if let Some(existing) = self.find_blob_by_blake3(&blake3_hash).await? {
            let existing_key = existing.annex_key().clone();
            info!(
                "Content already exists in blob store with key: {}",
                existing_key
            );

            // Update original_filenames array if this is a new filename
            self.add_original_filename(&existing_key, filename).await?;

            if let Err(e) = self
                .publish_blob_event(
                    "blob.ingested",
                    BlobIngestedPayload {
                        blob_id: existing_key.clone(),
                        size_bytes: existing.size_bytes,
                        mime_type: existing.mime_type.clone(),
                        checksum_blake3: blake3_hash,
                        deduplicated: true,
                        original_filename: filename.to_string(),
                    },
                    &existing,
                )
                .await
            {
                warn!(
                    error = %e,
                    "Failed to emit blob.ingested event for deduplicated blob bytes"
                );
            }

            return Ok(existing);
        }

        // Create a unique temporary file without predictable naming to avoid symlink attacks
        let temp_file_path = create_secure_temp_path("sinex_blob", "tmp")
            .wrap_err("Failed to allocate secure temporary file path")?;

        let mut temp_file = tokio::fs::File::create(&temp_file_path)
            .await
            .wrap_err("Failed to create temporary blob file")?;
        temp_file
            .write_all(content)
            .await
            .wrap_err("Failed to write blob bytes to temporary file")?;
        temp_file
            .sync_all()
            .await
            .wrap_err("Failed to flush temporary blob file")?;
        drop(temp_file);

        // Add to git-annex
        let annex_key = self
            .annex
            .add_file(temp_file_path.as_path())
            .await
            .wrap_err("Failed to add buffered upload to git-annex")?;
        info!("Added to git-annex with key: {}", annex_key.key);

        if let Err(e) = tokio::fs::remove_file(&temp_file_path).await {
            warn!(
                error = %e,
                path = %temp_file_path,
                "Failed to remove temporary blob file after ingest"
            );
        }

        // Create blob record in database
        let size_bytes = content.len() as i64;

        // Parse the annex key to get backend and hash
        let (backend, _, _) = Blob::parse_annex_key(&annex_key.key)
            .ok_or_else(|| eyre!("Invalid annex key format"))?;

        let blob = Blob::builder()
            .annex_backend(backend)
            .content_hash(annex_key.hash.clone())
            .original_filename(filename.to_string())
            .size_bytes(size_bytes)
            .mime_type(content_type.to_string())
            .checksum_blake3(blake3_hash.clone())
            .build();

        let blob_metadata = self.insert_blob(&blob).await?;
        let blob_key = blob_metadata.annex_key().clone();
        info!("Successfully ingested blob: {}", blob_key);

        if let Err(e) = self
            .publish_blob_event(
                "blob.ingested",
                BlobIngestedPayload {
                    blob_id: blob_key.clone(),
                    size_bytes,
                    mime_type: Some(content_type.to_string()),
                    checksum_blake3: blake3_hash,
                    deduplicated: false,
                    original_filename: filename.to_string(),
                },
                &blob_metadata,
            )
            .await
        {
            warn!(
                error = %e,
                "Failed to emit blob.ingested event for newly ingested file"
            );
        }

        Ok(blob_metadata)
    }

    /// Retrieve blob content as bytes
    pub async fn retrieve_content(&self, annex_key: &str) -> Result<Vec<u8>> {
        let start = Instant::now();

        // Ensure content is available locally
        self.annex.get_content(annex_key).await?;

        // Find the actual file path
        let path = self.find_symlink_path(annex_key).await?;

        // Read the content
        let content = tokio::fs::read(&path)
            .await
            .wrap_err("Failed to read blob content")?;

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
            let computed = format!("{:x}", hasher.finalize());

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
                bail!(
                    "Blob content hash mismatch for {} (expected {}, got {})",
                    annex_key,
                    expected,
                    computed
                );
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
                    bail!(
                        "Blob BLAKE3 hash mismatch for {} (expected {}, got {})",
                        annex_key,
                        expected_blake3,
                        computed
                    );
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
    pub async fn get_blob_path(&self, annex_key: &str) -> Result<Utf8PathBuf> {
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
    pub async fn verify_blob(&self, annex_key: &str) -> Result<bool> {
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
    async fn find_blob_by_blake3(&self, blake3_hash: &str) -> Result<Option<Blob>> {
        self.db_pool
            .blobs()
            .find_by_blake3(blake3_hash)
            .await
            .wrap_err("Failed to query blob by BLAKE3 hash")
    }

    /// Insert new blob metadata into database
    pub async fn insert_blob(&self, blob: &Blob) -> Result<Blob> {
        self.db_pool
            .blobs()
            .insert(blob.clone())
            .await
            .wrap_err("Failed to insert blob")
    }

    /// Get blob metadata by annex key
    pub async fn get_blob_metadata(&self, annex_key: &str) -> Result<Blob> {
        let (backend, size, hash_fragment) =
            Blob::parse_annex_key(annex_key).ok_or_else(|| eyre!("Invalid annex key format"))?;

        self.db_pool
            .blobs()
            .get_by_content(&backend, &hash_fragment, size)
            .await
            .wrap_err("Failed to get blob metadata")?
            .ok_or_else(|| eyre!("Blob not found with key: {}", annex_key))
    }

    /// Update verification status
    async fn update_verification_status(&self, annex_key: &str, status: &str) -> Result<()> {
        // First get the blob to get its ID
        let blob = self.get_blob_metadata(annex_key).await?;
        self.db_pool
            .blobs()
            .update_verification_status(blob.id, status)
            .await
            .wrap_err("Failed to update verification status")
    }

    /// Add original filename to existing blob
    async fn add_original_filename(&self, annex_key: &str, filename: &str) -> Result<()> {
        // First get the blob to get its ID
        let blob = self.get_blob_metadata(annex_key).await?;
        self.db_pool
            .blobs()
            .add_original_filename(blob.id, filename)
            .await
            .wrap_err("Failed to add original filename")
    }

    /// Find content path in repository for annex key
    async fn find_symlink_path(&self, annex_key: &str) -> Result<Utf8PathBuf> {
        let output = AsyncCommand::new("git-annex")
            .arg("contentlocation")
            .arg(annex_key)
            .current_dir(self.annex.repo_path())
            .output()
            .await
            .wrap_err("Failed to run git-annex contentlocation")?;

        if !output.status.success() {
            bail!(
                "git-annex contentlocation failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        let relative = String::from_utf8(output.stdout)
            .wrap_err("Invalid UTF-8 from git-annex contentlocation")?;
        let trimmed = relative.trim();
        if trimmed.is_empty() {
            bail!(
                "git-annex contentlocation returned empty path for {}",
                annex_key
            );
        }

        let path = self.annex.repo_path().join(trimmed);
        Ok(path)
    }

    /// Simple MIME type detection
    pub fn detect_mime_type(file_path: &Utf8Path) -> Result<String> {
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
    pub async fn emit_storage_stats(&self) -> Result<()> {
        // Get storage statistics from blob repository
        let stats = self
            .db_pool
            .blobs()
            .get_storage_stats()
            .await
            .wrap_err("Failed to get storage statistics")?;

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
            .await
            .wrap_err("Failed to register metrics source material")?;

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
                    return Err(eyre!(
                        "Failed to emit blob storage statistics: event channel closed"
                    ))
                }
            }
        } else {
            debug!("BlobManager event emission disabled; skipping storage.statistics event");
        }

        Ok(())
    }
}
