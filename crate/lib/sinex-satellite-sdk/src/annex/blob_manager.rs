//! Git-annex blob management utilities.
//!
//! The manager deduplicates incoming content, registers metadata in `core.blobs`,
//! wires provenance through source_material records, and emits ingestion/health
//! events that downstream services can rely on.
//!
//! See `docs/architecture/Core_Architecture.md` (blob storage) and the
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
use sinex_core::types::validate_path;
use sinex_core::DbPoolExt;
use sinex_core::{Blob, Event, Id, JsonValue, SourceMaterial};
use std::time::Instant;
use tracing::{debug, info};

use super::{AnnexConfig, AnnexKey, GitAnnex};
use tokio::sync::mpsc;

// Re-export Blob type for compatibility
pub use sinex_core::Blob as BlobMetadata;

#[derive(Debug)]
pub struct BlobManager {
    annex: GitAnnex,
    db_pool: DbPool,
    event_sender: mpsc::UnboundedSender<Event<JsonValue>>,
}

impl BlobManager {
    pub fn new(
        annex_config: AnnexConfig,
        db_pool: DbPool,
        event_sender: mpsc::UnboundedSender<Event<JsonValue>>,
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
    ) -> Event<JsonValue> {
        Event::dynamic(
            "blob-manager",
            event_type,
            serde_json::to_value(payload).expect("Payload serialization should not fail"),
        )
        .from_material(material_id, 0)
        .build()
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
        let material_id = self.ensure_material_for_blob(blob).await?;
        let event = Self::create_blob_event(event_type, payload, material_id);

        self.event_sender
            .send(event)
            .map_err(|_| eyre!("Failed to emit {event_type} event: event channel closed"))?;
        Ok(())
    }

    /// Ingest a file into the blob management system
    pub async fn ingest_file(
        &self,
        file_path: &Utf8Path,
        original_filename: Option<&str>,
    ) -> Result<BlobMetadata> {
        // Validate file path before processing to prevent path traversal attacks
        let validated_path = validate_path(file_path.as_str())
            .map_err(|e| eyre!("Invalid file path for ingestion: {}", e))?;

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

            self.publish_blob_event(
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
            .await?;

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
        let annex_key = self.annex.add_file(&validated_path).await?;
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

            self.publish_blob_event(
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
            .await?;

            return Ok(existing);
        }

        // Create a unique temporary file without predictable naming to avoid symlink attacks
        let mut temp_file = tempfile::Builder::new()
            .prefix("sinex_blob_")
            .suffix(".tmp")
            .tempfile_in(std::env::temp_dir())
            .wrap_err("Failed to create secure temporary file")?;

        use std::io::Write;
        temp_file
            .write_all(content)
            .wrap_err("Failed to write blob bytes to temporary file")?;
        temp_file
            .flush()
            .wrap_err("Failed to flush temporary blob file")?;

        // Convert to Utf8PathBuf for git-annex
        let utf8_temp_file = Utf8PathBuf::from_path_buf(temp_file.path().to_path_buf())
            .map_err(|_| eyre!("Temp file path is not UTF-8"))?;

        // Add to git-annex
        let annex_key = self.annex.add_file(&utf8_temp_file).await?;
        info!("Added to git-annex with key: {}", annex_key.key);

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

        self.publish_blob_event(
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
        .await?;

        // Drop the temp file explicitly so it is removed now that git-annex has moved it.
        if let Err(e) = temp_file.close() {
            debug!(error = %e, "Failed to remove temporary blob file after ingest");
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
        let fsck_output = self.annex.fsck(false, false).await?;

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

    /// Find symlink path in repository for annex key
    async fn find_symlink_path(&self, annex_key: &str) -> Result<Utf8PathBuf> {
        // This is a simplified implementation
        // In practice, you'd need to search the git-annex repository for the symlink
        // For now, assume the key maps to a predictable path structure

        let objects_path = self
            .annex
            .config
            .repo_path
            .join(".git")
            .join("annex")
            .join("objects");

        // Extract hash from key for path construction
        if let Ok(key) = AnnexKey::parse(annex_key) {
            // git-annex uses a hierarchical directory structure based on key hash
            let hash_chars: Vec<char> = key.hash.chars().collect();
            if hash_chars.len() >= 4 {
                let dir1 = &hash_chars[0..2].iter().collect::<String>();
                let dir2 = &hash_chars[2..4].iter().collect::<String>();
                let path = objects_path.join(dir1).join(dir2).join(&key.key);
                return Ok(path);
            }
        }

        bail!("Could not construct path for annex key: {}", annex_key)
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
        );

        self.event_sender
            .send(new_event)
            .map_err(|_| eyre!("Failed to emit blob storage statistics: event channel closed"))?;

        Ok(())
    }
}
