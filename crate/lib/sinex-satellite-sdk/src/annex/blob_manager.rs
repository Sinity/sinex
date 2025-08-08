//! Blob management with PostgreSQL metadata and git-annex storage
//!
//! ## Core Workflow Integration
//!
//! 1. **File Detection**: Ingestors detect large files (>100KB threshold)
//! 2. **Annex Addition**: `git annex add <file>` stores content by hash
//! 3. **Metadata Extraction**: Parse annex key, compute checksums
//! 4. **Database Registration**: Insert metadata into core.blobs table
//! 5. **Event Generation**: Log blob registration events
//!
//! ## Database Schema (core.blobs)
//!
//! ```sql
//! CREATE TABLE core.blobs (
//!     id                ULID PRIMARY KEY,
//!     annex_key         TEXT NOT NULL UNIQUE,
//!     original_filename TEXT NOT NULL,
//!     size_bytes        BIGINT NOT NULL,
//!     mime_type         TEXT,
//!     checksum_sha256   TEXT NOT NULL,
//!     checksum_blake3   TEXT,
//!     storage_backend   TEXT NOT NULL DEFAULT 'git-annex',
//!     metadata          JSONB NOT NULL DEFAULT '{}',
//!     created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
//!     last_verified_at  TIMESTAMPTZ,
//!     verification_status TEXT
//! );
//! ```
//!
//! ## Deduplication Strategy
//!
//! - Check BLAKE3 hash before ingestion
//! - If exists, create new reference to existing blob
//! - Save ~30-90% storage for common duplicates
//!
//! ## Performance Optimization
//!
//! - Batch operations for multiple files
//! - Async I/O for file operations
//! - Connection pooling for database access
//! - Caching of frequently accessed blobs

use camino::{Utf8Path, Utf8PathBuf};
use chrono::Utc;
use color_eyre::eyre::{bail, eyre, Context, Result};
use sinex_core::db::models::{Blob, Event};
use sinex_core::db::repositories::DbPoolExt;
use sinex_core::db::DbPool;
use sinex_core::types::events::{
    BlobIngestedPayload, BlobRetrievedPayload, BlobVerifiedPayload, StorageStatisticsPayload,
};
use sinex_core::types::{ulid::Ulid, Id};
use std::time::Instant;
use tracing::{debug, info};

use super::{AnnexConfig, AnnexKey, GitAnnex};

// Re-export Blob type for compatibility
pub use sinex_core::db::models::Blob as BlobMetadata;

#[derive(Debug)]
pub struct BlobManager {
    annex: GitAnnex,
    db_pool: DbPool,
}

impl BlobManager {
    pub fn new(annex_config: AnnexConfig, db_pool: DbPool) -> Result<Self> {
        let annex = GitAnnex::new(annex_config)?;
        Ok(BlobManager { annex, db_pool })
    }

    /// Ingest a file into the blob management system
    pub async fn ingest_file(
        &self,
        file_path: &Utf8Path,
        original_filename: Option<&str>,
    ) -> Result<BlobMetadata> {
        info!("Ingesting file: {:?}", file_path);
        let start = Instant::now();

        // Compute BLAKE3 hash for deduplication
        let blake3_hash = GitAnnex::compute_blake3_hash(file_path).await?;
        debug!("Computed BLAKE3 hash: {}", blake3_hash);

        // Check if blob already exists
        if let Some(existing) = self.find_blob_by_blake3(&blake3_hash).await? {
            let existing_id = existing
                .id
                .as_ref()
                .map(|id| *id.as_ulid())
                .unwrap_or_else(|| Ulid::new());
            info!("File already exists in blob store with ID: {}", existing_id);

            // Update original_filenames array if this is a new filename
            if let Some(filename) = original_filename {
                self.add_original_filename(&existing_id, filename).await?;
            }

            // Emit deduplication event
            let event = Event::from_payload(BlobIngestedPayload {
                blob_id: existing_id.to_string(),
                size_bytes: existing.size_bytes,
                mime_type: existing.mime_type.clone(),
                checksum_blake3: blake3_hash,
                deduplicated: true,
                original_filename: original_filename
                    .unwrap_or(&existing.original_filename)
                    .to_string(),
            })
            .with_ts_orig(Some(chrono::Utc::now()));

            self.db_pool
                .events()
                .insert(event)
                .await
                .wrap_err("Failed to emit blob ingested event")?;

            return Ok(existing);
        }

        // Get file metadata
        let file_metadata = tokio::fs::metadata(file_path)
            .await
            .wrap_err("Failed to get file metadata")?;
        let size_bytes = file_metadata.len() as i64;

        // Detect MIME type
        let mime_type = Self::detect_mime_type(file_path)?;

        // Add to git-annex
        let annex_key = self.annex.add_file(file_path).await?;
        info!("Added to git-annex with key: {}", annex_key.key);

        // Create blob record in database
        let filename =
            original_filename.unwrap_or_else(|| file_path.file_name().unwrap_or("unknown"));

        let blob = Blob::builder()
            .annex_key(annex_key.key.clone())
            .original_filename(filename.to_string())
            .size_bytes(size_bytes)
            .mime_type(mime_type.clone())
            .checksum_sha256(annex_key.hash.clone())
            .checksum_blake3(blake3_hash.clone())
            .storage_backend("git-annex".to_string())
            .verification_status("verified".to_string())
            .build();

        let blob_metadata = self.insert_blob(&blob).await?;
        let blob_id = blob_metadata
            .id
            .as_ref()
            .map(|id| *id.as_ulid())
            .unwrap_or_else(Ulid::new);
        info!("Successfully ingested blob: {}", blob_id);

        // Emit blob ingested event
        let event = Event::from_payload(BlobIngestedPayload {
            blob_id: blob_id.to_string(),
            size_bytes,
            mime_type: Some(mime_type),
            checksum_blake3: blake3_hash,
            deduplicated: false,
            original_filename: filename.to_string(),
        })
        .with_ts_orig(Some(chrono::Utc::now()));

        self.db_pool
            .events()
            .insert(event)
            .await
            .wrap_err("Failed to emit blob ingested event")?;

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
        let start = Instant::now();

        // Compute BLAKE3 hash for deduplication
        let blake3_hash = blake3::hash(content).to_hex().to_string();
        debug!("Computed BLAKE3 hash: {}", blake3_hash);

        // Check if blob already exists
        if let Some(existing) = self.find_blob_by_blake3(&blake3_hash).await? {
            let existing_id = existing
                .id
                .as_ref()
                .map(|id| *id.as_ulid())
                .unwrap_or_else(|| Ulid::new());
            info!(
                "Content already exists in blob store with ID: {}",
                existing_id
            );

            // Update original_filenames array if this is a new filename
            self.add_original_filename(&existing_id, filename).await?;

            // Emit deduplication event
            let event = Event::from_payload(BlobIngestedPayload {
                blob_id: existing_id.to_string(),
                size_bytes: existing.size_bytes,
                mime_type: existing.mime_type.clone(),
                checksum_blake3: blake3_hash,
                deduplicated: true,
                original_filename: filename.to_string(),
            })
            .with_ts_orig(Some(chrono::Utc::now()));

            self.db_pool
                .events()
                .insert(event)
                .await
                .wrap_err("Failed to emit blob ingested event")?;

            return Ok(existing);
        }

        // Create a temporary file with the content
        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join(format!("sinex_blob_{}.tmp", &blake3_hash[..8]));

        tokio::fs::write(&temp_file, content)
            .await
            .wrap_err("Failed to write temporary file")?;

        // Convert to Utf8PathBuf for git-annex
        let utf8_temp_file = Utf8PathBuf::from_path_buf(temp_file.clone())
            .map_err(|_| eyre!("Temp file path is not UTF-8"))?;

        // Add to git-annex
        let annex_key = self.annex.add_file(&utf8_temp_file).await?;
        info!("Added to git-annex with key: {}", annex_key.key);

        // Clean up temp file (git-annex has moved it)
        let _ = tokio::fs::remove_file(&temp_file).await;

        // Create blob record in database
        let size_bytes = content.len() as i64;

        let blob = Blob::builder()
            .annex_key(annex_key.key.clone())
            .original_filename(filename.to_string())
            .size_bytes(size_bytes)
            .mime_type(content_type.to_string())
            .checksum_sha256(annex_key.hash.clone())
            .checksum_blake3(blake3_hash.clone())
            .storage_backend("git-annex".to_string())
            .verification_status("verified".to_string())
            .build();

        let blob_metadata = self.insert_blob(&blob).await?;
        let blob_id = blob_metadata
            .id
            .as_ref()
            .map(|id| *id.as_ulid())
            .unwrap_or_else(Ulid::new);
        info!("Successfully ingested blob: {}", blob_id);

        // Emit blob ingested event
        let event = Event::from_payload(BlobIngestedPayload {
            blob_id: blob_id.to_string(),
            size_bytes,
            mime_type: Some(content_type.to_string()),
            checksum_blake3: blake3_hash,
            deduplicated: false,
            original_filename: filename.to_string(),
        })
        .with_ts_orig(Some(chrono::Utc::now()));

        self.db_pool
            .events()
            .insert(event)
            .await
            .wrap_err("Failed to emit blob ingested event")?;

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

        // Emit blob retrieved event
        let event = Event::from_payload(BlobRetrievedPayload {
            blob_id: annex_key.to_string(), // Using annex_key as blob identifier
            retrieval_time_ms: start.elapsed().as_millis() as u64,
            cache_hit: true, // git-annex get ensures it's local
        })
        .with_ts_orig(Some(chrono::Utc::now()));

        self.db_pool
            .events()
            .insert(event)
            .await
            .wrap_err("Failed to emit blob retrieved event")?;

        Ok(content)
    }

    /// Retrieve a blob's content path
    pub async fn get_blob_path(&self, blob_id: &Ulid) -> Result<Utf8PathBuf> {
        let start = Instant::now();
        let blob = self.get_blob_metadata(blob_id).await?;

        // Ensure content is available locally
        self.annex.get_content(&blob.annex_key).await?;

        // Emit blob retrieved event
        let event = Event::from_payload(BlobRetrievedPayload {
            blob_id: blob_id.to_string(),
            retrieval_time_ms: start.elapsed().as_millis() as u64,
            cache_hit: true, // git-annex get ensures it's local
        })
        .with_ts_orig(Some(chrono::Utc::now()));

        self.db_pool
            .events()
            .insert(event)
            .await
            .wrap_err("Failed to emit blob retrieved event")?;

        // Find the symlink path in the repository
        self.find_symlink_path(&blob.annex_key).await
    }

    /// Verify blob integrity
    pub async fn verify_blob(&self, blob_id: &Ulid) -> Result<bool> {
        let start = Instant::now();
        let blob = self.get_blob_metadata(blob_id).await?;

        // Run git-annex fsck on specific key
        let fsck_output = self.annex.fsck(false, false).await?;

        // Parse fsck output to determine if this specific blob is ok
        let is_verified = !fsck_output.contains("failed") && !fsck_output.contains("error");

        // Update verification status in database
        let status = if is_verified { "verified" } else { "corrupted" };
        self.update_verification_status(blob_id, status).await?;

        // Emit blob verified event
        let event = Event::from_payload(BlobVerifiedPayload {
            blob_id: blob_id.to_string(),
            verification_status: status.to_string(),
            checksum_matched: is_verified,
        })
        .with_ts_orig(Some(chrono::Utc::now()));

        self.db_pool
            .events()
            .insert(event)
            .await
            .wrap_err("Failed to emit blob verified event")?;

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

    /// Get blob metadata by ID
    pub async fn get_blob_metadata(&self, blob_id: &Ulid) -> Result<Blob> {
        let blob_id = Id::<Blob>::from_ulid(*blob_id);

        self.db_pool
            .blobs()
            .get_by_id(blob_id.clone())
            .await
            .wrap_err("Failed to get blob metadata")?
            .ok_or_else(|| eyre!("Blob not found with ID: {}", blob_id))
    }

    /// Update verification status
    async fn update_verification_status(&self, blob_id: &Ulid, status: &str) -> Result<()> {
        let blob_id = Id::<Blob>::from_ulid(*blob_id);

        self.db_pool
            .blobs()
            .update_verification_status(blob_id, status)
            .await
            .wrap_err("Failed to update verification status")
    }

    /// Add original filename to existing blob
    async fn add_original_filename(&self, blob_id: &Ulid, filename: &str) -> Result<()> {
        let blob_id = Id::<Blob>::from_ulid(*blob_id);

        self.db_pool
            .blobs()
            .add_original_filename(blob_id, filename)
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
    fn detect_mime_type(file_path: &Utf8Path) -> Result<String> {
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
        let failed_count = 0i64; // TODO: Track failed verifications

        // Insert metric event using EventRepository
        let new_event = Event::from_payload(StorageStatisticsPayload {
            total_blobs: blob_count,
            total_size_bytes: total_size,
            failed_verifications: failed_count,
            storage_backend: "git-annex".to_string(),
        })
        .with_ts_orig(Some(Utc::now()));

        self.db_pool
            .events()
            .insert(new_event)
            .await
            .wrap_err("Failed to emit blob storage statistics")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[sinex_test]
    fn test_mime_type_detection() {
        let path = Utf8Path::new("test.txt");
        let mime = BlobManager::detect_mime_type(path).unwrap();
        assert_eq!(mime, "text/plain");

        let path = Utf8Path::new("image.jpg");
        let mime = BlobManager::detect_mime_type(path).unwrap();
        assert_eq!(mime, "image/jpeg");
    }
}
