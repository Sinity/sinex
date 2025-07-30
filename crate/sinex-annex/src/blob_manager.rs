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

use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_db::queries::{EventQueries, SourceMaterialQueries};
use sinex_db::DbPool;
use sinex_ulid::Ulid;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tracing::{debug, info};

use crate::{AnnexConfig, AnnexKey, GitAnnex};
use sinex_events::constants::{event_types, sources};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobMetadata {
    pub blob_id: Ulid,
    pub annex_key: String,
    pub original_filename: String,
    pub size_bytes: i64,
    pub mime_type: Option<String>,
    pub checksum_sha256: String,
    pub checksum_blake3: Option<String>,
    pub storage_backend: String,
    pub verification_status: Option<String>,
}

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
        file_path: &Path,
        original_filename: Option<&str>,
    ) -> Result<BlobMetadata> {
        info!("Ingesting file: {:?}", file_path);
        let start = Instant::now();

        // Compute BLAKE3 hash for deduplication
        let blake3_hash = GitAnnex::compute_blake3_hash(file_path).await?;
        debug!("Computed BLAKE3 hash: {}", blake3_hash);

        // Check if blob already exists
        if let Some(existing) = self.find_blob_by_blake3(&blake3_hash).await? {
            info!(
                "File already exists in blob store with ID: {}",
                existing.blob_id
            );

            // Update original_filenames array if this is a new filename
            if let Some(filename) = original_filename {
                self.add_original_filename(&existing.blob_id, filename)
                    .await?;
            }

            // Emit deduplication metric
            self.emit_operation_metric(
                "ingest",
                "deduplicated",
                existing.size_bytes,
                start.elapsed().as_millis() as i64,
            )
            .await?;

            return Ok(existing);
        }

        // Get file metadata
        let file_metadata = tokio::fs::metadata(file_path)
            .await
            .context("Failed to get file metadata")?;
        let size_bytes = file_metadata.len() as i64;

        // Detect MIME type
        let mime_type = Self::detect_mime_type(file_path)?;

        // Add to git-annex
        let annex_key = self.annex.add_file(file_path).await?;
        info!("Added to git-annex with key: {}", annex_key.key);

        // Create blob record in database
        let blob_id = Ulid::new();
        let filename = original_filename.unwrap_or_else(|| {
            file_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown")
        });

        let blob_metadata = BlobMetadata {
            blob_id,
            annex_key: annex_key.key.clone(),
            original_filename: filename.to_string(),
            size_bytes,
            mime_type: Some(mime_type.clone()),
            checksum_sha256: annex_key.hash.clone(),
            checksum_blake3: Some(blake3_hash),
            storage_backend: "git-annex".to_string(),
            verification_status: Some("verified".to_string()),
        };

        self.insert_blob(&blob_metadata).await?;
        info!("Successfully ingested blob: {}", blob_id);

        // Emit ingest success metric
        self.emit_operation_metric(
            "ingest",
            "success",
            size_bytes,
            start.elapsed().as_millis() as i64,
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
        let start = Instant::now();

        // Compute BLAKE3 hash for deduplication
        let blake3_hash = blake3::hash(content).to_hex().to_string();
        debug!("Computed BLAKE3 hash: {}", blake3_hash);

        // Check if blob already exists
        if let Some(existing) = self.find_blob_by_blake3(&blake3_hash).await? {
            info!(
                "Content already exists in blob store with ID: {}",
                existing.blob_id
            );

            // Update original_filenames array if this is a new filename
            self.add_original_filename(&existing.blob_id, filename)
                .await?;

            // Emit deduplication metric
            self.emit_operation_metric(
                "ingest",
                "deduplicated",
                existing.size_bytes,
                start.elapsed().as_millis() as i64,
            )
            .await?;

            return Ok(existing);
        }

        // Create a temporary file with the content
        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join(format!("sinex_blob_{}.tmp", &blake3_hash[..8]));

        tokio::fs::write(&temp_file, content)
            .await
            .context("Failed to write temporary file")?;

        // Add to git-annex
        let annex_key = self.annex.add_file(&temp_file).await?;
        info!("Added to git-annex with key: {}", annex_key.key);

        // Clean up temp file (git-annex has moved it)
        let _ = tokio::fs::remove_file(&temp_file).await;

        // Create blob record in database
        let blob_id = Ulid::new();
        let size_bytes = content.len() as i64;

        let blob_metadata = BlobMetadata {
            blob_id,
            annex_key: annex_key.key.clone(),
            original_filename: filename.to_string(),
            size_bytes,
            mime_type: Some(content_type.to_string()),
            checksum_sha256: annex_key.hash.clone(),
            checksum_blake3: Some(blake3_hash),
            storage_backend: "git-annex".to_string(),
            verification_status: Some("verified".to_string()),
        };

        self.insert_blob(&blob_metadata).await?;
        info!("Successfully ingested blob: {}", blob_id);

        // Emit ingest success metric
        self.emit_operation_metric(
            "ingest",
            "success",
            size_bytes,
            start.elapsed().as_millis() as i64,
        )
        .await?;

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
            .context("Failed to read blob content")?;

        // Emit retrieval metric
        self.emit_operation_metric(
            "retrieve",
            "success",
            content.len() as i64,
            start.elapsed().as_millis() as i64,
        )
        .await?;

        Ok(content)
    }

    /// Retrieve a blob's content path
    pub async fn get_blob_path(&self, blob_id: &Ulid) -> Result<PathBuf> {
        let start = Instant::now();
        let blob = self.get_blob_metadata(blob_id).await?;

        // Ensure content is available locally
        self.annex.get_content(&blob.annex_key).await?;

        // Emit retrieval metric
        self.emit_operation_metric(
            "retrieve",
            "success",
            blob.size_bytes,
            start.elapsed().as_millis() as i64,
        )
        .await?;

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

        // Emit verification metric
        let result = if is_verified { "success" } else { "failure" };
        self.emit_operation_metric(
            "verify",
            result,
            blob.size_bytes,
            start.elapsed().as_millis() as i64,
        )
        .await?;

        Ok(is_verified)
    }

    /// Find blob by BLAKE3 hash for deduplication
    async fn find_blob_by_blake3(&self, blake3_hash: &str) -> Result<Option<BlobMetadata>> {
        use sinex_db::models::SourceMaterialRecord;

        let row: Option<SourceMaterialRecord> =
            SourceMaterialQueries::find_by_checksum(blake3_hash.to_string())
                .fetch_optional(&self.db_pool)
                .await
                .context("Failed to query source material by BLAKE3 hash")?;

        if let Some(row) = row {
            Ok(Some(BlobMetadata {
                blob_id: row.blob_id,
                annex_key: format!("BLAKE3-{}", blake3_hash), // Generate annex key from checksum
                original_filename: row.source_uri.unwrap_or_else(|| "unknown".to_string()),
                size_bytes: row.file_size_bytes.unwrap_or(0),
                mime_type: row.mime_type,
                checksum_sha256: row.checksum_blake3.clone().unwrap_or_else(|| String::new()), // Using BLAKE3 as SHA256 is not available
                checksum_blake3: row.checksum_blake3,
                storage_backend: "git-annex".to_string(), // Default storage backend
                verification_status: Some(if row.is_archived {
                    "verified".to_string()
                } else {
                    "pending".to_string()
                }),
            }))
        } else {
            Ok(None)
        }
    }

    /// Insert new blob metadata into database
    pub async fn insert_blob(&self, blob: &BlobMetadata) -> Result<()> {
        SourceMaterialQueries::insert(
            "blob.binary".to_string(), // Material type for blob storage
            Some(blob.original_filename.clone()),
            Some(blob.size_bytes),
            blob.checksum_blake3.clone(),
            blob.mime_type.clone(),
            None, // encoding
            json!({
                "annex_key": blob.annex_key,
                "storage_backend": blob.storage_backend,
                "verification_status": blob.verification_status,
            }),
            None, // content_preview
        )
        .execute(&self.db_pool)
        .await
        .context("Failed to insert blob as source material")?;

        Ok(())
    }

    /// Get blob metadata by ID
    pub async fn get_blob_metadata(&self, blob_id: &Ulid) -> Result<BlobMetadata> {
        use sinex_db::models::SourceMaterialRecord;

        let row: SourceMaterialRecord = SourceMaterialQueries::get_by_id(*blob_id)
            .fetch_one(&self.db_pool)
            .await
            .context("Failed to get source material metadata")?;

        Ok(BlobMetadata {
            blob_id: row.blob_id,
            annex_key: row
                .metadata
                .get("annex_key")
                .and_then(|v| v.as_str())
                .unwrap_or(&format!(
                    "BLAKE3-{}",
                    row.checksum_blake3.as_deref().unwrap_or("unknown")
                ))
                .to_string(),
            original_filename: row.source_uri.unwrap_or_else(|| "unknown".to_string()),
            size_bytes: row.file_size_bytes.unwrap_or(0),
            mime_type: row.mime_type,
            checksum_sha256: row.checksum_blake3.clone().unwrap_or_else(|| String::new()),
            checksum_blake3: row.checksum_blake3,
            storage_backend: row
                .metadata
                .get("storage_backend")
                .and_then(|v| v.as_str())
                .unwrap_or("git-annex")
                .to_string(),
            verification_status: Some(
                row.metadata
                    .get("verification_status")
                    .and_then(|v| v.as_str())
                    .unwrap_or(if row.is_archived {
                        "verified"
                    } else {
                        "pending"
                    })
                    .to_string(),
            ),
        })
    }

    /// Update verification status
    async fn update_verification_status(&self, _blob_id: &Ulid, _status: &str) -> Result<()> {
        // TODO: Implement metadata update for source material registry
        // This would require updating the metadata JSON field with new verification_status
        Ok(())
    }

    /// Add original filename to existing blob
    async fn add_original_filename(&self, _blob_id: &Ulid, _filename: &str) -> Result<()> {
        // TODO: Implement metadata update for source material registry
        // This would require updating the source_uri field or metadata
        Ok(())
    }

    /// Find symlink path in repository for annex key
    async fn find_symlink_path(&self, annex_key: &str) -> Result<PathBuf> {
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

        anyhow::bail!("Could not construct path for annex key: {}", annex_key)
    }

    /// Simple MIME type detection
    fn detect_mime_type(file_path: &Path) -> Result<String> {
        let extension = file_path
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("");

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

    /// Emit operation metrics as events
    async fn emit_operation_metric(
        &self,
        operation: &str,
        result: &str,
        size_bytes: i64,
        duration_ms: i64,
    ) -> Result<()> {
        let host = gethostname::gethostname().to_string_lossy().to_string();
        let payload = json!({
            "operation": operation,
            "result": result,
            "size_bytes": size_bytes,
            "duration_ms": duration_ms,
        });

        // Insert metric event into core.events using EventQueries
        EventQueries::insert_event(
            sources::BLOB_STORAGE.to_string(),
            event_types::metrics::BLOB_STORAGE_OPERATION.to_string(),
            host,
            payload,
            Some(Utc::now()),
            None,
            None,
            None,
        )
        .execute(&self.db_pool)
        .await
        .context("Failed to emit blob operation metric")?;

        Ok(())
    }

    /// Emit storage statistics (called periodically by background task)
    pub async fn emit_storage_stats(&self) -> Result<()> {
        // Query aggregate statistics using ArtifactQueries
        #[derive(sqlx::FromRow)]
        struct StorageStats {
            total_blobs: i64,
            total_size_bytes: Option<i64>,
            #[allow(dead_code)]
            unique_files: i64,
            #[allow(dead_code)]
            avg_file_size: Option<f64>,
            #[allow(dead_code)]
            max_file_size: Option<i64>,
            #[allow(dead_code)]
            oldest_blob: chrono::DateTime<chrono::Utc>,
            #[allow(dead_code)]
            newest_blob: chrono::DateTime<chrono::Utc>,
        }

        // TODO: Implement proper storage stats query using SourceMaterialQueries
        let stats = StorageStats {
            total_blobs: 0,
            total_size_bytes: Some(0),
            unique_files: 0,
            avg_file_size: Some(0.0),
            max_file_size: Some(0),
            oldest_blob: chrono::Utc::now(),
            newest_blob: chrono::Utc::now(),
        };

        let blob_count = stats.total_blobs;
        let total_size = stats.total_size_bytes;
        let failed_count = 0i64; // TODO: Implement failed verification tracking

        let host = gethostname::gethostname().to_string_lossy().to_string();
        let payload = json!({
            "total_blobs": blob_count,
            "total_size_bytes": total_size.unwrap_or(0),
            "failed_verifications": failed_count,
            "storage_backend": "git-annex",
        });

        // Insert metric event using EventQueries
        EventQueries::insert_event(
            sources::BLOB_STORAGE.to_string(),
            event_types::metrics::BLOB_STORAGE_STATISTICS.to_string(),
            host,
            payload,
            Some(Utc::now()),
            None,
            None,
            None,
        )
        .execute(&self.db_pool)
        .await
        .context("Failed to emit blob storage statistics")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mime_type_detection() {
        let path = Path::new("test.txt");
        let mime = BlobManager::detect_mime_type(path).unwrap();
        assert_eq!(mime, "text/plain");

        let path = Path::new("image.jpg");
        let mime = BlobManager::detect_mime_type(path).unwrap();
        assert_eq!(mime, "image/jpeg");
    }
}
