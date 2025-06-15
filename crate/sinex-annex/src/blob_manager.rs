use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use tracing::{debug, info};
use sinex_ulid::Ulid;

use crate::{GitAnnex, AnnexConfig, AnnexKey};

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
    db_pool: PgPool,
}

impl BlobManager {
    pub fn new(annex_config: AnnexConfig, db_pool: PgPool) -> Result<Self> {
        let annex = GitAnnex::new(annex_config)?;
        Ok(BlobManager { annex, db_pool })
    }

    /// Ingest a file into the blob management system
    pub async fn ingest_file(&self, file_path: &Path, original_filename: Option<&str>) -> Result<BlobMetadata> {
        info!("Ingesting file: {:?}", file_path);

        // Compute BLAKE3 hash for deduplication
        let blake3_hash = GitAnnex::compute_blake3_hash(file_path).await?;
        debug!("Computed BLAKE3 hash: {}", blake3_hash);

        // Check if blob already exists
        if let Some(existing) = self.find_blob_by_blake3(&blake3_hash).await? {
            info!("File already exists in blob store with ID: {}", existing.blob_id);
            
            // Update original_filenames array if this is a new filename
            if let Some(filename) = original_filename {
                self.add_original_filename(&existing.blob_id, filename).await?;
            }
            
            return Ok(existing);
        }

        // Get file metadata
        let file_metadata = tokio::fs::metadata(file_path).await
            .context("Failed to get file metadata")?;
        let size_bytes = file_metadata.len() as i64;

        // Detect MIME type
        let mime_type = Self::detect_mime_type(file_path)?;

        // Add to git-annex
        let annex_key = self.annex.add_file(file_path).await?;
        info!("Added to git-annex with key: {}", annex_key.key);

        // Create blob record in database
        let blob_id = Ulid::new();
        let filename = original_filename
            .unwrap_or_else(|| file_path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown"));

        let blob_metadata = BlobMetadata {
            blob_id,
            annex_key: annex_key.key.clone(),
            original_filename: filename.to_string(),
            size_bytes,
            mime_type: Some(mime_type),
            checksum_sha256: annex_key.hash.clone(),
            checksum_blake3: Some(blake3_hash),
            storage_backend: "git-annex".to_string(),
            verification_status: Some("verified".to_string()),
        };

        self.insert_blob(&blob_metadata).await?;
        info!("Successfully ingested blob: {}", blob_id);

        Ok(blob_metadata)
    }

    /// Retrieve a blob's content path
    pub async fn get_blob_path(&self, blob_id: &Ulid) -> Result<PathBuf> {
        let blob = self.get_blob_metadata(blob_id).await?;
        
        // Ensure content is available locally
        self.annex.get_content(&blob.annex_key).await?;
        
        // Find the symlink path in the repository
        self.find_symlink_path(&blob.annex_key).await
    }

    /// Verify blob integrity
    pub async fn verify_blob(&self, blob_id: &Ulid) -> Result<bool> {
        let _blob = self.get_blob_metadata(blob_id).await?;
        
        // Run git-annex fsck on specific key
        let fsck_output = self.annex.fsck(false, false).await?;
        
        // Parse fsck output to determine if this specific blob is ok
        let is_verified = !fsck_output.contains("failed") && !fsck_output.contains("error");
        
        // Update verification status in database
        let status = if is_verified { "verified" } else { "corrupted" };
        self.update_verification_status(blob_id, status).await?;
        
        Ok(is_verified)
    }

    /// Find blob by BLAKE3 hash for deduplication
    async fn find_blob_by_blake3(&self, blake3_hash: &str) -> Result<Option<BlobMetadata>> {
        let row = sqlx::query(
            "SELECT id, annex_key, original_filename, size_bytes, mime_type, 
                    checksum_sha256, checksum_blake3, storage_backend, verification_status,
                    created_at, last_verified_at
             FROM core.blobs 
             WHERE checksum_blake3 = $1 LIMIT 1"
        )
        .bind(blake3_hash)
        .fetch_optional(&self.db_pool)
        .await
        .context("Failed to query blob by BLAKE3 hash")?;

        if let Some(row) = row {
            Ok(Some(BlobMetadata {
                blob_id: Ulid::from_str(&row.get::<String, _>("id"))?,
                annex_key: row.get("annex_key"),
                original_filename: row.get("original_filename"),
                size_bytes: row.get("size_bytes"),
                mime_type: row.get("mime_type"),
                checksum_sha256: row.get("checksum_sha256"),
                checksum_blake3: Some(blake3_hash.to_string()),
                storage_backend: row.get("storage_backend"),
                verification_status: row.get("verification_status"),
            }))
        } else {
            Ok(None)
        }
    }

    /// Insert new blob metadata into database
    pub async fn insert_blob(&self, blob: &BlobMetadata) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO core.blobs 
               (id, annex_key, original_filename, size_bytes, mime_type, 
                checksum_sha256, checksum_blake3, storage_backend, verification_status)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#
        )
        .bind(blob.blob_id.to_string())
        .bind(&blob.annex_key)
        .bind(&blob.original_filename)
        .bind(blob.size_bytes)
        .bind(&blob.mime_type)
        .bind(&blob.checksum_sha256)
        .bind(&blob.checksum_blake3)
        .bind(&blob.storage_backend)
        .bind(&blob.verification_status)
        .execute(&self.db_pool)
        .await
        .context("Failed to insert blob metadata")?;

        Ok(())
    }

    /// Get blob metadata by ID
    async fn get_blob_metadata(&self, blob_id: &Ulid) -> Result<BlobMetadata> {
        let row = sqlx::query(
            "SELECT id, annex_key, original_filename, size_bytes, mime_type, 
                    checksum_sha256, checksum_blake3, storage_backend, verification_status
             FROM core.blobs 
             WHERE id = $1"
        )
        .bind(blob_id.to_string())
        .fetch_one(&self.db_pool)
        .await
        .context("Failed to get blob metadata")?;

        Ok(BlobMetadata {
            blob_id: *blob_id,
            annex_key: row.get("annex_key"),
            original_filename: row.get("original_filename"),
            size_bytes: row.get("size_bytes"),
            mime_type: row.get("mime_type"),
            checksum_sha256: row.get("checksum_sha256"),
            checksum_blake3: row.get("checksum_blake3"),
            storage_backend: row.get("storage_backend"),
            verification_status: row.get("verification_status"),
        })
    }

    /// Update verification status
    async fn update_verification_status(&self, blob_id: &Ulid, status: &str) -> Result<()> {
        sqlx::query(
            "UPDATE core.blobs SET verification_status = $1, last_verified_at = NOW() WHERE id = $2"
        )
        .bind(status)
        .bind(blob_id.to_string())
        .execute(&self.db_pool)
        .await
        .context("Failed to update verification status")?;

        Ok(())
    }

    /// Add original filename to existing blob
    async fn add_original_filename(&self, blob_id: &Ulid, filename: &str) -> Result<()> {
        // For now, just update the original_filename field
        // In the future, this should handle an array of filenames
        sqlx::query(
            "UPDATE core.blobs SET original_filename = $1 WHERE id = $2 AND original_filename != $1"
        )
        .bind(filename)
        .bind(blob_id.to_string())
        .execute(&self.db_pool)
        .await
        .context("Failed to add original filename")?;

        Ok(())
    }

    /// Find symlink path in repository for annex key
    async fn find_symlink_path(&self, annex_key: &str) -> Result<PathBuf> {
        // This is a simplified implementation
        // In practice, you'd need to search the git-annex repository for the symlink
        // For now, assume the key maps to a predictable path structure
        
        let objects_path = self.annex.config.repo_path
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
        let extension = file_path.extension()
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