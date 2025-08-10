//! Content service for managing event content and source material

use crate::error::{ServiceError, ServiceResult};
use sinex_core::db::DbPool;
use sinex_satellite_sdk::annex::BlobManager;
use std::sync::Arc;

pub struct ContentService {
    blob_manager: Arc<BlobManager>,
}

impl ContentService {
    pub fn new(_pool: DbPool, blob_manager: Arc<BlobManager>) -> Self {
        Self { blob_manager }
    }

    /// Store large content as blob and return source material reference
    pub async fn store_large_content(
        &self,
        content: &[u8],
        filename: &str,
        content_type: &str,
        _source: &str,
    ) -> ServiceResult<String> {
        // Store in git-annex (blob manager handles source material registration automatically)
        let blob_metadata = self
            .blob_manager
            .ingest_from_bytes(content, filename, content_type)
            .await
            .map_err(|e| ServiceError::OperationFailed(format!("Blob storage failed: {}", e)))?;

        // The blob manager has already created the source material record
        // Return the annex key for referencing the stored content
        Ok(blob_metadata.annex_key)
    }

    /// Store content via BlobManager (convenience method for store_large_content)
    pub async fn store_content(
        &self,
        content: &[u8],
        filename: &str,
        content_type: &str,
        _source: &str,
    ) -> ServiceResult<String> {
        self.store_large_content(content, filename, content_type, _source)
            .await
    }

    /// Retrieve content by annex key
    pub async fn retrieve_content(&self, annex_key: &str) -> ServiceResult<Vec<u8>> {
        // Retrieve from blob storage directly
        self.blob_manager
            .retrieve_content(annex_key)
            .await
            .map_err(|e| ServiceError::OperationFailed(format!("Content retrieval failed: {}", e)))
    }

    /// Get content metadata by blob ID
    pub async fn get_content_metadata(
        &self,
        blob_id: sinex_core::types::ulid::Ulid,
    ) -> ServiceResult<sinex_satellite_sdk::annex::BlobMetadata> {
        // Get blob metadata from blob manager
        let blob_metadata = self
            .blob_manager
            .get_blob_metadata(&blob_id)
            .await
            .map_err(|e| {
                ServiceError::OperationFailed(format!("Failed to get blob metadata: {}", e))
            })?;

        Ok(blob_metadata)
    }

    /// Verify content integrity by blob ID
    pub async fn verify_content(
        &self,
        blob_id: sinex_core::types::ulid::Ulid,
    ) -> ServiceResult<bool> {
        // Use blob manager verification
        self.blob_manager.verify_blob(&blob_id).await.map_err(|e| {
            ServiceError::OperationFailed(format!("Content verification failed: {}", e))
        })
    }
}
