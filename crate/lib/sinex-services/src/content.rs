//! Content service for managing event content and source material

use crate::error::{Result as ServiceResult, SinexError};
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

    /// Store content as blob and return annex key
    ///
    /// All content is stored via the blob manager regardless of size, providing
    /// consistent storage, deduplication, and source material tracking.
    pub async fn store_content(
        &self,
        content: &[u8],
        filename: &str,
        content_type: &str,
        _source: &str,
    ) -> ServiceResult<String> {
        let blob_metadata = self
            .blob_manager
            .ingest_from_bytes(content, filename, content_type)
            .await
            .map_err(|e| {
                SinexError::service(format!("Blob storage failed: {}", e))
                    .with_operation("blob_manager.ingest_from_bytes")
                    .with_context("filename", filename)
                    .with_context("content_type", content_type)
            })?;

        Ok(blob_metadata.annex_key())
    }

    /// Retrieve content by annex key
    pub async fn retrieve_content(&self, annex_key: &str) -> ServiceResult<Vec<u8>> {
        self.blob_manager
            .retrieve_content(annex_key)
            .await
            .map_err(|e| {
                SinexError::service(format!("Content retrieval failed: {}", e))
                    .with_operation("blob_manager.retrieve_content")
                    .with_context("annex_key", annex_key)
            })
    }

    /// Get content metadata by annex key
    pub async fn get_content_metadata(
        &self,
        annex_key: &str,
    ) -> ServiceResult<sinex_satellite_sdk::annex::BlobMetadata> {
        let blob_metadata = self
            .blob_manager
            .get_blob_metadata(annex_key)
            .await
            .map_err(|e| {
                SinexError::service(format!("Failed to get blob metadata: {}", e))
                    .with_operation("blob_manager.get_blob_metadata")
                    .with_context("annex_key", annex_key)
            })?;

        Ok(blob_metadata)
    }

    /// Verify content integrity by annex key
    pub async fn verify_content(&self, annex_key: &str) -> ServiceResult<bool> {
        self.blob_manager.verify_blob(annex_key).await.map_err(|e| {
            SinexError::service(format!("Content verification failed: {}", e))
                .with_operation("blob_manager.verify_blob")
                .with_context("annex_key", annex_key)
        })
    }
}
