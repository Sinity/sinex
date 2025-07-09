//! Content service for managing event content and artifacts

use crate::error::{ServiceError, ServiceResult};
use sinex_annex::BlobManager;
use sinex_db::{artifacts, DbPool};
use std::sync::Arc;

pub struct ContentService {
    pool: DbPool,
    blob_manager: Arc<BlobManager>,
}

impl ContentService {
    pub fn new(pool: DbPool, blob_manager: Arc<BlobManager>) -> Self {
        Self { pool, blob_manager }
    }
    
    /// Store large content as blob and return artifact reference
    pub async fn store_large_content(
        &self,
        content: &[u8],
        filename: &str,
        content_type: &str,
        source: &str,
    ) -> ServiceResult<String> {
        // Store in git-annex
        let blob_metadata = self.blob_manager
            .ingest_from_bytes(content, filename, content_type)
            .await
            .map_err(|e| ServiceError::OperationFailed(format!("Blob storage failed: {}", e)))?;
        
        // Create artifact record
        let artifact = artifacts::create_artifact(
            &self.pool,
            &blob_metadata.annex_key,
            &blob_metadata.blob_id.to_string(),
            source,
            blob_metadata.size_bytes as i64,
            None, // No associated event yet
        )
        .await?;
        
        Ok(artifact.annex_key)
    }
    
    /// Retrieve content by annex key
    pub async fn retrieve_content(&self, annex_key: &str) -> ServiceResult<Vec<u8>> {
        // First check if artifact exists
        let artifact = artifacts::get_artifact_by_annex_key(&self.pool, annex_key)
            .await?
            .ok_or_else(|| ServiceError::NotFound(format!("Artifact not found: {}", annex_key)))?;
        
        // Retrieve from blob storage
        self.blob_manager
            .retrieve_content(annex_key)
            .await
            .map_err(|e| ServiceError::OperationFailed(format!("Content retrieval failed: {}", e)))
    }
}