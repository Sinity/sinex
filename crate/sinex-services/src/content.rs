//! Content service for managing event content and artifacts

use crate::error::{ServiceError, ServiceResult};
use sinex_annex::BlobManager;
use sinex_db::{artifacts, DbPool};
use sinex_db::models::CreateArtifactInput;
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
        let _artifact = artifacts::create_artifact(
            &self.pool,
            CreateArtifactInput {
                artifact_type: "blob".to_string(),
                title: filename.to_string(),
                source_url: None,
                original_path: Some(filename.to_string()),
                mime_type: Some(content_type.to_string()),
                size_bytes: Some(blob_metadata.size_bytes),
                checksum: Some(blob_metadata.checksum_sha256.clone()),
                metadata: Some(serde_json::json!({
                    "annex_key": blob_metadata.annex_key,
                    "source": source,
                })),
                created_from_event_id: None,
                blob_id: Some(blob_metadata.blob_id),
            },
        )
        .await?;
        
        Ok(blob_metadata.annex_key)
    }
    
    /// Retrieve content by annex key
    pub async fn retrieve_content(&self, annex_key: &str) -> ServiceResult<Vec<u8>> {
        // Retrieve from blob storage directly
        self.blob_manager
            .retrieve_content(annex_key)
            .await
            .map_err(|e| ServiceError::OperationFailed(format!("Content retrieval failed: {}", e)))
    }
}