#![doc = include_str!("../docs/content.md")]

//! Content service entry points for binary payload workflows.

use sinex_db::DbPool;
use sinex_db::repositories::DbPoolExt;
use sinex_db::repositories::state::Operation;
use sinex_node_sdk::content_store::{BlobMetadata, ContentStoreManager};
use sinex_primitives::domain::OperationStatus;
use sinex_primitives::error::{Result, SinexError};
use std::sync::Arc;
use std::time::Instant;
use tracing::warn;

pub struct ContentService {
    content_store: Arc<ContentStoreManager>,
    pool: DbPool,
}

impl ContentService {
    #[must_use]
    pub fn new(pool: DbPool, content_store: Arc<ContentStoreManager>) -> Self {
        Self {
            content_store,
            pool,
        }
    }

    /// Get the database pool
    #[must_use]
    pub fn pool(&self) -> &DbPool {
        &self.pool
    }

    async fn record_operation(
        &self,
        operation_type: &str,
        operator: &str,
        scope: serde_json::Value,
        result_status: OperationStatus,
        result_message: Option<String>,
        preview_summary: Option<serde_json::Value>,
        duration_ms: Option<i32>,
    ) -> Result<()> {
        self.pool
            .state()
            .log_operation(Operation {
                id: None,
                operation_type: operation_type.to_string(),
                operator: operator.to_string(),
                scope: Some(scope),
                result_status,
                result_message,
                preview_summary,
                duration_ms,
            })
            .await
            .map(|_| ())
    }

    /// Store content as blob and return content-store key.
    ///
    /// All content is stored via the content store regardless of size, providing
    /// consistent storage, deduplication, and source material tracking.
    pub async fn store_content(
        &self,
        content: &[u8],
        filename: &str,
        content_type: &str,
        source: &str,
        operator: &str,
    ) -> Result<String> {
        let started = Instant::now();
        let scope = serde_json::json!({
            "filename": filename,
            "content_type": content_type,
            "size_bytes": content.len(),
            "source": source
        });

        let blob_metadata = match self
            .content_store
            .ingest_from_bytes(content, filename, content_type)
            .await
        {
            Ok(metadata) => metadata,
            Err(e) => {
                let debug_error = format!("{e:?}");
                let duration_ms = elapsed_ms(started.elapsed());
                if let Err(err) = self
                    .record_operation(
                        "content.store",
                        operator,
                        scope.clone(),
                        OperationStatus::Failed,
                        Some(debug_error.clone()),
                        None,
                        duration_ms,
                    )
                    .await
                {
                    warn!(
                        error = %err,
                        operator,
                        source,
                        filename,
                        "Failed to record content.store failure"
                    );
                }

                warn!(filename = %filename, error = %debug_error, "Blob ingestion error");
                return Err(
                    SinexError::service(format!("Blob storage failed: {debug_error}"))
                        .with_operation("content_store.ingest_from_bytes")
                        .with_context("filename", filename)
                        .with_context("content_type", content_type),
                );
            }
        };

        let duration_ms = elapsed_ms(started.elapsed());
        let preview = serde_json::json!({
            "content_key": blob_metadata.content_key(),
            "size_bytes": blob_metadata.size_bytes,
            "content_hash": blob_metadata.content_hash,
            "checksum_blake3": blob_metadata.checksum_blake3,
        });
        if let Err(err) = self
            .record_operation(
                "content.store",
                operator,
                scope,
                OperationStatus::Success,
                None,
                Some(preview),
                duration_ms,
            )
            .await
        {
            warn!(
                error = %err,
                operator,
                source,
                filename,
                "Failed to record content.store success"
            );
        }

        Ok(blob_metadata.content_key())
    }

    /// Retrieve content by content-store key.
    pub async fn retrieve_content(&self, content_key: &str) -> Result<Vec<u8>> {
        self.content_store
            .retrieve_content(content_key)
            .await
            .map_err(|e| {
                SinexError::service(format!("Content retrieval failed: {e}"))
                    .with_operation("content_store.retrieve_content")
                    .with_context("content_key", content_key)
            })
    }

    /// Get content metadata by content-store key.
    pub async fn get_content_metadata(&self, content_key: &str) -> Result<BlobMetadata> {
        let blob_metadata = self
            .content_store
            .get_blob_metadata(content_key)
            .await
            .map_err(|e| {
                SinexError::service(format!("Failed to get blob metadata: {e}"))
                    .with_operation("content_store.get_blob_metadata")
                    .with_context("content_key", content_key)
            })?;

        Ok(blob_metadata)
    }

    /// Verify content integrity by content-store key.
    pub async fn verify_content(&self, content_key: &str) -> Result<bool> {
        self.content_store.verify_blob(content_key).await.map_err(|e| {
            SinexError::service(format!("Content verification failed: {e}"))
                .with_operation("content_store.verify_blob")
                .with_context("content_key", content_key)
        })
    }
}

fn elapsed_ms(duration: std::time::Duration) -> Option<i32> {
    let millis = duration.as_millis().min(i32::MAX as u128);
    i32::try_from(millis).ok()
}
