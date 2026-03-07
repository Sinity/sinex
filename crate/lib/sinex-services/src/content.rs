#![doc = include_str!("../docs/content.md")]

//! Content service entry points for binary payload workflows.

use sinex_db::DbPool;
use sinex_node_sdk::annex::BlobManager;
use sinex_primitives::error::{Result, SinexError};
use std::sync::Arc;
use std::time::Instant;
use tracing::warn;

pub struct ContentService {
    blob_manager: Arc<BlobManager>,
    pool: DbPool,
}

impl ContentService {
    pub fn new(pool: DbPool, blob_manager: Arc<BlobManager>) -> Self {
        Self { blob_manager, pool }
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
        result_status: &str,
        result_message: Option<&str>,
        preview_summary: Option<serde_json::Value>,
        duration_ms: Option<i32>,
    ) -> std::result::Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            INSERT INTO core.operations_log (
                operation_type,
                operator,
                scope,
                result_status,
                result_message,
                preview_summary,
                duration_ms
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
            operation_type,
            operator,
            scope,
            result_status,
            result_message,
            preview_summary,
            duration_ms
        )
        .execute(&self.pool)
        .await?;

        Ok(())
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
        source: &str,
    ) -> Result<String> {
        let started = Instant::now();
        let scope = serde_json::json!({
            "filename": filename,
            "content_type": content_type,
            "size_bytes": content.len(),
            "source": source
        });

        let blob_metadata = match self
            .blob_manager
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
                        source,
                        scope.clone(),
                        "failure",
                        Some(&debug_error),
                        None,
                        duration_ms,
                    )
                    .await
                {
                    warn!(error = %err, "Failed to record content.store failure");
                }

                warn!(filename = %filename, error = %debug_error, "Blob ingestion error");
                return Err(
                    SinexError::service(format!("Blob storage failed: {debug_error}"))
                        .with_operation("blob_manager.ingest_from_bytes")
                        .with_context("filename", filename)
                        .with_context("content_type", content_type),
                );
            }
        };

        let duration_ms = elapsed_ms(started.elapsed());
        let preview = serde_json::json!({
            "annex_key": blob_metadata.annex_key(),
            "size_bytes": blob_metadata.size_bytes,
            "content_hash": blob_metadata.content_hash,
            "checksum_blake3": blob_metadata.checksum_blake3,
        });
        if let Err(err) = self
            .record_operation(
                "content.store",
                source,
                scope,
                "success",
                None,
                Some(preview),
                duration_ms,
            )
            .await
        {
            warn!(error = %err, "Failed to record content.store success");
        }

        Ok(blob_metadata.annex_key())
    }

    /// Retrieve content by annex key
    pub async fn retrieve_content(&self, annex_key: &str) -> Result<Vec<u8>> {
        self.blob_manager
            .retrieve_content(annex_key)
            .await
            .map_err(|e| {
                SinexError::service(format!("Content retrieval failed: {e}"))
                    .with_operation("blob_manager.retrieve_content")
                    .with_context("annex_key", annex_key)
            })
    }

    /// Get content metadata by annex key
    pub async fn get_content_metadata(
        &self,
        annex_key: &str,
    ) -> Result<sinex_node_sdk::annex::BlobMetadata> {
        let blob_metadata = self
            .blob_manager
            .get_blob_metadata(annex_key)
            .await
            .map_err(|e| {
                SinexError::service(format!("Failed to get blob metadata: {e}"))
                    .with_operation("blob_manager.get_blob_metadata")
                    .with_context("annex_key", annex_key)
            })?;

        Ok(blob_metadata)
    }

    /// Verify content integrity by annex key
    pub async fn verify_content(&self, annex_key: &str) -> Result<bool> {
        self.blob_manager.verify_blob(annex_key).await.map_err(|e| {
            SinexError::service(format!("Content verification failed: {e}"))
                .with_operation("blob_manager.verify_blob")
                .with_context("annex_key", annex_key)
        })
    }
}

fn elapsed_ms(duration: std::time::Duration) -> Option<i32> {
    let millis = duration.as_millis().min(i32::MAX as u128);
    i32::try_from(millis).ok()
}
