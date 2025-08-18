//! MaterialSliceStream interface for ingestors
//!
//! Provides streaming access to source materials with temporal integrity

use chrono::{DateTime, Utc};
use color_eyre::eyre::{eyre, Result};
use sinex_core::types::Ulid;
use sqlx::PgPool;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_stream::{Stream, StreamExt};
use tracing::{debug, error};

/// Material slice for streaming
#[derive(Debug, Clone)]
pub struct MaterialSlice {
    pub material_id: Ulid,
    pub offset_start: i64,
    pub offset_end: i64,
    pub ts_capture_start: DateTime<Utc>,
    pub ts_capture_end: DateTime<Utc>,
    pub data: Vec<u8>,
    pub metadata: serde_json::Value,
}

/// Control frames for MaterialSliceStream as per TARGET_final.md
#[derive(Debug, Clone)]
pub enum StreamFrame {
    /// A data slice from the material
    Slice(MaterialSlice),
    /// Indicates rotation to a new material
    RotationBoundary {
        old_material_id: Ulid,
        new_material_id: Ulid,
        rotation_reason: String,
    },
    /// Indicates end of the current material
    EndOfMaterial {
        material_id: Ulid,
        final_offset: i64,
    },
    /// Indicates a gap in the material
    Gap {
        material_id: Ulid,
        gap_start_offset: i64,
        gap_end_offset: i64,
        reason: String,
    },
}

/// MaterialSliceStream for ingestors
pub struct MaterialSliceStream {
    db_pool: PgPool,
    material_id: Ulid,
    current_offset: i64,
    batch_size: usize,
    finished: bool,
    /// Buffer for current batch of slices
    buffer: VecDeque<MaterialSlice>,
}

impl MaterialSliceStream {
    /// Create new material slice stream
    pub fn new(db_pool: PgPool, material_id: Ulid, batch_size: usize) -> Self {
        Self {
            db_pool,
            material_id,
            current_offset: 0,
            batch_size,
            finished: false,
            buffer: VecDeque::new(),
        }
    }

    /// Fetch next batch of slices
    async fn fetch_next_batch(&mut self) -> Result<Vec<MaterialSlice>> {
        if self.finished {
            return Ok(vec![]);
        }

        // Query temporal ledger for slices
        let slices = sqlx::query!(
            r#"
            SELECT 
                source_material_id as "material_id: Ulid",
                offset_start,
                offset_end,
                ts_capture,
                note
            FROM raw.temporal_ledger
            WHERE source_material_id = $1::ulid
            AND offset_start >= $2
            ORDER BY offset_start
            LIMIT $3
            "#,
            self.material_id as Ulid,
            self.current_offset,
            self.batch_size as i64,
        )
        .fetch_all(&self.db_pool)
        .await?;

        if slices.is_empty() {
            self.finished = true;
            return Ok(vec![]);
        }

        // Convert to MaterialSlice
        let mut result = Vec::new();

        for record in slices {
            // Load actual data from storage backend
            let data = self
                .load_material_data(record.material_id, record.offset_start, record.offset_end)
                .await
                .unwrap_or_else(|e| {
                    error!("Failed to load material data: {}", e);
                    vec![] // Return empty data on error
                });

            let slice = MaterialSlice {
                material_id: record.material_id,
                offset_start: record.offset_start,
                offset_end: record.offset_end,
                ts_capture_start: record.ts_capture,
                ts_capture_end: record.ts_capture, // Same timestamp for single capture time
                data,
                metadata: serde_json::from_str(&record.note.unwrap_or("{}".to_string()))
                    .unwrap_or_default(),
            };

            self.current_offset = record.offset_end;
            result.push(slice);
        }

        debug!(
            "Fetched {} slices for material {}, offset now at {}",
            result.len(),
            self.material_id,
            self.current_offset
        );

        Ok(result)
    }

    /// Load material data from storage backend
    async fn load_material_data(
        &self,
        material_id: Ulid,
        offset_start: i64,
        offset_end: i64,
    ) -> Result<Vec<u8>> {
        // Query the source material registry to get storage information
        let material = sqlx::query!(
            r#"
            SELECT 
                optional_blob_id as "optional_blob_id: Ulid"
            FROM raw.source_material_registry
            WHERE id = $1::ulid
            "#,
            material_id as Ulid
        )
        .fetch_optional(&self.db_pool)
        .await?
        .ok_or_else(|| eyre!("Material not found: {}", material_id))?;

        if let Some(blob_id) = material.optional_blob_id {
            // Load from external blob storage
            match self.load_blob_data(blob_id).await {
                Ok(blob_data) => {
                    // Extract slice from blob based on offsets
                    let start = offset_start as usize;
                    let end = offset_end as usize;
                    if end <= blob_data.len() && start <= end {
                        Ok(blob_data[start..end].to_vec())
                    } else {
                        error!(
                            "Blob data size {} is smaller than slice end offset {}",
                            blob_data.len(),
                            end
                        );
                        Ok(vec![])
                    }
                }
                Err(e) => {
                    error!("Failed to load blob {}: {}", blob_id, e);
                    Ok(vec![])
                }
            }
        } else {
            // No blob associated with this material
            error!("Material {} has no associated blob", material_id);
            Ok(vec![])
        }
    }

    /// Load blob data from storage backend
    async fn load_blob_data(&self, blob_id: Ulid) -> Result<Vec<u8>> {
        // First query the blob metadata
        let blob = sqlx::query!(
            r#"
            SELECT 
                annex_key,
                size_bytes,
                checksum_sha256,
                storage_backend
            FROM core.blobs
            WHERE id = $1::uuid
            "#,
            sinex_core::ulid_to_uuid(blob_id),
        )
        .fetch_optional(&self.db_pool)
        .await?
        .ok_or_else(|| eyre!("Blob {} not found", blob_id))?;

        match blob.storage_backend.as_str() {
            "git-annex" => {
                // Load from git-annex storage
                let annex_path = std::path::Path::new(".git/annex/objects")
                    .join(&blob.annex_key[0..2])
                    .join(&blob.annex_key[2..4])
                    .join(&blob.annex_key);

                if annex_path.exists() {
                    tokio::fs::read(&annex_path)
                        .await
                        .map_err(|e| eyre!("Failed to read annex file: {}", e))
                } else {
                    Err(eyre!("Annex file not found at {:?}", annex_path))
                }
            }
            "filesystem" => {
                // Load from filesystem path stored in annex_key
                let path = std::path::Path::new(&blob.annex_key);
                tokio::fs::read(path)
                    .await
                    .map_err(|e| eyre!("Failed to read file: {}", e))
            }
            "s3" => {
                // S3 support requires additional dependencies (AWS SDK)
                // To implement S3 support:
                // 1. Add aws-sdk-s3 to Cargo.toml dependencies
                // 2. Implement S3Client initialization with credentials
                // 3. Use blob.annex_key as S3 object key to retrieve data
                Err(eyre!(
                    "S3 storage backend not implemented. Blob {} uses S3 storage \
                     but S3 support requires AWS SDK dependencies and configuration. \
                     Consider using git-annex or filesystem storage backends instead.",
                    blob_id
                ))
            }
            backend => Err(eyre!("Unknown storage backend: {}", backend)),
        }
    }
}

impl MaterialSliceStream {
    /// Get next slice if available (non-blocking)
    pub async fn next_slice(&mut self) -> Result<Option<MaterialSlice>> {
        // If we have slices in buffer, return the next one
        if let Some(slice) = self.buffer.pop_front() {
            return Ok(Some(slice));
        }

        // If we're finished, no more slices
        if self.finished {
            return Ok(None);
        }

        // Fetch next batch
        let slices = self.fetch_next_batch().await?;

        if slices.is_empty() {
            self.finished = true;
            return Ok(None);
        }

        // Add to buffer and return first slice
        self.buffer.extend(slices);
        Ok(self.buffer.pop_front())
    }

    /// Convert to async stream
    pub fn into_stream(self) -> impl Stream<Item = Result<MaterialSlice>> {
        async_stream::stream! {
            let mut stream = self;
            while let Some(slice) = stream.next_slice().await? {
                yield Ok(slice);
            }
        }
    }
}

// Note: gRPC service implementation is in grpc_server.rs
