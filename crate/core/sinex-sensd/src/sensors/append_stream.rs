//! Append stream sensor for continuous data sources
//!
//! Handles sockets, logs, and other append-only data streams

use crate::{
    config::SensorConfig,
    job_manager::{SensorJob, SensorType},
    material_rotation::{MaterialRotationManager, RotationPolicy},
    temporal_ledger::{LedgerEntry, TemporalLedger},
};
use blake3;
use bytes::BytesMut;
use chrono::Utc;
use color_eyre::eyre::{eyre, Result};
use sinex_core::types::Ulid;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tracing::{debug, error, info};

/// Append stream sensor
pub struct AppendStreamSensor {
    temporal_ledger: Arc<TemporalLedger>,
    config: SensorConfig,
}

impl AppendStreamSensor {
    /// Create new append stream sensor
    pub fn new(temporal_ledger: Arc<TemporalLedger>, config: SensorConfig) -> Result<Self> {
        Ok(Self {
            temporal_ledger,
            config,
        })
    }

    /// Process a job
    pub async fn process_job(
        &self,
        job: &SensorJob,
        temporal_ledger: &Arc<TemporalLedger>,
    ) -> Result<Ulid> {
        info!("Processing append_stream job for {}", job.target_uri);

        // Create rotation manager for continuous stream
        let rotation_policy = RotationPolicy {
            max_bytes: 10 * 1024 * 1024, // 10MB for sockets
            max_age_seconds: 300,        // 5 minutes
            overlap_duration_ms: 50,     // 50ms overlap
        };

        let rotation_manager = MaterialRotationManager::new(
            temporal_ledger.clone(),
            rotation_policy,
            SensorType::AppendStream.to_string(),
            job.target_uri.clone(),
        );

        // Get or create initial material (ensures zero-gap from start)
        let mut current_material_id = rotation_manager.get_or_create_material().await?;

        // Connect to socket
        let mut stream = UnixStream::connect(&job.target_uri)
            .await
            .map_err(|e| eyre!("Failed to connect to socket {}: {}", job.target_uri, e))?;

        // Create buffer
        let mut buffer = BytesMut::with_capacity(self.config.socket_buffer_size);
        let mut offset = 0i64;
        let mut total_bytes = 0i64;

        // Read from stream
        loop {
            let capture_start = Utc::now();

            // Read data
            let bytes_read = stream.read_buf(&mut buffer).await?;

            if bytes_read == 0 {
                // End of stream
                break;
            }

            let capture_end = Utc::now();

            // Check if rotation is needed (maintains zero-gap invariant)
            if let Some(new_material_id) = rotation_manager.check_rotation(total_bytes).await? {
                info!(
                    "Rotating material for {}: {} -> {}",
                    job.target_uri, current_material_id, new_material_id
                );
                current_material_id = new_material_id;
                offset = 0; // Reset offset for new material
            }

            // Get current active material (handles rotation state)
            let active_material = rotation_manager.get_active_material().await?;

            // Calculate hash of the data slice
            let slice_hash = blake3::hash(&buffer).to_hex().to_string();

            // Record ledger entry
            let entry = LedgerEntry {
                source_material_id: active_material,
                offset_start: offset,
                offset_end: offset + bytes_read as i64,
                ts_capture: capture_end,
                offset_kind: "byte".to_string(),
                precision: "exact".to_string(),
                clock: "wall".to_string(),
                source_type: SensorType::AppendStream.to_string(),
                note: Some(
                    serde_json::json!({
                        "bytes_read": bytes_read,
                        "source": job.target_uri,
                        "slice_hash": slice_hash,
                        "capture_start": capture_start,
                        "capture_end": capture_end,
                    })
                    .to_string(),
                ),
            };

            temporal_ledger.record_entry(entry).await?;

            // Update rotation manager's byte counter
            rotation_manager
                .update_bytes_written(bytes_read as i64)
                .await?;

            // Update offsets
            offset += bytes_read as i64;
            total_bytes += bytes_read as i64;

            // Clear buffer for next read
            buffer.clear();

            debug!(
                "Read {} bytes from {}, total: {}, material: {}",
                bytes_read, job.target_uri, total_bytes, active_material
            );

            // Verify zero-gap invariant is maintained
            assert!(
                rotation_manager.verify_zero_gap_invariant().await?,
                "Zero-gap invariant violated!"
            );
        }

        // Force final rotation to close the stream properly
        let final_material = rotation_manager.force_rotation("stream_ended").await?;

        info!(
            "Completed append_stream job for {}, {} bytes captured across materials",
            job.target_uri, total_bytes
        );

        Ok(final_material)
    }
}
