//! Append stream sensor for continuous data sources
//!
//! Handles sockets, logs, and other append-only data streams

use crate::{
    config::SensorConfig,
    job_manager::SensorJob,
    temporal_ledger::{LedgerEntry, TemporalLedger},
};
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
        info!("Processing append_stream job for {}", job.target_path);

        // Create material record
        let material_id = temporal_ledger
            .create_material(
                "append_stream",
                &job.target_path,
                Some("application/octet-stream"),
            )
            .await?;

        // Connect to socket
        let mut stream = UnixStream::connect(&job.target_path)
            .await
            .map_err(|e| eyre!("Failed to connect to socket {}: {}", job.target_path, e))?;

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

            // Record ledger entry
            let entry = LedgerEntry {
                material_id,
                offset_start: offset,
                offset_end: offset + bytes_read as i64,
                ts_capture_start: capture_start,
                ts_capture_end: capture_end,
                slice_hash: None, // TODO: Calculate hash
                capture_metadata: serde_json::json!({
                    "bytes_read": bytes_read,
                    "source": job.target_path,
                }),
            };

            temporal_ledger.record_entry(entry).await?;

            // Update offsets
            offset += bytes_read as i64;
            total_bytes += bytes_read as i64;

            // Clear buffer for next read
            buffer.clear();

            debug!(
                "Read {} bytes from {}, total: {}",
                bytes_read, job.target_path, total_bytes
            );
        }

        // Finalize material
        temporal_ledger
            .finalize_material(material_id, "completed", total_bytes)
            .await?;

        info!(
            "Completed append_stream job for {}, {} bytes captured",
            job.target_path, total_bytes
        );

        Ok(material_id)
    }
}
