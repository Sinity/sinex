//! Append stream sensor for continuous data sources
//!
//! Handles sockets, logs, and other append-only data streams

use crate::{
    acquisition_manager::AcquisitionManager,
    job_manager::{SensorExecutor, SensorJob, SensorType},
    SatelliteError, SatelliteResult,
};
use async_trait::async_trait;
use sinex_core::types::Ulid;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;
use tracing::{debug, info};

/// Append stream sensor configuration
#[derive(Debug, Clone)]
pub struct AppendStreamConfig {
    /// Buffer size for socket reads
    pub socket_buffer_size: usize,
}

impl Default for AppendStreamConfig {
    fn default() -> Self {
        Self {
            socket_buffer_size: 64 * 1024, // 64KB
        }
    }
}

/// Append stream sensor for continuous data acquisition
pub struct AppendStreamSensor {
    acquisition_manager: Arc<AcquisitionManager>,
    config: AppendStreamConfig,
}

impl AppendStreamSensor {
    /// Create new append stream sensor
    pub fn new(acquisition_manager: Arc<AcquisitionManager>, config: AppendStreamConfig) -> Self {
        Self {
            acquisition_manager,
            config,
        }
    }

    /// Process an append stream job
    async fn process_stream(&self, job: &SensorJob) -> SatelliteResult<Ulid> {
        info!("Processing append_stream job for {}", job.target_uri);

        let mut stream = UnixStream::connect(&job.target_uri).await.map_err(|e| {
            SatelliteError::Processing(format!(
                "Failed to connect to socket {}: {}",
                job.target_uri, e
            ))
        })?;

        let mut handle = self
            .acquisition_manager
            .begin_material(&job.target_uri)
            .await?;
        let material_id = handle.material_id;

        let mut buffer = vec![0u8; self.config.socket_buffer_size];
        let mut total_bytes = 0i64;

        loop {
            let bytes_read = stream.read(&mut buffer).await.map_err(|e| {
                SatelliteError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to read from socket: {}", e),
                ))
            })?;

            if bytes_read == 0 {
                break;
            }

            self.acquisition_manager
                .append_slice(&mut handle, &buffer[..bytes_read])
                .await?;

            total_bytes += bytes_read as i64;

            debug!(
                "Read {} bytes from {}, total: {}",
                bytes_read, job.target_uri, total_bytes
            );

            // Check if rotation needed based on policy
            if self.acquisition_manager.should_rotate(&handle).await {
                info!(
                    "Rotating material for {}: {} bytes total",
                    job.target_uri, total_bytes
                );

                self.acquisition_manager
                    .finalize(handle, "size rotation")
                    .await?;

                handle = self
                    .acquisition_manager
                    .begin_material(&job.target_uri)
                    .await?;
            }
        }

        self.acquisition_manager
            .finalize(handle, "stream ended")
            .await?;

        info!(
            "Completed append_stream job for {}, {} bytes captured",
            job.target_uri, total_bytes
        );

        Ok(material_id)
    }
}

#[async_trait]
impl SensorExecutor for AppendStreamSensor {
    async fn process_job(&self, job: &SensorJob) -> SatelliteResult<Ulid> {
        self.process_stream(job).await
    }

    fn sensor_type(&self) -> SensorType {
        SensorType::AppendStream
    }
}
