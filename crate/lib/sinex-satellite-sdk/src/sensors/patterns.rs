//! Sensor patterns for different data acquisition strategies
//!
//! Implements various sensor patterns:
//! - batched_pull: Accumulate data before forwarding
//! - replace_snapshot: Full state snapshots
//! - multi_file: Handle directories with multiple files

use crate::{acquisition_manager::AcquisitionManager, SatelliteResult};
use sinex_core::types::Ulid;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::sync::Mutex;
use tokio::time::interval;
use tracing::{debug, info};

/// Batched pull sensor pattern
/// Accumulates data slices before batch processing
pub struct BatchedPullSensor {
    acquisition_manager: Arc<AcquisitionManager>,
    batch_size: usize,
    batch_timeout: Duration,
    buffer: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl BatchedPullSensor {
    pub fn new(
        acquisition_manager: Arc<AcquisitionManager>,
        batch_size: usize,
        batch_timeout: Duration,
    ) -> Self {
        Self {
            acquisition_manager,
            batch_size,
            batch_timeout,
            buffer: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Process incoming data with batching
    pub async fn process_data(
        &self,
        data: Vec<u8>,
        source_id: &str,
    ) -> SatelliteResult<Option<Ulid>> {
        let mut buffer = self.buffer.lock().await;
        buffer.push(data);

        if buffer.len() >= self.batch_size {
            let material_id = self.flush_batch(&mut buffer, source_id).await?;
            Ok(Some(material_id))
        } else {
            Ok(None)
        }
    }

    /// Flush accumulated batch
    async fn flush_batch(
        &self,
        buffer: &mut Vec<Vec<u8>>,
        source_id: &str,
    ) -> SatelliteResult<Ulid> {
        if buffer.is_empty() {
            return Err(crate::SatelliteError::Processing(
                "Cannot flush empty batch".to_string(),
            ));
        }

        info!("Flushing batch of {} items", buffer.len());

        let mut handle = self.acquisition_manager.begin_material(source_id).await?;
        let material_id = handle.material_id;

        for data in buffer.drain(..) {
            self.acquisition_manager
                .append_slice(&mut handle, &data)
                .await?;
        }

        self.acquisition_manager
            .finalize(handle, "batch complete")
            .await?;

        Ok(material_id)
    }

    /// Start background timer for timeout-based flushing
    pub fn start_timeout_flusher(self: Arc<Self>, source_id: String) {
        tokio::spawn(async move {
            let mut ticker = interval(self.batch_timeout);

            loop {
                ticker.tick().await;

                let mut buf = self.buffer.lock().await;
                if !buf.is_empty() {
                    debug!("Timeout flush: {} items", buf.len());
                    if let Err(e) = self.flush_batch(&mut buf, &source_id).await {
                        tracing::warn!("Failed to flush on timeout: {}", e);
                    }
                }
            }
        });
    }
}

/// Replace snapshot sensor pattern
/// For sources that provide full state snapshots rather than incremental updates
pub struct ReplaceSnapshotSensor {
    acquisition_manager: Arc<AcquisitionManager>,
    current_snapshot: Arc<Mutex<Option<Vec<u8>>>>,
}

impl ReplaceSnapshotSensor {
    pub fn new(acquisition_manager: Arc<AcquisitionManager>) -> Self {
        Self {
            acquisition_manager,
            current_snapshot: Arc::new(Mutex::new(None)),
        }
    }

    /// Process a new snapshot, replacing the old one
    pub async fn process_snapshot(
        &self,
        snapshot: Vec<u8>,
        source_path: &str,
    ) -> SatelliteResult<Ulid> {
        let mut current = self.current_snapshot.lock().await;

        info!(
            "Processing replace_snapshot for {} ({}B)",
            source_path,
            snapshot.len()
        );

        let mut handle = self.acquisition_manager.begin_material(source_path).await?;
        let material_id = handle.material_id;

        self.acquisition_manager
            .append_slice(&mut handle, &snapshot)
            .await?;

        self.acquisition_manager
            .finalize(handle, "snapshot_complete")
            .await?;

        *current = Some(snapshot);

        Ok(material_id)
    }

    /// Get current snapshot if any
    pub async fn get_current_snapshot(&self) -> Option<Vec<u8>> {
        self.current_snapshot.lock().await.clone()
    }
}

/// Multi-file sensor pattern
/// Handles directories containing multiple files that need coordinated processing
pub struct MultiFileSensor {
    acquisition_manager: Arc<AcquisitionManager>,
    file_queue: Arc<Mutex<VecDeque<PathBuf>>>,
    source_identifier: String,
}

impl MultiFileSensor {
    pub fn new(acquisition_manager: Arc<AcquisitionManager>, source_identifier: String) -> Self {
        Self {
            acquisition_manager,
            file_queue: Arc::new(Mutex::new(VecDeque::new())),
            source_identifier,
        }
    }

    /// Scan directory and queue files for processing
    pub async fn scan_directory(&self, dir_path: &Path) -> SatelliteResult<Vec<PathBuf>> {
        let mut entries = fs::read_dir(dir_path).await.map_err(|e| {
            crate::SatelliteError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to read directory: {}", e),
            ))
        })?;
        let mut files = Vec::new();

        while let Some(entry) = entries.next_entry().await.map_err(|e| {
            crate::SatelliteError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to read entry: {}", e),
            ))
        })? {
            let path = entry.path();
            if path.is_file() {
                files.push(path.clone());
                self.file_queue.lock().await.push_back(path);
            }
        }

        info!("Queued {} files from {}", files.len(), dir_path.display());
        Ok(files)
    }

    /// Process next file in queue
    pub async fn process_next_file(&self) -> SatelliteResult<Option<Ulid>> {
        let path = {
            let mut queue = self.file_queue.lock().await;
            queue.pop_front()
        };

        match path {
            Some(file_path) => {
                info!("Processing file: {}", file_path.display());

                let mut handle = self
                    .acquisition_manager
                    .begin_material(&self.source_identifier)
                    .await?;
                let material_id = handle.material_id;

                let content = fs::read(&file_path).await.map_err(|e| {
                    crate::SatelliteError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to read file: {}", e),
                    ))
                })?;

                self.acquisition_manager
                    .append_slice(&mut handle, &content)
                    .await?;

                self.acquisition_manager
                    .finalize(handle, &format!("file: {}", file_path.display()))
                    .await?;

                Ok(Some(material_id))
            }
            None => {
                debug!("No more files in queue");
                Ok(None)
            }
        }
    }

    /// Process all queued files
    pub async fn process_all_files(&self) -> SatelliteResult<Vec<Ulid>> {
        let mut material_ids = Vec::new();

        while let Some(material_id) = self.process_next_file().await? {
            material_ids.push(material_id);
        }

        info!("Processed {} files", material_ids.len());
        Ok(material_ids)
    }
}
