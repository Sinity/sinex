//! Sensor patterns for different data acquisition strategies
//!
//! Implements the various sensor patterns described in TARGET_final.md:
//! - batched_pull: Accumulate events before forwarding
//! - replace_snapshot: Update snapshots with full replacement
//! - multi_file: Handle directories with multiple files

use crate::{
    material_rotation::{MaterialRotationManager, RotationPolicy},
    temporal_ledger::{LedgerEntry, TemporalLedger},
};
use chrono::Utc;
use color_eyre::eyre::{eyre, Result};
use sinex_core::types::Ulid;
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{fs, sync::Mutex, time::interval};
use tracing::{debug, info, warn};

/// Batched pull sensor pattern
/// Accumulates events in memory before batch processing
pub struct BatchedPullSensor {
    temporal_ledger: Arc<TemporalLedger>,
    batch_size: usize,
    batch_timeout: Duration,
    event_buffer: Arc<Mutex<Vec<LedgerEntry>>>,
}

impl BatchedPullSensor {
    pub fn new(
        temporal_ledger: Arc<TemporalLedger>,
        batch_size: usize,
        batch_timeout: Duration,
    ) -> Self {
        Self {
            temporal_ledger,
            batch_size,
            batch_timeout,
            event_buffer: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Process incoming data with batching
    pub async fn process_data(&self, data: Vec<u8>, material_id: Ulid, offset: i64) -> Result<()> {
        let mut buffer = self.event_buffer.lock().await;

        // Create ledger entry
        let entry = LedgerEntry {
            source_material_id: material_id,
            offset_start: offset,
            offset_end: offset + data.len() as i64,
            ts_capture: Utc::now(),
            offset_kind: "byte".to_string(),
            precision: "millisecond".to_string(),
            clock: "system".to_string(),
            source_type: "batched_pull".to_string(),
        };

        buffer.push(entry);

        // Check if we should flush
        if buffer.len() >= self.batch_size {
            self.flush_batch(&mut buffer).await?;
        }

        Ok(())
    }

    /// Flush accumulated batch
    async fn flush_batch(&self, buffer: &mut Vec<LedgerEntry>) -> Result<()> {
        if buffer.is_empty() {
            return Ok(());
        }

        info!("Flushing batch of {} entries", buffer.len());

        for entry in buffer.drain(..) {
            self.temporal_ledger.record_entry(entry).await?;
        }

        Ok(())
    }

    /// Start background timer for timeout-based flushing
    pub async fn start_timeout_flusher(&self) {
        let buffer = self.event_buffer.clone();
        let ledger = self.temporal_ledger.clone();
        let timeout = self.batch_timeout;

        tokio::spawn(async move {
            let mut ticker = interval(timeout);

            loop {
                ticker.tick().await;

                let mut buf = buffer.lock().await;
                if !buf.is_empty() {
                    debug!("Timeout flush: {} entries", buf.len());
                    for entry in buf.drain(..) {
                        if let Err(e) = ledger.record_entry(entry).await {
                            warn!("Failed to record entry on timeout flush: {}", e);
                        }
                    }
                }
            }
        });
    }
}

/// Replace snapshot sensor pattern
/// For sources that provide full state snapshots rather than incremental updates
pub struct ReplaceSnapshotSensor {
    temporal_ledger: Arc<TemporalLedger>,
    current_snapshot: Arc<Mutex<Option<Vec<u8>>>>,
}

impl ReplaceSnapshotSensor {
    pub fn new(temporal_ledger: Arc<TemporalLedger>) -> Self {
        Self {
            temporal_ledger,
            current_snapshot: Arc::new(Mutex::new(None)),
        }
    }

    /// Process a new snapshot, replacing the old one
    pub async fn process_snapshot(&self, snapshot: Vec<u8>, source_path: &str) -> Result<Ulid> {
        let mut current = self.current_snapshot.lock().await;

        // Create new material for this snapshot
        let material_id = self
            .temporal_ledger
            .create_material(
                source_path,
                "replace_snapshot",
                Some(source_path),
                Some("application/json"),
            )
            .await?;

        info!(
            "Processing replace_snapshot for {} ({}B)",
            source_path,
            snapshot.len()
        );

        // Record the snapshot as a single ledger entry
        let entry = LedgerEntry {
            source_material_id: material_id,
            offset_start: 0,
            offset_end: snapshot.len() as i64,
            ts_capture: Utc::now(),
            offset_kind: "byte".to_string(),
            precision: "millisecond".to_string(),
            clock: "system".to_string(),
            source_type: "replace_snapshot".to_string(),
        };

        self.temporal_ledger.record_entry(entry).await?;

        // Get length before moving
        let snapshot_len = snapshot.len() as i64;

        // Replace current snapshot
        *current = Some(snapshot);

        // Finalize the material immediately (snapshots are complete units)
        self.temporal_ledger
            .finalize_material(material_id, "snapshot_complete", Some(snapshot_len))
            .await?;

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
    temporal_ledger: Arc<TemporalLedger>,
    rotation_manager: Option<MaterialRotationManager>,
    file_queue: Arc<Mutex<VecDeque<PathBuf>>>,
}

impl MultiFileSensor {
    pub fn new(temporal_ledger: Arc<TemporalLedger>) -> Self {
        Self {
            temporal_ledger,
            rotation_manager: None,
            file_queue: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Initialize with rotation policy for continuous monitoring
    pub fn with_rotation(mut self, policy: RotationPolicy, source_path: String) -> Self {
        self.rotation_manager = Some(MaterialRotationManager::new(
            self.temporal_ledger.clone(),
            policy,
            "multi_file".to_string(),
            source_path,
        ));
        self
    }

    /// Scan directory and queue files for processing
    pub async fn scan_directory(&self, dir_path: &Path) -> Result<Vec<PathBuf>> {
        let mut entries = fs::read_dir(dir_path).await?;
        let mut files = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
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
    pub async fn process_next_file(&self) -> Result<Option<Ulid>> {
        let path = {
            let mut queue = self.file_queue.lock().await;
            queue.pop_front()
        };

        match path {
            Some(file_path) => {
                info!("Processing file: {}", file_path.display());

                // Get or create material (with rotation if configured)
                let material_id = if let Some(ref mgr) = self.rotation_manager {
                    mgr.get_or_create_material().await?
                } else {
                    // One-shot material per file
                    self.temporal_ledger
                        .create_material(
                            &file_path.to_string_lossy(),
                            "multi_file",
                            Some(&file_path.to_string_lossy()),
                            None,
                        )
                        .await?
                };

                // Read file metadata
                let metadata = fs::metadata(&file_path).await?;
                let file_size = metadata.len() as i64;

                // Record ledger entry for this file
                let entry = LedgerEntry {
                    source_material_id: material_id,
                    offset_start: 0,
                    offset_end: file_size,
                    ts_capture: Utc::now(),
                    offset_kind: "byte".to_string(),
                    precision: "millisecond".to_string(),
                    clock: "system".to_string(),
                    source_type: "multi_file".to_string(),
                };

                self.temporal_ledger.record_entry(entry).await?;

                // Check rotation if configured
                if let Some(ref mgr) = self.rotation_manager {
                    mgr.update_bytes_written(file_size).await?;
                    mgr.check_rotation(file_size).await?;
                }

                Ok(Some(material_id))
            }
            None => {
                debug!("No more files in queue");
                Ok(None)
            }
        }
    }

    /// Process all queued files
    pub async fn process_all_files(&self) -> Result<Vec<Ulid>> {
        let mut material_ids = Vec::new();

        while let Some(material_id) = self.process_next_file().await? {
            material_ids.push(material_id);
        }

        // Force rotation to finalize if using rotation manager
        if let Some(ref mgr) = self.rotation_manager {
            let final_id = mgr.force_rotation("batch_complete").await?;
            if !material_ids.contains(&final_id) {
                material_ids.push(final_id);
            }
        }

        info!("Processed {} files", material_ids.len());
        Ok(material_ids)
    }
}

/// Sensor pattern selector
pub enum SensorPattern {
    BatchedPull(BatchedPullSensor),
    ReplaceSnapshot(ReplaceSnapshotSensor),
    MultiFile(MultiFileSensor),
}

impl SensorPattern {
    /// Create a batched pull sensor
    pub fn batched_pull(
        temporal_ledger: Arc<TemporalLedger>,
        batch_size: usize,
        timeout_secs: u64,
    ) -> Self {
        Self::BatchedPull(BatchedPullSensor::new(
            temporal_ledger,
            batch_size,
            Duration::from_secs(timeout_secs),
        ))
    }

    /// Create a replace snapshot sensor
    pub fn replace_snapshot(temporal_ledger: Arc<TemporalLedger>) -> Self {
        Self::ReplaceSnapshot(ReplaceSnapshotSensor::new(temporal_ledger))
    }

    /// Create a multi-file sensor
    pub fn multi_file(temporal_ledger: Arc<TemporalLedger>) -> Self {
        Self::MultiFile(MultiFileSensor::new(temporal_ledger))
    }

    /// Create a multi-file sensor with rotation
    pub fn multi_file_with_rotation(
        temporal_ledger: Arc<TemporalLedger>,
        policy: RotationPolicy,
        source_path: String,
    ) -> Self {
        Self::MultiFile(MultiFileSensor::new(temporal_ledger).with_rotation(policy, source_path))
    }
}
