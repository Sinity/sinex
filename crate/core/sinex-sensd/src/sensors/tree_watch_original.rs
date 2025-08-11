//! Tree watch sensor for file system monitoring
//!
//! Monitors file system changes and captures file content

use crate::{
    config::SensorConfig,
    job_manager::SensorJob,
    temporal_ledger::{LedgerEntry, TemporalLedger},
};
use chrono::Utc;
use color_eyre::eyre::{eyre, Result};
use notify::{Event, EventKind, RecursiveMode, Watcher};
use sinex_core::types::Ulid;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

/// Tree watch sensor
pub struct TreeWatchSensor {
    temporal_ledger: Arc<TemporalLedger>,
    config: SensorConfig,
}

impl TreeWatchSensor {
    /// Create new tree watch sensor
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
        info!("Processing tree_watch job for {}", job.target_path);

        // Create material record
        let material_id = temporal_ledger
            .create_material("tree_watch", &job.target_path, Some("directory"))
            .await?;

        // Set up file watcher
        let (tx, mut rx) = mpsc::channel(100);

        let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
            if let Ok(event) = res {
                let _ = tx.blocking_send(event);
            }
        })?;

        // Watch path
        watcher.watch(Path::new(&job.target_path), RecursiveMode::Recursive)?;

        info!("Watching path: {}", job.target_path);

        let mut total_files = 0;
        let mut total_bytes = 0i64;

        // Process events (for demo, just process first batch)
        // In real implementation, this would run continuously
        let timeout = tokio::time::Duration::from_secs(5);
        let start = tokio::time::Instant::now();

        while start.elapsed() < timeout {
            tokio::select! {
                Some(event) = rx.recv() => {
                    if let Err(e) = self.process_fs_event(
                        &event,
                        material_id,
                        temporal_ledger,
                        &mut total_files,
                        &mut total_bytes,
                    ).await {
                        error!("Error processing event: {}", e);
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                    // Check timeout
                    if start.elapsed() >= timeout {
                        break;
                    }
                }
            }
        }

        // Finalize material
        temporal_ledger
            .finalize_material(material_id, "completed", total_bytes)
            .await?;

        info!(
            "Completed tree_watch job for {}, {} files, {} bytes captured",
            job.target_path, total_files, total_bytes
        );

        Ok(material_id)
    }

    /// Process a file system event
    async fn process_fs_event(
        &self,
        event: &Event,
        material_id: Ulid,
        temporal_ledger: &Arc<TemporalLedger>,
        total_files: &mut usize,
        total_bytes: &mut i64,
    ) -> Result<()> {
        match event.kind {
            EventKind::Create(_) | EventKind::Modify(_) => {
                for path in &event.paths {
                    if path.is_file() {
                        // Capture file content
                        let capture_start = Utc::now();

                        let metadata = fs::metadata(&path).await?;
                        let file_size = metadata.len() as i64;

                        let capture_end = Utc::now();

                        // Record ledger entry
                        let entry = LedgerEntry {
                            material_id,
                            offset_start: *total_bytes,
                            offset_end: *total_bytes + file_size,
                            ts_capture_start: capture_start,
                            ts_capture_end: capture_end,
                            slice_hash: None,
                            capture_metadata: serde_json::json!({
                                "path": path.to_string_lossy(),
                                "size": file_size,
                                "event_kind": format!("{:?}", event.kind),
                            }),
                        };

                        temporal_ledger.record_entry(entry).await?;

                        *total_files += 1;
                        *total_bytes += file_size;

                        debug!("Captured file: {} ({} bytes)", path.display(), file_size);
                    }
                }
            }
            _ => {
                // Ignore other events for now
            }
        }

        Ok(())
    }
}
