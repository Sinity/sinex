//! Tree watch sensor for file system monitoring (SECURED VERSION)
//!
//! Monitors file system changes and captures file content with comprehensive
//! path validation and security policies.

use crate::{
    config::SensorConfig,
    job_manager::{SensorJob, SensorType},
    temporal_ledger::{LedgerEntry, TemporalLedger},
};
use blake3;
use camino::Utf8Path;
use chrono::Utc;
use color_eyre::eyre::{eyre, Result};
use notify::{Event, EventKind, RecursiveMode, Watcher};
use sinex_core::types::{
    validation::{validate_path, validate_watch_path, FileWatchingSecurityPolicy},
    Ulid,
};
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Tree watch sensor with security enhancements
pub struct TreeWatchSensor {
    temporal_ledger: Arc<TemporalLedger>,
    config: SensorConfig,
    /// Security policy for file watching operations
    security_policy: FileWatchingSecurityPolicy,
}

impl TreeWatchSensor {
    /// Create new tree watch sensor with default restrictive security policy
    pub fn new(temporal_ledger: Arc<TemporalLedger>, config: SensorConfig) -> Result<Self> {
        // Use restrictive policy for production security
        let security_policy = FileWatchingSecurityPolicy::restrictive();

        Ok(Self {
            temporal_ledger,
            config,
            security_policy,
        })
    }

    /// Create new tree watch sensor with custom security policy
    pub fn with_policy(
        temporal_ledger: Arc<TemporalLedger>,
        config: SensorConfig,
        security_policy: FileWatchingSecurityPolicy,
    ) -> Result<Self> {
        Ok(Self {
            temporal_ledger,
            config,
            security_policy,
        })
    }

    /// Process a job with security validation
    pub async fn process_job(
        &self,
        job: &SensorJob,
        temporal_ledger: &Arc<TemporalLedger>,
    ) -> Result<Ulid> {
        info!("Processing tree_watch job for {}", job.target_uri);

        // SECURITY: Validate the target path before processing
        let validated_path =
            validate_watch_path(&job.target_uri, &self.security_policy).map_err(|e| {
                eyre!(
                    "Security validation failed for path '{}': {}",
                    job.target_uri,
                    e
                )
            })?;

        info!("Path validation passed for: {}", validated_path.as_str());

        // Create material record using validated path
        let material_id = temporal_ledger
            .create_material(
                &format!("tree_watch:{}", validated_path.as_str()),
                "tree_watch",
                Some(validated_path.as_str()),
                Some("directory"),
            )
            .await?;

        // Set up file watcher
        let (tx, mut rx) = mpsc::channel(100);

        let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
            if let Ok(event) = res {
                let _ = tx.blocking_send(event);
            }
        })?;

        // SECURITY: Watch the validated path only
        watcher.watch(validated_path.as_std_path(), RecursiveMode::Recursive)?;

        info!("Watching validated path: {}", validated_path.as_str());

        let mut total_files = 0;
        let mut total_bytes = 0i64;

        // Process events (for demo, just process first batch)
        // In real implementation, this would run continuously
        let timeout = tokio::time::Duration::from_secs(5);
        let start = tokio::time::Instant::now();

        // Store watch root for validation of discovered files
        let watch_root = validated_path.as_str().to_string();

        while start.elapsed() < timeout {
            tokio::select! {
                Some(event) = rx.recv() => {
                    if let Err(e) = self.process_fs_event(
                        &event,
                        material_id,
                        temporal_ledger,
                        &mut total_files,
                        &mut total_bytes,
                        &watch_root, // Pass watch root for security validation
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
            .finalize_material(material_id, "completed", Some(total_bytes))
            .await?;

        info!(
            "Completed tree_watch job for {}, {} files, {} bytes captured",
            validated_path.as_str(),
            total_files,
            total_bytes
        );

        Ok(material_id)
    }

    /// Process a file system event with security validation
    async fn process_fs_event(
        &self,
        event: &Event,
        material_id: Ulid,
        temporal_ledger: &Arc<TemporalLedger>,
        total_files: &mut usize,
        total_bytes: &mut i64,
        watch_root: &str, // Watch root for validation
    ) -> Result<()> {
        match event.kind {
            EventKind::Create(_) | EventKind::Modify(_) => {
                for path in &event.paths {
                    // SECURITY: Validate discovered file paths
                    let path_str = path.to_string_lossy();

                    // Validate that discovered path is within watch boundaries and secure
                    match sinex_core::types::validation::validate_discovered_file(
                        &path_str,
                        watch_root,
                        &self.security_policy,
                    ) {
                        Ok(_validated_file_path) => {
                            // Path validation passed, continue processing
                        }
                        Err(e) => {
                            warn!(
                                "Skipping invalid discovered file path '{}': {}",
                                path_str, e
                            );
                            continue;
                        }
                    }

                    if path.is_file() {
                        // Capture file content
                        let capture_start = Utc::now();

                        let metadata = fs::metadata(&path).await?;
                        let file_size = metadata.len() as i64;

                        // Read file content for hashing (only for small files to avoid memory issues)
                        let slice_hash = if file_size <= 10_485_760 {
                            // 10MB limit
                            match fs::read(&path).await {
                                Ok(content) => Some(blake3::hash(&content).to_hex().to_string()),
                                Err(e) => {
                                    warn!("Failed to read file for hashing: {}", e);
                                    None
                                }
                            }
                        } else {
                            // For large files, hash just the metadata
                            let meta_hash = blake3::hash(path.to_string_lossy().as_bytes());
                            Some(meta_hash.to_hex().to_string())
                        };

                        let capture_end = Utc::now();

                        // Record ledger entry
                        let entry = LedgerEntry {
                            source_material_id: material_id,
                            offset_start: *total_bytes,
                            offset_end: *total_bytes + file_size,
                            offset_kind: "byte".to_string(),
                            ts_capture: capture_start, // Use start time as the capture timestamp
                            precision: "exact".to_string(),
                            clock: "wall".to_string(),
                            source_type: "realtime_capture".to_string(),
                        };

                        temporal_ledger.record_entry(entry).await?;

                        *total_files += 1;
                        *total_bytes += file_size;

                        debug!(
                            "Captured validated file: {} ({} bytes)",
                            path.display(),
                            file_size
                        );
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_tree_watch_sensor_security_validation() {
        // Test that the sensor rejects dangerous paths
        let temp_ledger = Arc::new(
            TemporalLedger::new_in_memory()
                .await
                .expect("Failed to create in-memory temporal ledger for testing"),
        );
        let config = SensorConfig::default();

        let sensor = TreeWatchSensor::new(temp_ledger.clone(), config)
            .expect("Failed to create TreeWatchSensor for testing");

        // Create a test job with a dangerous path
        let dangerous_job = SensorJob {
            job_id: Ulid::new(),
            sensor_type: SensorType::TreeWatch,
            target_uri: "/etc/passwd".to_string(), // This should be rejected
            config: serde_json::Value::Null,
            status: crate::job_manager::JobStatus::Pending,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            error_message: None,
            material_id: None,
        };

        // This should fail due to security validation
        let result = sensor.process_job(&dangerous_job, &temp_ledger).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Security validation failed"));
    }

    #[tokio::test]
    async fn test_tree_watch_sensor_valid_path() {
        let temp_ledger = Arc::new(
            TemporalLedger::new_in_memory()
                .await
                .expect("Failed to create in-memory temporal ledger for testing"),
        );
        let config = SensorConfig::default();

        // Use permissive policy for testing
        let permissive_policy = FileWatchingSecurityPolicy::permissive();
        let sensor = TreeWatchSensor::with_policy(temp_ledger.clone(), config, permissive_policy)
            .expect("Failed to create TreeWatchSensor with permissive policy for testing");

        // Create temporary directory for testing
        let temp_dir = TempDir::new().expect("Failed to create temporary directory for testing");
        let temp_path = temp_dir
            .path()
            .to_str()
            .expect("Failed to convert temp path to string");

        let safe_job = SensorJob {
            job_id: Ulid::new(),
            sensor_type: SensorType::TreeWatch,
            target_uri: temp_path.to_string(),
            config: serde_json::Value::Null,
            status: crate::job_manager::JobStatus::Pending,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            error_message: None,
            material_id: None,
        };

        // This should succeed with a valid path
        let result = sensor.process_job(&safe_job, &temp_ledger).await;
        assert!(result.is_ok());
    }
}
