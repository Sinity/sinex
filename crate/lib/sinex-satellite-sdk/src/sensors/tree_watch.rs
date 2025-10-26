//! Tree watch sensor for file system monitoring
//!
//! Monitors file system changes and captures file content with security validation

use crate::{
    acquisition_manager::AcquisitionManager,
    job_manager::{SensorExecutor, SensorJob, SensorType},
    SatelliteError, SatelliteResult,
};
use async_trait::async_trait;
use notify::{Event, EventKind, RecursiveMode, Watcher};
use sinex_core::types::{
    validation::{validate_discovered_file, validate_watch_path, FileWatchingSecurityPolicy},
    Ulid,
};
use std::sync::Arc;
use tokio::fs;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Tree watch sensor configuration
#[derive(Debug, Clone)]
pub struct TreeWatchConfig {
    /// Timeout for watching operations (seconds)
    pub watch_timeout_secs: u64,
    /// Maximum file size for content capture (bytes)
    pub max_file_size: u64,
}

impl Default for TreeWatchConfig {
    fn default() -> Self {
        Self {
            watch_timeout_secs: 30,
            max_file_size: 10 * 1024 * 1024, // 10MB
        }
    }
}

/// Tree watch sensor for file system monitoring
pub struct TreeWatchSensor {
    acquisition_manager: Arc<AcquisitionManager>,
    config: TreeWatchConfig,
    security_policy: FileWatchingSecurityPolicy,
}

impl TreeWatchSensor {
    /// Create new tree watch sensor with restrictive security policy
    pub fn new(acquisition_manager: Arc<AcquisitionManager>, config: TreeWatchConfig) -> Self {
        let security_policy = FileWatchingSecurityPolicy::restrictive();

        Self {
            acquisition_manager,
            config,
            security_policy,
        }
    }

    /// Create new tree watch sensor with custom security policy
    pub fn with_policy(
        acquisition_manager: Arc<AcquisitionManager>,
        config: TreeWatchConfig,
        security_policy: FileWatchingSecurityPolicy,
    ) -> Self {
        Self {
            acquisition_manager,
            config,
            security_policy,
        }
    }

    /// Process a tree watch job
    async fn process_watch(&self, job: &SensorJob) -> SatelliteResult<Ulid> {
        info!("Processing tree_watch job for {}", job.target_uri);

        let validated_path =
            validate_watch_path(&job.target_uri, &self.security_policy).map_err(|e| {
                SatelliteError::Processing(format!(
                    "Security validation failed for path '{}': {}",
                    job.target_uri, e
                ))
            })?;

        info!("Path validation passed for: {}", validated_path.as_str());

        let mut handle = self
            .acquisition_manager
            .begin_material(&format!("tree_watch:{}", validated_path.as_str()))
            .await?;
        let material_id = handle.material_id;

        let (tx, mut rx) = mpsc::channel(100);

        let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
            if let Ok(event) = res {
                let _ = tx.blocking_send(event);
            }
        })
        .map_err(|e| SatelliteError::Processing(format!("Failed to create watcher: {}", e)))?;

        watcher
            .watch(validated_path.as_std_path(), RecursiveMode::Recursive)
            .map_err(|e| SatelliteError::Processing(format!("Failed to watch path: {}", e)))?;

        info!("Watching validated path: {}", validated_path.as_str());

        let mut total_files = 0;
        let timeout = tokio::time::Duration::from_secs(self.config.watch_timeout_secs);
        let start = tokio::time::Instant::now();
        let watch_root = validated_path.as_str().to_string();

        while start.elapsed() < timeout {
            tokio::select! {
                Some(event) = rx.recv() => {
                    if let Err(e) = self.process_fs_event(
                        &event,
                        &mut handle,
                        &mut total_files,
                        &watch_root,
                    ).await {
                        error!("Error processing event: {}", e);
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                    if start.elapsed() >= timeout {
                        break;
                    }
                }
            }
        }

        self.acquisition_manager
            .finalize(handle, "watch completed")
            .await?;

        info!(
            "Completed tree_watch job for {}, {} files captured",
            validated_path.as_str(),
            total_files
        );

        Ok(material_id)
    }

    /// Process a file system event with security validation
    async fn process_fs_event(
        &self,
        event: &Event,
        handle: &mut crate::SourceMaterialHandle,
        total_files: &mut usize,
        watch_root: &str,
    ) -> SatelliteResult<()> {
        match event.kind {
            EventKind::Create(_) | EventKind::Modify(_) => {
                for path in &event.paths {
                    let path_str = path.to_string_lossy();

                    match validate_discovered_file(&path_str, watch_root, &self.security_policy) {
                        Ok(_validated_file_path) => {}
                        Err(e) => {
                            warn!(
                                "Skipping invalid discovered file path '{}': {}",
                                path_str, e
                            );
                            continue;
                        }
                    }

                    if path.is_file() {
                        let metadata = fs::metadata(&path).await.map_err(|e| {
                            SatelliteError::Io(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                format!("Failed to get metadata: {}", e),
                            ))
                        })?;

                        let file_size = metadata.len();

                        if file_size <= self.config.max_file_size {
                            match fs::read(&path).await {
                                Ok(content) => {
                                    self.acquisition_manager
                                        .append_slice(handle, &content)
                                        .await?;

                                    *total_files += 1;

                                    debug!(
                                        "Captured validated file: {} ({} bytes)",
                                        path.display(),
                                        file_size
                                    );
                                }
                                Err(e) => {
                                    warn!("Failed to read file: {}", e);
                                }
                            }
                        } else {
                            warn!(
                                "Skipping large file ({}  bytes > {} max): {}",
                                file_size,
                                self.config.max_file_size,
                                path.display()
                            );
                        }
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }
}

#[async_trait]
impl SensorExecutor for TreeWatchSensor {
    async fn process_job(&self, job: &SensorJob) -> SatelliteResult<Ulid> {
        self.process_watch(job).await
    }

    fn sensor_type(&self) -> SensorType {
        SensorType::TreeWatch
    }
}
