use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use notify::{EventKind, RecursiveMode, Watcher};
use notify_debouncer_full::new_debouncer;
use serde::{Deserialize, Serialize};
use sinex_shared::{event_type_constants, sources, RawEventBuilder, SimpleIngestor};
use sinex_db::models::RawEvent;
use std::fs;
use std::path::Path;
use std::sync::mpsc as std_mpsc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::config::FilesystemConfig;

/// File event payloads
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileCreatedPayload {
    pub path: String,
    pub object_type: ObjectType,
    pub size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permissions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blake3_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileModifiedPayload {
    pub path: String,
    pub object_type: ObjectType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modification_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blake3_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileDeletedPayload {
    pub path: String,
    pub object_type: ObjectType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRenamedPayload {
    pub path: String,
    pub new_path: String,
    pub object_type: ObjectType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blake3_hash: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ObjectType {
    File,
    Directory,
}

/// Filesystem ingestor that watches for file system events
pub struct FilesystemIngestor {
    config: FilesystemConfig,
}

impl FilesystemIngestor {
    pub fn new(config: FilesystemConfig) -> Self {
        Self { config }
    }

    /// Start watching and send events through the provided channel
    async fn watch(&mut self, event_tx: mpsc::Sender<RawEvent>) -> Result<()> {
        info!(
            watch_dirs = ?self.config.watch_directories,
            exclude_patterns = ?self.config.exclude_patterns,
            debounce_ms = self.config.debounce_ms,
            hash_files = self.config.hash_files,
            "Starting filesystem watcher"
        );

        // Set up filesystem watcher
        let (notify_tx, notify_rx) = std_mpsc::channel();
        let mut debouncer = new_debouncer(
            Duration::from_millis(self.config.debounce_ms),
            None,
            notify_tx,
        )?;

        // Add watch directories
        for dir in &self.config.watch_directories {
            let dir_str = dir.to_str()
                .ok_or_else(|| anyhow::anyhow!("Invalid UTF-8 in path: {}", dir.display()))?;
            let expanded_path = shellexpand::tilde(dir_str).to_string();
            let path = Path::new(&expanded_path);
            
            if path.exists() {
                info!("Watching directory: {}", path.display());
                debouncer.watcher().watch(path, RecursiveMode::Recursive)?;
            } else {
                warn!("Directory does not exist, skipping: {}", path.display());
            }
        }

        // Process filesystem events
        let event_tx_clone = event_tx.clone();
        let config = self.config.clone();
        
        // Use a blocking task for the notify receiver
        tokio::task::spawn_blocking(move || {
            for result in notify_rx {
                match result {
                    Ok(events) => {
                        debug!(
                            event_count = events.len(),
                            "Received filesystem events batch"
                        );
                        for event in events {
                            if let Some(raw_event) = Self::process_notify_event(&event, &config) {
                                debug!(
                                    event_type = %raw_event.event_type,
                                    path = ?event.paths.first(),
                                    "Processed filesystem event"
                                );
                                if let Err(e) = event_tx_clone.blocking_send(raw_event) {
                                    error!(
                                        error = %e,
                                        "Failed to send filesystem event - channel closed"
                                    );
                                    break;
                                }
                            }
                        }
                    }
                    Err(errors) => {
                        for error in errors {
                            error!("Notify error: {:?}", error);
                        }
                    }
                }
            }
        });

        // Keep the watcher alive
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }

    /// Process a notify event into a RawEvent
    fn process_notify_event(
        event: &notify_debouncer_full::DebouncedEvent,
        config: &FilesystemConfig,
    ) -> Option<RawEvent> {
        let path = event.paths.first()?;
        let path_str = path.to_string_lossy().to_string();

        // Check exclude/include patterns
        if !Self::should_process_path(&path_str, &config.exclude_patterns, &config.include_patterns) {
            debug!(
                path = %path_str,
                "Path filtered out by exclude/include patterns"
            );
            return None;
        }

        let object_type = if path.is_dir() {
            ObjectType::Directory
        } else {
            ObjectType::File
        };

        let (event_type, payload) = match &event.kind {
            EventKind::Create(_) => {
                let (size, permissions) = if path.exists() {
                    match fs::metadata(path) {
                        Ok(metadata) => {
                            let size = metadata.len();
                            let permissions = if cfg!(unix) {
                                use std::os::unix::fs::PermissionsExt;
                                Some(format!("{:o}", metadata.permissions().mode() & 0o777))
                            } else {
                                None
                            };
                            (size, permissions)
                        }
                        Err(e) => {
                            debug!("Failed to get metadata for {}: {}", path.display(), e);
                            (0, None)
                        }
                    }
                } else {
                    (0, None)
                };

                let hash = if config.hash_files && object_type == ObjectType::File && size > 0 {
                    Self::hash_file(path, config.max_hash_size_bytes)
                } else {
                    None
                };

                (
                    event_type_constants::filesystem::FILE_CREATED,
                    serde_json::to_value(FileCreatedPayload {
                        path: path_str,
                        object_type,
                        size,
                        permissions,
                        blake3_hash: hash,
                    }).ok()?,
                )
            }
            EventKind::Modify(_) => {
                let new_size = if path.exists() {
                    match fs::metadata(path) {
                        Ok(metadata) => Some(metadata.len()),
                        Err(e) => {
                            debug!("Failed to get metadata for {}: {}", path.display(), e);
                            None
                        }
                    }
                } else {
                    None
                };

                let hash = if config.hash_files && object_type == ObjectType::File {
                    Self::hash_file(path, config.max_hash_size_bytes)
                } else {
                    None
                };

                (
                    event_type_constants::filesystem::FILE_MODIFIED,
                    serde_json::to_value(FileModifiedPayload {
                        path: path_str,
                        object_type,
                        old_size: None, // We don't track the old size currently
                        new_size,
                        modification_type: Some("content".to_string()),
                        blake3_hash: hash,
                    }).ok()?,
                )
            }
            EventKind::Remove(_) => {
                (
                    event_type_constants::filesystem::FILE_DELETED,
                    serde_json::to_value(FileDeletedPayload {
                        path: path_str,
                        object_type,
                    }).ok()?,
                )
            }
            _ => return None, // Ignore other event types for now
        };

        Some(
            RawEventBuilder::new(sources::FILESYSTEM, event_type, payload)
                .with_orig_timestamp(Utc::now())
                .build()
        )
    }

    /// Check if a path should be processed based on patterns
    fn should_process_path(path: &str, excludes: &[String], includes: &[String]) -> bool {
        // Check excludes first
        for pattern in excludes {
            if glob::Pattern::new(pattern).map_or(false, |p| p.matches(path)) {
                // Check if there's an include pattern that overrides
                for include in includes {
                    if glob::Pattern::new(include).map_or(false, |p| p.matches(path)) {
                        return true;
                    }
                }
                return false;
            }
        }
        
        true
    }

    /// Hash a file using BLAKE3
    fn hash_file(path: &Path, max_size: u64) -> Option<String> {
        match fs::metadata(path) {
            Ok(metadata) if metadata.len() <= max_size => {
                match fs::read(path) {
                    Ok(contents) => {
                        let hash = blake3::hash(&contents);
                        Some(hash.to_hex().to_string())
                    }
                    Err(e) => {
                        debug!("Failed to read file for hashing: {}: {}", path.display(), e);
                        None
                    }
                }
            }
            _ => None,
        }
    }
}

// SimpleIngestor implementation for use with IngestorRuntime
#[async_trait]
impl SimpleIngestor for FilesystemIngestor {
    fn name() -> &'static str {
        "filesystem-ingestor"
    }
    
    fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
    
    async fn capture_events(&mut self, event_tx: mpsc::Sender<RawEvent>) -> Result<()> {
        self.watch(event_tx).await
    }
}