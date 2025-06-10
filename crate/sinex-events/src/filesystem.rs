use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use async_trait::async_trait;
use notify::Watcher;
use tokio::sync::mpsc;
use tracing::{debug, info, warn, error};

use sinex_core::{EventType, EventSource, Result, event_type_constants, sources};
use sinex_db::models::RawEvent;

// ============================================================================
// Event Payloads
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FileCreatedPayload {
    pub path: PathBuf,
    pub size: u64,
    pub created_at: DateTime<Utc>,
    pub permissions: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FileModifiedPayload {
    pub path: PathBuf,
    pub size: u64,
    pub modified_at: DateTime<Utc>,
    pub modification_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FileDeletedPayload {
    pub path: PathBuf,
    pub deleted_at: DateTime<Utc>,
}

// ============================================================================
// Event Types
// ============================================================================

pub struct FileCreated;
impl EventType for FileCreated {
    type Payload = FileCreatedPayload;
    type SourceImpl = FilesystemWatcher;
    const EVENT_NAME: &'static str = event_type_constants::filesystem::FILE_CREATED;
}

pub struct FileModified;
impl EventType for FileModified {
    type Payload = FileModifiedPayload;
    type SourceImpl = FilesystemWatcher;
    const EVENT_NAME: &'static str = event_type_constants::filesystem::FILE_MODIFIED;
}

pub struct FileDeleted;
impl EventType for FileDeleted {
    type Payload = FileDeletedPayload;
    type SourceImpl = FilesystemWatcher;
    const EVENT_NAME: &'static str = event_type_constants::filesystem::FILE_DELETED;
}

// ============================================================================
// Event Source
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilesystemConfig {
    pub watch_patterns: Vec<String>,
    pub ignore_patterns: Vec<String>,
    pub debounce_ms: u64,
    pub max_depth: Option<usize>,
}

impl Default for FilesystemConfig {
    fn default() -> Self {
        Self {
            watch_patterns: vec!["**/*".to_string()],
            ignore_patterns: vec![
                "target/**".to_string(),
                "**/.git/**".to_string(),
                "**/node_modules/**".to_string(),
            ],
            debounce_ms: 100,
            max_depth: None,
        }
    }
}

pub struct FilesystemWatcher {
    config: FilesystemConfig,
}

impl FilesystemWatcher {
    fn process_notify_event(
        event: &notify_debouncer_full::DebouncedEvent,
        config: &FilesystemConfig,
    ) -> Option<RawEvent> {
        let path = event.paths.first()?;
        let path_str = path.to_string_lossy().to_string();
        
        // Check ignore patterns
        for pattern in &config.ignore_patterns {
            if glob::Pattern::new(pattern).ok()?.matches(&path_str) {
                debug!("Ignoring path due to pattern: {}", path_str);
                return None;
            }
        }
        
        // Determine event type and create payload
        let (event_type, payload) = match event.kind {
            notify::EventKind::Create(_) => {
                let metadata = std::fs::metadata(path).ok()?;
                let payload = FileCreatedPayload {
                    path: path.to_path_buf(),
                    size: metadata.len(),
                    created_at: Utc::now(),
                    permissions: None, // TODO: Add Unix permissions
                };
                (event_type_constants::filesystem::FILE_CREATED, serde_json::to_value(payload).ok()?)
            }
            notify::EventKind::Modify(_) => {
                let metadata = std::fs::metadata(path).ok()?;
                let payload = FileModifiedPayload {
                    path: path.to_path_buf(),
                    size: metadata.len(),
                    modified_at: Utc::now(),
                    modification_type: "content".to_string(),
                };
                (event_type_constants::filesystem::FILE_MODIFIED, serde_json::to_value(payload).ok()?)
            }
            notify::EventKind::Remove(_) => {
                let payload = FileDeletedPayload {
                    path: path.to_path_buf(),
                    deleted_at: Utc::now(),
                };
                (event_type_constants::filesystem::FILE_DELETED, serde_json::to_value(payload).ok()?)
            }
            _ => return None,
        };
        
        Some(RawEvent {
            id: sinex_ulid::Ulid::new(),
            source: sources::FILESYSTEM.to_string(),
            event_type: event_type.to_string(),
            ts_ingest: Utc::now(),
            ts_orig: Some(Utc::now()),
            host: gethostname::gethostname().to_string_lossy().to_string(),
            ingestor_version: Some("0.1.0".to_string()),
            payload_schema_id: None,
            payload,
        })
    }
}

#[async_trait]
impl EventSource for FilesystemWatcher {
    type Config = FilesystemConfig;
    
    const SOURCE_NAME: &'static str = sources::FILESYSTEM;
    
    async fn initialize(config: Self::Config) -> Result<Self> {
        info!(
            patterns = ?config.watch_patterns,
            "Initializing filesystem watcher"
        );
        Ok(Self { config })
    }
    
    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> Result<()> {
        info!(
            patterns = ?self.config.watch_patterns,
            ignore = ?self.config.ignore_patterns,
            debounce_ms = self.config.debounce_ms,
            "Starting filesystem event stream"
        );
        
        // Set up filesystem watcher with debouncing
        let (notify_tx, notify_rx) = std::sync::mpsc::channel();
        let mut debouncer = notify_debouncer_full::new_debouncer(
            std::time::Duration::from_millis(self.config.debounce_ms),
            None,
            notify_tx,
        ).map_err(|e| sinex_core::CoreError::Other(format!("Failed to create debouncer: {}", e)))?;
        
        // Watch all matching paths
        for pattern in &self.config.watch_patterns {
            // Expand home directory
            let expanded = shellexpand::tilde(pattern);
            
            // Find all paths matching the pattern
            for entry in glob::glob(&expanded)
                .map_err(|e| sinex_core::CoreError::Other(format!("Invalid glob pattern: {}", e)))? 
            {
                match entry {
                    Ok(path) => {
                        if path.exists() {
                            info!("Watching path: {}", path.display());
                            debouncer.watcher()
                                .watch(&path, notify::RecursiveMode::Recursive)
                                .map_err(|e| sinex_core::CoreError::Other(format!("Failed to watch path: {}", e)))?;
                        }
                    }
                    Err(e) => warn!("Failed to process glob entry: {}", e),
                }
            }
        }
        
        // Process events in a separate task
        let config = self.config.clone();
        let event_tx = tx.clone();
        
        tokio::task::spawn_blocking(move || {
            for result in notify_rx {
                match result {
                    Ok(events) => {
                        for event in events {
                            if let Some(raw_event) = Self::process_notify_event(&event, &config) {
                                if let Err(e) = event_tx.blocking_send(raw_event) {
                                    error!("Failed to send event: {}", e);
                                    return;
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
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
    }
}