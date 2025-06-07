use anyhow::Result;
use chrono::Utc;
use notify::{EventKind, RecursiveMode, Watcher};
use notify_debouncer_full::new_debouncer;
use serde::{Deserialize, Serialize};
use sinex_shared::{
    create_heartbeat_event, event_type_constants, sources,
    AgentMetrics, AgentStatus, EventSink, DlqManager, RawEvent, RawEventBuilder,
    RetryConfig, retry_db_operation,
};
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error, info, warn};

use crate::config::FilesystemConfig;

/// File event payloads
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileCreatedPayload {
    pub path: String,
    pub object_type: ObjectType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blake3_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileModifiedPayload {
    pub path: String,
    pub object_type: ObjectType,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ObjectType {
    File,
    Directory,
}

/// Filesystem watcher
pub struct FilesystemWatcher {
    config: FilesystemConfig,
    event_sink: Arc<dyn EventSink>,
    dlq: Arc<DlqManager>,
    metrics: Arc<Mutex<AgentMetrics>>,
    retry_config: RetryConfig,
    event_batch: Arc<Mutex<Vec<RawEvent>>>,
}

impl FilesystemWatcher {
    pub fn new(config: FilesystemConfig, event_sink: Arc<dyn EventSink>) -> Result<Self> {
        let dlq = Arc::new(DlqManager::new("filesystem-ingestor")?);
        let metrics = Arc::new(Mutex::new(AgentMetrics::new(
            "filesystem-ingestor",
            env!("CARGO_PKG_VERSION"),
        )));
        
        let retry_config = RetryConfig {
            max_retries: config.max_retries,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(config.retry_delay_secs),
            exponential_base: 2,
        };

        Ok(Self {
            config,
            event_sink,
            dlq,
            metrics,
            retry_config,
            event_batch: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Start the filesystem watcher
    pub async fn start(self) -> Result<()> {
        info!(
            agent_name = "filesystem-ingestor",
            version = env!("CARGO_PKG_VERSION"),
            watch_dirs = ?self.config.watch_directories,
            exclude_patterns = ?self.config.exclude_patterns,
            debounce_ms = self.config.debounce_ms,
            batch_size = self.config.batch_size_events,
            hash_files = self.config.hash_files,
            "Starting filesystem watcher"
        );

        let (event_tx, mut event_rx) = mpsc::channel(1000);
        
        // Spawn batch processor task
        let event_sink = Arc::clone(&self.event_sink);
        let dlq = Arc::clone(&self.dlq);
        let retry_config = self.retry_config.clone();
        let metrics = Arc::clone(&self.metrics);
        let event_batch = Arc::clone(&self.event_batch);
        let batch_size = self.config.batch_size_events;
        let batch_timeout = Duration::from_millis(self.config.batch_timeout_ms);
        
        let batch_processor = tokio::spawn(async move {
            let mut interval = time::interval(batch_timeout);
            
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        Self::flush_batch(&event_sink, &dlq, &retry_config, &metrics, &event_batch).await;
                    }
                    Some(event) = event_rx.recv() => {
                        let should_flush = {
                            let mut batch = event_batch.lock().unwrap();
                            batch.push(event);
                            batch.len() >= batch_size
                        };
                        
                        if should_flush {
                            Self::flush_batch(&event_sink, &dlq, &retry_config, &metrics, &event_batch).await;
                        }
                    }
                }
            }
        });

        // Spawn heartbeat task
        let heartbeat_tx = event_tx.clone();
        let metrics_clone = Arc::clone(&self.metrics);
        let heartbeat_interval = self.config.heartbeat_interval_secs;
        
        let heartbeat_task = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(heartbeat_interval));
            loop {
                interval.tick().await;
                let heartbeat = {
                    let metrics = metrics_clone.lock().unwrap();
                    metrics.create_heartbeat(AgentStatus::Running)
                };
                let event = create_heartbeat_event(heartbeat);
                if heartbeat_tx.send(event).await.is_err() {
                    break;
                }
            }
        });

        // Set up filesystem watcher
        let (notify_tx, notify_rx) = std::sync::mpsc::channel();
        let mut debouncer = new_debouncer(
            Duration::from_millis(self.config.debounce_ms),
            None,
            notify_tx,
        )?;

        // Add watch directories
        for dir in &self.config.watch_directories {
            let expanded_path = shellexpand::tilde(dir.to_str().unwrap()).to_string();
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
        
        let fs_event_processor = tokio::task::spawn_blocking(move || {
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
                                let _ = event_tx_clone.blocking_send(raw_event);
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

        // Wait for tasks
        tokio::try_join!(batch_processor, heartbeat_task)?;
        fs_event_processor.await?;

        Ok(())
    }

    /// Process a notify event into a RawEvent
    fn process_notify_event(event: &notify_debouncer_full::DebouncedEvent, config: &FilesystemConfig) -> Option<RawEvent> {
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
                let hash = if config.hash_files && object_type == ObjectType::File {
                    Self::hash_file(path, config.max_hash_size_bytes)
                } else {
                    None
                };

                (
                    event_type_constants::filesystem::FILE_CREATED,
                    serde_json::to_value(FileCreatedPayload {
                        path: path_str,
                        object_type,
                        blake3_hash: hash,
                    }).ok()?,
                )
            }
            EventKind::Modify(_) => {
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

    /// Flush the event batch to the event sink
    async fn flush_batch(
        event_sink: &Arc<dyn EventSink>,
        dlq: &Arc<DlqManager>,
        retry_config: &RetryConfig,
        metrics: &Arc<Mutex<AgentMetrics>>,
        event_batch: &Arc<Mutex<Vec<RawEvent>>>,
    ) {
        let events = {
            let mut batch = event_batch.lock().unwrap();
            if batch.is_empty() {
                return;
            }
            std::mem::take(&mut *batch)
        };

        let event_count = events.len();
        debug!("Flushing batch of {} events", event_count);

        let result = retry_db_operation(retry_config, || async {
            event_sink.send_batch(&events).await.map_err(|e| e.into())
        })
        .await;

        match result {
            Ok(_) => {
                let mut m = metrics.lock().unwrap();
                for _ in 0..event_count {
                    m.increment_processed();
                }
                info!("Successfully inserted batch of {} events", event_count);
            }
            Err(e) => {
                error!("Failed to insert event batch after retries: {}", e);
                
                // Write each event to DLQ
                for event in events {
                    match dlq.write_event(event.clone(), e.to_string(), retry_config.max_retries).await {
                        Ok(dlq_path) => {
                            metrics.lock().unwrap().increment_dlq();
                            
                            // Try to emit DLQ notification
                            let dlq_event = dlq.create_dlq_notification(&event, dlq_path, e.to_string());
                            
                            if let Err(e2) = event_sink.send_event(&dlq_event).await {
                                let _ = dlq.log_critical_failure(&format!(
                                    "Failed to emit DLQ notification: {} (original error: {})",
                                    e2, e
                                ));
                            }
                        }
                        Err(dlq_err) => {
                            let _ = dlq.log_critical_failure(&format!(
                                "Failed to write to DLQ: {} (original error: {})",
                                dlq_err, e
                            ));
                        }
                    }
                }
            }
        }
    }
}

impl PartialEq for ObjectType {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (ObjectType::File, ObjectType::File) | (ObjectType::Directory, ObjectType::Directory)
        )
    }
}