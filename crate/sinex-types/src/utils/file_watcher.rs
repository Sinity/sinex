//! File system watcher implementation
//!
//! This module provides file system watching capabilities with
//! configurable event filtering and error handling.

use crate::error::{Result, SinexError};
use bon::Builder;
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

/// Configuration for file watcher
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct FileWatcherConfig {
    /// Paths to watch
    #[builder(default)]
    pub watch_paths: Vec<PathBuf>,
    /// Whether to watch recursively
    #[builder(default = true)]
    pub recursive: bool,
    /// Event types to monitor
    #[builder(default = vec![
        FileChangeKind::Created,
        FileChangeKind::Modified,
        FileChangeKind::Deleted,
    ])]
    pub event_kinds: Vec<FileChangeKind>,
    /// Debounce delay for events
    #[builder(default = Duration::from_millis(100))]
    pub debounce_delay: Duration,
    /// Maximum events to buffer
    #[builder(default = 1000)]
    pub max_buffer_size: usize,
}

/// File change event kinds
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FileChangeKind {
    Created,
    Modified,
    Deleted,
    Moved,
    Other,
}

/// File change event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileChangeEvent {
    pub path: PathBuf,
    pub kind: FileChangeKind,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// File system watcher
pub struct FileWatcher {
    config: FileWatcherConfig,
    _watcher: RecommendedWatcher,
    event_receiver: mpsc::Receiver<FileChangeEvent>,
}

impl FileWatcher {
    /// Create a new file watcher
    pub fn new(config: FileWatcherConfig) -> Result<Self> {
        let (event_sender, event_receiver) = mpsc::channel(config.max_buffer_size);
        let event_kinds = config.event_kinds.clone();

        let mut watcher = RecommendedWatcher::new(
            move |res: notify::Result<Event>| match res {
                Ok(event) => {
                    if let Some(change_event) = convert_notify_event(event, &event_kinds) {
                        if let Err(e) = event_sender.try_send(change_event) {
                            warn!("Failed to send file change event: {}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("File watcher error: {}", e);
                }
            },
            Config::default(),
        )
        .map_err(|e| {
            SinexError::io(format!("Failed to create file watcher: {}", e))
                .with_operation("notify::watcher::new")
        })?;

        // Watch all configured paths
        for path in &config.watch_paths {
            let mode = if config.recursive {
                RecursiveMode::Recursive
            } else {
                RecursiveMode::NonRecursive
            };

            watcher.watch(path, mode).map_err(|e| {
                SinexError::io(format!("Failed to watch path: {}", e))
                    .with_path(path)
                    .with_context("recursive", config.recursive)
                    .with_operation("watcher.watch")
            })?;

            debug!("Started watching path: {:?}", path);
        }

        Ok(Self {
            config,
            _watcher: watcher,
            event_receiver,
        })
    }

    /// Receive the next file change event
    pub async fn next_event(&mut self) -> Option<FileChangeEvent> {
        self.event_receiver.recv().await
    }

    /// Try to receive an event without blocking
    pub fn try_next_event(&mut self) -> Option<FileChangeEvent> {
        self.event_receiver.try_recv().ok()
    }

    /// Get the current configuration
    pub fn config(&self) -> &FileWatcherConfig {
        &self.config
    }
}

/// Convert notify event to our file change event
fn convert_notify_event(event: Event, allowed_kinds: &[FileChangeKind]) -> Option<FileChangeEvent> {
    let kind = match event.kind {
        EventKind::Create(_) => FileChangeKind::Created,
        EventKind::Modify(_) => FileChangeKind::Modified,
        EventKind::Remove(_) => FileChangeKind::Deleted,
        EventKind::Other => FileChangeKind::Other,
        _ => return None,
    };

    // Filter based on allowed kinds
    if !allowed_kinds.contains(&kind) {
        return None;
    }

    // Use the first path if multiple paths are present
    let path = event.paths.into_iter().next()?;

    Some(FileChangeEvent {
        path,
        kind,
        timestamp: chrono::Utc::now(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_file_watcher_builder() {
        let temp_dir = TempDir::new().unwrap();
        let watcher = FileWatcher::new(
            FileWatcherConfig::builder()
                .watch_paths(vec![temp_dir.path().to_path_buf()])
                .recursive(true)
                .debounce_delay(Duration::from_millis(50))
                .build(),
        );

        assert!(watcher.is_ok());
    }

    #[tokio::test]
    async fn test_file_watcher_events() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.txt");

        let mut watcher = FileWatcher::new(
            FileWatcherConfig::builder()
                .watch_paths(vec![temp_dir.path().to_path_buf()])
                .recursive(false)
                .event_kinds(vec![FileChangeKind::Created, FileChangeKind::Modified])
                .build(),
        )
        .unwrap();

        // Give the watcher time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create a file
        fs::write(&test_file, "test content").unwrap();

        // Wait for the event
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Check for events (may receive multiple events for a single file operation)
        let mut events = Vec::new();
        while let Some(event) = watcher.try_next_event() {
            events.push(event);
        }

        // Should have received at least one event
        assert!(!events.is_empty());

        // Verify the event is for our test file
        assert!(events.iter().any(|e| e.path == test_file));
    }
}
