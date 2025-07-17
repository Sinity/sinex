//! File system watcher implementation
//!
//! This module provides file system watching capabilities with
//! configurable event filtering and error handling.

use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use sinex_core_types::{CoreError, Result};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

/// Configuration for file watcher
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileWatcherConfig {
    /// Paths to watch
    pub watch_paths: Vec<PathBuf>,
    /// Whether to watch recursively
    pub recursive: bool,
    /// Event types to monitor
    pub event_kinds: Vec<FileChangeKind>,
    /// Debounce delay for events
    pub debounce_delay: Duration,
    /// Maximum events to buffer
    pub max_buffer_size: usize,
}

impl Default for FileWatcherConfig {
    fn default() -> Self {
        Self {
            watch_paths: Vec::new(),
            recursive: true,
            event_kinds: vec![
                FileChangeKind::Created,
                FileChangeKind::Modified,
                FileChangeKind::Deleted,
            ],
            debounce_delay: Duration::from_millis(100),
            max_buffer_size: 1000,
        }
    }
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

/// File watcher builder
pub struct FileWatcherBuilder {
    config: FileWatcherConfig,
}

impl FileWatcherBuilder {
    pub fn new() -> Self {
        Self {
            config: FileWatcherConfig::default(),
        }
    }

    pub fn watch_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.config.watch_paths.push(path.as_ref().to_path_buf());
        self
    }

    pub fn recursive(mut self, recursive: bool) -> Self {
        self.config.recursive = recursive;
        self
    }

    pub fn event_kinds(mut self, kinds: Vec<FileChangeKind>) -> Self {
        self.config.event_kinds = kinds;
        self
    }

    pub fn debounce_delay(mut self, delay: Duration) -> Self {
        self.config.debounce_delay = delay;
        self
    }

    pub fn max_buffer_size(mut self, size: usize) -> Self {
        self.config.max_buffer_size = size;
        self
    }

    pub fn build(self) -> Result<FileWatcher> {
        FileWatcher::new(self.config)
    }
}

impl Default for FileWatcherBuilder {
    fn default() -> Self {
        Self::new()
    }
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
        .map_err(|e| CoreError::Unknown(format!("Failed to create file watcher: {}", e)))?;

        // Watch all configured paths
        for path in &config.watch_paths {
            let mode = if config.recursive {
                RecursiveMode::Recursive
            } else {
                RecursiveMode::NonRecursive
            };

            watcher
                .watch(path, mode)
                .map_err(|e| CoreError::Unknown(format!("Failed to watch path {:?}: {}", path, e)))?;

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
        let watcher = FileWatcherBuilder::new()
            .watch_path(temp_dir.path())
            .recursive(true)
            .debounce_delay(Duration::from_millis(50))
            .build();

        assert!(watcher.is_ok());
    }

    #[tokio::test]
    async fn test_file_watcher_events() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.txt");

        let mut watcher = FileWatcherBuilder::new()
            .watch_path(temp_dir.path())
            .recursive(false)
            .event_kinds(vec![FileChangeKind::Created, FileChangeKind::Modified])
            .build()
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
