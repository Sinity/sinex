//! File watching abstraction for event sources
//!
//! This module provides a unified interface for file system monitoring,
//! reducing boilerplate across event sources that need to watch files.

use crate::{buffers, CoreError, Result};
use sinex_macros::with_context;
use notify::event::{DataChange, ModifyKind};
use notify::{Event, EventKind, RecursiveMode, Watcher};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;

/// File watcher configuration
#[derive(Debug, Clone)]
pub struct FileWatcherConfig {
    /// Paths to watch
    pub paths: Vec<PathBuf>,
    /// Whether to watch recursively
    pub recursive: bool,
    /// Channel buffer size
    pub channel_size: usize,
    /// Watch parent directories for file creation
    pub watch_parents: bool,
}

impl Default for FileWatcherConfig {
    fn default() -> Self {
        Self {
            paths: Vec::new(),
            recursive: false,
            channel_size: buffers::NOTIFICATION_CHANNEL_SIZE,
            watch_parents: false,
        }
    }
}

/// File change event
#[derive(Debug, Clone)]
pub struct FileChangeEvent {
    pub path: PathBuf,
    pub kind: FileChangeKind,
}

#[derive(Debug, Clone)]
pub enum FileChangeKind {
    Modified,
    Created,
    Deleted,
    Renamed { from: PathBuf, to: PathBuf },
}

/// File watcher abstraction
pub struct FileWatcher {
    _config: FileWatcherConfig,
    _watcher: notify::RecommendedWatcher,
    rx: mpsc::Receiver<FileChangeEvent>,
}

impl FileWatcher {
    /// Create a new file watcher
    #[with_context(operation = "create_file_watcher")]
    pub fn new(config: FileWatcherConfig) -> Result<Self> {
        let (tx, rx) = mpsc::channel(config.channel_size);
        let watched_paths = Arc::new(config.paths.clone());

        let mut watcher = notify::recommended_watcher(move |res: notify::Result<Event>| {
            if let Ok(event) = res {
                if let Some(change_event) = convert_notify_event(event, &watched_paths) {
                    let _ = tx.blocking_send(change_event);
                }
            }
        })
        .map_err(|e| CoreError::Configuration(format!("Failed to create file watcher: {}", e)))?;

        // Set up watches
        let mode = if config.recursive {
            RecursiveMode::Recursive
        } else {
            RecursiveMode::NonRecursive
        };

        for path in &config.paths {
            if path.exists() {
                watcher.watch(path, mode).map_err(|e| {
                    CoreError::Configuration(format!("Failed to watch path: {}", e))
                        .context().with_context("path", path.display().to_string()).build()
                })?;
            }
        }

        // Watch parent directories if requested
        if config.watch_parents {
            let mut watched_parents = std::collections::HashSet::new();
            for path in &config.paths {
                if let Some(parent) = path.parent() {
                    if watched_parents.insert(parent.to_path_buf()) && parent.exists() {
                        watcher
                            .watch(parent, RecursiveMode::NonRecursive)
                            .map_err(|e| {
                                CoreError::Configuration(format!("Failed to watch parent directory: {}", e))
                                    .context().with_context("parent", parent.display().to_string()).build()
                            })?;
                    }
                }
            }
        }

        Ok(Self {
            _config: config,
            _watcher: watcher,
            rx,
        })
    }

    /// Receive the next file change event
    pub async fn recv(&mut self) -> Option<FileChangeEvent> {
        self.rx.recv().await
    }

    /// Try to receive without blocking
    pub fn try_recv(&mut self) -> Option<FileChangeEvent> {
        self.rx.try_recv().ok()
    }
}

/// Builder for FileWatcher
pub struct FileWatcherBuilder {
    config: FileWatcherConfig,
}

impl Default for FileWatcherBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl FileWatcherBuilder {
    pub fn new() -> Self {
        Self {
            config: FileWatcherConfig::default(),
        }
    }

    pub fn watch_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.paths.push(path.into());
        self
    }

    pub fn watch_paths(mut self, paths: Vec<PathBuf>) -> Self {
        self.config.paths = paths;
        self
    }

    pub fn recursive(mut self, recursive: bool) -> Self {
        self.config.recursive = recursive;
        self
    }

    pub fn watch_parents(mut self, watch: bool) -> Self {
        self.config.watch_parents = watch;
        self
    }

    pub fn channel_size(mut self, size: usize) -> Self {
        self.config.channel_size = size;
        self
    }

    #[with_context(operation = "build_file_watcher")]
    pub fn build(self) -> Result<FileWatcher> {
        FileWatcher::new(self.config)
    }
}

/// Convert notify event to our simplified event type
fn convert_notify_event(event: Event, watched_paths: &[PathBuf]) -> Option<FileChangeEvent> {
    // Filter for relevant paths
    let relevant_path = event
        .paths
        .iter()
        .find(|p| watched_paths.iter().any(|w| p.starts_with(w) || w == *p))?
        .clone();

    let kind = match event.kind {
        EventKind::Modify(ModifyKind::Data(DataChange::Any)) => FileChangeKind::Modified,
        EventKind::Create(_) => FileChangeKind::Created,
        EventKind::Remove(_) => FileChangeKind::Deleted,
        EventKind::Modify(ModifyKind::Name(_)) if event.paths.len() >= 2 => {
            FileChangeKind::Renamed {
                from: event.paths[0].clone(),
                to: event.paths[1].clone(),
            }
        }
        _ => return None,
    };

    Some(FileChangeEvent {
        path: relevant_path,
        kind,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::fs;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_file_watcher_basic() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");

        // Create file first
        fs::write(&file_path, "initial").await.unwrap();

        // Create watcher
        let mut watcher = FileWatcherBuilder::new()
            .watch_path(&file_path)
            .build()
            .unwrap();

        // Modify file
        sleep(Duration::from_millis(10)).await;
        fs::write(&file_path, "modified").await.unwrap();

        // Wait for event
        let event = tokio::time::timeout(Duration::from_secs(1), watcher.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(event.path, file_path);
        assert!(matches!(event.kind, FileChangeKind::Modified));
    }
}
