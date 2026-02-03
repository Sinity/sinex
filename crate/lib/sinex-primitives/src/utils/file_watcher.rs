//! File system watcher implementation
//!
//! This module provides file system watching capabilities with
//! configurable event filtering, error handling, and comprehensive security validation.

use crate::{
    error::{Result, SinexError},
    validation::{validate_discovered_file, validate_watch_paths, FileWatchingSecurityPolicy},
};
use bon::Builder;
use camino::Utf8PathBuf;
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

/// Configuration for file watcher with security settings
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct FileWatcherConfig {
    /// Paths to watch (will be validated for security)
    #[builder(default)]
    pub watch_paths: Vec<Utf8PathBuf>,
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
    /// Security policy for file watching operations
    #[builder(default = FileWatchingSecurityPolicy::default())]
    pub security_policy: FileWatchingSecurityPolicy,
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
    pub path: Utf8PathBuf,
    pub kind: FileChangeKind,
    pub timestamp: crate::temporal::Timestamp,
}

/// File system watcher with security validation
#[derive(Debug)]
pub struct FileWatcher {
    config: FileWatcherConfig,
    _watcher: RecommendedWatcher,
    event_receiver: mpsc::Receiver<FileChangeEvent>,
    /// Validated watch roots for boundary checking
    _validated_watch_roots: Vec<Utf8PathBuf>,
}

impl FileWatcher {
    /// Create a new file watcher with security validation
    pub fn new(config: FileWatcherConfig) -> Result<Self> {
        // SECURITY: Validate all watch paths before setting up watchers
        let watch_path_strings: Vec<String> = config
            .watch_paths
            .iter()
            .map(|p| p.as_str().to_string())
            .collect();

        let validated_paths = validate_watch_paths(&watch_path_strings, &config.security_policy)
            .map_err(|e| SinexError::validation(format!("Watch path validation failed: {e}")))?;

        let (event_sender, event_receiver) = mpsc::channel(config.max_buffer_size);
        let event_kinds = config.event_kinds.clone();
        let security_policy = config.security_policy.clone();
        let validated_watch_roots = validated_paths.clone();

        let mut watcher = RecommendedWatcher::new(
            move |res: notify::Result<Event>| match res {
                Ok(event) => {
                    if let Some(change_event) = convert_notify_event_secure(
                        event,
                        &event_kinds,
                        &security_policy,
                        &validated_watch_roots,
                    ) {
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
            SinexError::io(format!("Failed to create file watcher: {e}"))
                .with_operation("notify::watcher::new")
        })?;

        // Watch all validated paths
        for path in &validated_paths {
            let mode = if config.recursive {
                RecursiveMode::Recursive
            } else {
                RecursiveMode::NonRecursive
            };

            watcher.watch(path.as_std_path(), mode).map_err(|e| {
                SinexError::io(format!("Failed to watch validated path: {e}"))
                    .with_path(path)
                    .with_context("recursive", config.recursive)
                    .with_operation("watcher.watch")
            })?;

            debug!("Started watching validated path: {:?}", path);
        }

        Ok(Self {
            config,
            _watcher: watcher,
            event_receiver,
            _validated_watch_roots: validated_paths,
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
    #[must_use]
    pub fn config(&self) -> &FileWatcherConfig {
        &self.config
    }
}

/// Convert notify event to our file change event with security validation
fn convert_notify_event_secure(
    event: Event,
    allowed_kinds: &[FileChangeKind],
    security_policy: &FileWatchingSecurityPolicy,
    validated_watch_roots: &[Utf8PathBuf],
) -> Option<FileChangeEvent> {
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
    let path_buf = event
        .paths
        .into_iter()
        .next()
        .and_then(|p| camino::Utf8PathBuf::from_path_buf(p).ok())?;

    // SECURITY: Validate discovered file path against watch roots
    let path_str = path_buf.as_str();

    // Find the appropriate watch root for validation
    let mut validated = false;
    for watch_root in validated_watch_roots {
        if let Ok(_) = validate_discovered_file(path_str, watch_root.as_str(), security_policy) {
            validated = true;
            break;
        }
        // Try next watch root
    }

    if !validated {
        warn!(
            "Rejecting file event for path outside of validated boundaries: {}",
            path_str
        );
        return None;
    }

    Some(FileChangeEvent {
        path: path_buf,
        kind,
        timestamp: crate::temporal::now(),
    })
}
