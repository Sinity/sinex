use async_trait::async_trait;
use chrono::Utc;
use notify::Watcher;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_core::{EventSender, Timestamp};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use tracing::{debug, error, info};

use sinex_core::{
    event_type_constants, sources, EventSource, EventSourceBase, EventSourceContext, EventType,
    RawEvent, Result,
};

// ============================================================================
// Event Payloads
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FileCreatedPayload {
    pub path: PathBuf,
    pub size: u64,
    pub created_at: Timestamp,
    pub permissions: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FileModifiedPayload {
    pub path: PathBuf,
    pub size: u64,
    pub modified_at: Timestamp,
    pub modification_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FileDeletedPayload {
    pub path: PathBuf,
    pub deleted_at: Timestamp,
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

/// File system monitor using the notify crate (inotify on Linux)
pub struct FilesystemMonitor {
    config: FilesystemConfig,
}

// Legacy alias for compatibility
pub type FilesystemWatcher = FilesystemMonitor;

impl FilesystemMonitor {
    async fn new(config: FilesystemConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn process_notify_event(
        event: &notify_debouncer_full::DebouncedEvent,
        config: &FilesystemConfig,
    ) -> Option<RawEvent> {
        let path = event.paths.first()?;
        let path_str = path.to_string_lossy().to_string();

        // Validate path for security issues
        match sinex_core::validation::validate_path(&path_str) {
            Ok(_) => {
                // Path is safe, continue processing
            }
            Err(e) => {
                error!("Path validation failed: {} - path: {}", e, path_str);
                return None;
            }
        }

        // Check if path matches any watch pattern
        let mut matches_watch = false;
        for pattern in &config.watch_patterns {
            let expanded = shellexpand::tilde(pattern);
            if let Ok(glob_pattern) = glob::Pattern::new(&expanded) {
                if glob_pattern.matches(&path_str) {
                    matches_watch = true;
                    break;
                }
            }
        }

        if !matches_watch {
            debug!("Path doesn't match any watch pattern: {}", path_str);
            return None;
        }

        // Check ignore patterns
        for pattern in &config.ignore_patterns {
            let expanded = shellexpand::tilde(pattern);
            if let Ok(glob_pattern) = glob::Pattern::new(&expanded) {
                let matches = if pattern.contains('/') || pattern.contains("**") {
                    // Pattern contains path separators
                    if pattern.starts_with('/') || pattern.contains("**/") {
                        // Absolute path or contains **/, match against full path
                        glob_pattern.matches(&path_str)
                    } else {
                        // Relative path pattern, check if it matches any suffix of the path
                        // Convert path to use forward slashes for consistent matching
                        let normalized_path = path_str.replace('\\', "/");
                        let path_components: Vec<&str> = normalized_path.split('/').collect();

                        // Try matching the pattern against all possible suffixes
                        let mut found_match = false;
                        for i in 0..path_components.len() {
                            let suffix = path_components[i..].join("/");
                            if glob_pattern.matches(&suffix) {
                                found_match = true;
                                break;
                            }
                        }
                        found_match
                    }
                } else {
                    // Pattern is filename-only, match against just the filename
                    if let Some(filename) = path.file_name().and_then(|f| f.to_str()) {
                        glob_pattern.matches(filename)
                    } else {
                        false
                    }
                };

                if matches {
                    debug!(
                        "Ignoring path due to pattern: {} (pattern: {})",
                        path_str, pattern
                    );
                    return None;
                }
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
                    permissions: {
                        #[cfg(unix)]
                        {
                            Some(metadata.permissions().mode())
                        }
                        #[cfg(not(unix))]
                        {
                            None
                        }
                    },
                };
                (
                    event_type_constants::filesystem::FILE_CREATED,
                    serde_json::to_value(payload).ok()?,
                )
            }
            notify::EventKind::Modify(_) => {
                let metadata = std::fs::metadata(path).ok()?;
                let payload = FileModifiedPayload {
                    path: path.to_path_buf(),
                    size: metadata.len(),
                    modified_at: Utc::now(),
                    modification_type: "content".to_string(),
                };
                (
                    event_type_constants::filesystem::FILE_MODIFIED,
                    serde_json::to_value(payload).ok()?,
                )
            }
            notify::EventKind::Remove(_) => {
                let payload = FileDeletedPayload {
                    path: path.to_path_buf(),
                    deleted_at: Utc::now(),
                };
                (
                    event_type_constants::filesystem::FILE_DELETED,
                    serde_json::to_value(payload).ok()?,
                )
            }
            _ => return None,
        };

        // Create event using helper - but we need 'self' for this
        // Since this is a static method, we'll keep the manual creation for now
        // and use the helper in stream_events instead
        Some(RawEvent {
            id: sinex_ulid::Ulid::new(),
            source: sources::FS.to_string(),
            event_type: event_type.to_string(),
            ts_ingest: Utc::now(),
            ts_orig: Some(Utc::now()),
            host: gethostname::gethostname().to_string_lossy().to_string(),
            ingestor_version: Some(env!("CARGO_PKG_VERSION").to_string()),
            payload_schema_id: None,
            payload,
        })
    }
}

// Implement EventSourceBase to get common functionality
impl EventSourceBase for FilesystemMonitor {}

#[async_trait]
impl EventSource for FilesystemMonitor {
    type Config = FilesystemConfig;

    const SOURCE_NAME: &'static str = sources::FS;

    async fn initialize(ctx: EventSourceContext) -> Result<Self> {
        // Use base trait for config parsing
        let config = <Self as EventSourceBase>::parse_config::<Self::Config>(&ctx).await?;

        info!(
            patterns = ?config.watch_patterns,
            "Initializing filesystem watcher"
        );
        Self::new(config).await
    }

    async fn stream_events(&mut self, tx: EventSender) -> Result<()> {
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
        )
        .map_err(|e| {
            sinex_core::CoreError::processing_failed()
                .with_operation("create_debouncer")
                .with_source(e)
                .build()
        })?;

        // Watch all matching paths
        let mut watched_paths = std::collections::HashSet::new();

        for pattern in &self.config.watch_patterns {
            // Expand home directory
            let expanded = shellexpand::tilde(pattern);

            // Extract the base directory from the pattern
            // For patterns like "/tmp/test-sinex/**/*", we want to watch "/tmp/test-sinex"
            let base_path_str = if expanded.contains("**") {
                // Find the path before the first wildcard
                expanded
                    .split("**")
                    .next()
                    .unwrap_or(&expanded)
                    .trim_end_matches('/')
                    .to_string()
            } else if expanded.contains('*') {
                // Find the parent directory of the wildcard
                std::path::Path::new(expanded.as_ref())
                    .parent()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_else(|| expanded.to_string())
            } else {
                expanded.to_string()
            };

            let base_path = std::path::Path::new(&base_path_str);

            // Create directory if it doesn't exist (for testing)
            if !base_path.exists() {
                info!("Creating directory: {}", base_path.display());
                std::fs::create_dir_all(base_path).map_err(|e| {
                    sinex_core::CoreError::io_error(base_path)
                        .with_operation("create_directory")
                        .with_source(e)
                        .build()
                })?;
            }

            // Watch the base directory
            if base_path.exists() && !watched_paths.contains(base_path) {
                info!("Watching directory: {}", base_path.display());
                debouncer
                    .watcher()
                    .watch(base_path, notify::RecursiveMode::Recursive)
                    .map_err(|e| {
                        sinex_core::CoreError::io_error(base_path)
                            .with_operation("watch_path")
                            .with_source(e)
                            .build()
                    })?;
                watched_paths.insert(base_path.to_path_buf());
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
