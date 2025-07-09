use async_trait::async_trait;
use notify::Watcher;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_core::{EventSender, Timestamp};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, error, info};

use sinex_core::{
    event_type_constants, sources, EventSource, EventSourceBase, EventSourceContext, EventType,
    RawEvent, Result, EventFactory, timeouts, filesystem,
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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FileMovedPayload {
    pub path: PathBuf,
    pub old_path: Option<PathBuf>,
    pub moved_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DirCreatedPayload {
    pub path: PathBuf,
    pub created_at: Timestamp,
    pub permissions: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DirDeletedPayload {
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

pub struct FileMoved;
impl EventType for FileMoved {
    type Payload = FileMovedPayload;
    type SourceImpl = FilesystemWatcher;
    const EVENT_NAME: &'static str = event_type_constants::filesystem::FILE_MOVED;
}

pub struct DirCreated;
impl EventType for DirCreated {
    type Payload = DirCreatedPayload;
    type SourceImpl = FilesystemWatcher;
    const EVENT_NAME: &'static str = event_type_constants::filesystem::DIR_CREATED;
}

pub struct DirDeleted;
impl EventType for DirDeleted {
    type Payload = DirDeletedPayload;
    type SourceImpl = FilesystemWatcher;
    const EVENT_NAME: &'static str = event_type_constants::filesystem::DIR_DELETED;
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

/// Rename operation tracking
#[derive(Debug, Clone)]
pub struct RenameOperation {
    pub source_path: PathBuf,
    pub timestamp: Instant,
    #[allow(dead_code)] // Used in HashMap operations but not directly accessed
    pub cookie: Option<u32>,
}

/// File system monitor using the notify crate (inotify on Linux)
pub struct FilesystemMonitor {
    config: FilesystemConfig,
    watch_roots: Vec<PathBuf>,
    #[allow(dead_code)] // Used in async context where struct field access is limited
    event_factory: EventFactory,
    // Instance-based rename tracking for better isolation and testability
    rename_tracker: Arc<Mutex<HashMap<u32, RenameOperation>>>,
}

// Legacy alias for compatibility
pub type FilesystemWatcher = FilesystemMonitor;

impl FilesystemMonitor {
    async fn new(config: FilesystemConfig) -> Result<Self> {
        Ok(Self { 
            config,
            watch_roots: Vec::new(),
            event_factory: EventFactory::new(sources::FS),
            rename_tracker: Arc::new(Mutex::new(HashMap::new())),
        })
    }


    /// Clean up old rename operations that didn't complete
    fn cleanup_old_rename_operations(rename_tracker: &Arc<Mutex<HashMap<u32, RenameOperation>>>) {
        const RENAME_TIMEOUT: Duration = timeouts::RENAME_OPERATION_TIMEOUT;
        
        if let Ok(mut tracker) = rename_tracker.lock() {
            let now = Instant::now();
            let mut to_remove = Vec::new();
            
            for (cookie, rename_op) in tracker.iter() {
                if now.duration_since(rename_op.timestamp) > RENAME_TIMEOUT {
                    debug!("Cleaning up orphaned rename operation: cookie {} from {}", 
                          cookie, rename_op.source_path.display());
                    to_remove.push(*cookie);
                }
            }
            
            for cookie in to_remove {
                tracker.remove(&cookie);
            }
        }
    }

    // Static version for use in spawn_blocking closure
    fn process_notify_event_static(
        event: &notify_debouncer_full::DebouncedEvent,
        config: &FilesystemConfig,
        watch_roots: &[PathBuf],
        event_factory: &EventFactory,
        rename_tracker: &Arc<Mutex<HashMap<u32, RenameOperation>>>,
    ) -> Option<RawEvent> {
        let path = event.paths.first()?;
        let path_str = path.to_string_lossy().to_string();

        // Validate path stays within watch roots
        let mut path_valid = false;
        for root in watch_roots {
            match sinex_core::validation::validate_path_within_root(&path_str, &root.to_string_lossy()) {
                Ok(_) => {
                    path_valid = true;
                    break;
                }
                Err(_) => continue,
            }
        }
        
        if !path_valid {
            error!("Path validation failed: path '{}' escapes all watch roots", path_str);
            return None;
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

        // Create event using EventFactory
        let raw_event = match event.kind {
            notify::EventKind::Create(create_kind) => {
                match create_kind {
                    notify::event::CreateKind::File | notify::event::CreateKind::Any => {
                        let metadata = std::fs::metadata(path).ok()?;
                        let mut builder = event_factory.filesystem()
                            .path(path.to_string_lossy())
                            .created()
                            .size(metadata.len());

                        #[cfg(unix)]
                        {
                            builder = builder.permissions(metadata.permissions().mode());
                        }

                        builder.build()
                    }
                    notify::event::CreateKind::Folder => {
                        let mut builder = event_factory.filesystem()
                            .path(path.to_string_lossy())
                            .created();

                        #[cfg(unix)]
                        {
                            if let Ok(metadata) = std::fs::metadata(path) {
                                builder = builder.permissions(metadata.permissions().mode());
                            }
                        }

                        builder.build()
                    }
                    notify::event::CreateKind::Other => {
                        if path.is_dir() {
                            let mut builder = event_factory.filesystem()
                                .path(path.to_string_lossy())
                                .created();

                            #[cfg(unix)]
                            {
                                if let Ok(metadata) = std::fs::metadata(path) {
                                    builder = builder.permissions(metadata.permissions().mode());
                                }
                            }

                            builder.build()
                        } else {
                            let metadata = std::fs::metadata(path).ok()?;
                            let mut builder = event_factory.filesystem()
                                .path(path.to_string_lossy())
                                .created()
                                .size(metadata.len());

                            #[cfg(unix)]
                            {
                                builder = builder.permissions(metadata.permissions().mode());
                            }

                            builder.build()
                        }
                    }
                }
            }
            notify::EventKind::Modify(modify_kind) => {
                match modify_kind {
                    notify::event::ModifyKind::Name(name_kind) => {
                        // Enhanced rename detection using inotify cookies
                        match name_kind {
                            notify::event::RenameMode::From => {
                                // File being renamed FROM this path
                                // Note: Cookie handling simplified for newer notify versions
                                let cookie_u32 = 0u32; // Default cookie value
                                let rename_op = RenameOperation {
                                    source_path: path.to_path_buf(),
                                    timestamp: Instant::now(),
                                    cookie: Some(cookie_u32),
                                };
                                
                                if let Ok(mut tracker) = rename_tracker.lock() {
                                    tracker.insert(cookie_u32, rename_op);
                                    debug!("Tracked rename FROM: {} with cookie {}", path.display(), cookie_u32);
                                }
                                
                                // Don't emit event yet - wait for the TO event
                                return None;
                            }
                            notify::event::RenameMode::To => {
                                // File being renamed TO this path
                                // Note: Cookie handling simplified for newer notify versions
                                let cookie_u32 = 0u32; // Default cookie value
                                        
                                // Look for matching FROM operation
                                if let Ok(mut tracker) = rename_tracker.lock() {
                                    if let Some(rename_op) = tracker.remove(&cookie_u32) {
                                        debug!("Completed rename: {} -> {} with cookie {}", 
                                              rename_op.source_path.display(), 
                                              path.display(), 
                                              cookie_u32);
                                        
                                        // Emit proper move event with both paths
                                        return Some(event_factory.filesystem()
                                            .path(path.to_string_lossy())
                                            .moved_from(rename_op.source_path.to_string_lossy().to_string())
                                            .build());
                                    }
                                }
                                
                                // Fallback: treat as create if no matching FROM found
                                let metadata = std::fs::metadata(path).ok()?;
                                let mut builder = event_factory.filesystem()
                                    .path(path.to_string_lossy())
                                    .created()
                                    .size(metadata.len());

                                #[cfg(unix)]
                                {
                                    builder = builder.permissions(metadata.permissions().mode());
                                }

                                builder.build()
                            }
                            _ => {
                                // Other rename operations - treat as generic move
                                event_factory.filesystem()
                                    .path(path.to_string_lossy())
                                    .moved_from("unknown")
                                    .build()
                            }
                        }
                    }
                    _ => {
                        // Regular modification
                        if let Ok(metadata) = std::fs::metadata(path) {
                            event_factory.filesystem()
                                .path(path.to_string_lossy())
                                .modified()
                                .size(metadata.len())
                                .build()
                        } else {
                            event_factory.filesystem()
                                .path(path.to_string_lossy())
                                .modified()
                                .build()
                        }
                    }
                }
            }
            notify::EventKind::Remove(_remove_kind) => {
                // For all remove types, create a deleted event
                event_factory.filesystem()
                    .path(path.to_string_lossy())
                    .deleted()
                    .build()
            }
            _ => return None,
        };

        Some(raw_event)
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
        self.watch_roots.clear();

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
                
                // Add to watch roots for validation
                if let Ok(canonical) = base_path.canonicalize() {
                    self.watch_roots.push(canonical);
                }
            }
        }

        // Process events in a separate task
        let config = self.config.clone();
        let event_tx = tx.clone();
        let watch_roots = self.watch_roots.clone();
        let event_factory = EventFactory::new(sources::FS);
        let rename_tracker = self.rename_tracker.clone();

        tokio::task::spawn_blocking(move || {
            for result in notify_rx {
                match result {
                    Ok(events) => {
                        for event in events {
                            if let Some(raw_event) = Self::process_notify_event_static(&event, &config, &watch_roots, &event_factory, &rename_tracker) {
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

        // Start rename cleanup task to handle orphaned rename operations
        let cleanup_tracker = self.rename_tracker.clone();
        tokio::task::spawn(async move {
            let mut cleanup_interval = tokio::time::interval(filesystem::CLEANUP_INTERVAL);
            loop {
                cleanup_interval.tick().await;
                Self::cleanup_old_rename_operations(&cleanup_tracker);
            }
        });

        // Keep the watcher alive
        loop {
            tokio::time::sleep(filesystem::WATCHER_KEEPALIVE_INTERVAL).await;
        }
    }
}
