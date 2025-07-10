/// Typed filesystem monitor - uses strongly typed events
use async_trait::async_trait;
use notify::Watcher;
use std::path::PathBuf;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::{debug, error, info};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use sinex_core::{
    sources, EventSourceContext, Result,
    strongly_typed_events::{
        TypedEventSender, EventEnvelope, TypedFilesystemEventBuilder,
        EnforcedTypedEventSource,
    },
    filesystem, timeouts,
};

use crate::filesystem::{FilesystemConfig, RenameOperation};

/// Typed filesystem monitor using strongly typed events
pub struct TypedFilesystemMonitor {
    config: FilesystemConfig,
    watch_roots: Vec<PathBuf>,
    rename_tracker: Arc<Mutex<HashMap<u32, RenameOperation>>>,
}

#[async_trait]
impl EnforcedTypedEventSource for TypedFilesystemMonitor {
    type Config = FilesystemConfig;
    const SOURCE_NAME: &'static str = sources::FS;

    async fn initialize(ctx: EventSourceContext) -> Result<Self> {
        let config: FilesystemConfig = serde_json::from_value(ctx.config.clone())
            .map_err(|e| sinex_core::CoreError::Configuration(format!("Failed to parse config: {}", e)))?;
        
        info!(
            patterns = ?config.watch_patterns,
            "Initializing typed filesystem monitor"
        );

        Ok(Self {
            config,
            watch_roots: Vec::new(),
            rename_tracker: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    async fn stream_typed_events(&mut self, tx: TypedEventSender) -> Result<()> {
        info!(
            patterns = ?self.config.watch_patterns,
            ignore = ?self.config.ignore_patterns,
            debounce_ms = self.config.debounce_ms,
            "Starting typed filesystem event stream"
        );

        // Set up filesystem watcher with debouncing
        let (notify_tx, notify_rx) = std::sync::mpsc::channel();
        let mut debouncer = notify_debouncer_full::new_debouncer(
            std::time::Duration::from_millis(self.config.debounce_ms),
            None,
            notify_tx,
        )
        .map_err(|e| {
            sinex_core::CoreError::Other(format!("Failed to create debouncer: {}", e))
        })?;

        // Watch all matching paths
        let mut watched_paths = std::collections::HashSet::new();
        self.watch_roots.clear();

        for pattern in &self.config.watch_patterns {
            let expanded = shellexpand::tilde(pattern);
            let base_path_str = if expanded.contains("**") {
                expanded
                    .split("**")
                    .next()
                    .unwrap_or(&expanded)
                    .trim_end_matches('/')
                    .to_string()
            } else if expanded.contains('*') {
                std::path::Path::new(expanded.as_ref())
                    .parent()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_else(|| expanded.to_string())
            } else {
                expanded.to_string()
            };

            let base_path = std::path::Path::new(&base_path_str);

            if !base_path.exists() {
                info!("Creating directory: {}", base_path.display());
                std::fs::create_dir_all(base_path).map_err(|e| {
                    sinex_core::CoreError::Io(format!("Failed to create directory {}: {}", base_path.display(), e))
                })?;
            }

            if base_path.exists() && !watched_paths.contains(base_path) {
                info!("Watching directory: {}", base_path.display());
                debouncer
                    .watcher()
                    .watch(base_path, notify::RecursiveMode::Recursive)
                    .map_err(|e| {
                        sinex_core::CoreError::Io(format!("Failed to watch path {}: {}", base_path.display(), e))
                    })?;
                watched_paths.insert(base_path.to_path_buf());
                
                if let Ok(canonical) = base_path.canonicalize() {
                    self.watch_roots.push(canonical);
                }
            }
        }

        // Process events in a separate task
        let config = self.config.clone();
        let event_tx = tx.clone();
        let watch_roots = self.watch_roots.clone();
        let rename_tracker = self.rename_tracker.clone();

        tokio::task::spawn_blocking(move || {
            for result in notify_rx {
                match result {
                    Ok(events) => {
                        for event in events {
                            if let Some(envelope) = Self::process_notify_event_typed(
                                &event,
                                &config,
                                &watch_roots,
                                &rename_tracker,
                            ) {
                                if let Err(e) = event_tx.send(envelope) {
                                    error!("Failed to send typed event: {}", e);
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

        // Start rename cleanup task
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

impl TypedFilesystemMonitor {
    fn process_notify_event_typed(
        event: &notify_debouncer_full::DebouncedEvent,
        config: &FilesystemConfig,
        watch_roots: &[PathBuf],
        rename_tracker: &Arc<Mutex<HashMap<u32, RenameOperation>>>,
    ) -> Option<EventEnvelope> {
        let paths = &event.paths;
        if paths.is_empty() {
            return None;
        }

        let path = &paths[0];

        // Validate path is within watch roots
        let within_roots = watch_roots.iter().any(|root| {
            path.starts_with(root)
        });

        if !within_roots {
            debug!("Path outside watch roots: {:?}", path);
            return None;
        }

        // Check ignore patterns
        for pattern in &config.ignore_patterns {
            let glob_pattern = match glob::Pattern::new(pattern) {
                Ok(p) => p,
                Err(e) => {
                    error!("Invalid glob pattern '{}': {}", pattern, e);
                    continue;
                }
            };

            let path_str = path.to_string_lossy();
            let matches = if pattern.contains('/') {
                if pattern.starts_with("**/") {
                    let normalized_path = path_str.replace('\\', "/");
                    let path_components: Vec<&str> = normalized_path.split('/').collect();
                    let mut found_match = false;
                    for i in 0..path_components.len() {
                        let suffix = path_components[i..].join("/");
                        if glob_pattern.matches(&suffix) {
                            found_match = true;
                            break;
                        }
                    }
                    found_match
                } else {
                    glob_pattern.matches(&path_str)
                }
            } else {
                if let Some(filename) = path.file_name().and_then(|f| f.to_str()) {
                    glob_pattern.matches(filename)
                } else {
                    false
                }
            };

            if matches {
                debug!("Ignoring path due to pattern: {} (pattern: {})", path_str, pattern);
                return None;
            }
        }

        // Create typed event using TypedFilesystemEventBuilder
        let builder = TypedFilesystemEventBuilder::new(sources::FS);
        
        match event.kind {
            notify::EventKind::Create(create_kind) => {
                match create_kind {
                    notify::event::CreateKind::File | notify::event::CreateKind::Any => {
                        if let Ok(metadata) = std::fs::metadata(path) {
                            let permissions = {
                                #[cfg(unix)]
                                { Some(metadata.permissions().mode()) }
                                #[cfg(not(unix))]
                                { None }
                            };
                            
                            Some(builder.file_created(
                                path.to_string_lossy(),
                                metadata.len(),
                                permissions
                            ))
                        } else {
                            None
                        }
                    }
                    notify::event::CreateKind::Folder => {
                        let permissions = {
                            #[cfg(unix)]
                            {
                                std::fs::metadata(path)
                                    .ok()
                                    .map(|m| m.permissions().mode())
                            }
                            #[cfg(not(unix))]
                            { None }
                        };
                        
                        Some(builder.dir_created(path.to_string_lossy(), permissions))
                    }
                    notify::event::CreateKind::Other => {
                        if path.is_dir() {
                            let permissions = {
                                #[cfg(unix)]
                                {
                                    std::fs::metadata(path)
                                        .ok()
                                        .map(|m| m.permissions().mode())
                                }
                                #[cfg(not(unix))]
                                { None }
                            };
                            
                            Some(builder.dir_created(path.to_string_lossy(), permissions))
                        } else if let Ok(metadata) = std::fs::metadata(path) {
                            let permissions = {
                                #[cfg(unix)]
                                { Some(metadata.permissions().mode()) }
                                #[cfg(not(unix))]
                                { None }
                            };
                            
                            Some(builder.file_created(
                                path.to_string_lossy(),
                                metadata.len(),
                                permissions
                            ))
                        } else {
                            None
                        }
                    }
                }
            }
            notify::EventKind::Modify(modify_kind) => {
                match modify_kind {
                    notify::event::ModifyKind::Name(name_kind) => {
                        match name_kind {
                            notify::event::RenameMode::From => {
                                let cookie_u32 = 0u32; // Simplified for newer notify versions
                                let rename_op = RenameOperation {
                                    source_path: path.to_path_buf(),
                                    timestamp: Instant::now(),
                                    cookie: Some(cookie_u32),
                                };
                                
                                if let Ok(mut tracker) = rename_tracker.lock() {
                                    tracker.insert(cookie_u32, rename_op);
                                    debug!("Tracked rename FROM: {} with cookie {}", path.display(), cookie_u32);
                                }
                                
                                None
                            }
                            notify::event::RenameMode::To => {
                                let cookie_u32 = 0u32; // Simplified for newer notify versions
                                
                                let old_path = if let Ok(mut tracker) = rename_tracker.lock() {
                                    tracker.remove(&cookie_u32).map(|op| op.source_path)
                                } else {
                                    None
                                };
                                
                                Some(builder.file_moved(
                                    path.to_string_lossy(),
                                    old_path.map(|p| p.to_string_lossy().to_string())
                                ))
                            }
                            _ => None,
                        }
                    }
                    _ => {
                        if path.is_file() {
                            if let Ok(metadata) = std::fs::metadata(path) {
                                Some(builder.file_modified(
                                    path.to_string_lossy(),
                                    metadata.len(),
                                    format!("{:?}", modify_kind)
                                ))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                }
            }
            notify::EventKind::Remove(_) => {
                if path.exists() && path.is_dir() {
                    Some(builder.dir_deleted(path.to_string_lossy()))
                } else {
                    Some(builder.file_deleted(path.to_string_lossy()))
                }
            }
            _ => None,
        }
    }

    fn cleanup_old_rename_operations(rename_tracker: &Arc<Mutex<HashMap<u32, RenameOperation>>>) {
        let mut tracker = rename_tracker.lock().unwrap();
        let now = Instant::now();
        tracker.retain(|_, op| now.duration_since(op.timestamp) < timeouts::RENAME_OPERATION_TIMEOUT);
    }
}