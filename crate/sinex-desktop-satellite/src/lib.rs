//! Unified Desktop Satellite
//!
//! Coordinates multiple desktop event sources:
//! - Clipboard events (copy/cut/paste)
//! - Window manager events (Hyprland focus, movement, workspaces)

use async_trait::async_trait;
use sinex_satellite_sdk::{EventSource, EventSourceContext, SatelliteResult, SatelliteError};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

mod clipboard;
mod window_manager;

pub use clipboard::ClipboardWatcher;
pub use window_manager::WindowManagerWatcher;

// Re-export for convenience
pub use sinex_core::RawEvent;

/// Configuration for desktop satellite
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DesktopConfig {
    /// Enable clipboard monitoring
    pub clipboard_enabled: bool,
    /// Enable window manager monitoring  
    pub window_manager_enabled: bool,
    /// Window manager type (currently only "hyprland")
    pub window_manager_type: String,
    /// Clipboard monitoring interval (seconds)
    pub clipboard_poll_interval_secs: u64,
}

impl Default for DesktopConfig {
    fn default() -> Self {
        Self {
            clipboard_enabled: true,
            window_manager_enabled: true,
            window_manager_type: "hyprland".to_string(),
            clipboard_poll_interval_secs: 2,
        }
    }
}

/// Error types for desktop satellite
#[derive(Debug, thiserror::Error)]
pub enum DesktopSatelliteError {
    #[error("Event source error: {0}")]
    EventSource(String),
    
    #[error("Configuration error: {0}")]
    Configuration(String),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

impl From<DesktopSatelliteError> for sinex_satellite_sdk::SatelliteError {
    fn from(err: DesktopSatelliteError) -> Self {
        sinex_satellite_sdk::SatelliteError::EventSource(err.to_string())
    }
}

/// Unified desktop satellite
pub struct DesktopSatellite {
    context: Option<EventSourceContext>,
    config: DesktopConfig,
    clipboard_watcher: Option<ClipboardWatcher>,
    window_manager_watcher: Option<WindowManagerWatcher>,
}

impl DesktopSatellite {
    /// Create new desktop satellite
    pub fn new() -> Self {
        Self {
            context: None,
            config: DesktopConfig::default(),
            clipboard_watcher: None,
            window_manager_watcher: None,
        }
    }

    /// Create with specific configuration
    pub fn with_config(config: DesktopConfig) -> Self {
        Self {
            context: None,
            config,
            clipboard_watcher: None,
            window_manager_watcher: None,
        }
    }
}

#[async_trait]
impl EventSource for DesktopSatellite {
    fn source_name(&self) -> &str {
        "desktop"
    }

    async fn initialize(&mut self, context: EventSourceContext) -> SatelliteResult<()> {
        info!("Initializing desktop satellite");

        // Store context for later use
        self.context = Some(context);

        // Parse configuration from context if available
        if let Ok(config_str) = std::env::var("SINEX_DESKTOP_CONFIG") {
            if let Ok(config) = serde_json::from_str::<DesktopConfig>(&config_str) {
                self.config = config;
            }
        }

        // Initialize clipboard watcher if enabled
        if self.config.clipboard_enabled {
            match ClipboardWatcher::new(self.config.clipboard_poll_interval_secs).await {
                Ok(watcher) => {
                    self.clipboard_watcher = Some(watcher);
                    info!("Clipboard watcher initialized");
                }
                Err(e) => {
                    error!("Failed to initialize clipboard watcher: {}", e);
                    return Err(SatelliteError::EventSource(format!(
                        "Failed to initialize clipboard watcher: {}", e
                    )));
                }
            }
        }

        // Initialize window manager watcher if enabled
        if self.config.window_manager_enabled {
            match WindowManagerWatcher::new(self.config.window_manager_type.clone()).await {
                Ok(watcher) => {
                    self.window_manager_watcher = Some(watcher);
                    info!("Window manager watcher initialized");
                }
                Err(e) => {
                    error!("Failed to initialize window manager watcher: {}", e);
                    return Err(SatelliteError::EventSource(format!(
                        "Failed to initialize window manager watcher: {}", e
                    )));
                }
            }
        }

        info!("Desktop satellite initialization completed");
        Ok(())
    }

    async fn start_streaming(&mut self) -> SatelliteResult<()> {
        info!("Starting desktop event streaming");

        let mut tasks: Vec<JoinHandle<SatelliteResult<()>>> = Vec::new();

        // Get event sender from context
        let context = self.context.as_ref().ok_or_else(|| {
            SatelliteError::Lifecycle("EventSource not initialized".to_string())
        })?;
        let tx = context.event_sender.clone();

        // Start clipboard watcher
        if let Some(mut clipboard_watcher) = self.clipboard_watcher.take() {
            let tx_clipboard = tx.clone();
            let handle = tokio::spawn(async move {
                clipboard_watcher.start_streaming(tx_clipboard).await
            });
            tasks.push(handle);
        }

        // Start window manager watcher
        if let Some(mut window_manager_watcher) = self.window_manager_watcher.take() {
            let tx_wm = tx.clone();
            let handle = tokio::spawn(async move {
                window_manager_watcher.start_streaming(tx_wm).await
            });
            tasks.push(handle);
        }

        if tasks.is_empty() {
            warn!("No desktop watchers enabled, satellite will not produce events");
            // Keep the satellite running but idle
            tokio::time::sleep(tokio::time::Duration::from_secs(u64::MAX)).await;
            return Ok(());
        }

        // Wait for any task to complete (or fail)
        let (_result, _index, _remaining) = futures::future::select_all(tasks).await;

        // If we get here, one of the watchers has stopped
        error!("Desktop watcher stopped unexpectedly");
        Ok(())
    }

    async fn shutdown(&mut self) -> SatelliteResult<()> {
        info!("Shutting down desktop satellite");
        
        // Watchers will be dropped when the satellite is dropped
        // No explicit cleanup needed for now
        
        Ok(())
    }
}

impl Default for DesktopSatellite {
    fn default() -> Self {
        Self::new()
    }
}