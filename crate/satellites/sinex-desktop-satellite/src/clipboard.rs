//! Advanced clipboard watcher with rich metadata
//!
//! Monitors clipboard changes and text selection events with:
//! - BLAKE3 content hashing for deduplication
//! - Source application detection
//! - Window title capture
//! - File path extraction and URL detection
//! - Blob storage for large content
//! - Linux primary selection support
//!
//! ## Implementation Notes
//!
//! Currently uses polling approach with `copypasta` crate. The event-driven approach
//! documented in TIM-ClipboardMonitoring would be more efficient:
//! - Wayland: `wl-paste --watch` for event notifications (CPU <0.1%, ~95% less power)
//! - X11: XFIXES extension for selection change events
//!
//! ## Platform-Specific Clipboard Access
//!
//! ### Display Server Detection
//! - Check `WAYLAND_DISPLAY` env var for Wayland
//! - Check `DISPLAY` env var for X11
//! - Initialize appropriate backend based on detection
//!
//! ### MIME Type Handling
//! Current implementation analyzes content heuristically. Native clipboard APIs provide:
//! - Wayland: `wl-paste --list-types` for available MIME types
//! - X11: `TARGETS` atom request for available formats

use camino::Utf8PathBuf;
use chrono::Utc;
use copypasta::{ClipboardContext, ClipboardProvider};
use sinex_core::db::models::Event;
use sinex_satellite_sdk::annex::{AnnexConfig, BlobManager};
use sinex_satellite_sdk::SatelliteResult;
use sinex_core::types::Timestamp;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Rich clipboard content information
#[derive(Debug, Clone)]
struct ClipboardContent {
    text: String,
    hash: String,
    size_bytes: usize,
    content_type: String,
    text_preview: Option<String>,
    file_paths: Option<Vec<String>>,
    source_app: Option<String>,
    window_title: Option<String>,
    timestamp: Timestamp,
}

/// Clipboard history entry for deduplication
#[derive(Debug, Clone)]
struct ClipboardHistoryEntry {
    content_hash: String,
    _first_seen: Timestamp,
    last_seen: Timestamp,
    _content_type: String,
    copy_count: u32,
}

/// Advanced clipboard watcher with blob storage
pub struct ClipboardWatcher {
    poll_interval: Duration,
    last_content: Option<ClipboardContent>,
    last_primary_content: Option<ClipboardContent>,
    clipboard_history: VecDeque<ClipboardHistoryEntry>,
    max_preview_length: usize,
    max_content_size: usize,
    max_history_entries: usize,
    enable_primary_selection: bool,
    enable_history: bool,
    blob_manager: Option<BlobManager>,
}

impl ClipboardWatcher {
    /// Create new advanced clipboard watcher
    pub async fn new(poll_interval_secs: u64) -> SatelliteResult<Self> {
        let mut watcher = Self {
            poll_interval: Duration::from_secs(poll_interval_secs),
            last_content: None,
            last_primary_content: None,
            clipboard_history: VecDeque::new(),
            max_preview_length: 100,
            max_content_size: 10 * 1024 * 1024, // 10MB
            max_history_entries: 1000,
            enable_primary_selection: true,
            enable_history: true,
            blob_manager: None,
        };

        // Check for clipboard tools availability
        watcher.check_clipboard_tools().await?;

        // Initialize blob manager if database connection is available
        if let Some(blob_manager) = watcher.initialize_blob_manager().await {
            watcher.blob_manager = Some(blob_manager);
            info!("Blob manager initialized for large clipboard content");
        }

        info!(
            "Advanced clipboard watcher initialized with {}s polling interval",
            poll_interval_secs
        );
        Ok(watcher)
    }

    /// Check availability of clipboard tools
    async fn check_clipboard_tools(&self) -> SatelliteResult<()> {
        let wl_paste_available = Command::new("wl-paste")
            .arg("--version")
            .output()
            .await
            .map(|o| o.status.success())
            .unwrap_or(false);

        let xclip_available = Command::new("xclip")
            .arg("-version")
            .output()
            .await
            .map(|o| o.status.success())
            .unwrap_or(false);

        if !wl_paste_available && !xclip_available {
            return Err(sinex_satellite_sdk::SatelliteError::Processing(
                "Neither wl-clipboard nor xclip found. Install one for clipboard monitoring"
                    .to_string(),
            ));
        }

        info!(
            "Clipboard tools available - wl-paste: {}, xclip: {}",
            wl_paste_available, xclip_available
        );
        Ok(())
    }

    /// Initialize blob manager for large content storage
    async fn initialize_blob_manager(&self) -> Option<BlobManager> {
        // Try to get database URL from environment
        let db_url = std::env::var("DATABASE_URL").ok()?;
        let annex_path = std::env::var("SINEX_ANNEX_PATH")
            .unwrap_or_else(|_| "/tmp/sinex-clipboard-annex".to_string());

        // Create database pool
        let pool = match sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&db_url)
            .await
        {
            Ok(pool) => pool,
            Err(e) => {
                warn!("Failed to connect to database for blob storage: {}", e);
                return None;
            }
        };

        // Setup annex configuration
        let annex_config = AnnexConfig {
            repo_path: Utf8PathBuf::from(annex_path),
            num_copies: Some(2),
            large_files: None,
        };

        match BlobManager::new(annex_config, pool) {
            Ok(manager) => Some(manager),
            Err(e) => {
                warn!("Failed to create blob manager: {}", e);
                None
            }
        }
    }

    /// Calculate content hash using BLAKE3
    fn calculate_hash(&self, content: &str) -> String {
        blake3::hash(content.as_bytes()).to_hex().to_string()
    }

    /// Analyze clipboard content to determine type and extract metadata
    fn analyze_content(&self, content: &str) -> (String, Option<String>, Option<Vec<String>>) {
        // Detect if it's a file path/URI list
        if content.starts_with("file://")
            || (content.lines().all(|l| l.starts_with('/') || l.is_empty())
                && content.lines().count() > 0)
        {
            let file_paths = self.extract_file_paths(content);
            ("files".to_string(), None, file_paths)
        }
        // Detect if it's an image (base64 or binary)
        else if content.len() > 100 && content.chars().all(|c| c.is_ascii_graphic()) {
            ("image".to_string(), None, None)
        }
        // Detect URLs
        else if content.starts_with("http://") || content.starts_with("https://") {
            let preview = Some(content.chars().take(self.max_preview_length).collect());
            ("url".to_string(), preview, None)
        }
        // Default to text
        else {
            let preview = if content.len() > self.max_preview_length {
                Some(format!(
                    "{}...",
                    content
                        .chars()
                        .take(self.max_preview_length)
                        .collect::<String>()
                ))
            } else {
                Some(content.to_string())
            };
            ("text".to_string(), preview, None)
        }
    }

    /// Extract file paths from clipboard content
    fn extract_file_paths(&self, content: &str) -> Option<Vec<String>> {
        if content.starts_with("file://") {
            Some(
                content
                    .lines()
                    .filter_map(|line| {
                        line.strip_prefix("file://")
                            .and_then(|p| urlencoding::decode(p).ok())
                            .map(|p| {
                                // Sanitize the path components
                                let path_str = p.to_string();
                                let path = std::path::Path::new(&path_str);
                                if let Some(filename) = path.file_name().and_then(|f| f.to_str()) {
                                    // Use sanitization but fall back to original if it fails
                                    let sanitized_name =
                                        sinex_types::sanitize_filename_component(filename)
                                            .unwrap_or_else(|_| filename.to_string());
                                    path.parent()
                                        .map(|parent| {
                                            parent
                                                .join(&sanitized_name)
                                                .to_string_lossy()
                                                .to_string()
                                        })
                                        .unwrap_or_else(|| sanitized_name)
                                } else {
                                    path_str
                                }
                            })
                    })
                    .collect(),
            )
        } else if content.lines().all(|l| l.starts_with('/') || l.is_empty()) {
            Some(
                content
                    .lines()
                    .filter(|l| !l.is_empty())
                    .map(|l| {
                        // Sanitize the path components
                        let path = std::path::Path::new(l);
                        if let Some(filename) = path.file_name().and_then(|f| f.to_str()) {
                            // Use sanitization but fall back to original if it fails
                            let sanitized_name = sinex_types::sanitize_filename_component(filename)
                                .unwrap_or_else(|_| filename.to_string());
                            path.parent()
                                .map(|parent| {
                                    parent.join(&sanitized_name).to_string_lossy().to_string()
                                })
                                .unwrap_or_else(|| sanitized_name)
                        } else {
                            l.to_string()
                        }
                    })
                    .collect(),
            )
        } else {
            None
        }
    }

    /// Get active window application name
    async fn get_active_window_app(&self) -> Option<String> {
        // Try Hyprland first
        if let Ok(output) = Command::new("hyprctl")
            .args(["activewindow", "-j"])
            .output()
            .await
        {
            if output.status.success() {
                if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&output.stdout) {
                    return json
                        .get("class")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                }
            }
        }

        // Try xdotool for X11
        if let Ok(output) = Command::new("xdotool")
            .args(["getactivewindow", "getwindowclassname"])
            .output()
            .await
        {
            if output.status.success() {
                return Some(String::from_utf8_lossy(&output.stdout).trim().to_string());
            }
        }

        None
    }

    /// Get active window title
    async fn get_active_window_title(&self) -> Option<String> {
        // Try Hyprland first
        if let Ok(output) = Command::new("hyprctl")
            .args(["activewindow", "-j"])
            .output()
            .await
        {
            if output.status.success() {
                if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&output.stdout) {
                    return json
                        .get("title")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                }
            }
        }

        // Try xdotool for X11
        if let Ok(output) = Command::new("xdotool")
            .args(["getactivewindow", "getwindowname"])
            .output()
            .await
        {
            if output.status.success() {
                return Some(String::from_utf8_lossy(&output.stdout).trim().to_string());
            }
        }

        None
    }

    /// Find original hash for deduplication
    fn find_original_hash(&self, content_hash: &str) -> Option<String> {
        self.clipboard_history
            .iter()
            .find(|e| e.content_hash == content_hash)
            .map(|e| e.content_hash.clone())
    }

    /// Update clipboard history with new entry
    fn update_history(&mut self, content_hash: String, content_type: String) {
        if !self.enable_history {
            return;
        }

        let now = Utc::now();

        // Check if already in history
        if let Some(entry) = self
            .clipboard_history
            .iter_mut()
            .find(|e| e.content_hash == content_hash)
        {
            // Update existing entry
            entry.last_seen = now;
            entry.copy_count += 1;
        } else {
            // Add new entry
            self.clipboard_history.push_back(ClipboardHistoryEntry {
                content_hash,
                _first_seen: now,
                last_seen: now,
                _content_type: content_type,
                copy_count: 1,
            });

            // Trim history if needed
            if self.clipboard_history.len() > self.max_history_entries {
                self.clipboard_history.pop_front();
            }
        }
    }

    /// Store large content using blob manager
    async fn store_large_content(
        &self,
        content: &str,
        _content_hash: &str,
    ) -> Result<(String, Option<String>), String> {
        let blob_manager = self
            .blob_manager
            .as_ref()
            .ok_or_else(|| "BlobManager not configured for large content storage".to_string())?;

        // Use BlobManager to ingest content directly from bytes
        let metadata = blob_manager
            .ingest_from_bytes(content.as_bytes(), "clipboard_content", "text/plain")
            .await
            .map_err(|e| format!("Failed to ingest clipboard content: {}", e))?;

        debug!(
            "Stored clipboard content via BlobManager: {:?} ({})",
            metadata.id, metadata.annex_key
        );

        Ok((metadata.annex_key, metadata.id.map(|id| id.to_string())))
    }

    /// Get enriched clipboard content with metadata
    async fn get_clipboard_content(&self) -> Option<ClipboardContent> {
        // Try to get content via external tools first for better compatibility
        let text = self
            .get_clipboard_content_external("clipboard")
            .await
            .or_else(|| self.get_clipboard_content_fallback());

        if let Some(text) = text {
            if text.is_empty() {
                return None;
            }

            let hash = self.calculate_hash(&text);
            let size_bytes = text.len();
            let (content_type, text_preview, file_paths) = self.analyze_content(&text);
            let source_app = self.get_active_window_app().await;
            let window_title = self.get_active_window_title().await;
            let timestamp = Utc::now();

            Some(ClipboardContent {
                text,
                hash,
                size_bytes,
                content_type,
                text_preview,
                file_paths,
                source_app,
                window_title,
                timestamp,
            })
        } else {
            None
        }
    }

    /// Get clipboard content using external tools
    async fn get_clipboard_content_external(&self, selection: &str) -> Option<String> {
        // Try Wayland first
        let wl_selection = match selection {
            "clipboard" => "",
            "primary" => "-p",
            _ => return None,
        };

        let wl_result = Command::new("wl-paste")
            .args(if wl_selection.is_empty() {
                vec![]
            } else {
                vec![wl_selection]
            })
            .arg("--no-newline")
            .output()
            .await;

        if let Ok(output) = wl_result {
            if output.status.success() {
                return Some(String::from_utf8_lossy(&output.stdout).to_string());
            }
        }

        // Fall back to X11
        let x_selection = match selection {
            "clipboard" => "-selection clipboard",
            "primary" => "-selection primary",
            _ => return None,
        };

        let x_result = Command::new("xclip")
            .arg("-o")
            .args(x_selection.split_whitespace())
            .output()
            .await;

        if let Ok(output) = x_result {
            if output.status.success() {
                return Some(String::from_utf8_lossy(&output.stdout).to_string());
            }
        }

        None
    }

    /// Fallback to copypasta for clipboard access
    fn get_clipboard_content_fallback(&self) -> Option<String> {
        match ClipboardContext::new() {
            Ok(mut ctx) => match ctx.get_contents() {
                Ok(text) => Some(text),
                Err(e) => {
                    debug!("Failed to get clipboard contents via copypasta: {}", e);
                    None
                }
            },
            Err(e) => {
                warn!("Failed to create clipboard context: {}", e);
                None
            }
        }
    }

    /// Get current primary selection content (Linux)
    async fn get_primary_selection_content(&self) -> Option<ClipboardContent> {
        if !self.enable_primary_selection {
            return None;
        }

        let text = self.get_clipboard_content_external("primary").await?;

        if text.is_empty() {
            return None;
        }

        let hash = self.calculate_hash(&text);
        let size_bytes = text.len();
        let (content_type, text_preview, file_paths) = self.analyze_content(&text);
        let source_app = self.get_active_window_app().await;
        let window_title = self.get_active_window_title().await;
        let timestamp = Utc::now();

        Some(ClipboardContent {
            text,
            hash,
            size_bytes,
            content_type,
            text_preview,
            file_paths,
            source_app,
            window_title,
            timestamp,
        })
    }

    /// Create rich clipboard changed event
    async fn create_clipboard_event(
        &self,
        content: &ClipboardContent,
        operation: &str,
    ) -> Result<Event, sinex_types::error::SinexError> {
        // Check if this is a re-copy
        let original_hash = if self.enable_history {
            self.find_original_hash(&content.hash)
        } else {
            None
        };

        // Handle large content with blob storage
        let (text_preview, annex_key, blob_id) = if content.size_bytes > self.max_content_size {
            match self.store_large_content(&content.text, &content.hash).await {
                Ok((key, id)) => {
                    info!(
                        "Stored large clipboard content ({} bytes) in blob storage: {}",
                        content.size_bytes, key
                    );
                    (
                        Some("[Content stored in blob storage]".to_string()),
                        Some(key),
                        id,
                    )
                }
                Err(e) => {
                    error!("Failed to store large content in blob storage: {}", e);
                    (
                        Some("[Content too large - storage failed]".to_string()),
                        None,
                        None,
                    )
                }
            }
        } else {
            (content.text_preview.clone(), None, None)
        };

        let file_count = content.file_paths.as_ref().map(|paths| paths.len());

        let event = Event::from_payload(sinex_types::events::ClipboardCopiedPayload {
            operation: operation.to_string(),
            content_type: content.content_type.clone(),
            content_size: content.size_bytes,
            text_preview,
            file_count,
            file_paths: content.file_paths.clone(),
            source_app: content.source_app.clone(),
            window_title: content.window_title.clone(),
            content_hash: content.hash.clone(),
            original_hash,
            annex_key,
            blob_id,
        })
        .with_ts_orig(Some(content.timestamp));

        Ok(event)
    }

    /// Create rich primary selection event
    async fn create_primary_selection_event(
        &self,
        content: &ClipboardContent,
    ) -> Result<Event, sinex_types::error::SinexError> {
        // Check if this is a re-selection
        let original_hash = if self.enable_history {
            self.find_original_hash(&content.hash)
        } else {
            None
        };

        // Handle large content with blob storage
        let (text_preview, annex_key, blob_id) = if content.size_bytes > self.max_content_size {
            match self.store_large_content(&content.text, &content.hash).await {
                Ok((key, id)) => {
                    info!(
                        "Stored large primary selection content ({} bytes) in blob storage: {}",
                        content.size_bytes, key
                    );
                    (
                        Some("[Content stored in blob storage]".to_string()),
                        Some(key),
                        id,
                    )
                }
                Err(e) => {
                    error!(
                        "Failed to store large primary selection in blob storage: {}",
                        e
                    );
                    (
                        Some("[Content too large - storage failed]".to_string()),
                        None,
                        None,
                    )
                }
            }
        } else {
            (content.text_preview.clone(), None, None)
        };

        let event = Event::from_payload(sinex_types::events::ClipboardSelectedPayload {
            selection_type: "primary".to_string(),
            content_type: content.content_type.clone(),
            content_size: content.size_bytes,
            text_preview,
            source_app: content.source_app.clone(),
            content_hash: content.hash.clone(),
            original_hash,
            annex_key,
            blob_id,
        })
        .with_ts_orig(Some(content.timestamp));

        Ok(event)
    }

    /// Check for clipboard changes with enhanced monitoring
    async fn check_clipboard_changes(
        &mut self,
        tx: &mpsc::UnboundedSender<Event>,
    ) -> SatelliteResult<()> {
        // Check main clipboard
        if let Some(current_content) = self.get_clipboard_content().await {
            let content_changed = match &self.last_content {
                Some(last) => last.hash != current_content.hash,
                None => true,
            };

            if content_changed {
                debug!(
                    "Clipboard changed: {} bytes, hash: {}, type: {}, app: {:?}",
                    current_content.size_bytes,
                    &current_content.hash[..8],
                    current_content.content_type,
                    current_content.source_app
                );

                let event = self
                    .create_clipboard_event(&current_content, "copy")
                    .await?;

                if tx.send(event).is_err() {
                    warn!("Event channel closed");
                    return Ok(());
                }

                // Update history
                self.update_history(
                    current_content.hash.clone(),
                    current_content.content_type.clone(),
                );

                self.last_content = Some(current_content);
            }
        }

        // Check primary selection (Linux)
        if self.enable_primary_selection {
            if let Some(current_primary) = self.get_primary_selection_content().await {
                let primary_changed = match &self.last_primary_content {
                    Some(last) => last.hash != current_primary.hash,
                    None => true,
                };

                if primary_changed {
                    debug!(
                        "Primary selection changed: {} bytes, hash: {}, type: {}, app: {:?}",
                        current_primary.size_bytes,
                        &current_primary.hash[..8],
                        current_primary.content_type,
                        current_primary.source_app
                    );

                    let event = self
                        .create_primary_selection_event(&current_primary)
                        .await?;

                    if tx.send(event).is_err() {
                        warn!("Event channel closed");
                        return Ok(());
                    }

                    // Update history
                    self.update_history(
                        current_primary.hash.clone(),
                        current_primary.content_type.clone(),
                    );

                    self.last_primary_content = Some(current_primary);
                }
            }
        }

        Ok(())
    }

    /// Start streaming events
    pub async fn start_streaming(
        &mut self,
        tx: mpsc::UnboundedSender<Event>,
    ) -> SatelliteResult<()> {
        info!("Starting clipboard event streaming");

        let mut poll_interval = interval(self.poll_interval);

        loop {
            poll_interval.tick().await;

            if let Err(e) = self.check_clipboard_changes(&tx).await {
                error!("Error checking clipboard changes: {}", e);
                // Continue polling even if there's an error
            }
        }
    }
}
