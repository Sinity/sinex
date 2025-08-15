//! Clipboard watcher with sensd source material capture
//!
//! Monitors clipboard changes and text selection events, capturing them as source material
//! for later event creation with proper provenance tracking.
//!
//! ## Architecture
//!
//! This module follows the sensd pattern:
//! 1. **Source Material Capture**: Clipboard content → raw.source_material_registry
//! 2. **Temporal Ledger**: Precise timing → raw.temporal_ledger
//! 3. **Event Generation**: Material processing → events with Provenance::Material
//!
//! ## Features
//!
//! - BLAKE3 content hashing for deduplication
//! - Source application detection via window manager integration
//! - File path extraction and URL detection
//! - Support for both clipboard and primary selection
//! - Comprehensive metadata capture

// Use local facade for common types
use crate::common::*;

// Clipboard-specific imports
use copypasta::{ClipboardContext, ClipboardProvider};
use sinex_core::types::Ulid;
use sqlx::PgPool;

const DEFAULT_MAX_PREVIEW_LENGTH: usize = 100;
const DEFAULT_MAX_CONTENT_SIZE: usize = 10 * 1024 * 1024; // 10MB
const DEFAULT_MAX_HISTORY_ENTRIES: usize = 1000;

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

/// Clipboard watcher with sensd source material capture
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
    // sensd integration
    db_pool: Option<PgPool>,
    source_identifier: String,
}

impl ClipboardWatcher {
    /// Create new clipboard watcher with sensd integration
    pub async fn new(poll_interval_secs: u64, db_pool: Option<PgPool>) -> SatelliteResult<Self> {
        let mut watcher = Self {
            poll_interval: Duration::from_secs(poll_interval_secs),
            last_content: None,
            last_primary_content: None,
            clipboard_history: VecDeque::new(),
            max_preview_length: DEFAULT_MAX_PREVIEW_LENGTH,
            max_content_size: DEFAULT_MAX_CONTENT_SIZE,
            max_history_entries: DEFAULT_MAX_HISTORY_ENTRIES,
            enable_primary_selection: true,
            enable_history: true,
            db_pool,
            source_identifier: "desktop_clipboard".to_string(),
        };

        // Check for clipboard tools availability
        watcher.check_clipboard_tools().await?;

        info!(
            "Clipboard watcher initialized with {}s polling interval (sensd mode)",
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
            return Err(processing_error(
                "Neither wl-clipboard nor xclip found. Install one for clipboard monitoring",
            ));
        }

        info!(
            "Clipboard tools available - wl-paste: {}, xclip: {}",
            wl_paste_available, xclip_available
        );
        Ok(())
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
        path_utils::extract_file_paths(content)
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
    fn find_original_hash(&self, content_hash: &str) -> Option<&str> {
        if self
            .clipboard_history
            .iter()
            .any(|e| e.content_hash == content_hash)
        {
            Some(content_hash)
        } else {
            None
        }
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

    /// Store clipboard content as source material in sensd
    async fn store_clipboard_source_material(
        &self,
        content: &ClipboardContent,
        selection_type: &str,
    ) -> SatelliteResult<Option<Ulid>> {
        let Some(db_pool) = &self.db_pool else {
            warn!("No database pool available for source material storage");
            return Ok(None);
        };

        let material_id = Ulid::new();
        let now = Utc::now();

        // Prepare metadata
        let metadata = serde_json::json!({
            "selection_type": selection_type,
            "content_type": content.content_type,
            "content_size": content.size_bytes,
            "text_preview": content.text_preview,
            "file_paths": content.file_paths,
            "source_app": content.source_app,
            "window_title": content.window_title,
            "content_hash": content.hash,
            "original_hash": self.find_original_hash(&content.hash).map(|h| h.to_string()),
        });

        // Store in source_material_registry
        let data_bytes = content.text.as_bytes();
        if data_bytes.len() <= self.max_content_size {
            // Store inline
            sqlx::query!(
                r#"
                INSERT INTO raw.source_material_registry (
                    source_material_id, source_identifier, created_at,
                    data, total_bytes, content_type, metadata,
                    source_type, status, material_type, source_uri
                )
                VALUES ($1::ulid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                "#,
                material_id as Ulid,     // $1 - source_material_id
                self.source_identifier,  // $2 - source_identifier
                now,                     // $3 - created_at
                data_bytes,              // $4 - data
                data_bytes.len() as i64, // $5 - total_bytes
                "text/plain",            // $6 - content_type
                serde_json::to_string(&metadata).unwrap_or_else(|_| "{}".to_string()), // $7 - metadata
                "clipboard",    // $8 - source_type
                "finalized",    // $9 - status
                "clipboard",    // $10 - material_type
                "clipboard://", // $11 - source_uri
            )
            .execute(db_pool)
            .await?;
        } else {
            // TODO: Large content would need blob storage
            warn!("Large clipboard content not yet supported, skipping");
            return Ok(None);
        }

        // Create temporal ledger entry
        sqlx::query!(
            r#"
            INSERT INTO raw.temporal_ledger (
                entry_id, material_id, offset_start, offset_end, 
                offset_kind, ts_capture, precision, clock, source_type,
                proximity_hint, note
            )
            VALUES (gen_ulid()::ulid, $1::ulid, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
            material_id as Ulid,     // $1 - material_id
            0i64,                    // $2 - offset_start
            data_bytes.len() as i64, // $3 - offset_end
            "byte",                  // $4 - offset_kind
            content.timestamp,       // $5 - ts_capture
            "millisecond",           // $6 - precision
            "wall",                  // $7 - clock
            "realtime_capture",      // $8 - source_type
            serde_json::json!({}),   // $9 - proximity_hint
            metadata.to_string(),    // $10 - note
        )
        .execute(db_pool)
        .await?;

        info!(
            "Stored clipboard {} source material: {} bytes, hash: {}",
            selection_type,
            content.size_bytes,
            &content.hash[..8]
        );

        Ok(Some(material_id))
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

    /// Check for clipboard changes and store as source material
    async fn check_clipboard_changes(&mut self) -> SatelliteResult<()> {
        self.check_main_clipboard().await?;
        if self.enable_primary_selection {
            self.check_primary_selection().await?;
        }
        Ok(())
    }

    /// Start monitoring clipboard changes (sensd mode)
    pub async fn start_monitoring(&mut self) -> SatelliteResult<()> {
        info!("Starting clipboard monitoring (sensd mode)");

        let mut poll_interval = interval(self.poll_interval);

        loop {
            poll_interval.tick().await;

            if let Err(e) = self.check_clipboard_changes().await {
                error!("Error checking clipboard changes: {}", e);
                // Continue polling even if there's an error
            }
        }
    }

    /// Check main clipboard for changes
    async fn check_main_clipboard(&mut self) -> SatelliteResult<()> {
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

                // Store as source material (not event!)
                self.store_clipboard_source_material(&current_content, "clipboard")
                    .await?;

                // Update history
                self.update_history(
                    current_content.hash.clone(),
                    current_content.content_type.clone(),
                );

                self.last_content = Some(current_content);
            }
        }
        Ok(())
    }

    /// Check primary selection for changes
    async fn check_primary_selection(&mut self) -> SatelliteResult<()> {
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

                // Store as source material (not event!)
                self.store_clipboard_source_material(&current_primary, "primary")
                    .await?;

                // Update history
                self.update_history(
                    current_primary.hash.clone(),
                    current_primary.content_type.clone(),
                );

                self.last_primary_content = Some(current_primary);
            }
        }
        Ok(())
    }
}
