#![doc = include_str!("../docs/clipboard.md")]

// Use local facade for common types
use crate::common::*;
use sinex_primitives::events::payloads::{ClipboardCopiedPayload, ClipboardSelectedPayload};
use sinex_primitives::events::EventPayload;
use sinex_primitives::Seconds;
use sinex_primitives::{Id, Ulid};
use sinex_node_sdk::stage_as_you_go::StageAsYouGoContext;
use tokio::sync::watch;

// Clipboard-specific imports
use arboard::{Clipboard, GetExtLinux, LinuxClipboardKind};
use copypasta::{ClipboardContext, ClipboardProvider};

/// Maximum length of text preview included in clipboard events
///
/// Longer content will be truncated to this length with "..." appended.
/// The full content is always stored in source material.
const DEFAULT_MAX_PREVIEW_LENGTH: usize = 100;

/// Maximum clipboard content size (warning threshold)
///
/// Content larger than this will generate a warning but will still be processed
/// and stored. This is not a hard limit but a warning indicator for unusually
/// large clipboard contents (e.g., accidental copying of large files).
const DEFAULT_MAX_CONTENT_SIZE: usize = 10 * 1024 * 1024; // 10MB

/// Maximum number of entries in clipboard deduplication history
///
/// The history tracks content hashes to detect duplicates and enable deduplication.
/// When this limit is reached, the oldest entries are removed (FIFO).
const DEFAULT_MAX_HISTORY_ENTRIES: usize = 1000;

/// Timeout for window manager queries (hyprctl, xdotool)
///
/// When capturing clipboard events, we query the active window for context.
/// This timeout prevents hanging if the window manager is unresponsive.
const CLIPBOARD_COMMAND_TIMEOUT: Duration = Duration::from_secs(5);

/// Polling interval for clipboard monitoring
///
/// This is the delay between clipboard checks using the native arboard API.
/// 100ms provides responsive detection while being efficient. This value is
/// hardcoded rather than using the poll_interval_secs parameter to ensure
/// optimal performance with the native clipboard API.
const CLIPBOARD_POLL_INTERVAL: Duration = Duration::from_millis(100);

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
    _timestamp: Timestamp,
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

/// Clipboard watcher with Stage-as-You-Go capture
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
    // Stage-as-you-go context for JetStream capture
    stage_context: Option<StageAsYouGoContext>,
    shutdown_rx: watch::Receiver<bool>,
    source_identifier: String,
}

impl ClipboardWatcher {
    /// Create new clipboard watcher with Stage-as-You-Go integration
    pub async fn new(
        _poll_interval_secs: Seconds, // Reserved for future configurability
        stage_context: StageAsYouGoContext,
        shutdown_rx: watch::Receiver<bool>,
    ) -> NodeResult<Self> {
        // The poll_interval_secs parameter is reserved for future configurability
        let poll_interval = CLIPBOARD_POLL_INTERVAL;

        let watcher = Self {
            poll_interval,
            last_content: None,
            last_primary_content: None,
            clipboard_history: VecDeque::new(),
            max_preview_length: DEFAULT_MAX_PREVIEW_LENGTH,
            max_content_size: DEFAULT_MAX_CONTENT_SIZE,
            max_history_entries: DEFAULT_MAX_HISTORY_ENTRIES,
            enable_primary_selection: true, // PRIMARY selection via arboard GetExtLinux
            enable_history: true,
            stage_context: Some(stage_context),
            shutdown_rx,
            source_identifier: "desktop_clipboard".to_string(),
        };

        info!(
            "Clipboard watcher initialized with 100ms polling interval (stage-as-you-go mode, native arboard, PRIMARY selection enabled)"
        );
        Ok(watcher)
    }

    /// Calculate content hash using BLAKE3
    fn calculate_hash(&self, content: &str) -> String {
        blake3::hash(content.as_bytes()).to_hex().to_string()
    }

    /// Validate clipboard content for UTF-8 validity and detect binary data
    fn validate_clipboard_content(&self, text: &str) -> Option<String> {
        // Check for null bytes (indicator of binary data)
        if text.contains('\0') {
            debug!("Clipboard content contains null bytes, treating as binary");
            return None;
        }

        // Check for excessive control characters (potential binary)
        let control_char_count = text
            .chars()
            .filter(|c| c.is_control() && *c != '\n' && *c != '\r' && *c != '\t')
            .count();
        let total_chars = text.chars().count();

        if total_chars > 0 && (control_char_count as f64 / total_chars as f64) > 0.1 {
            debug!(
                "Clipboard content has {}% control characters, treating as binary",
                (control_char_count as f64 / total_chars as f64) * 100.0
            );
            return None;
        }

        // Check for valid UTF-8 sequences (already validated by String type, but double-check)
        if !text.is_char_boundary(0) || !text.is_char_boundary(text.len()) {
            debug!("Clipboard content has invalid UTF-8 boundaries");
            return None;
        }

        Some(text.to_string())
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
        // Try Hyprland first with timeout
        if let Ok(output) = tokio::time::timeout(
            CLIPBOARD_COMMAND_TIMEOUT,
            Command::new("hyprctl")
                .args(["activewindow", "-j"])
                .output(),
        )
        .await
        {
            if let Ok(output) = output {
                if output.status.success() {
                    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&output.stdout) {
                        return json
                            .get("class")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                    }
                }
            }
        }

        // Try xdotool for X11 with timeout
        if let Ok(output) = tokio::time::timeout(
            CLIPBOARD_COMMAND_TIMEOUT,
            Command::new("xdotool")
                .args(["getactivewindow", "getwindowclassname"])
                .output(),
        )
        .await
        {
            if let Ok(output) = output {
                if output.status.success() {
                    return Some(String::from_utf8_lossy(&output.stdout).trim().to_string());
                }
            }
        }

        None
    }

    /// Get active window title
    async fn get_active_window_title(&self) -> Option<String> {
        // Try Hyprland first with timeout
        if let Ok(output) = tokio::time::timeout(
            CLIPBOARD_COMMAND_TIMEOUT,
            Command::new("hyprctl")
                .args(["activewindow", "-j"])
                .output(),
        )
        .await
        {
            if let Ok(output) = output {
                if output.status.success() {
                    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&output.stdout) {
                        return json
                            .get("title")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                    }
                }
            }
        }

        // Try xdotool for X11 with timeout
        if let Ok(output) = tokio::time::timeout(
            CLIPBOARD_COMMAND_TIMEOUT,
            Command::new("xdotool")
                .args(["getactivewindow", "getwindowname"])
                .output(),
        )
        .await
        {
            if let Ok(output) = output {
                if output.status.success() {
                    return Some(String::from_utf8_lossy(&output.stdout).trim().to_string());
                }
            }
        }

        None
    }

    /// Find original hash for deduplication
    fn find_original_hash<'a>(&self, content_hash: &'a str) -> Option<&'a str> {
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

        let now = Timestamp::now();

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

    /// Store clipboard content as source material via Stage-as-You-Go
    async fn store_clipboard_source_material(
        &self,
        content: &ClipboardContent,
        selection_type: &str,
    ) -> NodeResult<Ulid> {
        let stage_context = self.stage_context.as_ref().ok_or_else(|| {
            SinexError::lifecycle("Stage-as-you-go context not initialized".to_string())
        })?;

        let data_bytes = content.text.as_bytes();
        if data_bytes.len() > self.max_content_size {
            warn!(
                size = data_bytes.len(),
                limit = self.max_content_size,
                "Clipboard payload exceeds configured size limit; still staging full content"
            );
        }

        let metadata = self.build_clipboard_metadata(content, selection_type);
        let material_id = stage_context
            .register_in_flight(&self.source_identifier, Some(selection_type), metadata)
            .await?;

        let mut event = if selection_type == "primary" {
            ClipboardSelectedPayload {
                selection_type: selection_type.to_string(),
                content_type: content.content_type.clone(),
                content_size: data_bytes.len(),
                text_preview: content.text_preview.clone(),
                source_app: content.source_app.clone(),
                content_hash: content.hash.clone(),
                original_hash: self
                    .find_original_hash(&content.hash)
                    .map(|h| h.to_string()),
                annex_key: None,
                blob_id: None,
            }
            .from_material(material_id)
            .with_offset_start(0)
            .map_err(|e| SinexError::processing(format!("Failed to set offset_start: {e}")))?
            .with_offset_end(data_bytes.len() as i64)
            .map_err(|e| SinexError::processing(format!("Failed to set offset_end: {e}")))?
            .build()
            .map_err(|e| SinexError::processing(format!("Failed to build event: {e}")))?
            .to_json_event()
            .map_err(|e| {
                SinexError::processing(format!("Failed to serialize clipboard event: {e}"))
            })?
        } else {
            ClipboardCopiedPayload {
                operation: "copy".to_string(),
                content_type: content.content_type.clone(),
                content_size: data_bytes.len(),
                text_preview: content.text_preview.clone(),
                file_count: content.file_paths.as_ref().map(|paths| paths.len()),
                file_paths: content.file_paths.clone(),
                source_app: content.source_app.clone(),
                window_title: content.window_title.clone(),
                content_hash: content.hash.clone(),
                original_hash: self
                    .find_original_hash(&content.hash)
                    .map(|h| h.to_string()),
                annex_key: None,
                blob_id: None,
            }
            .from_material(material_id)
            .with_offset_start(0)
            .map_err(|e| SinexError::processing(format!("Failed to set offset_start: {e}")))?
            .with_offset_end(data_bytes.len() as i64)
            .map_err(|e| SinexError::processing(format!("Failed to set offset_end: {e}")))?
            .build()
            .map_err(|e| SinexError::processing(format!("Failed to build event: {e}")))?
            .to_json_event()
            .map_err(|e| {
                SinexError::processing(format!("Failed to serialize clipboard event: {e}"))
            })?
        };
        event.id = Some(Id::from_ulid(Ulid::new()));

        stage_context
            .emit_event_with_provenance(event, material_id, Some(0), Some(data_bytes.len() as i64))
            .await?;

        stage_context
            .finalize_source_material(
                material_id,
                data_bytes,
                Some(self.mime_type_for_content(&content.content_type)),
                Some("utf-8"),
            )
            .await?;

        info!(
            "Staged clipboard {} source material: {} bytes, hash: {}",
            selection_type,
            data_bytes.len(),
            &content.hash[..8]
        );

        Ok(material_id)
    }

    fn build_clipboard_metadata(
        &self,
        content: &ClipboardContent,
        selection_type: &str,
    ) -> JsonValue {
        serde_json::json!({
            "selection_type": selection_type,
            "content_type": content.content_type,
            "content_size": content.size_bytes,
            "text_preview": content.text_preview,
            "file_paths": content.file_paths,
            "source_app": content.source_app,
            "window_title": content.window_title,
            "content_hash": content.hash,
            "original_hash": self.find_original_hash(&content.hash).map(|h| h.to_string()),
        })
    }

    fn mime_type_for_content(&self, content_type: &str) -> &'static str {
        match content_type {
            "files" => "text/uri-list",
            "image" => "application/octet-stream",
            _ => "text/plain",
        }
    }

    /// Get enriched clipboard content with metadata
    async fn get_clipboard_content(&self) -> Option<ClipboardContent> {
        // Try native arboard first, fall back to copypasta
        let text = self
            .get_clipboard_content_native()
            .or_else(|| self.get_clipboard_content_fallback());

        if let Some(text) = text {
            if text.is_empty() {
                return None;
            }

            // Validate content for UTF-8 and binary detection
            let validated_text = self.validate_clipboard_content(&text)?;

            let hash = self.calculate_hash(&validated_text);
            let size_bytes = validated_text.len();
            let (content_type, text_preview, file_paths) = self.analyze_content(&validated_text);
            let source_app = self.get_active_window_app().await;
            let window_title = self.get_active_window_title().await;
            let timestamp = OffsetDateTime::now_utc().into();

            Some(ClipboardContent {
                text: validated_text,
                hash,
                size_bytes,
                content_type,
                text_preview,
                file_paths,
                source_app,
                window_title,
                _timestamp: timestamp,
            })
        } else {
            None
        }
    }

    /// Get clipboard content using native arboard API (CLIPBOARD selection)
    fn get_clipboard_content_native(&self) -> Option<String> {
        match Clipboard::new() {
            Ok(mut clipboard) => {
                match clipboard
                    .get()
                    .clipboard(LinuxClipboardKind::Clipboard)
                    .text()
                {
                    Ok(text) => Some(text),
                    Err(e) => {
                        debug!("Failed to get CLIPBOARD contents via arboard: {}", e);
                        None
                    }
                }
            }
            Err(e) => {
                debug!("Failed to create arboard clipboard: {}", e);
                None
            }
        }
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
    /// Uses arboard's GetExtLinux trait to access PRIMARY selection
    async fn get_primary_selection_content(&self) -> Option<ClipboardContent> {
        if !self.enable_primary_selection {
            return None;
        }

        // Use arboard's GetExtLinux to read PRIMARY selection
        match Clipboard::new() {
            Ok(mut clipboard) => {
                match clipboard
                    .get()
                    .clipboard(LinuxClipboardKind::Primary)
                    .text()
                {
                    Ok(text) => {
                        if text.is_empty() {
                            return None;
                        }

                        // Validate content for UTF-8 and binary detection
                        let validated_text = self.validate_clipboard_content(&text)?;

                        let hash = self.calculate_hash(&validated_text);
                        let size_bytes = validated_text.len();
                        let text_preview = if validated_text.len() > self.max_preview_length {
                            Some(
                                validated_text
                                    .chars()
                                    .take(self.max_preview_length)
                                    .collect(),
                            )
                        } else {
                            None
                        };

                        Some(ClipboardContent {
                            text: validated_text,
                            hash,
                            size_bytes,
                            content_type: "text/plain".to_string(),
                            text_preview,
                            file_paths: None,
                            source_app: None,
                            window_title: None,
                            _timestamp: Timestamp::now(),
                        })
                    }
                    Err(e) => {
                        debug!("Failed to get PRIMARY selection via arboard: {}", e);
                        None
                    }
                }
            }
            Err(e) => {
                debug!("Failed to create arboard clipboard for PRIMARY: {}", e);
                None
            }
        }
    }

    /// Check for clipboard changes and store as source material
    async fn check_clipboard_changes(&mut self) -> NodeResult<()> {
        self.check_main_clipboard().await?;
        if self.enable_primary_selection {
            self.check_primary_selection().await?;
        }
        Ok(())
    }

    /// Start monitoring clipboard changes (stage-as-you-go mode)
    pub async fn start_monitoring(&mut self) -> NodeResult<()> {
        info!("Starting clipboard monitoring (stage-as-you-go mode)");

        let mut poll_interval = interval(self.poll_interval);

        loop {
            tokio::select! {
                _ = poll_interval.tick() => {}
                shutdown_result = self.shutdown_rx.changed() => {
                    if shutdown_result.is_err() || *self.shutdown_rx.borrow() {
                        info!("Clipboard watcher shutdown requested");
                        return Ok(());
                    }
                }
            }

            if let Err(e) = self.check_clipboard_changes().await {
                error!("Error checking clipboard changes: {}", e);
                // Continue polling even if there's an error
            }
        }
    }

    /// Check main clipboard for changes
    async fn check_main_clipboard(&mut self) -> NodeResult<()> {
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
    async fn check_primary_selection(&mut self) -> NodeResult<()> {
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

#[cfg(test)]
impl ClipboardWatcher {
    /// Lightweight stub used by unit tests so we don't require wl-paste/xclip.
    pub fn stub() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            last_content: None,
            last_primary_content: None,
            clipboard_history: VecDeque::new(),
            max_preview_length: DEFAULT_MAX_PREVIEW_LENGTH,
            max_content_size: DEFAULT_MAX_CONTENT_SIZE,
            max_history_entries: DEFAULT_MAX_HISTORY_ENTRIES,
            enable_primary_selection: false,
            enable_history: false,
            stage_context: None,
            shutdown_rx: watch::channel(false).1,
            source_identifier: "desktop_clipboard_stub".to_string(),
        }
    }
}

#[cfg(test)]
impl ClipboardWatcher {
    async fn test_watcher(
        max_content_size: usize,
        stage_context: StageAsYouGoContext,
    ) -> NodeResult<Self> {
        let mut watcher = ClipboardWatcher::stub();
        watcher.stage_context = Some(stage_context);
        watcher.max_content_size = max_content_size;
        Ok(watcher)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_node_sdk::acquisition_manager::AcquisitionManager;
    use std::sync::Arc;
    use time::OffsetDateTime;
    use tokio::sync::mpsc;
    use xtask::sandbox::{sinex_test, EphemeralNats, TestContext, TestResult};

    fn sample_clipboard_content(text: &str, watcher: &ClipboardWatcher) -> ClipboardContent {
        ClipboardContent {
            text: text.to_string(),
            hash: watcher.calculate_hash(text),
            size_bytes: text.len(),
            content_type: "text".to_string(),
            text_preview: Some(text.chars().take(32).collect()),
            file_paths: None,
            source_app: Some("test-app".to_string()),
            window_title: Some("test-window".to_string()),
            _timestamp: Timestamp::now(),
        }
    }

    async fn build_stage_context() -> TestResult<(
        StageAsYouGoContext,
        EphemeralNats,
        mpsc::Receiver<Event<JsonValue>>,
    )> {
        let nats = EphemeralNats::start().await?;
        let nats_client = nats.connect().await?;
        AcquisitionManager::bootstrap_streams(&nats_client).await?;

        let acquisition = Arc::new(AcquisitionManager::with_defaults(
            nats_client,
            "desktop",
            "/desktop",
        ));
        let (event_tx, event_rx) = mpsc::channel::<Event<JsonValue>>(
            sinex_primitives::buffers::DEFAULT_EVENT_CHANNEL_SIZE,
        );
        let context = StageAsYouGoContext::from_sender(acquisition, event_tx, false);
        Ok((context, nats, event_rx))
    }

    #[sinex_test(timeout = 60)]
    async fn clipboard_large_content_is_persisted(_ctx: TestContext) -> TestResult<()> {
        let (stage_context, _nats, _event_rx) = build_stage_context().await?;
        let watcher = ClipboardWatcher::test_watcher(16, stage_context).await?;
        let large_text = "A".repeat(1024);
        let content = sample_clipboard_content(&large_text, &watcher);

        watcher
            .store_clipboard_source_material(&content, "primary")
            .await?;

        Ok(())
    }

    #[sinex_test(timeout = 60)]
    async fn desktop_clipboard_requires_database_pool(_ctx: TestContext) -> TestResult<()> {
        let (stage_context, _nats, _event_rx) = build_stage_context().await?;
        let watcher =
            ClipboardWatcher::test_watcher(DEFAULT_MAX_CONTENT_SIZE, stage_context).await?;
        let content = sample_clipboard_content("clipboard text", &watcher);

        watcher
            .store_clipboard_source_material(&content, "primary")
            .await?;

        Ok(())
    }
}
