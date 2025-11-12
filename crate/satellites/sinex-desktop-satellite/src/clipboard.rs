#![doc = include_str!("../doc/clipboard.md")]

// Use local facade for common types
use crate::common::*;
use sinex_core::environment;
use sinex_core::types::Ulid;
use sinex_satellite_sdk::annex::GitAnnex;
use std::sync::Arc;
use tokio::fs;

// Clipboard-specific imports
use copypasta::{ClipboardContext, ClipboardProvider};
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
    blob_manager: Option<Arc<BlobManager>>,
}

impl ClipboardWatcher {
    async fn initialize_blob_manager(db_pool: PgPool) -> SatelliteResult<Option<Arc<BlobManager>>> {
        let repo_path = Self::resolve_annex_repo();
        if let Err(e) = fs::create_dir_all(repo_path.as_std_path()).await {
            warn!(error = %e, "Failed to create annex directory for clipboard watcher");
            return Ok(None);
        }

        if !repo_path.join(".git").exists() {
            if let Err(e) = GitAnnex::init(&repo_path, Some("desktop-clipboard")).await {
                warn!(error = %e, "Failed to initialize git-annex repository for clipboard watcher");
                return Ok(None);
            }
        }

        let (blob_event_tx, mut blob_event_rx) = mpsc::unbounded_channel::<Event<JsonValue>>();
        tokio::spawn(async move {
            while let Some(event) = blob_event_rx.recv().await {
                debug!(?event, "Clipboard blob manager emitted event");
            }
        });

        let annex_config = AnnexConfig {
            repo_path,
            num_copies: None,
            large_files: None,
        };

        match BlobManager::new(annex_config, db_pool, blob_event_tx) {
            Ok(manager) => Ok(Some(Arc::new(manager))),
            Err(e) => {
                warn!(error = %e, "Failed to initialize clipboard blob manager");
                Ok(None)
            }
        }
    }

    fn resolve_annex_repo() -> Utf8PathBuf {
        if let Ok(path) = std::env::var("SINEX_ANNEX_PATH") {
            return Utf8PathBuf::from(path);
        }

        let default_path = environment::environment().work_directory("/tmp/sinex/annex");
        Utf8PathBuf::from_path_buf(default_path)
            .unwrap_or_else(|_| Utf8PathBuf::from("/tmp/sinex/annex"))
    }

    /// Create new clipboard watcher with sensd integration
    pub async fn new(poll_interval_secs: u64, db_pool: Option<PgPool>) -> SatelliteResult<Self> {
        let blob_manager = if let Some(pool) = &db_pool {
            Self::initialize_blob_manager(pool.clone()).await?
        } else {
            None
        };

        let watcher = Self {
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
            blob_manager,
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
        let data_bytes = content.text.as_bytes();
        let storage = if data_bytes.len() <= self.max_content_size {
            ClipboardStorage::Inline(content.size_bytes)
        } else if let Some(reference) = self.ingest_large_clipboard_content(content).await? {
            ClipboardStorage::Annex(reference)
        } else {
            warn!(
                "Large clipboard content ({:?} bytes) skipped due to missing blob manager",
                content.size_bytes
            );
            return Ok(None);
        };

        let metadata = self.build_clipboard_metadata(content, selection_type, &storage);
        let material_kind = "annex";

        sqlx::query!(
            r#"
            INSERT INTO raw.source_material_registry (
                id, source_identifier, staged_at,
                material_kind, timing_info_type, metadata,
                status, staged_by
            )
            VALUES ($1::ulid, $2, $3, $4, $5, $6, $7, $8)
            "#,
            material_id as Ulid,    // $1 - id
            self.source_identifier, // $2 - source_identifier
            now,                    // $3 - staged_at
            material_kind,          // $4 - material_kind
            "realtime",             // $5 - timing_info_type
            metadata,
            "completed",         // $7 - status
            "clipboard-monitor", // $8 - staged_by
        )
        .execute(db_pool)
        .await?;

        // Create temporal ledger entry
        sqlx::query!(
            r#"
            INSERT INTO raw.temporal_ledger (
                id, source_material_id, offset_start, offset_end, 
                offset_kind, ts_capture, precision, clock, source_type
            )
            VALUES ($1::ulid, $2::ulid, $3, $4, $5, $6, $7, $8, $9)
            "#,
            Ulid::new() as Ulid,     // $1 - id
            material_id as Ulid,     // $2 - source_material_id
            0i64,                    // $3 - offset_start
            data_bytes.len() as i64, // $4 - offset_end
            "byte",                  // $5 - offset_kind
            content.timestamp,       // $6 - ts_capture
            "exact",                 // $7 - precision
            "wall",                  // $8 - clock
            "realtime_capture",      // $9 - source_type
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

    fn build_clipboard_metadata(
        &self,
        content: &ClipboardContent,
        selection_type: &str,
        storage: &ClipboardStorage,
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
            "storage": storage.to_metadata(),
        })
    }

    async fn ingest_large_clipboard_content(
        &self,
        content: &ClipboardContent,
    ) -> SatelliteResult<Option<AnnexBlobReference>> {
        let Some(manager) = &self.blob_manager else {
            return Ok(None);
        };

        let filename = format!(
            "clipboard-{}-{}.txt",
            content.timestamp.format("%Y%m%dT%H%M%S"),
            &content.hash[..8]
        );
        let mime_type = match content.content_type.as_str() {
            "image" => "application/octet-stream",
            "files" => "text/plain",
            _ => "text/plain",
        };

        let blob = manager
            .ingest_from_bytes(content.text.as_bytes(), &filename, mime_type)
            .await
            .map_err(|e| SatelliteError::Processing(e.to_string()))?;

        let blob_id = Ulid::from(blob.id.as_ulid().clone());

        Ok(Some(AnnexBlobReference {
            blob_id,
            annex_key: blob.annex_key(),
            size_bytes: blob.size_bytes,
            mime_type: blob.mime_type.or_else(|| Some(mime_type.to_string())),
            checksum_blake3: blob.checksum_blake3,
        }))
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

#[derive(Debug, Clone)]
enum ClipboardStorage {
    Inline(usize),
    Annex(AnnexBlobReference),
}

impl ClipboardStorage {
    fn to_metadata(&self) -> JsonValue {
        match self {
            ClipboardStorage::Inline(size) => serde_json::json!({
                "strategy": "inline",
                "size_bytes": size,
            }),
            ClipboardStorage::Annex(reference) => serde_json::json!({
                "strategy": "annex_blob",
                "blob_id": reference.blob_id.to_string(),
                "annex_key": reference.annex_key,
                "mime_type": reference.mime_type,
                "size_bytes": reference.size_bytes,
                "checksum_blake3": reference.checksum_blake3,
            }),
        }
    }
}

#[derive(Debug, Clone)]
struct AnnexBlobReference {
    blob_id: Ulid,
    annex_key: String,
    size_bytes: i64,
    mime_type: Option<String>,
    checksum_blake3: Option<String>,
}

#[cfg(test)]
impl ClipboardWatcher {
    async fn test_watcher(
        max_content_size: usize,
        db_pool: Option<PgPool>,
    ) -> SatelliteResult<Self> {
        let mut watcher = ClipboardWatcher::new(1, db_pool).await?;
        watcher.max_content_size = max_content_size;
        Ok(watcher)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use sinex_test_utils::{sinex_test, TestContext};

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
            timestamp: Utc::now(),
        }
    }

    #[sinex_test]
    async fn clipboard_large_content_is_persisted(ctx: TestContext) -> color_eyre::Result<()> {
        let watcher = ClipboardWatcher::test_watcher(16, Some(ctx.pool.clone())).await?;
        let large_text = "A".repeat(1024);
        let content = sample_clipboard_content(&large_text, &watcher);

        let stored = watcher
            .store_clipboard_source_material(&content, "primary")
            .await?;

        assert!(
            stored.is_some(),
            "Large clipboard captures should be persisted rather than silently skipped"
        );

        Ok(())
    }

    #[sinex_test]
    async fn desktop_clipboard_requires_database_pool() -> color_eyre::Result<()> {
        let watcher = ClipboardWatcher::test_watcher(DEFAULT_MAX_CONTENT_SIZE, None).await?;
        let content = sample_clipboard_content("clipboard text", &watcher);

        let stored = watcher
            .store_clipboard_source_material(&content, "primary")
            .await?;

        assert!(
            stored.is_some(),
            "Clipboard ingestion should no longer require a direct Postgres pool"
        );

        Ok(())
    }
}
