use async_trait::async_trait;
use chrono::Utc;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use tracing::{debug, error, info};

use sinex_annex::{AnnexConfig, BlobManager, BlobMetadata, GitAnnex};
use sinex_core::{
    ChannelSenderExt, EventSender, EventSource, EventSourceBase, EventSourceContext, EventType,
    JsonValue, Result, Timestamp,
};
use sinex_db::DbPool;

// ============================================================================
// Event Payloads
// ============================================================================

/// Clipboard content change event
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ClipboardChangedPayload {
    /// Operation type (copy, cut, paste)
    pub operation: String,
    /// Content type (text, image, files, etc)
    pub content_type: String,
    /// Size of content in bytes
    pub content_size: usize,
    /// First 100 chars of text content (if text)
    pub text_preview: Option<String>,
    /// Number of files (if files)
    pub file_count: Option<usize>,
    /// File paths (if files)
    pub file_paths: Option<Vec<String>>,
    /// Source application if available
    pub source_app: Option<String>,
    /// Window title if available
    pub window_title: Option<String>,
    /// Content hash (BLAKE3) for deduplication
    pub content_hash: String,
    /// If this is a re-copy, hash of first occurrence
    pub original_hash: Option<String>,
    /// Git-annex key if content was stored externally
    pub annex_key: Option<String>,
    /// Blob ID from core_blobs table if stored
    pub blob_id: Option<String>,
    /// Timestamp
    pub timestamp: Timestamp,
}

/// Clipboard selection event (Linux primary selection)
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ClipboardSelectionPayload {
    /// Selection type (primary, secondary, clipboard)
    pub selection_type: String,
    /// Content type
    pub content_type: String,
    /// Size of content
    pub content_size: usize,
    /// Text preview
    pub text_preview: Option<String>,
    /// Source application
    pub source_app: Option<String>,
    /// Content hash for deduplication
    pub content_hash: String,
    /// If this is a re-selection, hash of first occurrence
    pub original_hash: Option<String>,
    /// Git-annex key if content was stored externally
    pub annex_key: Option<String>,
    /// Blob ID from core_blobs table if stored
    pub blob_id: Option<String>,
    /// Timestamp
    pub timestamp: Timestamp,
}

// ============================================================================
// Event Types
// ============================================================================

pub struct ClipboardChanged;
impl EventType for ClipboardChanged {
    type Payload = ClipboardChangedPayload;
    type SourceImpl = ClipboardMonitor;
    const EVENT_NAME: &'static str = "clipboard.content.changed";
}

pub struct ClipboardSelection;
impl EventType for ClipboardSelection {
    type Payload = ClipboardSelectionPayload;
    type SourceImpl = ClipboardMonitor;
    const EVENT_NAME: &'static str = "clipboard.selection.changed";
}

// ============================================================================
// Event Source Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClipboardConfig {
    /// Monitor standard clipboard
    pub monitor_clipboard: bool,
    /// Monitor primary selection (Linux)
    pub monitor_primary: bool,
    /// Monitor secondary selection (rarely used)
    pub monitor_secondary: bool,
    /// Polling interval in milliseconds
    pub poll_interval_ms: u64,
    /// Include file content hashes
    pub hash_file_content: bool,
    /// Maximum preview length
    pub max_preview_length: usize,
    /// Store clipboard history
    pub enable_history: bool,
    /// Maximum history entries
    pub max_history_entries: usize,
    /// Maximum content size to fully capture (bytes)
    pub max_content_size: usize,
    /// Git-annex repository path for large content
    pub annex_repo_path: Option<String>,
}

impl Default for ClipboardConfig {
    fn default() -> Self {
        Self {
            monitor_clipboard: true,
            monitor_primary: true,
            monitor_secondary: false,
            poll_interval_ms: 500,
            hash_file_content: false,
            max_preview_length: 100,
            enable_history: true,
            max_history_entries: 1000,
            max_content_size: 10 * 1024 * 1024, // 10MB default
            annex_repo_path: None,
        }
    }
}

// ============================================================================
// Event Source Implementation
// ============================================================================

pub struct ClipboardMonitor {
    config: ClipboardConfig,
    last_clipboard: Option<String>,
    last_primary: Option<String>,
    clipboard_history: Vec<ClipboardHistoryEntry>,
    git_annex: Option<GitAnnex>,
    db_pool: Option<DbPool>,
}

#[derive(Clone)]
struct ClipboardHistoryEntry {
    content_hash: String,
    #[allow(dead_code)]
    first_seen: Timestamp,
    last_seen: Timestamp,
    #[allow(dead_code)]
    content_type: String,
    copy_count: u32,
}

// Implement EventSourceBase to get common functionality
impl EventSourceBase for ClipboardMonitor {}

#[async_trait]
impl EventSource for ClipboardMonitor {
    type Config = ClipboardConfig;

    const SOURCE_NAME: &'static str = "clipboard.monitor";

    async fn initialize(ctx: EventSourceContext) -> Result<Self> {
        // Use base trait for config parsing
        let config = <Self as EventSourceBase>::parse_config::<Self::Config>(&ctx).await?;

        info!("Initializing clipboard monitor");

        // Check for required tools - try direct execution instead of 'which'
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
            error!("Neither wl-clipboard nor xclip found. Install one for clipboard monitoring");
            return Err(sinex_core::CoreError::Other(
                "Neither wl-clipboard nor xclip found. Install one for clipboard monitoring"
                    .to_string(),
            ));
        }

        info!(
            "Clipboard tools available - wl-paste: {}, xclip: {}",
            wl_paste_available, xclip_available
        );

        // Initialize git-annex if configured
        let annex_repo_path = ctx
            .annex_repo_path
            .clone()
            .or(config.annex_repo_path.clone());
        let git_annex = if let Some(ref repo_path) = annex_repo_path {
            let path = std::path::PathBuf::from(repo_path);

            // Initialize git-annex repository if it doesn't exist
            if !path.join(".git").exists() {
                GitAnnex::init(&path, Some("sinex-clipboard-annex"))
                    .await
                    .map_err(|e| {
                        sinex_core::CoreError::Other(format!(
                            "Failed to initialize git-annex: {}",
                            e
                        ))
                    })?;
            }

            let annex_config = AnnexConfig {
                repo_path: path.clone(),
                num_copies: Some(2),
                large_files: None,
            };

            let git_annex = GitAnnex::new(annex_config).map_err(|e| {
                sinex_core::CoreError::Other(format!("Failed to create GitAnnex: {}", e))
            })?;

            Some(git_annex)
        } else {
            None
        };

        // Create instance and set additional fields
        let mut instance = Self::new(config).await?;
        instance.git_annex = git_annex;
        instance.db_pool = ctx.db_pool;
        Ok(instance)
    }

    async fn stream_events(&mut self, tx: EventSender) -> Result<()> {
        info!("Starting clipboard monitoring");

        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(
            self.config.poll_interval_ms,
        ));

        loop {
            interval.tick().await;

            // Monitor standard clipboard
            if self.config.monitor_clipboard {
                if let Err(e) = self.check_clipboard(&tx, "clipboard").await {
                    error!("Error checking clipboard: {}", e);
                }
            }

            // Monitor primary selection (Linux)
            if self.config.monitor_primary {
                if let Err(e) = self.check_clipboard(&tx, "primary").await {
                    error!("Error checking primary selection: {}", e);
                }
            }
        }
    }
}

impl ClipboardMonitor {
    async fn new(config: ClipboardConfig) -> Result<Self> {
        Ok(Self {
            config,
            last_clipboard: None,
            last_primary: None,
            clipboard_history: Vec::new(),
            git_annex: None,
            db_pool: None,
        })
    }

    async fn check_clipboard(&mut self, tx: &EventSender, selection: &str) -> Result<()> {
        let content = self.get_clipboard_content(selection).await?;

        // Check which last content to compare against
        let last_content = match selection {
            "clipboard" => &self.last_clipboard,
            "primary" => &self.last_primary,
            _ => return Ok(()),
        };

        // Check if content changed
        if Some(&content) != last_content.as_ref() {
            debug!("Clipboard {} changed", selection);

            // Detect content type
            let (content_type, preview) = self.analyze_content(&content);
            let file_paths = self.extract_file_paths(&content);
            let source_app = self.get_active_window_app().await;
            let window_title = self.get_active_window_title().await;

            // Calculate content hash
            let content_hash = self.calculate_hash(&content);

            // Check if this is a re-copy
            let original_hash = if self.config.enable_history {
                self.find_original_hash(&content_hash)
            } else {
                None
            };

            // Handle large content with git-annex
            let (text_preview, annex_key, blob_id) = if content.len() > self.config.max_content_size
            {
                match self.store_large_content(&content, &content_hash).await {
                    Ok((key, id)) => {
                        info!(
                            "Stored large clipboard content ({} bytes) in git-annex: {}",
                            content.len(),
                            key
                        );
                        (
                            Some("[Content stored in git-annex]".to_string()),
                            Some(key),
                            id,
                        )
                    }
                    Err(e) => {
                        error!("Failed to store large content in git-annex: {}", e);
                        (
                            Some("[Content too large - storage failed]".to_string()),
                            None,
                            None,
                        )
                    }
                }
            } else {
                (preview.clone(), None, None)
            };

            // Create appropriate event
            if selection == "clipboard" {
                let payload = ClipboardChangedPayload {
                    operation: "copy".to_string(), // We can't distinguish copy/cut
                    content_type: content_type.clone(),
                    content_size: content.len(),
                    text_preview,
                    file_count: None,
                    file_paths,
                    source_app,
                    window_title,
                    content_hash: content_hash.clone(),
                    original_hash: original_hash.clone(),
                    annex_key: annex_key.clone(),
                    blob_id: blob_id.clone(),
                    timestamp: Utc::now(),
                };

                let event =
                    self.create_event(ClipboardChanged::EVENT_NAME, serde_json::to_value(payload)?);
                tx.send_or_log(event, "clipboard_changed").await?;
            } else {
                let payload = ClipboardSelectionPayload {
                    selection_type: selection.to_string(),
                    content_type: content_type.clone(),
                    content_size: content.len(),
                    text_preview,
                    source_app,
                    content_hash: content_hash.clone(),
                    original_hash: original_hash.clone(),
                    annex_key: annex_key.clone(),
                    blob_id: blob_id.clone(),
                    timestamp: Utc::now(),
                };

                let event = self.create_event(
                    ClipboardSelection::EVENT_NAME,
                    serde_json::to_value(payload)?,
                );
                tx.send_or_log(event, "clipboard_selection").await?;
            }

            // Update history
            if self.config.enable_history {
                self.update_history(content_hash, content_type);
            }

            // Update last content
            match selection {
                "clipboard" => self.last_clipboard = Some(content),
                "primary" => self.last_primary = Some(content),
                _ => {}
            }
        }

        Ok(())
    }

    async fn get_clipboard_content(&self, selection: &str) -> Result<String> {
        // Try Wayland first
        let wl_selection = match selection {
            "clipboard" => "",
            "primary" => "-p",
            _ => return Ok(String::new()),
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
                return Ok(String::from_utf8_lossy(&output.stdout).to_string());
            }
        }

        // Fall back to X11
        let x_selection = match selection {
            "clipboard" => "-selection clipboard",
            "primary" => "-selection primary",
            _ => return Ok(String::new()),
        };

        let x_result = Command::new("xclip")
            .arg("-o")
            .args(x_selection.split_whitespace())
            .output()
            .await;

        if let Ok(output) = x_result {
            if output.status.success() {
                return Ok(String::from_utf8_lossy(&output.stdout).to_string());
            }
        }

        Ok(String::new())
    }

    fn analyze_content(&self, content: &str) -> (String, Option<String>) {
        // Detect if it's a file path/URI list
        if content.starts_with("file://")
            || (content.lines().all(|l| l.starts_with('/') || l.is_empty())
                && content.lines().count() > 0)
        {
            ("files".to_string(), None)
        }
        // Detect if it's an image (base64 or binary)
        else if content.len() > 100 && content.chars().all(|c| c.is_ascii_graphic()) {
            ("image".to_string(), None)
        }
        // Detect URLs
        else if content.starts_with("http://") || content.starts_with("https://") {
            (
                "url".to_string(),
                Some(
                    content
                        .chars()
                        .take(self.config.max_preview_length)
                        .collect(),
                ),
            )
        }
        // Default to text
        else {
            let preview = if content.len() > self.config.max_preview_length {
                Some(format!(
                    "{}...",
                    content
                        .chars()
                        .take(self.config.max_preview_length)
                        .collect::<String>()
                ))
            } else {
                Some(content.to_string())
            };
            ("text".to_string(), preview)
        }
    }

    fn extract_file_paths(&self, content: &str) -> Option<Vec<String>> {
        if content.starts_with("file://") {
            Some(
                content
                    .lines()
                    .filter_map(|line| {
                        line.strip_prefix("file://")
                            .and_then(|p| urlencoding::decode(p).ok())
                            .map(|p| p.to_string())
                    })
                    .collect(),
            )
        } else if content.lines().all(|l| l.starts_with('/') || l.is_empty()) {
            Some(
                content
                    .lines()
                    .filter(|l| !l.is_empty())
                    .map(|l| l.to_string())
                    .collect(),
            )
        } else {
            None
        }
    }

    async fn get_active_window_app(&self) -> Option<String> {
        // Try Hyprland first
        if let Ok(output) = Command::new("hyprctl")
            .args(["activewindow", "-j"])
            .output()
            .await
        {
            if output.status.success() {
                if let Ok(json) = serde_json::from_slice::<JsonValue>(&output.stdout) {
                    return json
                        .get("class")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                }
            }
        }

        // Try xdotool
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

    async fn get_active_window_title(&self) -> Option<String> {
        // Try Hyprland first
        if let Ok(output) = Command::new("hyprctl")
            .args(["activewindow", "-j"])
            .output()
            .await
        {
            if output.status.success() {
                if let Ok(json) = serde_json::from_slice::<JsonValue>(&output.stdout) {
                    return json
                        .get("title")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                }
            }
        }

        None
    }

    fn calculate_hash(&self, content: &str) -> String {
        blake3::hash(content.as_bytes()).to_hex().to_string()
    }

    fn find_original_hash(&self, content_hash: &str) -> Option<String> {
        self.clipboard_history
            .iter()
            .find(|e| e.content_hash == content_hash)
            .map(|e| e.content_hash.clone())
    }

    fn update_history(&mut self, content_hash: String, content_type: String) {
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
            self.clipboard_history.push(ClipboardHistoryEntry {
                content_hash,
                first_seen: now,
                last_seen: now,
                content_type,
                copy_count: 1,
            });

            // Trim history if needed
            if self.clipboard_history.len() > self.config.max_history_entries {
                self.clipboard_history.remove(0);
            }
        }
    }

    async fn store_large_content(
        &mut self,
        content: &str,
        content_hash: &str,
    ) -> Result<(String, Option<String>)> {
        // Check if we have git-annex configured
        let git_annex = self.git_annex.as_ref().ok_or_else(|| {
            sinex_core::CoreError::Other(
                "Git-annex not configured for large content storage".to_string(),
            )
        })?;

        // Create a temporary file with the content
        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join(format!("clipboard_{}.tmp", content_hash));

        tokio::fs::write(&temp_file, content.as_bytes())
            .await
            .map_err(|e| {
                sinex_core::CoreError::Other(format!("Failed to write temporary file: {}", e))
            })?;

        // Add to git-annex
        let annex_key = git_annex.add_file(&temp_file).await.map_err(|e| {
            sinex_core::CoreError::Other(format!("Failed to add file to git-annex: {}", e))
        })?;

        // Clean up temp file (git-annex has moved it)
        let _ = tokio::fs::remove_file(&temp_file).await;

        // Store blob metadata if we have database access
        let blob_id = if let Some(ref db_pool) = self.db_pool {
            let annex_config = AnnexConfig {
                repo_path: git_annex.repo_path().to_path_buf(),
                num_copies: None,
                large_files: None,
            };
            let blob_manager = BlobManager::new(annex_config, db_pool.clone()).map_err(|e| {
                sinex_core::CoreError::processing_failed()
                    .with_operation("create_blob_manager")
                    .with_source(e)
                    .build()
            })?;

            let blob_metadata = BlobMetadata {
                blob_id: sinex_ulid::Ulid::new(),
                annex_key: annex_key.key.clone(),
                original_filename: "clipboard_content".to_string(),
                size_bytes: content.len() as i64,
                mime_type: Some("text/plain".to_string()),
                checksum_sha256: annex_key.hash.clone(),
                checksum_blake3: Some(content_hash.to_string()),
                storage_backend: "git-annex".to_string(),
                verification_status: Some("verified".to_string()),
            };

            match blob_manager.insert_blob(&blob_metadata).await {
                Ok(_) => {
                    debug!(
                        "Stored blob metadata for clipboard content: {}",
                        blob_metadata.blob_id
                    );
                    Some(blob_metadata.blob_id.to_string())
                }
                Err(e) => {
                    error!("Failed to store blob metadata: {}", e);
                    None
                }
            }
        } else {
            debug!("No database connection available, skipping blob metadata storage");
            None
        };

        Ok((annex_key.key, blob_id))
    }

    // Removed - now using EventSourceBase::create_event
}
