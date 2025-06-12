use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::process::Command;
use tracing::{error, info, debug};

use sinex_core::{EventType, EventSource, Result};
use sinex_db::models::RawEvent;

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
    /// Timestamp
    pub timestamp: DateTime<Utc>,
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
    /// Timestamp
    pub timestamp: DateTime<Utc>,
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
    /// Store large content externally (like git-annex)
    pub external_storage_path: Option<String>,
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
            external_storage_path: None,
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
}

#[derive(Clone)]
struct ClipboardHistoryEntry {
    content_hash: String,
    first_seen: DateTime<Utc>,
    last_seen: DateTime<Utc>,
    content_type: String,
    copy_count: u32,
}

#[async_trait]
impl EventSource for ClipboardMonitor {
    type Config = ClipboardConfig;
    
    const SOURCE_NAME: &'static str = "clipboard.monitor";
    
    async fn initialize(config: Self::Config) -> Result<Self> {
        info!("Initializing clipboard monitor");
        
        // Check for required tools
        let wl_paste_available = Command::new("which")
            .arg("wl-paste")
            .output()
            .await
            .map(|o| o.status.success())
            .unwrap_or(false);
            
        let xclip_available = Command::new("which")
            .arg("xclip")
            .output()
            .await
            .map(|o| o.status.success())
            .unwrap_or(false);
            
        if !wl_paste_available && !xclip_available {
            return Err(sinex_core::CoreError::Other(
                "Neither wl-clipboard nor xclip found. Install one for clipboard monitoring".to_string()
            ));
        }
        
        info!(
            "Clipboard tools available - wl-paste: {}, xclip: {}", 
            wl_paste_available, 
            xclip_available
        );
        
        Ok(Self {
            config,
            last_clipboard: None,
            last_primary: None,
            clipboard_history: Vec::new(),
        })
    }
    
    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> Result<()> {
        info!("Starting clipboard monitoring");
        
        let mut interval = tokio::time::interval(
            tokio::time::Duration::from_millis(self.config.poll_interval_ms)
        );
        
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
    async fn check_clipboard(
        &mut self,
        tx: &mpsc::Sender<RawEvent>,
        selection: &str,
    ) -> Result<()> {
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
            
            // Handle large content
            let (text_preview, _stored_externally) = if content.len() > self.config.max_content_size {
                // For large content, store externally if configured
                if let Some(ref _storage_path) = self.config.external_storage_path {
                    // TODO: Implement external storage (git-annex style)
                    debug!("Large clipboard content ({} bytes) would be stored externally", content.len());
                }
                (Some("[Content too large for preview]".to_string()), true)
            } else {
                (preview.clone(), false)
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
                    timestamp: Utc::now(),
                };
                
                let event = self.create_event(
                    ClipboardChanged::EVENT_NAME,
                    serde_json::to_value(payload)?
                );
                tx.send(event).await.map_err(|_| sinex_core::CoreError::Other(
                    "Channel closed".to_string()
                ))?;
            } else {
                let payload = ClipboardSelectionPayload {
                    selection_type: selection.to_string(),
                    content_type: content_type.clone(),
                    content_size: content.len(),
                    text_preview,
                    source_app,
                    content_hash: content_hash.clone(),
                    original_hash: original_hash.clone(),
                    timestamp: Utc::now(),
                };
                
                let event = self.create_event(
                    ClipboardSelection::EVENT_NAME,
                    serde_json::to_value(payload)?
                );
                tx.send(event).await.map_err(|_| sinex_core::CoreError::Other(
                    "Channel closed".to_string()
                ))?;
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
            .args(if wl_selection.is_empty() { vec![] } else { vec![wl_selection] })
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
        if content.starts_with("file://") || 
           (content.lines().all(|l| l.starts_with('/') || l.is_empty()) && 
            content.lines().count() > 0) {
            ("files".to_string(), None)
        }
        // Detect if it's an image (base64 or binary)
        else if content.len() > 100 && content.chars().all(|c| c.is_ascii_graphic()) {
            ("image".to_string(), None)
        }
        // Detect URLs
        else if content.starts_with("http://") || content.starts_with("https://") {
            ("url".to_string(), Some(content.chars().take(self.config.max_preview_length).collect()))
        }
        // Default to text
        else {
            let preview = if content.len() > self.config.max_preview_length {
                Some(format!(
                    "{}...",
                    content.chars().take(self.config.max_preview_length).collect::<String>()
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
                            .map(|p| urlencoding::decode(p).ok())
                            .flatten()
                            .map(|p| p.to_string())
                    })
                    .collect()
            )
        } else if content.lines().all(|l| l.starts_with('/') || l.is_empty()) {
            Some(
                content
                    .lines()
                    .filter(|l| !l.is_empty())
                    .map(|l| l.to_string())
                    .collect()
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
                if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&output.stdout) {
                    return json.get("class")
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
                if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&output.stdout) {
                    return json.get("title")
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
        if let Some(entry) = self.clipboard_history.iter_mut().find(|e| e.content_hash == content_hash) {
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
    
    fn create_event(&self, event_type: &str, payload: serde_json::Value) -> RawEvent {
        RawEvent {
            id: sinex_ulid::Ulid::new(),
            source: Self::SOURCE_NAME.to_string(),
            event_type: event_type.to_string(),
            ts_ingest: Utc::now(),
            ts_orig: Some(Utc::now()),
            host: gethostname::gethostname().to_string_lossy().to_string(),
            ingestor_version: Some(env!("CARGO_PKG_VERSION").to_string()),
            payload_schema_id: None,
            payload,
        }
    }
}