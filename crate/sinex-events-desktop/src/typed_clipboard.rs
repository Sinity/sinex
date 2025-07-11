/// Typed clipboard monitor - uses strongly typed events
use async_trait::async_trait;
use chrono::Utc;
use tracing::{debug, error, info};
use tokio::process::Command;

use sinex_core::sources;
use sinex_events::{
    EnforcedTypedEventSource, TypedEventError, TypedEventResult, TypedEventSender,
    TypedClipboardEventBuilder,
};

use crate::clipboard::{ClipboardConfig, ClipboardHistoryEntry};

/// Typed clipboard monitor using strongly typed events
pub struct TypedClipboardMonitor {
    config: ClipboardConfig,
    last_clipboard: Option<String>,
    last_primary: Option<String>,
    clipboard_history: Vec<ClipboardHistoryEntry>,
}

#[async_trait]
impl EnforcedTypedEventSource for TypedClipboardMonitor {
    type Config = ClipboardConfig;
    const SOURCE_NAME: &'static str = sources::CLIPBOARD;

    async fn initialize(config_value: serde_json::Value) -> TypedEventResult<Self> {
        let config: ClipboardConfig = serde_json::from_value(config_value).map_err(|e| {
            TypedEventError::Serialization(format!("Failed to parse clipboard config: {}", e))
        })?;

        info!("Initializing typed clipboard monitor");

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
            return Err(TypedEventError::Other(
                "Neither wl-clipboard nor xclip found".to_string(),
            ));
        }

        info!(
            "Clipboard tools available - wl-paste: {}, xclip: {}",
            wl_paste_available, xclip_available
        );

        Ok(Self {
            config,
            last_clipboard: None,
            last_primary: None,
            clipboard_history: Vec::new(),
        })
    }

    async fn stream_typed_events(&mut self, tx: TypedEventSender) -> TypedEventResult<()> {
        info!("Starting typed clipboard monitoring");

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

impl TypedClipboardMonitor {
    async fn check_clipboard(&mut self, tx: &TypedEventSender, selection: &str) -> TypedEventResult<()> {
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
            let source_app = self.get_active_window_app().await;

            // Calculate content hash
            let content_hash = self.calculate_hash(&content);

            // Simplified payload creation - no git-annex, blob storage, or complex features
            let builder = TypedClipboardEventBuilder::new(sources::CLIPBOARD);

            // Create appropriate event based on selection type
            let event = if selection == "clipboard" {
                builder.content_copied(
                    content_type.clone(),
                    content.len() as u64,
                    preview,
                    Some(content_hash.clone()),
                    source_app,
                )
            } else {
                builder.content_selected(
                    content_type.clone(),
                    content.len() as u64,
                    preview,
                    selection.to_string(),
                )
            };

            // Send event
            tx.send(event).map_err(|e| {
                TypedEventError::ChannelSend(format!("Failed to send clipboard event: {}", e))
            })?;

            // Update history
            if self.config.enable_history {
                self.update_history(content_hash, content_type.clone());
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

    async fn get_clipboard_content(&self, selection: &str) -> TypedEventResult<String> {
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

    fn calculate_hash(&self, content: &str) -> String {
        blake3::hash(content.as_bytes()).to_hex().to_string()
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
}