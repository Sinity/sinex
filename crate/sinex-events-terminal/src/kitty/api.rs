/// High-level semantic API for Kitty operations
/// 
/// This module provides clean, high-level functions for interacting with Kitty,
/// hiding the complexity of JSON command construction and response parsing.

use crate::kitty::protocol::KittyProtocol;
use crate::kitty::state::{KittyWindow, KittyProcess};
use sinex_core::Result;
use serde_json::json;
use tracing::{debug, error};

/// High-level Kitty API
pub struct KittyApi {
    protocol: KittyProtocol,
}

impl KittyApi {
    pub fn new() -> Self {
        Self {
            protocol: KittyProtocol::new(),
        }
    }

    /// Initialize the API by discovering and connecting to Kitty socket
    pub async fn initialize(&mut self) -> Result<()> {
        match self.protocol.discover_socket().await {
            Ok(socket_path) => {
                debug!("Connected to Kitty socket: {}", socket_path);
                Ok(())
            }
            Err(e) => {
                error!("Failed to connect to Kitty: {}", e);
                Err(e)
            }
        }
    }

    /// Check if API is connected to Kitty
    pub fn is_connected(&self) -> bool {
        self.protocol.is_connected()
    }

    /// Get all tabs and windows from Kitty
    pub async fn get_tabs_and_windows(&self) -> Result<(Vec<TabInfo>, Vec<KittyWindow>)> {
        let ls_command = json!({"cmd": "ls"});
        let response = self.protocol.send_command(ls_command).await?;
        
        let mut tabs = Vec::new();
        let mut windows = Vec::new();
        
        if let Some(tabs_array) = response.as_array() {
            for (tab_index, tab) in tabs_array.iter().enumerate() {
                // Extract tab information
                if let (Some(tab_id), Some(tab_title), Some(is_focused)) = (
                    tab.get("id").and_then(|i| i.as_i64()),
                    tab.get("title").and_then(|t| t.as_str()),
                    tab.get("is_focused").and_then(|f| f.as_bool())
                ) {
                    let tab_id_str = tab_id.to_string();
                    tabs.push(TabInfo {
                        id: tab_id_str.clone(),
                        title: tab_title.to_string(),
                        index: tab_index as u32,
                        is_focused,
                    });
                
                    // Extract windows from this tab
                    if let Some(tab_windows) = tab.get("windows").and_then(|w| w.as_array()) {
                        for window in tab_windows {
                            if let Some(id) = window.get("id").and_then(|i| i.as_i64()) {
                                // Extract foreground processes if available
                                let mut foreground_processes = Vec::new();
                                if let Some(processes) = window.get("foreground_processes").and_then(|p| p.as_array()) {
                                    for process in processes {
                                        if let (Some(pid), Some(name)) = (
                                            process.get("pid").and_then(|p| p.as_u64()),
                                            process.get("name").and_then(|n| n.as_str())
                                        ) {
                                            foreground_processes.push(KittyProcess {
                                                pid: pid as u32,
                                                name: name.to_string(),
                                            });
                                        }
                                    }
                                }
                                
                                windows.push(KittyWindow {
                                    id,
                                    cwd: window.get("cwd").and_then(|c| c.as_str()).map(String::from),
                                    foreground_processes,
                                    last_cmd_exit_status: window.get("last_cmd_exit_status").and_then(|s| s.as_i64()).map(|s| s as i32),
                                    parent_tab_id: tab_id_str.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }
        
        Ok((tabs, windows))
    }

    /// Get the last command output from a specific window
    pub async fn get_last_command_output(&self, window_id: &str) -> Result<String> {
        self.get_window_text(window_id, "last_cmd_output").await
    }

    /// Get all scrollback content from a specific window
    pub async fn get_scrollback(&self, window_id: &str) -> Result<String> {
        self.get_window_text(window_id, "all").await
    }

    /// Get text from a specific window with a given extent
    pub async fn get_window_text(&self, window_id: &str, extent: &str) -> Result<String> {
        let get_text_command = json!({
            "cmd": "get-text",
            "match": format!("id:{}", window_id),
            "extent": extent
        });

        let response = self.protocol.send_command(get_text_command).await?;
        
        if let Some(text) = response.get("text").and_then(|t| t.as_str()) {
            Ok(text.to_string())
        } else {
            Err(sinex_core::CoreError::Other(format!("No text content in Kitty response for extent: {}", extent)))
        }
    }

    /// Send a command to a specific window
    pub async fn send_command_to_window(&self, window_id: &str, command: &str) -> Result<()> {
        let send_command = json!({
            "cmd": "send-text",
            "match": format!("id:{}", window_id),
            "text": command
        });

        self.protocol.send_command(send_command).await?;
        Ok(())
    }

    /// Create a new tab
    pub async fn create_tab(&self, title: Option<&str>) -> Result<String> {
        let mut new_tab_command = json!({"cmd": "new-tab"});
        
        if let Some(title) = title {
            new_tab_command["title"] = json!(title);
        }

        let response = self.protocol.send_command(new_tab_command).await?;
        
        if let Some(tab_id) = response.get("id").and_then(|i| i.as_i64()) {
            Ok(tab_id.to_string())
        } else {
            Err(sinex_core::CoreError::Other("Failed to get new tab ID from response".to_string()))
        }
    }

    /// Close a specific tab
    pub async fn close_tab(&self, tab_id: &str) -> Result<()> {
        let close_command = json!({
            "cmd": "close-tab",
            "match": format!("id:{}", tab_id)
        });

        self.protocol.send_command(close_command).await?;
        Ok(())
    }

    /// Focus a specific tab
    pub async fn focus_tab(&self, tab_id: &str) -> Result<()> {
        let focus_command = json!({
            "cmd": "focus-tab",
            "match": format!("id:{}", tab_id)
        });

        self.protocol.send_command(focus_command).await?;
        Ok(())
    }

    /// Get the currently focused tab
    pub async fn get_focused_tab(&self) -> Result<Option<TabInfo>> {
        let (tabs, _) = self.get_tabs_and_windows().await?;
        Ok(tabs.into_iter().find(|tab| tab.is_focused))
    }

    /// Set window title
    pub async fn set_window_title(&self, window_id: &str, title: &str) -> Result<()> {
        let title_command = json!({
            "cmd": "set-tab-title",
            "match": format!("id:{}", window_id),
            "title": title
        });

        self.protocol.send_command(title_command).await?;
        Ok(())
    }
}

/// Tab information structure
#[derive(Debug, Clone)]
pub struct TabInfo {
    pub id: String,
    pub title: String,
    pub index: u32,
    pub is_focused: bool,
}

impl Default for KittyApi {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_api() {
        let api = KittyApi::new();
        assert!(!api.is_connected());
    }

    #[test]
    fn test_tab_info_creation() {
        let tab = TabInfo {
            id: "1".to_string(),
            title: "Test Tab".to_string(),
            index: 0,
            is_focused: true,
        };
        
        assert_eq!(tab.id, "1");
        assert_eq!(tab.title, "Test Tab");
        assert_eq!(tab.index, 0);
        assert!(tab.is_focused);
    }
}