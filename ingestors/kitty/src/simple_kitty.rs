use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::os::unix::fs::FileTypeExt;
use std::process::Command;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};
use tracing::{debug, error, info, warn};

use crate::config::KittyConfig;
use sinex_shared::{
    SimpleIngestor, event_types::{self, RawEventBuilder}, sources,
    agent_events::{AgentError, ErrorSeverity}, create_error_event,
};
use sinex_db::models::RawEvent;

/// Command execution event payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandExecutedPayload {
    pub command_string: String,
    pub cwd: String,
    pub exit_code: i32,
    pub ts_start_orig: DateTime<Utc>,
    pub ts_end_orig: DateTime<Utc>,
}

/// Kitty window information
#[derive(Debug, Clone)]
struct KittyWindow {
    id: u32,
    #[allow(dead_code)]
    pid: u32,
    cwd: String,
    #[allow(dead_code)]
    title: String,
}

/// Simplified Kitty ingestor that only handles event capture
pub struct SimpleKittyIngestor {
    config: KittyConfig,
    last_command_times: HashMap<u32, DateTime<Utc>>,
}

impl SimpleKittyIngestor {
    pub fn new(config: KittyConfig) -> Self {
        Self {
            config,
            last_command_times: HashMap::new(),
        }
    }
    
    /// Poll Kitty for new commands
    async fn poll_kitty_commands(&mut self, tx: &mpsc::Sender<RawEvent>) -> Result<()> {
        // Find Kitty sockets
        let sockets = Self::find_kitty_sockets(&self.config.socket_path)?;
        
        if sockets.is_empty() {
            debug!("No Kitty sockets found");
            return Ok(());
        }

        for socket in sockets {
            // Get list of windows
            let windows = match Self::get_kitty_windows(&socket) {
                Ok(windows) => windows,
                Err(e) => {
                    error!("Failed to get windows from socket {}: {}", socket, e);
                    continue;
                }
            };
            
            // Track active window IDs for cleanup
            let active_window_ids: Vec<u32> = windows.iter().map(|w| w.id).collect();
            
            for window in windows {
                // Get command history for this window
                if let Ok(commands) = Self::get_window_commands(&socket, window.id) {
                    let now = Utc::now();
                    let last_time = self.last_command_times
                        .get(&window.id)
                        .cloned()
                        .unwrap_or(now - chrono::Duration::hours(1));

                    for cmd in &commands {
                        if cmd.ts_end_orig > last_time {
                            debug!(
                                window_id = window.id,
                                command = %cmd.command_string,
                                exit_code = cmd.exit_code,
                                duration_ms = (cmd.ts_end_orig - cmd.ts_start_orig).num_milliseconds(),
                                "New command detected"
                            );
                            
                            let payload = CommandExecutedPayload {
                                command_string: cmd.command_string.clone(),
                                cwd: window.cwd.clone(),
                                exit_code: cmd.exit_code,
                                ts_start_orig: cmd.ts_start_orig,
                                ts_end_orig: cmd.ts_end_orig,
                            };

                            let event = RawEventBuilder::new(
                                sources::TERMINAL_KITTY,
                                event_types::event_types::terminal::COMMAND_EXECUTED,
                                serde_json::to_value(payload)?,
                            )
                            .with_orig_timestamp(cmd.ts_end_orig)
                            .build();

                            tx.send(event).await?;
                            
                            info!(
                                window_id = window.id,
                                command = %cmd.command_string,
                                "Captured terminal command"
                            );
                        }
                    }

                    // Update last command time
                    if let Some(last_cmd) = commands.last() {
                        self.last_command_times.insert(window.id, last_cmd.ts_end_orig);
                    }
                }
            }
            
            // Clean up entries for closed windows
            let closed_windows: Vec<u32> = self.last_command_times.keys()
                .filter(|id| !active_window_ids.contains(id))
                .cloned()
                .collect();
            
            for closed_id in closed_windows {
                debug!("Removing tracking for closed window: {}", closed_id);
                self.last_command_times.remove(&closed_id);
            }
            
            if self.last_command_times.len() > 100 {
                warn!("Tracking {} windows - possible memory leak?", self.last_command_times.len());
            }
        }

        Ok(())
    }

    /// Find Kitty sockets matching the pattern
    fn find_kitty_sockets(pattern: &str) -> Result<Vec<String>> {
        use glob::glob;
        
        debug!("Searching for Kitty sockets with pattern: {}", pattern);
        let mut sockets = Vec::new();
        
        // Use glob to properly expand the pattern
        for entry in glob(pattern).context("Failed to read glob pattern")? {
            match entry {
                Ok(path) => {
                    debug!("Found potential socket: {:?}", path);
                    // Verify it's a socket
                    match path.metadata() {
                        Ok(metadata) => {
                            if metadata.file_type().is_socket() {
                                if let Some(path_str) = path.to_str() {
                                    info!("Found valid Kitty socket: {}", path_str);
                                    sockets.push(path_str.to_string());
                                } else {
                                    warn!("Socket path contains invalid UTF-8: {:?}", path);
                                }
                            } else {
                                debug!("Path exists but is not a socket: {:?}", path);
                            }
                        }
                        Err(e) => {
                            warn!("Failed to get metadata for path {:?}: {}", path, e);
                        }
                    }
                }
                Err(e) => warn!("Error reading socket path: {}", e),
            }
        }
        
        info!("Found {} Kitty socket(s)", sockets.len());
        Ok(sockets)
    }

    /// Get list of windows from Kitty
    fn get_kitty_windows(socket: &str) -> Result<Vec<KittyWindow>> {
        debug!("Getting window list from socket: {}", socket);
        let output = Command::new("kitty")
            .arg("@")
            .arg("--to")
            .arg(format!("unix:{}", socket))
            .arg("ls")
            .output()
            .context("Failed to list Kitty windows")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!("Kitty command failed for socket {}: {}", socket, stderr);
            return Err(anyhow::anyhow!("Failed to get Kitty windows: {}", stderr));
        }

        // Parse JSON output
        let data: serde_json::Value = serde_json::from_slice(&output.stdout)
            .context("Failed to parse Kitty ls output")?;
        
        debug!(
            socket = %socket,
            window_count = data.as_array().map(|a| a.len()).unwrap_or(0),
            "Parsed Kitty window list"
        );

        let mut windows = Vec::new();
        
        if let Some(os_windows) = data.as_array() {
            for os_window in os_windows {
                if let Some(tabs) = os_window["tabs"].as_array() {
                    for tab in tabs {
                        if let Some(wins) = tab["windows"].as_array() {
                            for win in wins {
                                if let (Some(id), Some(pid), Some(cwd), Some(title)) = (
                                    win["id"].as_u64(),
                                    win["pid"].as_u64(),
                                    win["cwd"].as_str(),
                                    win["title"].as_str(),
                                ) {
                                    windows.push(KittyWindow {
                                        id: id as u32,
                                        pid: pid as u32,
                                        cwd: cwd.to_string(),
                                        title: title.to_string(),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(windows)
    }

    /// Get command history for a window (mock implementation)
    fn get_window_commands(_socket: &str, _window_id: u32) -> Result<Vec<CommandExecutedPayload>> {
        // Note: Kitty doesn't directly expose command history via remote control
        // This is a simplified implementation that could be enhanced with:
        // 1. Shell integration markers
        // 2. Terminal scrollback parsing
        // 3. Integration with shell history files
        
        // For now, we'll use a placeholder that could be extended
        warn!("Command history extraction from Kitty is limited - consider shell integration");
        
        // In a real implementation, you might:
        // 1. Use Kitty's scrollback_lines command to get recent output
        // 2. Parse shell prompt patterns
        // 3. Integrate with shell history files based on PID
        
        Ok(Vec::new())
    }
}

#[async_trait::async_trait]
impl SimpleIngestor for SimpleKittyIngestor {
    fn name() -> &'static str {
        "kitty-ingestor"
    }
    
    fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
    
    async fn capture_events(&mut self, event_tx: mpsc::Sender<RawEvent>) -> Result<()> {
        info!(
            agent_name = Self::name(),
            version = Self::version(),
            config.polling_interval = %self.config.polling_interval_secs,
            "Starting Kitty terminal capture"
        );
        
        let mut interval = time::interval(Duration::from_secs(self.config.polling_interval_secs));
        
        loop {
            interval.tick().await;
            
            if let Err(e) = self.poll_kitty_commands(&event_tx).await {
                error!("Error polling Kitty commands: {}", e);
                
                // Send error event
                let error = AgentError {
                    agent_name: Self::name().to_string(),
                    error_message: format!("Failed to poll Kitty commands: {}", e),
                    error_context: "command_polling".to_string(),
                    severity: ErrorSeverity::Warning,
                    original_event_id_if_related: None,
                };
                let _ = event_tx.send(create_error_event(error)).await;
            }
        }
    }
}