use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_core::{EventSender, JsonValue, Timestamp};
use std::path::PathBuf;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error, info, warn};

use sinex_core::{EventType, EventSource, EventSourceContext, EventSourceBase, Result, event_type_constants, sources, RawEvent};

// ============================================================================
// Event Payloads
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CommandExecutedPayload {
    pub command_string: String,
    pub cwd: String,
    pub exit_code: i32,
    pub ts_start_orig: Timestamp,
    pub ts_end_orig: Timestamp,
}

// ============================================================================
// Event Types
// ============================================================================

pub struct CommandExecuted;
impl EventType for CommandExecuted {
    type Payload = CommandExecutedPayload;
    type SourceImpl = KittySocketListener;
    const EVENT_NAME: &'static str = event_type_constants::terminal::COMMAND_EXECUTED;
}

// ============================================================================
// Event Source
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KittyConfig {
    pub socket_path: String,
    pub polling_interval_secs: u64,
}

impl Default for KittyConfig {
    fn default() -> Self {
        Self {
            socket_path: "/tmp/kitty".to_string(),
            polling_interval_secs: 2,
        }
    }
}

/// Kitty window information
#[derive(Debug, Clone)]
struct KittyWindow {
    id: u32,
    #[allow(dead_code)]
    pid: u32,
    #[allow(dead_code)]
    cwd: String,
    #[allow(dead_code)]
    title: String,
}

pub struct KittySocketListener {
    config: KittyConfig,
    last_command_times: Arc<Mutex<HashMap<u32, Timestamp>>>,
}

// Implement EventSourceBase to get common functionality
impl EventSourceBase for KittySocketListener {}

#[async_trait]
impl EventSource for KittySocketListener {
    type Config = KittyConfig;
    
    const SOURCE_NAME: &'static str = sources::TERMINAL_KITTY;
    
    async fn initialize(ctx: EventSourceContext) -> Result<Self> {
        // Use base trait for config parsing
        let config = <Self as EventSourceBase>::parse_config::<Self::Config>(&ctx).await?;
        
        info!(
            socket_path = ?config.socket_path,
            "Initializing Kitty socket listener"
        );
        Self::new(config).await
    }
    
    async fn stream_events(&mut self, tx: EventSender) -> Result<()> {
        info!(
            socket_path = ?self.config.socket_path,
            polling_interval = self.config.polling_interval_secs,
            "Starting Kitty terminal event source"
        );
        
        let mut interval = time::interval(Duration::from_secs(self.config.polling_interval_secs));
        
        loop {
            interval.tick().await;
            
            if let Err(e) = self.poll_kitty_commands(&tx).await {
                error!("Error polling Kitty commands: {}", e);
                // Continue polling despite errors
            }
        }
    }
}

impl KittySocketListener {
    async fn new(config: KittyConfig) -> Result<Self> {
        Ok(Self { 
            config,
            last_command_times: Arc::new(Mutex::new(HashMap::new())),
        })
    }
    
    async fn poll_kitty_commands(&self, tx: &EventSender) -> Result<()> {
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
                    warn!("Failed to get windows from socket {}: {}", socket, e);
                    continue;
                }
            };
            
            // Track active window IDs for cleanup
            let active_window_ids: Vec<u32> = windows.iter().map(|w| w.id).collect();
            
            for window in windows {
                // Get command history for this window
                if let Ok(commands) = Self::get_window_commands(&socket, window.id) {
                    let now = Utc::now();
                    let last_time = {
                        let times = self.last_command_times.lock().unwrap();
                        times.get(&window.id).cloned().unwrap_or(now - chrono::Duration::hours(1))
                    };

                    for cmd in &commands {
                        if cmd.ts_end_orig > last_time {
                            debug!(
                                window_id = window.id,
                                command = %cmd.command_string,
                                exit_code = cmd.exit_code,
                                "New command detected"
                            );
                            
                            let event = self.create_event(
                                event_type_constants::terminal::COMMAND_EXECUTED,
                                serde_json::to_value(cmd)?
                            );

                            tx.send(event).await.map_err(|_| sinex_core::CoreError::Other("Channel closed".to_string()))?;
                            
                            info!(
                                window_id = window.id,
                                command = %cmd.command_string,
                                "Captured terminal command"
                            );
                        }
                    }

                    // Update last command time
                    if let Some(last_cmd) = commands.last() {
                        let mut times = self.last_command_times.lock().unwrap();
                        times.insert(window.id, last_cmd.ts_end_orig);
                    }
                }
            }
            
            // Clean up entries for closed windows
            {
                let mut times = self.last_command_times.lock().unwrap();
                times.retain(|id, _| active_window_ids.contains(id));
                
                if times.len() > 100 {
                    warn!("Tracking {} windows - possible memory leak?", times.len());
                }
            }
        }

        Ok(())
    }

    fn find_kitty_sockets(pattern: &str) -> Result<Vec<String>> {
        use glob::glob;
        
        debug!("Searching for Kitty sockets with pattern: {}", pattern);
        let mut sockets = Vec::new();
        
        for entry in glob(pattern).map_err(|e| sinex_core::CoreError::Other(format!("Failed to read glob pattern: {}", e)))? {
            match entry {
                Ok(path) => {
                    // Verify it's a socket
                    match path.metadata() {
                        Ok(metadata) => {
                            #[cfg(unix)]
                            {
                                use std::os::unix::fs::FileTypeExt;
                                if metadata.file_type().is_socket() {
                                    if let Some(path_str) = path.to_str() {
                                        info!("Found valid Kitty socket: {}", path_str);
                                        sockets.push(path_str.to_string());
                                    }
                                }
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

    fn get_kitty_windows(socket: &str) -> Result<Vec<KittyWindow>> {
        use std::process::Command;
        
        debug!("Getting window list from socket: {}", socket);
        let output = Command::new("kitty")
            .arg("@")
            .arg("--to")
            .arg(format!("unix:{}", socket))
            .arg("ls")
            .output()
            .map_err(|e| sinex_core::CoreError::Other(format!("Failed to list Kitty windows: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(sinex_core::CoreError::Other(format!("Failed to get Kitty windows: {}", stderr)));
        }

        // Parse JSON output
        let data: JsonValue = serde_json::from_slice(&output.stdout)
            .map_err(|e| sinex_core::CoreError::Other(format!("Failed to parse Kitty ls output: {}", e)))?;
        
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

    fn get_window_commands(_socket: &str, _window_id: u32) -> Result<Vec<CommandExecutedPayload>> {
        // Note: Kitty doesn't directly expose command history via remote control
        // This is a simplified implementation that could be enhanced with:
        // 1. Shell integration markers
        // 2. Terminal scrollback parsing
        // 3. Integration with shell history files
        
        warn!("Command history extraction from Kitty is limited - consider shell integration");
        
        Ok(Vec::new())
    }
}

// Alternative source for command execution (example of multiple sources)
pub struct BashHistoryWatcher {
    #[allow(dead_code)]
    history_file: PathBuf,
}

#[async_trait]
impl EventSource for BashHistoryWatcher {
    type Config = PathBuf; // Just the history file path
    
    const SOURCE_NAME: &'static str = "terminal.bash_history";
    
    async fn initialize(ctx: EventSourceContext) -> Result<Self> {
        let config: Self::Config = serde_json::from_value(ctx.config)
            .map_err(|e| sinex_core::CoreError::Configuration(format!("Failed to parse config: {}", e)))?;
        
        Ok(Self { history_file: config })
    }
    
    async fn stream_events(&mut self, _tx: EventSender) -> Result<()> {
        // Watch bash history file for changes
        Ok(())
    }
}