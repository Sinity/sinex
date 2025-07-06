use async_trait::async_trait;
use regex::Regex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_core::{
    EventType, EventSource, EventSourceContext, EventSender, RawEventBuilder, Result,
};
use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, SystemTime};
use tokio::net::UnixStream;
use tokio::time::sleep;

/// Configuration for Kitty event source
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KittyConfig {
    pub poll_interval_seconds: u64,
    pub socket_path: Option<String>,
    pub enabled: bool,
}

impl Default for KittyConfig {
    fn default() -> Self {
        Self {
            poll_interval_seconds: 5,
            socket_path: None,
            enabled: true,
        }
    }
}

/// Kitty terminal command execution event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KittyCommandExecuted;

impl EventType for KittyCommandExecuted {
    type Payload = KittyCommandExecutedPayload;
    type SourceImpl = KittyEventSource;
    const EVENT_NAME: &'static str = "command.executed";
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct KittyCommandExecutedPayload {
    pub command: String,
    pub working_directory: Option<String>,
    pub kitty_window_id: String,
    pub kitty_tab_id: String,
    pub exit_status: Option<i32>,
    pub execution_time_ms: Option<u64>,
    pub prompt_detected: bool,
    pub scrollback_hash: Option<String>,
}

/// Kitty scrollback buffer capture event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KittyScrollbackCaptured;

impl EventType for KittyScrollbackCaptured {
    type Payload = KittyScrollbackCapturedPayload;
    type SourceImpl = KittyEventSource;
    const EVENT_NAME: &'static str = "scrollback.captured";
}

/// Kitty tab created event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KittyTabCreated;

impl EventType for KittyTabCreated {
    type Payload = KittyTabCreatedPayload;
    type SourceImpl = KittyEventSource;
    const EVENT_NAME: &'static str = "tab.created";
}

/// Kitty tab focused event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KittyTabFocused;

impl EventType for KittyTabFocused {
    type Payload = KittyTabFocusedPayload;
    type SourceImpl = KittyEventSource;
    const EVENT_NAME: &'static str = "tab.focused";
}

/// Kitty tab closed event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KittyTabClosed;

impl EventType for KittyTabClosed {
    type Payload = KittyTabClosedPayload;
    type SourceImpl = KittyEventSource;
    const EVENT_NAME: &'static str = "tab.closed";
}

/// Kitty process changed event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KittyProcessChanged;

impl EventType for KittyProcessChanged {
    type Payload = KittyProcessChangedPayload;
    type SourceImpl = KittyEventSource;
    const EVENT_NAME: &'static str = "process.changed";
}

/// Kitty configuration changed event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KittyConfigChanged;

impl EventType for KittyConfigChanged {
    type Payload = KittyConfigChangedPayload;
    type SourceImpl = KittyEventSource;
    const EVENT_NAME: &'static str = "config.changed";
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct KittyScrollbackCapturedPayload {
    pub kitty_window_id: String,
    pub content_hash: String,
    pub line_count: u32,
    pub scrollback_size_bytes: u64,
    pub capture_timestamp: String,
    pub content_preview: String, // First 200 chars for debugging
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct KittyTabCreatedPayload {
    pub kitty_tab_id: String,
    pub kitty_window_id: String,
    pub tab_title: String,
    pub tab_index: u32,
    pub is_active: bool,
    pub creation_timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct KittyTabFocusedPayload {
    pub kitty_tab_id: String,
    pub kitty_window_id: String,
    pub tab_title: String,
    pub tab_index: u32,
    pub previous_tab_id: Option<String>,
    pub focus_timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct KittyTabClosedPayload {
    pub kitty_tab_id: String,
    pub kitty_window_id: String,
    pub tab_title: String,
    pub tab_index: u32,
    pub was_active: bool,
    pub closure_timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct KittyProcessChangedPayload {
    pub kitty_window_id: String,
    pub kitty_tab_id: String,
    pub previous_process: Option<KittyProcessInfo>,
    pub current_process: KittyProcessInfo,
    pub change_timestamp: String,
    pub working_directory: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct KittyConfigChangedPayload {
    pub change_type: String, // "font_size", "color_scheme", "opacity", "other"
    pub setting_name: String,
    pub previous_value: Option<String>,
    pub current_value: String,
    pub change_timestamp: String,
    pub affected_windows: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct KittyProcessInfo {
    pub pid: u32,
    pub name: String,
    pub cmdline: Option<String>,
    pub parent_pid: Option<u32>,
}

/// Kitty event source for comprehensive terminal monitoring
pub struct KittyEventSource {
    socket_path: Option<String>,
    poll_interval: Duration,
    window_states: HashMap<String, KittyWindowState>,
    tab_states: HashMap<String, KittyTabState>,
    process_states: HashMap<String, KittyProcessInfo>,
    prompt_patterns: Vec<Regex>,
    last_scrollback_hashes: HashMap<String, String>,
    last_focused_tab: Option<String>,
    last_config_hash: Option<String>,
}

#[derive(Debug, Clone)]
struct KittyWindowState {
    window_id: String,
    tab_id: String,
    last_command: Option<String>,
    working_directory: Option<String>,
    last_prompt_time: Option<SystemTime>,
    command_start_time: Option<SystemTime>,
}

#[derive(Debug, Clone)]
struct KittyTabState {
    tab_id: String,
    window_id: String,
    title: String,
    index: u32,
    is_active: bool,
    creation_time: Option<SystemTime>,
    last_focus_time: Option<SystemTime>,
}

#[derive(Debug, Deserialize)]
struct KittyLsResponse {
    tabs: Vec<KittyTab>,
}

#[derive(Debug, Deserialize)]
struct KittyTab {
    id: i64,
    windows: Vec<KittyWindow>,
}

#[derive(Debug, Deserialize)]
struct KittyWindow {
    id: i64,
    title: String,
    cwd: Option<String>,
    foreground_processes: Vec<KittyProcess>,
}

#[derive(Debug, Deserialize)]
struct KittyProcess {
    pid: u32,
    name: String,
}

impl KittyEventSource {
    pub fn new() -> Self {
        Self {
            socket_path: None,
            poll_interval: Duration::from_secs(2),
            window_states: HashMap::new(),
            tab_states: HashMap::new(),
            process_states: HashMap::new(),
            prompt_patterns: Self::create_prompt_patterns(),
            last_scrollback_hashes: HashMap::new(),
            last_focused_tab: None,
            last_config_hash: None,
        }
    }

    fn create_prompt_patterns() -> Vec<Regex> {
        vec![
            // Basic bash/zsh prompts
            Regex::new(r"^\$ (.+)$").unwrap(),
            Regex::new(r"^# (.+)$").unwrap(),
            
            // Starship prompt
            Regex::new(r"^❯ (.+)$").unwrap(),
            
            // Oh-my-zsh variations
            Regex::new(r"^➜\s+[^\s]+\s+(.+)$").unwrap(),
            Regex::new(r"^.*%\s+(.+)$").unwrap(),
            
            // Fish shell
            Regex::new(r"^.*>\s+(.+)$").unwrap(),
            
            // Custom prompts with timestamps
            Regex::new(r"^\[\d{2}:\d{2}:\d{2}\].*\$\s+(.+)$").unwrap(),
        ]
    }

    async fn discover_kitty_socket(&mut self) -> Result<String> {
        // Try common socket locations
        let socket_candidates = vec![
            format!("/tmp/kitty_socket_{}", std::process::id()),
            format!("/tmp/kitty-{}.sock", whoami::username()),
            "/run/user/1000/kitty.sock".to_string(), // Common UID for main user
            "/tmp/kitty.sock".to_string(),
        ];

        for candidate in socket_candidates {
            if Path::new(&candidate).exists() {
                // Test connection
                if let Ok(_) = UnixStream::connect(&candidate).await {
                    self.socket_path = Some(candidate.clone());
                    return Ok(candidate);
                }
            }
        }

        Err(sinex_core::CoreError::Other("No accessible Kitty socket found".to_string()))
    }

    async fn send_kitty_command(&self, command: serde_json::Value) -> Result<serde_json::Value> {
        let socket_path = self.socket_path
            .as_ref()
            .ok_or_else(|| sinex_core::CoreError::Other("No Kitty socket configured".to_string()))?;

        let mut stream = UnixStream::connect(socket_path)
            .await
            .map_err(|e| sinex_core::CoreError::Io(format!("Failed to connect to Kitty socket: {}", e)))?;

        let cmd_str = command.to_string();
        let framed_cmd = format!("\x1bP@kitty-cmd{}\x1b\\", cmd_str);

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        
        stream.write_all(framed_cmd.as_bytes()).await?;
        stream.flush().await?;

        let mut response_buffer = Vec::new();
        stream.read_to_end(&mut response_buffer).await?;

        let response_str = String::from_utf8(response_buffer)
            .map_err(|e| sinex_core::CoreError::Other(format!("Invalid UTF-8 in Kitty response: {}", e)))?;

        // Extract JSON from framed response
        if let Some(start) = response_str.find('{') {
            if let Some(end) = response_str.rfind('}') {
                let json_str = &response_str[start..=end];
                return Ok(serde_json::from_str(json_str)?);
            }
        }

        Err(sinex_core::CoreError::Other(format!("Could not parse Kitty response: {}", response_str)))
    }

    async fn get_kitty_windows(&self) -> Result<Vec<KittyWindow>> {
        let ls_command = serde_json::json!({"cmd": "ls"});
        let response = self.send_kitty_command(ls_command).await?;
        
        // Parse the response to extract window information
        let mut windows = Vec::new();
        
        if let Some(tabs) = response.as_array() {
            for tab in tabs {
                if let Some(tab_windows) = tab.get("windows").and_then(|w| w.as_array()) {
                    for window in tab_windows {
                        if let (Some(id), Some(title)) = (
                            window.get("id").and_then(|i| i.as_i64()),
                            window.get("title").and_then(|t| t.as_str())
                        ) {
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
                                title: title.to_string(),
                                cwd: window.get("cwd").and_then(|c| c.as_str()).map(String::from),
                                foreground_processes,
                            });
                        }
                    }
                }
            }
        }
        
        Ok(windows)
    }

    async fn get_scrollback_content(&self, window_id: &str) -> Result<String> {
        let get_text_command = serde_json::json!({
            "cmd": "get-text",
            "match": format!("id:{}", window_id),
            "extent": "scrollback"
        });

        let response = self.send_kitty_command(get_text_command).await?;
        
        if let Some(text) = response.get("text").and_then(|t| t.as_str()) {
            Ok(text.to_string())
        } else {
            Err(sinex_core::CoreError::Other("No text content in Kitty response".to_string()))
        }
    }

    fn parse_commands_from_scrollback(&self, content: &str) -> Vec<String> {
        let mut commands = Vec::new();
        
        for line in content.lines() {
            for pattern in &self.prompt_patterns {
                if let Some(captures) = pattern.captures(line) {
                    if let Some(command) = captures.get(1) {
                        let cmd = command.as_str().trim();
                        if !cmd.is_empty() && !commands.contains(&cmd.to_string()) {
                            commands.push(cmd.to_string());
                        }
                    }
                }
            }
        }
        
        commands
    }

    async fn process_window_commands(&mut self, window: &KittyWindow, tx: &EventSender) -> Result<()> {
        let window_id = window.id.to_string();
        
        // Process changed if foreground processes changed
        if let Some(current_process) = window.foreground_processes.first() {
            let current_process_info = KittyProcessInfo {
                pid: current_process.pid,
                name: current_process.name.clone(),
                cmdline: None, // Would need additional call to get cmdline
                parent_pid: None,
            };
            
            let process_changed = self.process_states
                .get(&window_id)
                .map(|prev| prev.pid != current_process_info.pid || prev.name != current_process_info.name)
                .unwrap_or(true);
            
            if process_changed {
                let previous_process = self.process_states.get(&window_id).cloned();
                
                let process_payload = KittyProcessChangedPayload {
                    kitty_window_id: window_id.clone(),
                    kitty_tab_id: "0".to_string(), // Would need actual tab ID
                    previous_process,
                    current_process: current_process_info.clone(),
                    change_timestamp: chrono::Utc::now().to_rfc3339(),
                    working_directory: window.cwd.clone(),
                };
                
                let process_event = RawEventBuilder::new(
                    "terminal.kitty",
                    "process.changed",
                    serde_json::to_value(process_payload)?,
                ).build();
                
                tx.send(process_event).await
                    .map_err(|e| sinex_core::CoreError::Other(format!("Failed to send process change event: {}", e)))?;
                
                // Update stored process state
                self.process_states.insert(window_id.clone(), current_process_info);
            }
        }
        
        // Get scrollback content
        let scrollback = self.get_scrollback_content(&window_id).await?;
        
        // Calculate content hash
        let content_hash = blake3::hash(scrollback.as_bytes()).to_hex().to_string();
        
        // Check if scrollback changed
        let scrollback_changed = self.last_scrollback_hashes
            .get(&window_id)
            .map(|hash| hash != &content_hash)
            .unwrap_or(true);
        
        if scrollback_changed {
            // Emit scrollback capture event
            let scrollback_payload = KittyScrollbackCapturedPayload {
                kitty_window_id: window_id.clone(),
                content_hash: content_hash.clone(),
                line_count: scrollback.lines().count() as u32,
                scrollback_size_bytes: scrollback.len() as u64,
                capture_timestamp: chrono::Utc::now().to_rfc3339(),
                content_preview: scrollback.chars().take(200).collect(),
            };

            let scrollback_event = RawEventBuilder::new(
                "terminal.kitty",
                "scrollback.captured",
                serde_json::to_value(scrollback_payload)?,
            ).build();

            tx.send(scrollback_event).await
                .map_err(|e| sinex_core::CoreError::Other(format!("Failed to send scrollback capture event: {}", e)))?;

            // Update stored hash
            self.last_scrollback_hashes.insert(window_id.clone(), content_hash.clone());
        }

        // Parse commands from scrollback
        let commands = self.parse_commands_from_scrollback(&scrollback);
        
        // Get or create window state
        let window_state = self.window_states
            .entry(window_id.clone())
            .or_insert_with(|| KittyWindowState {
                window_id: window_id.clone(),
                tab_id: "0".to_string(), // Would need to get actual tab ID
                last_command: None,
                working_directory: window.cwd.clone(),
                last_prompt_time: None,
                command_start_time: None,
            });

        // Check for new commands
        for command in commands {
            if window_state.last_command.as_ref() != Some(&command) {
                // New command detected
                let command_payload = KittyCommandExecutedPayload {
                    command: command.clone(),
                    working_directory: window.cwd.clone(),
                    kitty_window_id: window_id.clone(),
                    kitty_tab_id: window_state.tab_id.clone(),
                    exit_status: None, // Would need process monitoring for this
                    execution_time_ms: None,
                    prompt_detected: true,
                    scrollback_hash: Some(content_hash.clone()),
                };

                let command_event = RawEventBuilder::new(
                    "terminal.kitty",
                    "command.executed",
                    serde_json::to_value(command_payload)?,
                ).build();

                tx.send(command_event).await
                    .map_err(|e| sinex_core::CoreError::Other(format!("Failed to send command execution event: {}", e)))?;

                // Update window state
                window_state.last_command = Some(command);
                window_state.last_prompt_time = Some(SystemTime::now());
            }
        }

        Ok(())
    }
}

#[async_trait]
impl EventSource for KittyEventSource {
    type Config = KittyConfig;
    const SOURCE_NAME: &'static str = "terminal.kitty";

    async fn initialize(_ctx: EventSourceContext) -> Result<Self>
    where
        Self: Sized,
    {
        let mut source = Self::new();
        
        // Try to discover Kitty socket
        if let Err(e) = source.discover_kitty_socket().await {
            tracing::warn!("Failed to discover Kitty socket: {}. Kitty monitoring will be limited.", e);
        } else {
            tracing::info!("Kitty socket discovered at: {:?}", source.socket_path);
        }

        Ok(source)
    }

    async fn stream_events(&mut self, tx: EventSender) -> Result<()> {
        if self.socket_path.is_none() {
            tracing::warn!("No Kitty socket available, skipping Kitty event streaming");
            return Ok(());
        }

        loop {
            match self.get_kitty_windows().await {
                Ok(windows) => {
                    for window in windows {
                        if let Err(e) = self.process_window_commands(&window, &tx).await {
                            tracing::error!("Failed to process window {}: {}", window.id, e);
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to get Kitty windows: {}", e);
                    
                    // Try to rediscover socket
                    if let Err(rediscover_err) = self.discover_kitty_socket().await {
                        tracing::warn!("Failed to rediscover Kitty socket: {}", rediscover_err);
                    }
                }
            }

            sleep(self.poll_interval).await;
        }
    }
}

impl Default for KittyEventSource {
    fn default() -> Self {
        Self::new()
    }
}