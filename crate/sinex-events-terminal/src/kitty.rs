use async_trait::async_trait;
use regex::Regex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_core::{
    EventType, EventSource, EventSourceBase, EventSourceContext, EventSender, 
    Result, EventFactory, ErrorContext, CoreError, BackoffHelper,
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

/// Kitty terminal command completion event (command + output)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KittyCommandCompleted;

impl EventType for KittyCommandCompleted {
    type Payload = KittyCommandCompletedPayload;
    type SourceImpl = KittyEventSource;
    const EVENT_NAME: &'static str = "command.completed";
}

/// Legacy command execution event (kept for backward compatibility)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KittyCommandExecuted;

impl EventType for KittyCommandExecuted {
    type Payload = KittyCommandExecutedPayload;
    type SourceImpl = KittyEventSource;
    const EVENT_NAME: &'static str = "command.started";
}


#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct KittyCommandCompletedPayload {
    pub command: String,
    pub command_output: String,
    pub working_directory: Option<String>,
    pub kitty_window_id: String,
    pub kitty_tab_id: String,
    pub exit_status: Option<i32>,
    pub execution_time_ms: Option<u64>,
    pub output_size_bytes: u64,
    pub output_line_count: u32,
    pub shell_integration_used: bool,
    pub completion_timestamp: String,
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
pub struct KittyProcessInfo {
    pub pid: u32,
    pub name: String,
    pub cmdline: Option<String>,
    pub parent_pid: Option<u32>,
}

/// Kitty scrollback incremental capture event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KittyScrollbackIncremental;

impl EventType for KittyScrollbackIncremental {
    type Payload = KittyScrollbackIncrementalPayload;
    type SourceImpl = KittyEventSource;
    const EVENT_NAME: &'static str = "content.streamed";
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct KittyScrollbackIncrementalPayload {
    pub kitty_window_id: String,
    pub new_lines: Vec<String>,
    pub line_start_offset: u32,
    pub capture_timestamp: String,
}

/// Kitty event source for comprehensive terminal monitoring
/// 
/// Architecture:
/// - Real-time command completion via shell integration (last_cmd_output)
/// - Incremental scrollback capture (new lines only) as safety net
/// - Tab focus change detection with real tab IDs
/// - Process change monitoring per window
pub struct KittyEventSource {
    socket_path: Option<String>,
    poll_interval: Duration,
    window_states: HashMap<String, KittyWindowState>,
    prompt_patterns: Vec<Regex>,
    last_scrollback_line_counts: HashMap<String, u32>,  // Track line count per window
    last_focused_tab: Option<String>,
    process_states: HashMap<String, KittyProcessInfo>,
    event_factory: EventFactory,
    operation_backoff: BackoffHelper,
}

#[derive(Debug, Clone)]
struct KittyWindowState {
    tab_id: String,
    last_command: Option<String>,
    last_prompt_time: Option<SystemTime>,
}



#[derive(Debug, Deserialize)]
struct KittyWindow {
    id: i64,
    cwd: Option<String>,
    foreground_processes: Vec<KittyProcess>,
    last_cmd_exit_status: Option<i32>,
    parent_tab_id: String,
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
            poll_interval: Duration::from_millis(500),
            window_states: HashMap::new(),
            prompt_patterns: Self::create_prompt_patterns(),
            last_scrollback_line_counts: HashMap::new(),
            last_focused_tab: None,
            process_states: HashMap::new(),
            event_factory: EventFactory::new("terminal.kitty"),
            operation_backoff: BackoffHelper::new()
                .with_initial_delay(Duration::from_millis(500))
                .with_max_delay(Duration::from_secs(60))
                .with_multiplier(2),
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
        // Try common socket locations with configurable tmp directory
        let tmp_dir = std::env::var("SINEX_TMP_DIR").unwrap_or_else(|_| "/tmp".to_string());
        let current_uid = unsafe { libc::getuid() };
        let socket_candidates = vec![
            format!("{}/kitty_socket_{}", tmp_dir, std::process::id()),
            format!("{}/kitty-{}.sock", tmp_dir, whoami::username()),
            format!("/run/user/{}/kitty.sock", current_uid),
            format!("{}/kitty.sock", tmp_dir),
        ];

        for candidate in &socket_candidates {
            if Path::new(&candidate).exists() {
                // Test connection
                if UnixStream::connect(&candidate).await.is_ok() {
                    self.socket_path = Some(candidate.clone());
                    return Ok(candidate.clone());
                }
            }
        }

        Err(ErrorContext::new(CoreError::Io("No accessible Kitty socket found".to_string()))
            .with_operation("discover_kitty_socket")
            .with_context("attempted_paths", format!("{:?}", socket_candidates))
            .build())
    }

    async fn send_kitty_command(&self, command: serde_json::Value) -> Result<serde_json::Value> {
        let socket_path = self.socket_path
            .as_ref()
            .ok_or_else(|| ErrorContext::new(CoreError::Configuration("No Kitty socket configured".to_string()))
                .with_operation("send_kitty_command")
                .build())?;

        let mut stream = UnixStream::connect(socket_path)
            .await
            .map_err(|e| ErrorContext::new(CoreError::Io(format!("Failed to connect to socket: {}", e)))
                .with_operation("send_kitty_command")
                .with_context("socket_path", socket_path)
                .build())?;

        let cmd_str = command.to_string();
        let framed_cmd = format!("\x1bP@kitty-cmd{}\x1b\\", cmd_str);

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        
        stream.write_all(framed_cmd.as_bytes()).await?;
        stream.flush().await?;

        let mut response_buffer = Vec::new();
        stream.read_to_end(&mut response_buffer).await?;

        let response_buffer_len = response_buffer.len();
        let response_str = String::from_utf8(response_buffer)
            .map_err(|e| ErrorContext::new(CoreError::Serialization(format!("Invalid UTF-8 in response: {}", e)))
                .with_operation("send_kitty_command")
                .with_context("response_length", response_buffer_len.to_string())
                .build())?;

        // Extract JSON from framed response
        if let Some(start) = response_str.find('{') {
            if let Some(end) = response_str.rfind('}') {
                let json_str = &response_str[start..=end];
                return Ok(serde_json::from_str(json_str)?);
            }
        }

        Err(ErrorContext::new(CoreError::Serialization("Could not parse Kitty response as JSON".to_string()))
            .with_operation("send_kitty_command")
            .with_context("response_preview", response_str.chars().take(100).collect::<String>())
            .build())
    }


    async fn get_kitty_tabs_and_windows(&self) -> Result<(Vec<(String, String, u32, bool)>, Vec<KittyWindow>)> {
        let ls_command = serde_json::json!({"cmd": "ls"});
        let response = self.send_kitty_command(ls_command).await?;
        
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
                    tabs.push((
                        tab_id_str.clone(),
                        tab_title.to_string(),
                        tab_index as u32,
                        is_focused
                    ));
                
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


    async fn get_last_command_output(&self, window_id: &str) -> Result<String> {
        self.get_kitty_text(window_id, "last_cmd_output").await
    }

    async fn get_kitty_text(&self, window_id: &str, extent: &str) -> Result<String> {
        let get_text_command = serde_json::json!({
            "cmd": "get-text",
            "match": format!("id:{}", window_id),
            "extent": extent
        });

        let response = self.send_kitty_command(get_text_command).await?;
        
        if let Some(text) = response.get("text").and_then(|t| t.as_str()) {
            Ok(text.to_string())
        } else {
            Err(sinex_core::CoreError::Other(format!("No text content in Kitty response for extent: {}", extent)))
        }
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
                    kitty_tab_id: window.parent_tab_id.clone(),
                    previous_process,
                    current_process: current_process_info.clone(),
                    change_timestamp: chrono::Utc::now().to_rfc3339(),
                    working_directory: window.cwd.clone(),
                };
                
                let process_event = self.event_factory.create_event(
                    "process.changed",
                    serde_json::to_value(process_payload)?,
                );
                
                tx.send(process_event).await
                    .map_err(|e| ErrorContext::new(CoreError::Io(format!("Channel send failed: {}", e)))
                        .with_operation("process_window_commands")
                        .with_context("event_type", "process.changed")
                        .with_context("window_id", &window_id)
                        .build())?;
                
                // Update stored process state
                self.process_states.insert(window_id.clone(), current_process_info);
            }
        }
        
        // Try to get last command output using shell integration
        if let Ok(last_output) = self.get_last_command_output(&window_id).await {
            if !last_output.trim().is_empty() {
                let window_state = self.window_states
                    .entry(window_id.clone())
                    .or_insert_with(|| KittyWindowState {
                        tab_id: window.parent_tab_id.clone(),
                        last_command: None,
                        last_prompt_time: None,
                    });
                
                // Always capture command output - no deduplication
                if !last_output.trim().is_empty() {
                    // Try to extract command from the output (look for prompt patterns)
                    let extracted_command = KittyEventSource::extract_command_from_output(&self.prompt_patterns, &last_output);
                    
                    if let Some(command_text) = extracted_command {
                        // Create command completion event with both command and output
                        let completion_payload = KittyCommandCompletedPayload {
                            command: command_text.clone(),
                            command_output: last_output.clone(),
                            working_directory: window.cwd.clone(),
                            kitty_window_id: window_id.clone(),
                            kitty_tab_id: window_state.tab_id.clone(),
                            exit_status: window.last_cmd_exit_status,
                            execution_time_ms: None, // Requires tracking command start times - not implemented
                            output_size_bytes: last_output.len() as u64,
                            output_line_count: last_output.lines().count() as u32,
                            shell_integration_used: true,
                            completion_timestamp: chrono::Utc::now().to_rfc3339(),
                        };
                        
                        let completion_event = self.event_factory.create_event(
                            "command.completed",
                            serde_json::to_value(completion_payload)?,
                        );
                        
                        tx.send(completion_event).await
                            .map_err(|e| ErrorContext::new(CoreError::Io(format!("Channel send failed: {}", e)))
                                .with_operation("process_window_commands")
                                .with_context("event_type", "command.completed")
                                .with_context("window_id", &window_id)
                                .with_context("command", &command_text)
                                .build())?;
                        
                        // Update state
                        window_state.last_command = Some(command_text);
                        window_state.last_prompt_time = Some(SystemTime::now());
                    }
                }
            }
        }
        
        Ok(())
    }
    
    fn extract_command_from_output(prompt_patterns: &[Regex], output: &str) -> Option<String> {
        // Look for the last prompt line in the output to extract command
        let lines: Vec<&str> = output.lines().collect();
        
        // Search from the end for a prompt pattern
        for line in lines.iter().rev() {
            for pattern in prompt_patterns {
                if let Some(captures) = pattern.captures(line) {
                    if let Some(command) = captures.get(1) {
                        let cmd = command.as_str().trim();
                        if !cmd.is_empty() {
                            return Some(cmd.to_string());
                        }
                    }
                }
            }
        }
        
        None
    }


    async fn process_tab_focus_changes(&mut self, tabs: Vec<(String, String, u32, bool)>, tx: &EventSender) -> Result<()> {
        let timestamp = chrono::Utc::now().to_rfc3339();
        
        // Check for focus changes - just emit events when focus changes
        let current_focused = tabs.iter().find(|(_, _, _, is_focused)| *is_focused);
        if let Some((focused_tab_id, title, index, _)) = current_focused {
            if self.last_focused_tab.as_ref() != Some(focused_tab_id) {
                let previous_tab_id = self.last_focused_tab.clone();
                
                // Emit tab focused event
                let tab_focused_payload = KittyTabFocusedPayload {
                    kitty_tab_id: focused_tab_id.clone(),
                    kitty_window_id: "unknown".to_string(),
                    tab_title: title.clone(),
                    tab_index: *index,
                    previous_tab_id,
                    focus_timestamp: timestamp,
                };
                
                let tab_focused_event = self.event_factory.create_event(
                    "tab.focused",
                    serde_json::to_value(tab_focused_payload)?,
                );
                
                tx.send(tab_focused_event).await
                    .map_err(|e| ErrorContext::new(CoreError::Io(format!("Channel send failed: {}", e)))
                        .with_operation("process_tab_focus_changes")
                        .with_context("event_type", "tab.focused")
                        .with_context("tab_id", focused_tab_id)
                        .build())?;
                
                // Update last focused tab
                self.last_focused_tab = Some(focused_tab_id.clone());
            }
        }
        
        Ok(())
    }

    async fn capture_incremental_scrollback(&mut self, window: &KittyWindow, tx: &EventSender) -> Result<()> {
        let window_id = window.id.to_string();
        
        // Get current scrollback content
        let scrollback = self.get_kitty_text(&window_id, "all").await?;
        let current_lines: Vec<&str> = scrollback.lines().collect();
        let current_line_count = current_lines.len() as u32;
        
        // Get previous line count for this window
        let previous_line_count = self.last_scrollback_line_counts
            .get(&window_id)
            .copied()
            .unwrap_or(0);
        
        // Only capture if we have new lines
        if current_line_count > previous_line_count {
            let new_line_start = previous_line_count as usize;
            let new_lines: Vec<String> = current_lines[new_line_start..]
                .iter()
                .map(|s| s.to_string())
                .collect();
            
            let incremental_payload = KittyScrollbackIncrementalPayload {
                kitty_window_id: window_id.clone(),
                new_lines,
                line_start_offset: previous_line_count,
                capture_timestamp: chrono::Utc::now().to_rfc3339(),
            };

            let scrollback_event = self.event_factory.create_event(
                "scrollback.incremental",
                serde_json::to_value(incremental_payload)?,
            );

            tx.send(scrollback_event).await
                .map_err(|e| ErrorContext::new(CoreError::Io(format!("Channel send failed: {}", e)))
                    .with_operation("capture_incremental_scrollback")
                    .with_context("event_type", "scrollback.incremental")
                    .with_context("window_id", &window_id)
                    .with_context("line_count", current_line_count.to_string())
                    .build())?;
            
            tracing::debug!("Captured {} new lines for window {}", current_line_count - previous_line_count, window_id);
        }
        
        // Update stored line count for this window
        self.last_scrollback_line_counts.insert(window_id, current_line_count);
        
        Ok(())
    }
}

// Implement EventSourceBase for common functionality
impl EventSourceBase for KittyEventSource {}

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


        let mut last_scrollback_capture = SystemTime::now();
        let scrollback_interval = Duration::from_secs(180); // 3 minute intervals for incremental scrollback

        loop {
            match self.get_kitty_tabs_and_windows().await {
                Ok((tabs, windows)) => {
                    // Process tab focus changes
                    if let Err(e) = self.process_tab_focus_changes(tabs, &tx).await {
                        tracing::error!("Failed to process tab focus changes: {}", e);
                    }
                    
                    // Process windows
                    for window in windows {
                        // Process command completions (real-time with shell integration)
                        if let Err(e) = self.process_window_commands(&window, &tx).await {
                            tracing::error!("Failed to process window {}: {}", window.id, e);
                        }
                        
                        // Capture incremental scrollback every 3 minutes (safety net)
                        if last_scrollback_capture.elapsed().unwrap_or(Duration::ZERO) >= scrollback_interval {
                            if let Err(e) = self.capture_incremental_scrollback(&window, &tx).await {
                                tracing::error!("Failed to capture incremental scrollback for window {}: {}", window.id, e);
                            }
                        }
                    }
                    
                    // Update scrollback capture timestamp
                    if last_scrollback_capture.elapsed().unwrap_or(Duration::ZERO) >= scrollback_interval {
                        last_scrollback_capture = SystemTime::now();
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to get Kitty windows (current backoff: {:?}): {}", 
                                   self.operation_backoff.current_delay(), e);
                    
                    // Try to rediscover socket
                    if let Err(rediscover_err) = self.discover_kitty_socket().await {
                        tracing::warn!("Failed to rediscover Kitty socket: {}", rediscover_err);
                    }
                    
                    // Use exponential backoff for failures
                    self.operation_backoff.wait().await;
                    continue; // Skip the normal sleep
                }
            }

            // Reset backoff on successful operation
            self.operation_backoff.reset();
            sleep(self.poll_interval).await;
        }
    }
}

impl Default for KittyEventSource {
    fn default() -> Self {
        Self::new()
    }
}