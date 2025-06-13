use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error, info};
use std::collections::HashMap;

use sinex_core::{EventType, EventSource, EventSourceContext, Result};
use sinex_db::models::RawEvent;
use sqlx::{PgPool, Row};

// ============================================================================
// Event Payloads
// ============================================================================

/// Terminal scrollback captured
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TerminalScrollbackCapturedPayload {
    pub window_id: u32,
    pub terminal_type: String, // "kitty"
    pub cwd: String,
    pub window_title: String,
    pub scrollback_text: String,
    pub scrollback_lines: usize,
    pub includes_screen: bool,
    pub has_ansi_codes: bool,
    pub timestamp: DateTime<Utc>,
}

/// Command output captured (using shell integration)
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CommandOutputCapturedPayload {
    pub window_id: u32,
    pub command_text: Option<String>, // May not be available
    pub output_text: String,
    pub output_type: String, // "last_cmd_output", "last_non_empty_output", etc.
    pub cwd: String,
    pub timestamp: DateTime<Utc>,
}

// ============================================================================
// Event Types
// ============================================================================

pub struct TerminalScrollbackCaptured;
impl EventType for TerminalScrollbackCaptured {
    type Payload = TerminalScrollbackCapturedPayload;
    type SourceImpl = ScrollbackCapture;
    const EVENT_NAME: &'static str = "terminal.scrollback.captured";
}

pub struct CommandOutputCaptured;
impl EventType for CommandOutputCaptured {
    type Payload = CommandOutputCapturedPayload;
    type SourceImpl = ScrollbackCapture;
    const EVENT_NAME: &'static str = "terminal.command_output.captured";
}

// ============================================================================
// Event Source
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrollbackConfig {
    /// Kitty socket path
    pub kitty_socket_path: String,
    /// How often to capture scrollback (seconds)
    pub capture_interval_secs: u64,
    /// Maximum scrollback size to capture (lines)
    pub max_scrollback_lines: usize,
    /// Include ANSI escape codes
    pub include_ansi_codes: bool,
    /// Capture command output separately (requires shell integration)
    pub capture_command_output: bool,
    /// Save scrollback to files
    pub save_to_files: bool,
    /// Directory for scrollback files
    pub scrollback_dir: PathBuf,
    /// Capture scrollback on command execution
    #[serde(default)]
    pub capture_on_command: bool,
    /// Delay after command before capturing (milliseconds)
    #[serde(default = "default_command_capture_delay")]
    pub command_capture_delay_ms: u64,
}

fn default_command_capture_delay() -> u64 {
    500 // 500ms delay after command to capture output
}

impl Default for ScrollbackConfig {
    fn default() -> Self {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/home/user".to_string());
        Self {
            kitty_socket_path: "/tmp/kitty".to_string(),
            capture_interval_secs: 300, // 5 minutes
            max_scrollback_lines: 10000,
            include_ansi_codes: false,
            capture_command_output: true,
            save_to_files: false,  // Store in database by default
            scrollback_dir: PathBuf::from(&home).join(".local/share/sinex/scrollback"),
            capture_on_command: true,
            command_capture_delay_ms: default_command_capture_delay(),
            process_urgent_requests: true,
        }
    }
}

#[derive(Debug)]
struct KittyWindow {
    id: u32,
    _pid: u32,
    cwd: String,
    title: String,
}

pub struct ScrollbackCapture {
    config: ScrollbackConfig,
    last_capture_times: HashMap<u32, DateTime<Utc>>,
    last_scrollback_hashes: HashMap<u32, u64>,
    command_event_rx: Option<mpsc::Receiver<CommandExecutedEvent>>,
}

#[derive(Debug, Clone)]
struct CommandExecutedEvent {
    window_id: u32,
    timestamp: DateTime<Utc>,
}

#[async_trait]
impl EventSource for ScrollbackCapture {
    type Config = ScrollbackConfig;
    
    const SOURCE_NAME: &'static str = "ingestor.scrollback_capture";
    
    async fn initialize(ctx: EventSourceContext) -> Result<Self> {
        let config: Self::Config = serde_json::from_value(ctx.config)
            .map_err(|e| sinex_core::CoreError::Configuration(format!("Failed to parse config: {}", e)))?;
        
        info!("Initializing scrollback capture");
        
        if config.save_to_files {
            tokio::fs::create_dir_all(&config.scrollback_dir).await
                .map_err(|e| sinex_core::CoreError::Other(
                    format!("Failed to create scrollback directory: {}", e)
                ))?;
        }
        
        Ok(Self {
            config,
            last_capture_times: HashMap::new(),
            last_scrollback_hashes: HashMap::new(),
            command_event_rx: None,
            db_pool: ctx.db_pool.clone(),
        })
    }
    
    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> Result<()> {
        info!("Starting scrollback capture");
        
        let mut interval = time::interval(Duration::from_secs(self.config.capture_interval_secs));
        
        // Set up command event channel if capture_on_command is enabled
        let (cmd_tx, _cmd_rx) = if self.config.capture_on_command {
            let (tx, rx) = mpsc::channel(100);
            self.command_event_rx = Some(rx);
            (Some(tx), true)
        } else {
            (None, false)
        };
        
        // If we have command-triggered capture, start monitoring for command events
        if let Some(cmd_tx) = cmd_tx {
            let socket_path = self.config.kitty_socket_path.clone();
            tokio::spawn(async move {
                if let Err(e) = monitor_command_events(socket_path, cmd_tx).await {
                    error!("Command event monitor failed: {}", e);
                }
            });
        }
        
        // Set up urgent request monitoring if enabled
        let (urgent_tx, mut urgent_rx) = if self.config.process_urgent_requests && self.db_pool.is_some() {
            let (tx, rx) = mpsc::channel::<u32>(100);
            let pool = self.db_pool.clone().unwrap();
            let urgent_tx = tx.clone();
            
            tokio::spawn(async move {
                monitor_urgent_requests(pool, urgent_tx).await;
            });
            
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.capture_all_scrollbacks(&tx, false).await {
                        error!("Error capturing scrollbacks: {}", e);
                    }
                }
                Some(cmd_event) = async {
                    if let Some(rx) = &mut self.command_event_rx {
                        rx.recv().await
                    } else {
                        None
                    }
                } => {
                    // Wait for command output to complete
                    tokio::time::sleep(Duration::from_millis(self.config.command_capture_delay_ms)).await;
                    
                    if let Err(e) = self.capture_window_scrollback(&tx, cmd_event.window_id, true).await {
                        error!("Error capturing scrollback after command: {}", e);
                    }
                }
                Some(window_id) = async {
                    if let Some(rx) = &mut urgent_rx {
                        rx.recv().await
                    } else {
                        None
                    }
                } => {
                    info!("Processing urgent capture request for window {}", window_id);
                    if let Err(e) = self.capture_window_scrollback(&tx, window_id, false).await {
                        error!("Failed urgent capture for window {}: {}", window_id, e);
                    }
                }
            }
        }
    }
}

impl ScrollbackCapture {
    async fn capture_all_scrollbacks(&mut self, tx: &mpsc::Sender<RawEvent>, _incremental: bool) -> Result<()> {
        // Check if Kitty socket exists
        if !std::path::Path::new(&self.config.kitty_socket_path).exists() {
            debug!("Kitty socket not found at {}", self.config.kitty_socket_path);
            return Ok(());
        }
        
        // Get all Kitty windows
        let windows = match self.get_kitty_windows() {
            Ok(windows) => windows,
            Err(e) => {
                debug!("Failed to get Kitty windows: {}", e);
                return Ok(());
            }
        };
        
        for window in &windows {
            // Capture full scrollback
            if let Ok(scrollback) = self.get_window_scrollback(window.id, true).await {
                let payload = TerminalScrollbackCapturedPayload {
                    window_id: window.id,
                    terminal_type: "kitty".to_string(),
                    cwd: window.cwd.clone(),
                    window_title: window.title.clone(),
                    scrollback_text: scrollback.text.clone(),
                    scrollback_lines: scrollback.line_count,
                    includes_screen: true,
                    has_ansi_codes: self.config.include_ansi_codes,
                    timestamp: Utc::now(),
                };
                
                let event = create_event(
                    TerminalScrollbackCaptured::EVENT_NAME,
                    serde_json::to_value(payload)?
                );
                tx.send(event).await.map_err(|_| sinex_core::CoreError::Other(
                    "Channel closed".to_string()
                ))?;
                
                // Save to file if configured
                if self.config.save_to_files {
                    self.save_scrollback_to_file(&window, &scrollback).await?;
                }
            }
            
            // Capture command output if shell integration is available
            if self.config.capture_command_output {
                for output_type in &["last_cmd_output", "last_non_empty_output"] {
                    if let Ok(output) = self.get_command_output(window.id, output_type).await {
                        if !output.text.is_empty() {
                            let payload = CommandOutputCapturedPayload {
                                window_id: window.id,
                                command_text: None, // Would need parsing to extract
                                output_text: output.text,
                                output_type: output_type.to_string(),
                                cwd: window.cwd.clone(),
                                timestamp: Utc::now(),
                            };
                            
                            let event = create_event(
                                CommandOutputCaptured::EVENT_NAME,
                                serde_json::to_value(payload)?
                            );
                            tx.send(event).await.map_err(|_| sinex_core::CoreError::Other(
                    "Channel closed".to_string()
                ))?;
                        }
                    }
                }
            }
            
            self.last_capture_times.insert(window.id, Utc::now());
        }
        
        // Clean up old entries
        let active_ids: Vec<u32> = windows.iter().map(|w| w.id).collect();
        self.last_capture_times.retain(|id, _| active_ids.contains(id));
        
        Ok(())
    }
    
    fn get_kitty_windows(&self) -> Result<Vec<KittyWindow>> {
        use std::process::Command;
        
        let output = Command::new("kitty")
            .arg("@")
            .arg("--to")
            .arg(format!("unix:{}", self.config.kitty_socket_path))
            .arg("ls")
            .output()
            .map_err(|e| sinex_core::CoreError::Other(format!("Failed to run kitty @: {}", e)))?;
        
        if !output.status.success() {
            return Err(sinex_core::CoreError::Other(
                format!("kitty @ ls failed: {}", String::from_utf8_lossy(&output.stderr))
            ));
        }
        
        let data: serde_json::Value = serde_json::from_slice(&output.stdout)?;
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
                                        _pid: pid as u32,
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
    
    async fn get_window_scrollback(&self, window_id: u32, include_screen: bool) -> Result<ScrollbackText> {
        use std::process::Command;
        
        let extent = if include_screen { "all" } else { "scrollback" };
        let mut cmd = Command::new("kitty");
        cmd.arg("@")
            .arg("--to")
            .arg(format!("unix:{}", self.config.kitty_socket_path))
            .arg("get-text")
            .arg("--match")
            .arg(format!("id:{}", window_id))
            .arg("--extent")
            .arg(extent);
        
        if self.config.include_ansi_codes {
            cmd.arg("--ansi");
        }
        
        let output = cmd.output()
            .map_err(|e| sinex_core::CoreError::Other(format!("Failed to get scrollback: {}", e)))?;
        
        if !output.status.success() {
            return Err(sinex_core::CoreError::Other(
                format!("Failed to get scrollback: {}", String::from_utf8_lossy(&output.stderr))
            ));
        }
        
        let text = String::from_utf8_lossy(&output.stdout).to_string();
        let line_count = text.lines().count();
        
        // Limit lines if needed
        let text = if line_count > self.config.max_scrollback_lines {
            text.lines()
                .skip(line_count - self.config.max_scrollback_lines)
                .collect::<Vec<_>>()
                .join("\n")
        } else {
            text
        };
        
        Ok(ScrollbackText {
            text,
            line_count: line_count.min(self.config.max_scrollback_lines),
        })
    }
    
    async fn get_command_output(&self, window_id: u32, extent: &str) -> Result<ScrollbackText> {
        use std::process::Command;
        
        let mut cmd = Command::new("kitty");
        cmd.arg("@")
            .arg("--to")
            .arg(format!("unix:{}", self.config.kitty_socket_path))
            .arg("get-text")
            .arg("--match")
            .arg(format!("id:{}", window_id))
            .arg("--extent")
            .arg(extent);
        
        if self.config.include_ansi_codes {
            cmd.arg("--ansi");
        }
        
        let output = cmd.output()
            .map_err(|e| sinex_core::CoreError::Other(format!("Failed to get output: {}", e)))?;
        
        if !output.status.success() {
            // This might fail if shell integration isn't enabled
            return Ok(ScrollbackText {
                text: String::new(),
                line_count: 0,
            });
        }
        
        let text = String::from_utf8_lossy(&output.stdout).to_string();
        let line_count = text.lines().count();
        
        Ok(ScrollbackText {
            text,
            line_count,
        })
    }
    
    async fn save_scrollback_to_file(&self, window: &KittyWindow, scrollback: &ScrollbackText) -> Result<()> {
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
        let filename = format!("scrollback_{}_{}_w{}.txt", timestamp, window.title, window.id);
        let filepath = self.config.scrollback_dir.join(filename);
        
        tokio::fs::write(&filepath, &scrollback.text).await
            .map_err(|e| sinex_core::CoreError::Other(
                format!("Failed to save scrollback: {}", e)
            ))?;
        
        debug!("Saved scrollback to {:?}", filepath);
        Ok(())
    }
    
    async fn capture_window_scrollback(&mut self, tx: &mpsc::Sender<RawEvent>, window_id: u32, incremental: bool) -> Result<()> {
        // Get window info
        let windows = self.get_kitty_windows()?;
        let window = windows.iter().find(|w| w.id == window_id);
        
        if let Some(window) = window {
            // Capture scrollback
            if let Ok(scrollback) = self.get_window_scrollback(window.id, !incremental).await {
                // Calculate hash to detect changes
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut hasher = DefaultHasher::new();
                scrollback.text.hash(&mut hasher);
                let hash = hasher.finish();
                
                // Check if content changed (for incremental captures)
                if incremental {
                    if let Some(&last_hash) = self.last_scrollback_hashes.get(&window_id) {
                        if last_hash == hash {
                            debug!("Scrollback unchanged for window {}", window_id);
                            return Ok(());
                        }
                    }
                }
                
                self.last_scrollback_hashes.insert(window_id, hash);
                
                let payload = TerminalScrollbackCapturedPayload {
                    window_id: window.id,
                    terminal_type: "kitty".to_string(),
                    cwd: window.cwd.clone(),
                    window_title: window.title.clone(),
                    scrollback_text: scrollback.text.clone(),
                    scrollback_lines: scrollback.line_count,
                    includes_screen: !incremental,
                    has_ansi_codes: self.config.include_ansi_codes,
                    timestamp: Utc::now(),
                };
                
                let event = create_event(
                    TerminalScrollbackCaptured::EVENT_NAME,
                    serde_json::to_value(payload)?
                );
                tx.send(event).await.map_err(|_| sinex_core::CoreError::Other(
                    "Channel closed".to_string()
                ))?;
                
                // Save to file if configured
                if self.config.save_to_files {
                    self.save_scrollback_to_file(&window, &scrollback).await?;
                }
            }
        }
        
        Ok(())
    }
}

struct ScrollbackText {
    text: String,
    line_count: usize,
}

fn create_event(event_type: &str, payload: serde_json::Value) -> RawEvent {
    RawEvent {
        id: sinex_ulid::Ulid::new(),
        source: ScrollbackCapture::SOURCE_NAME.to_string(),
        event_type: event_type.to_string(),
        ts_ingest: Utc::now(),
        ts_orig: Some(Utc::now()),
        host: gethostname::gethostname().to_string_lossy().to_string(),
        ingestor_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        payload_schema_id: None,
        payload,
    }
}

// Monitor for command execution events from Kitty
async fn monitor_command_events(socket_path: String, tx: mpsc::Sender<CommandExecutedEvent>) -> Result<()> {
    use tokio::process::Command;
    use tokio::io::{AsyncBufReadExt, BufReader};
    
    // Start kitty remote control in watch mode
    let mut child = Command::new("kitty")
        .arg("@")
        .arg("--to")
        .arg(format!("unix:{}", socket_path))
        .arg("action")
        .arg("watch")
        .stdout(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| sinex_core::CoreError::Other(format!("Failed to start kitty @ watch: {}", e)))?;
    
    let stdout = child.stdout.take()
        .ok_or_else(|| sinex_core::CoreError::Other("Failed to capture stdout".to_string()))?;
    
    let reader = BufReader::new(stdout);
    let mut lines = reader.lines();
    
    while let Some(line) = lines.next_line().await? {
        // Parse Kitty action events
        if line.contains("on_key") && (line.contains("enter") || line.contains("ctrl+c")) {
            // Extract window ID from the event
            if let Some(window_id) = extract_window_id(&line) {
                let event = CommandExecutedEvent {
                    window_id,
                    timestamp: Utc::now(),
                };
                
                if let Err(_) = tx.send(event).await {
                    break; // Channel closed
                }
            }
        }
    }
    
    Ok(())
}

fn extract_window_id(line: &str) -> Option<u32> {
    // This is a simplified extraction - actual implementation would need to parse Kitty's event format
    // For now, we'll use a basic pattern match
    if let Some(start) = line.find("window_id:") {
        let id_str = &line[start + 10..];
        if let Some(end) = id_str.find(|c: char| !c.is_numeric()) {
            id_str[..end].parse().ok()
        } else {
            id_str.parse().ok()
        }
    } else {
        None
    }
}

/// Monitor for urgent capture requests in the database
async fn monitor_urgent_requests(pool: PgPool, tx: mpsc::Sender<u32>) {
    info!("Starting urgent capture request monitor");
    
    loop {
        // Query for recent urgent capture requests
        let query = r#"
            SELECT payload->>'window_id' as window_id
            FROM raw.events
            WHERE event_type = 'terminal.capture.urgent'
              AND ts_ingest > NOW() - INTERVAL '5 seconds'
              AND payload->>'window_id' IS NOT NULL
            ORDER BY ts_ingest DESC
            LIMIT 10
        "#;
        
        match sqlx::query_as::<_, (Option<String>,)>(query)
            .fetch_all(&pool)
            .await 
        {
            Ok(requests) => {
                for (window_id_str,) in requests {
                    if let Some(window_id_str) = window_id_str {
                        if let Ok(window_id) = window_id_str.parse::<u32>() {
                            info!("Found urgent capture request for window {}", window_id);
                            let _ = tx.send(window_id).await;
                        }
                    }
                }
            }
            Err(e) => {
                debug!("Error querying urgent requests: {}", e);
            }
        }
        
        // Check every second for urgent requests
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}