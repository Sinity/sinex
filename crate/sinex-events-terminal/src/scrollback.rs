use async_trait::async_trait;
use chrono::Utc;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error, info};

use sinex_core::{
    sources, ChannelSenderExt, EventSender, EventSource, EventSourceBase, EventSourceContext, EventType, JsonValue,
    Result, Timestamp, chunking::ChunkingService, EventFactory, ErrorContext, CoreError, RawEvent, timeouts,
};
use sinex_annex::{BlobManager, AnnexConfig};

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scrollback_text: Option<String>, // Only for small content
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scrollback_chunks: Option<Vec<serde_json::Value>>, // For chunked large content
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_annex_path: Option<String>, // Path in git-annex for large content
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_annex_key: Option<String>, // Git-annex key for large content
    pub scrollback_lines: usize,
    pub scrollback_size_bytes: u64,
    pub is_chunked: bool,
    pub chunk_count: Option<u32>,
    pub includes_screen: bool,
    pub has_ansi_codes: bool,
    pub timestamp: Timestamp,
}

/// Command output captured (using shell integration)
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CommandOutputCapturedPayload {
    pub window_id: u32,
    pub command_text: Option<String>, // May not be available
    pub output_text: String,
    pub output_type: String, // "last_cmd_output", "last_non_empty_output", etc.
    pub cwd: String,
    pub timestamp: Timestamp,
}

// ============================================================================
// Event Types
// ============================================================================

pub struct TerminalScrollbackCaptured;
impl EventType for TerminalScrollbackCaptured {
    type Payload = TerminalScrollbackCapturedPayload;
    type SourceImpl = ScrollbackCapture;
    const EVENT_NAME: &'static str = "scrollback.full";
}

pub struct CommandOutputCaptured;
impl EventType for CommandOutputCaptured {
    type Payload = CommandOutputCapturedPayload;
    type SourceImpl = ScrollbackCapture;
    const EVENT_NAME: &'static str = "command.output";
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
    /// Git-annex repository path for storing large scrollback content
    #[serde(default)]
    pub git_annex_repo: Option<PathBuf>,
    /// Automatically store large scrollback content in git-annex
    #[serde(default)]
    pub auto_annex: bool,
    /// Threshold for storing in git-annex instead of database (bytes)
    #[serde(default = "default_annex_threshold")]
    pub annex_threshold_bytes: usize,
    /// Capture scrollback on command execution
    #[serde(default)]
    pub capture_on_command: bool,
    /// Delay after command before capturing (milliseconds)
    #[serde(default = "default_command_capture_delay")]
    pub command_capture_delay_ms: u64,
    /// Enable chunking for large scrollback content (bytes threshold)
    #[serde(default = "default_chunking_threshold")]
    pub chunking_threshold_bytes: usize,
    /// Enable chunking (FastCDC)
    #[serde(default = "default_enable_chunking")]
    pub enable_chunking: bool,
}

fn default_command_capture_delay() -> u64 {
    500 // 500ms delay after command to capture output
}

fn default_chunking_threshold() -> usize {
    32_768 // 32KB threshold for chunking
}

fn default_enable_chunking() -> bool {
    true // Enable FastCDC chunking by default
}

fn default_annex_threshold() -> usize {
    64_000 // 64KB threshold for git-annex storage
}

impl Default for ScrollbackConfig {
    fn default() -> Self {
        let tmp_dir = std::env::var("SINEX_TMP_DIR").unwrap_or_else(|_| "/tmp".to_string());
        Self {
            kitty_socket_path: format!("{}/kitty", tmp_dir),
            capture_interval_secs: timeouts::KITTY_SCROLLBACK_INTERVAL.as_secs(), // 3 minutes for safety net
            max_scrollback_lines: 10000,
            include_ansi_codes: false,
            capture_command_output: true,
            git_annex_repo: Some(PathBuf::from("/realm/sinex-annex")), // Default annex repo
            auto_annex: true, // Store large content in git-annex by default
            annex_threshold_bytes: default_annex_threshold(),
            capture_on_command: true,
            command_capture_delay_ms: default_command_capture_delay(),
            chunking_threshold_bytes: default_chunking_threshold(),
            enable_chunking: default_enable_chunking(),
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
    last_capture_times: HashMap<u32, Timestamp>,
    last_scrollback_hashes: HashMap<u32, u64>,
    command_event_rx: Option<mpsc::Receiver<CommandExecutedEvent>>,
    chunking_service: Option<ChunkingService>,
    blob_manager: Option<BlobManager>,
    event_factory: EventFactory,
}

#[derive(Debug, Clone)]
struct CommandExecutedEvent {
    window_id: u32,
    #[allow(dead_code)]
    timestamp: Timestamp,
}

// Implement EventSourceBase to get common functionality
impl EventSourceBase for ScrollbackCapture {}

#[async_trait]
impl EventSource for ScrollbackCapture {
    type Config = ScrollbackConfig;

    const SOURCE_NAME: &'static str = sources::SHELL_SCROLLBACK;

    async fn initialize(ctx: EventSourceContext) -> Result<Self> {
        let config = <Self as EventSourceBase>::parse_config::<Self::Config>(&ctx).await?;

        info!("Initializing scrollback capture");

        // Initialize BlobManager if configured with both annex path and database
        let annex_repo_path = ctx
            .annex_repo_path
            .clone()
            .or(config.git_annex_repo.as_ref().map(|p| p.to_string_lossy().to_string()));
            
        let blob_manager = match (annex_repo_path.as_ref(), &ctx.db_pool) {
            (Some(repo_path), Some(db_pool)) => {
                let path = std::path::PathBuf::from(repo_path);

                // Initialize git-annex repository if it doesn't exist
                if !path.join(".git").exists() {
                    use sinex_annex::GitAnnex;
                    GitAnnex::init(&path, Some("sinex-scrollback-annex"))
                        .await
                        .map_err(|e| ErrorContext::new(CoreError::Configuration(format!("Failed to initialize git-annex: {}", e)))
                            .with_operation("initialize_scrollback_capture")
                            .with_context("repo_path", path.display().to_string())
                            .with_context("repo_name", "sinex-scrollback-annex")
                            .build())?;
                }

                let annex_config = AnnexConfig {
                    repo_path: path.clone(),
                    num_copies: Some(2),
                    large_files: None,
                };

                match BlobManager::new(annex_config, db_pool.clone()) {
                    Ok(manager) => Some(manager),
                    Err(e) => {
                        error!("Failed to create BlobManager: {}. Large scrollback content will not be stored.", e);
                        None
                    }
                }
            }
            _ => {
                if annex_repo_path.is_some() && ctx.db_pool.is_none() {
                    info!("Git-annex path configured but no database connection available. Large scrollback content will not be stored.");
                }
                None
            }
        };

        let chunking_service = if config.enable_chunking {
            Some(ChunkingService::with_default_config())
        } else {
            None
        };

        Ok(Self {
            config,
            last_capture_times: HashMap::new(),
            last_scrollback_hashes: HashMap::new(),
            command_event_rx: None,
            chunking_service,
            blob_manager,
            event_factory: EventFactory::new(Self::SOURCE_NAME),
        })
    }

    async fn stream_events(&mut self, tx: EventSender) -> Result<()> {
        info!("Starting scrollback capture");

        let mut interval = time::interval(Duration::from_secs(self.config.capture_interval_secs));

        // Set up command event channel if capture_on_command is enabled
        let (cmd_tx, _cmd_rx) = if self.config.capture_on_command {
            let (tx, rx) = mpsc::channel(sinex_core::buffers::NOTIFICATION_CHANNEL_SIZE);
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
            }
        }
    }
}

impl ScrollbackCapture {
    async fn capture_all_scrollbacks(
        &mut self,
        tx: &EventSender,
        _incremental: bool,
    ) -> Result<()> {
        // Check if Kitty socket exists
        if !std::path::Path::new(&self.config.kitty_socket_path).exists() {
            debug!(
                "Kitty socket not found at {}",
                self.config.kitty_socket_path
            );
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
                let scrollback_size_bytes = scrollback.text.len() as u64;
                let should_chunk = self.config.enable_chunking && 
                    scrollback_size_bytes > self.config.chunking_threshold_bytes as u64;
                let should_annex = self.config.auto_annex && 
                    scrollback_size_bytes > self.config.annex_threshold_bytes as u64;
                
                let (scrollback_text, scrollback_chunks, is_chunked, chunk_count, git_annex_path, git_annex_key) = if should_annex {
                    // Store in git-annex for large content
                    match self.store_in_git_annex(window, &scrollback).await {
                        Ok((annex_path, annex_key)) => {
                            (None, None, false, None, Some(annex_path), Some(annex_key))
                        }
                        Err(e) => {
                            error!("Failed to store scrollback in git-annex: {}, falling back to database", e);
                            // Fallback to chunking or inline storage
                            if should_chunk {
                                self.chunk_scrollback_content(&scrollback.text).unwrap_or_else(|_| {
                                    (Some(scrollback.text.clone()), None, false, None, None, None)
                                })
                            } else {
                                (Some(scrollback.text.clone()), None, false, None, None, None)
                            }
                        }
                    }
                } else if should_chunk {
                    // Chunk content for database storage
                    match self.chunk_scrollback_content(&scrollback.text) {
                        Ok((text, chunks, chunked, count, _, _)) => (text, chunks, chunked, count, None, None),
                        Err(e) => {
                            tracing::warn!("Failed to chunk scrollback for window {}: {}", window.id, e);
                            (Some(scrollback.text.clone()), None, false, None, None, None)
                        }
                    }
                } else {
                    // Store as text in database
                    (Some(scrollback.text.clone()), None, false, None, None, None)
                };

                let payload = TerminalScrollbackCapturedPayload {
                    window_id: window.id,
                    terminal_type: "kitty".to_string(),
                    cwd: window.cwd.clone(),
                    window_title: window.title.clone(),
                    scrollback_text,
                    scrollback_chunks,
                    git_annex_path,
                    git_annex_key,
                    scrollback_lines: scrollback.line_count,
                    scrollback_size_bytes,
                    is_chunked,
                    chunk_count,
                    includes_screen: true,
                    has_ansi_codes: self.config.include_ansi_codes,
                    timestamp: Utc::now(),
                };

                let event = self.create_event(
                    TerminalScrollbackCaptured::EVENT_NAME,
                    serde_json::to_value(payload)?,
                );
                tx.send_or_log(event, "scrollback_full").await?;
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

                            let event = self.create_event(
                                CommandOutputCaptured::EVENT_NAME,
                                serde_json::to_value(payload)?,
                            );
                            tx.send_or_log(event, "command_output").await?;
                        }
                    }
                }
            }

            self.last_capture_times.insert(window.id, Utc::now());
        }

        // Clean up old entries
        let active_ids: Vec<u32> = windows.iter().map(|w| w.id).collect();
        self.last_capture_times
            .retain(|id, _| active_ids.contains(id));

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
            .map_err(|e| {
                sinex_core::CoreError::processing_failed()
                    .with_operation("kitty_command")
                    .with_source(e)
                    .build()
            })?;

        if !output.status.success() {
            return Err(ErrorContext::new(CoreError::Io("kitty @ ls failed".to_string()))
                .with_operation("get_kitty_windows")
                .with_context("exit_status", output.status.to_string())
                .with_context("stderr", String::from_utf8_lossy(&output.stderr))
                .build());
        }

        let data: JsonValue = serde_json::from_slice(&output.stdout)?;
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

    async fn get_window_scrollback(
        &self,
        window_id: u32,
        include_screen: bool,
    ) -> Result<ScrollbackText> {
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

        let output = cmd.output().map_err(|e| {
            sinex_core::CoreError::processing_failed()
                .with_operation("kitty_get_scrollback")
                .with_source(e)
                .build()
        })?;

        if !output.status.success() {
            return Err(ErrorContext::new(CoreError::Io("Failed to get scrollback".to_string()))
                .with_operation("capture_scrollback_for_window")
                .with_context("window_id", window_id.to_string())
                .with_context("exit_status", output.status.to_string())
                .with_context("stderr", String::from_utf8_lossy(&output.stderr))
                .build());
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

        let output = cmd.output().map_err(|e| {
            sinex_core::CoreError::processing_failed()
                .with_operation("kitty_get_output")
                .with_source(e)
                .build()
        })?;

        if !output.status.success() {
            // This might fail if shell integration isn't enabled
            return Ok(ScrollbackText {
                text: String::new(),
                line_count: 0,
            });
        }

        let text = String::from_utf8_lossy(&output.stdout).to_string();
        let line_count = text.lines().count();

        Ok(ScrollbackText { text, line_count })
    }

    async fn store_in_git_annex(
        &self,
        window: &KittyWindow,
        scrollback: &ScrollbackText,
    ) -> Result<(String, String)> {
        let blob_manager = self.blob_manager.as_ref()
            .ok_or_else(|| ErrorContext::new(CoreError::Configuration("BlobManager not configured".to_string()))
                .with_operation("store_in_git_annex")
                .build())?;

        let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
        let filename = format!(
            "scrollback_{}_{}_w{}.txt",
            timestamp, window.title, window.id
        );

        // Use BlobManager to ingest content directly
        let metadata = blob_manager.ingest_from_bytes(
            scrollback.text.as_bytes(),
            &filename,
            "text/plain"
        ).await
            .map_err(|e| ErrorContext::new(CoreError::Io(format!("Failed to ingest scrollback content: {}", e)))
                .with_operation("store_in_git_annex")
                .with_context("filename", &filename)
                .with_context("window_id", window.id.to_string())
                .build())?;

        debug!("Stored scrollback via BlobManager: {} -> {} (blob_id: {})", 
                filename, metadata.annex_key, metadata.blob_id);
        Ok((filename, metadata.annex_key))
    }

    #[allow(clippy::type_complexity)]
    fn chunk_scrollback_content(&self, content: &str) -> Result<(Option<String>, Option<Vec<serde_json::Value>>, bool, Option<u32>, Option<String>, Option<String>)> {
        if let Some(ref chunking_service) = self.chunking_service {
            let chunks = chunking_service.chunk_string(content).map_err(|e| CoreError::Other(format!("Chunking failed: {}", e)))?;
            let chunk_count = chunks.len() as u32;
            let chunk_jsons: Vec<serde_json::Value> = chunks
                .into_iter()
                .map(|chunk| serde_json::to_value(chunk).unwrap_or(serde_json::Value::Null))
                .collect();
            Ok((None, Some(chunk_jsons), true, Some(chunk_count), None, None))
        } else {
            Ok((Some(content.to_string()), None, false, None, None, None))
        }
    }

    async fn capture_window_scrollback(
        &mut self,
        tx: &EventSender,
        window_id: u32,
        incremental: bool,
    ) -> Result<()> {
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

                let scrollback_size_bytes = scrollback.text.len() as u64;
                let should_chunk = self.config.enable_chunking && 
                    scrollback_size_bytes > self.config.chunking_threshold_bytes as u64;
                let should_annex = self.config.auto_annex && 
                    scrollback_size_bytes > self.config.annex_threshold_bytes as u64;
                
                let (scrollback_text, scrollback_chunks, is_chunked, chunk_count, git_annex_path, git_annex_key) = if should_annex {
                    // Store in git-annex for large content
                    match self.store_in_git_annex(window, &scrollback).await {
                        Ok((annex_path, annex_key)) => {
                            (None, None, false, None, Some(annex_path), Some(annex_key))
                        }
                        Err(e) => {
                            error!("Failed to store scrollback in git-annex: {}, falling back to database", e);
                            // Fallback to chunking or inline storage
                            if should_chunk {
                                self.chunk_scrollback_content(&scrollback.text).unwrap_or_else(|_| {
                                    (Some(scrollback.text.clone()), None, false, None, None, None)
                                })
                            } else {
                                (Some(scrollback.text.clone()), None, false, None, None, None)
                            }
                        }
                    }
                } else if should_chunk {
                    // Chunk content for database storage
                    match self.chunk_scrollback_content(&scrollback.text) {
                        Ok((text, chunks, chunked, count, _, _)) => (text, chunks, chunked, count, None, None),
                        Err(e) => {
                            tracing::warn!("Failed to chunk scrollback for window {}: {}", window_id, e);
                            (Some(scrollback.text.clone()), None, false, None, None, None)
                        }
                    }
                } else {
                    // Store as text in database
                    (Some(scrollback.text.clone()), None, false, None, None, None)
                };

                let payload = TerminalScrollbackCapturedPayload {
                    window_id: window.id,
                    terminal_type: "kitty".to_string(),
                    cwd: window.cwd.clone(),
                    window_title: window.title.clone(),
                    scrollback_text,
                    scrollback_chunks,
                    git_annex_path,
                    git_annex_key,
                    scrollback_lines: scrollback.line_count,
                    scrollback_size_bytes,
                    is_chunked,
                    chunk_count,
                    includes_screen: !incremental,
                    has_ansi_codes: self.config.include_ansi_codes,
                    timestamp: Utc::now(),
                };

                let event = self.create_event(
                    TerminalScrollbackCaptured::EVENT_NAME,
                    serde_json::to_value(payload)?,
                );
                tx.send_or_log(event, "scrollback_full").await?;
            }
        }

        Ok(())
    }
}

struct ScrollbackText {
    text: String,
    line_count: usize,
}

impl ScrollbackCapture {
    fn create_event(&self, event_type: &str, payload: JsonValue) -> RawEvent {
        self.event_factory.create_event(event_type, payload)
    }
}

// Monitor for command execution events from Kitty
async fn monitor_command_events(
    socket_path: String,
    tx: mpsc::Sender<CommandExecutedEvent>,
) -> Result<()> {
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::process::Command;

    // Start kitty remote control in watch mode
    let mut child = Command::new("kitty")
        .arg("@")
        .arg("--to")
        .arg(format!("unix:{}", socket_path))
        .arg("action")
        .arg("watch")
        .stdout(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| {
            sinex_core::CoreError::processing_failed()
                .with_operation("kitty_start_watch")
                .with_source(e)
                .build()
        })?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| 
            ErrorContext::new(CoreError::Io("Failed to capture stdout".to_string()))
                .with_operation("capture_command_output")
                .with_context("process", "journalctl")
                .build())?;

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

                if tx
                    .send_or_log(event, "scrollback_command_event")
                    .await
                    .is_err()
                {
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
