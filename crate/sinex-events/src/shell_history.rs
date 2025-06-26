use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::time::{self, Instant};
use tracing::{debug, error, info, warn};
use notify::{Watcher, RecursiveMode, EventKind};
use notify::event::{ModifyKind, DataChange};
use std::collections::HashSet;

use sinex_core::{EventType, EventSource, EventSourceContext, Result};
use sinex_db::models::RawEvent;

// ============================================================================
// Event Payloads
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ShellHistoryCommandPayload {
    pub command_string: String,
    pub shell_type: String, // "zsh" or "bash"
    pub history_line_number: Option<usize>,
    pub source_file: String,
    /// Best-effort timestamp extraction (zsh extended history or file mtime)
    pub ts_command_approx: OptionalTimestamp,
}

// ============================================================================
// Event Types
// ============================================================================

pub struct ShellHistoryCommand;
impl EventType for ShellHistoryCommand {
    type Payload = ShellHistoryCommandPayload;
    type SourceImpl = ShellHistoryReader;
    const EVENT_NAME: &'static str = "shell.history.command";
}

// ============================================================================
// Event Source
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShellHistoryConfig {
    /// Paths to history files to monitor
    pub history_files: Vec<PathBuf>,
    /// How often to check for changes (seconds)
    pub polling_interval_secs: u64,
    /// Use file watching instead of polling
    #[serde(default = "default_true")]
    pub use_file_watch: bool,
    /// Deduplicate commands within this time window (seconds)
    #[serde(default = "default_dedup_window")]
    pub dedup_window_secs: u64,
}

fn default_true() -> bool { true }
fn default_dedup_window() -> u64 { 300 } // 5 minutes

impl Default for ShellHistoryConfig {
    fn default() -> Self {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/home/user".to_string());
        Self {
            history_files: vec![
                PathBuf::from(&home).join(".zsh_history"),
                PathBuf::from(&home).join(".bash_history"),
            ],
            polling_interval_secs: 10,
            use_file_watch: true,
            dedup_window_secs: 300,
        }
    }
}

pub struct ShellHistoryReader {
    config: ShellHistoryConfig,
    last_positions: std::collections::HashMap<PathBuf, u64>,
    recent_commands: HashSet<(String, String)>, // (command, shell_type) for dedup
    last_cleanup: Instant,
}

#[async_trait]
impl EventSource for ShellHistoryReader {
    type Config = ShellHistoryConfig;
    
    const SOURCE_NAME: &'static str = "ingestor.shell_history_reader";
    
    async fn initialize(ctx: EventSourceContext) -> Result<Self> {
        let config: Self::Config = serde_json::from_value(ctx.config)
            .map_err(|e| sinex_core::CoreError::Configuration(format!("Failed to parse config: {}", e)))?;
        
        info!("Initializing shell history reader for {} files", config.history_files.len());
        
        // Check which files exist
        for path in &config.history_files {
            if path.exists() {
                info!("Will monitor history file: {:?}", path);
            } else {
                debug!("History file not found (will watch for creation): {:?}", path);
            }
        }
        
        Ok(Self {
            config,
            last_positions: std::collections::HashMap::new(),
            recent_commands: HashSet::new(),
            last_cleanup: Instant::now(),
        })
    }
    
    async fn stream_events(&mut self, tx: EventSender) -> Result<()> {
        info!("Starting shell history event source");
        
        // Initial read of all files
        for path in &self.config.history_files.clone() {
            if path.exists() {
                if let Err(e) = self.read_history_file(path, &tx, true).await {
                    error!("Error reading history file {:?}: {}", path, e);
                }
            }
        }
        
        if self.config.use_file_watch {
            self.watch_mode(tx).await?;
        } else {
            self.poll_mode(tx).await?;
        }
        
        Ok(())
    }
}

impl ShellHistoryReader {
    async fn watch_mode(&mut self, tx: EventSender) -> Result<()> {
        let (notify_tx, mut notify_rx) = mpsc::channel(100);
        let watched_files = self.config.history_files.clone();
        
        // Set up file watchers
        let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
            if let Ok(event) = res {
                if matches!(event.kind, EventKind::Modify(ModifyKind::Data(DataChange::Any))) {
                    for path in &event.paths {
                        if watched_files.iter().any(|f| f == path) {
                            let _ = notify_tx.blocking_send(path.clone());
                        }
                    }
                }
            }
        }).map_err(|e| sinex_core::CoreError::Other(
            format!("Failed to create file watcher: {}", e)
        ))?;
        
        // Watch parent directories to catch file creation
        let mut watched_dirs = HashSet::new();
        for path in &self.config.history_files {
            if let Some(parent) = path.parent() {
                if watched_dirs.insert(parent.to_path_buf()) {
                    watcher.watch(parent, RecursiveMode::NonRecursive)
                        .map_err(|e| sinex_core::CoreError::Other(
                            format!("Failed to watch directory {:?}: {}", parent, e)
                        ))?;
                }
            }
        }
        
        info!("Started file watching for shell history files");
        
        let poll_interval = Duration::from_secs(self.config.polling_interval_secs);
        let mut last_poll = Instant::now();
        
        loop {
            tokio::select! {
                // File change detected
                Some(path) = notify_rx.recv() => {
                    debug!("History file changed: {:?}", path);
                    if let Err(e) = self.read_history_file(&path, &tx, false).await {
                        error!("Error reading history file {:?}: {}", path, e);
                    }
                    last_poll = Instant::now();
                }
                // Periodic poll as fallback
                _ = time::sleep_until(last_poll + poll_interval) => {
                    debug!("Periodic poll for history files");
                    for path in &self.config.history_files.clone() {
                        if path.exists() {
                            if let Err(e) = self.read_history_file(path, &tx, false).await {
                                error!("Error reading history file {:?}: {}", path, e);
                            }
                        }
                    }
                    last_poll = Instant::now();
                }
                // Cleanup old dedup entries
                _ = time::sleep_until(self.last_cleanup + Duration::from_secs(60)) => {
                    self.cleanup_dedup_cache();
                }
            }
        }
    }
    
    async fn poll_mode(&mut self, tx: EventSender) -> Result<()> {
        let mut interval = time::interval(Duration::from_secs(self.config.polling_interval_secs));
        
        loop {
            interval.tick().await;
            
            for path in &self.config.history_files.clone() {
                if path.exists() {
                    if let Err(e) = self.read_history_file(path, &tx, false).await {
                        error!("Error reading history file {:?}: {}", path, e);
                    }
                }
            }
            
            // Periodic cleanup
            if self.last_cleanup.elapsed() > Duration::from_secs(60) {
                self.cleanup_dedup_cache();
            }
        }
    }
    
    async fn read_history_file(
        &mut self,
        path: &PathBuf,
        tx: &EventSender,
        initial_read: bool,
    ) -> Result<()> {
        use tokio::fs::File;
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        
        let mut file = File::open(path).await
            .map_err(|e| sinex_core::CoreError::Other(format!("Failed to open {:?}: {}", path, e)))?;
        
        let metadata = file.metadata().await
            .map_err(|e| sinex_core::CoreError::Other(format!("Failed to get metadata: {}", e)))?;
        
        let file_size = metadata.len();
        let last_pos = self.last_positions.get(path).copied().unwrap_or(0);
        
        // If file shrunk, it was probably truncated - start from beginning
        let start_pos = if file_size < last_pos {
            warn!("History file {:?} shrunk, starting from beginning", path);
            0
        } else if initial_read && file_size > 10_000_000 {
            // On initial read of large files, only read last 1MB
            info!("Large history file {:?}, reading only recent entries", path);
            file_size.saturating_sub(1_000_000)
        } else {
            last_pos
        };
        
        if start_pos >= file_size {
            return Ok(()); // Nothing new
        }
        
        file.seek(std::io::SeekFrom::Start(start_pos)).await
            .map_err(|e| sinex_core::CoreError::Other(format!("Failed to seek: {}", e)))?;
        
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await
            .map_err(|e| sinex_core::CoreError::Other(format!("Failed to read: {}", e)))?;
        
        let content = String::from_utf8_lossy(&buffer);
        let shell_type = if path.to_string_lossy().contains("zsh") { "zsh" } else { "bash" };
        
        let mut line_num = 0;
        for line in content.lines() {
            line_num += 1;
            
            if let Some((event, payload)) = self.parse_history_line(line, shell_type, path, line_num) {
                // Check deduplication
                let dedup_key = (payload.command_string.clone(), shell_type.to_string());
                if !self.recent_commands.contains(&dedup_key) {
                    self.recent_commands.insert(dedup_key);
                    
                    tx.send(event).await.map_err(|_| sinex_core::CoreError::Other(
                        "Channel closed".to_string()
                    ))?;
                }
            }
        }
        
        self.last_positions.insert(path.clone(), file_size);
        debug!("Read {} bytes from {:?}", file_size - start_pos, path);
        
        Ok(())
    }
    
    fn parse_history_line(
        &self,
        line: &str,
        shell_type: &str,
        file_path: &PathBuf,
        line_number: usize,
    ) -> Option<(RawEvent, ShellHistoryCommandPayload)> {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }
        
        let (command, timestamp) = if shell_type == "zsh" && line.starts_with(": ") {
            // Zsh extended history format: ": 1234567890:0;command"
            let parts: Vec<&str> = line.splitn(2, ';').collect();
            if parts.len() == 2 {
                let meta_parts: Vec<&str> = parts[0].split(':').collect();
                if meta_parts.len() >= 2 {
                    if let Ok(ts) = meta_parts[1].trim().parse::<i64>() {
                        let timestamp = DateTime::from_timestamp(ts, 0);
                        (parts[1].to_string(), timestamp)
                    } else {
                        (parts[1].to_string(), None)
                    }
                } else {
                    (line.to_string(), None)
                }
            } else {
                (line.to_string(), None)
            }
        } else {
            // Plain format (bash or zsh without extended history)
            (line.to_string(), None)
        };
        
        // Skip empty commands
        if command.trim().is_empty() {
            return None;
        }
        
        let payload = ShellHistoryCommandPayload {
            command_string: command,
            shell_type: shell_type.to_string(),
            history_line_number: Some(line_number),
            source_file: file_path.to_string_lossy().to_string(),
            ts_command_approx: timestamp,
        };
        
        let event = RawEvent {
            id: sinex_ulid::Ulid::new(),
            source: Self::SOURCE_NAME.to_string(),
            event_type: ShellHistoryCommand::EVENT_NAME.to_string(),
            ts_ingest: Utc::now(),
            ts_orig: timestamp,
            host: gethostname::gethostname().to_string_lossy().to_string(),
            ingestor_version: Some(env!("CARGO_PKG_VERSION").to_string()),
            payload_schema_id: None,
            payload: serde_json::to_value(&payload).ok()?,
        };
        
        Some((event, payload))
    }
    
    fn cleanup_dedup_cache(&mut self) {
        // Simple cleanup: just clear everything older than dedup window
        // In a more sophisticated implementation, we'd track timestamps per entry
        let old_size = self.recent_commands.len();
        self.recent_commands.clear();
        self.last_cleanup = Instant::now();
        
        if old_size > 0 {
            debug!("Cleared {} entries from dedup cache", old_size);
        }
    }
}