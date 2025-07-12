//! Unified Terminal Satellite for Sinex
//!
//! This satellite handles all terminal-related event sources:
//! - shell.atuin: Rich shell history from Atuin
//! - shell.history: Shell history file parsing  
//! - shell.kitty: Real-time Kitty terminal events
//! - shell.recording: Terminal session recording (asciinema)
//! - shell.scrollback: Terminal content capture

use async_trait::async_trait;
use serde_json::json;
use sinex_core::RawEvent;
use sinex_satellite_sdk::{
    event_source::{EventSource, EventSourceContext, ScannerArgs, ScanReport, ScannerEstimate},
    SatelliteError, SatelliteResult,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

mod atuin;
mod history;
mod kitty;
mod recording;
mod scanner;
mod scrollback;

pub use atuin::AtuinWatcher;
pub use history::HistoryWatcher;
pub use kitty::KittyWatcher;
pub use recording::RecordingWatcher;
pub use scanner::TerminalScanner;
pub use scrollback::ScrollbackWatcher;

/// Unified terminal satellite that coordinates multiple terminal event sources
pub struct TerminalSatellite {
    context: Option<EventSourceContext>,
    config: TerminalConfig,
    
    // Individual watchers
    atuin_watcher: Option<AtuinWatcher>,
    history_watcher: Option<HistoryWatcher>,
    kitty_watcher: Option<KittyWatcher>,
    recording_watcher: Option<RecordingWatcher>,
    scrollback_watcher: Option<ScrollbackWatcher>,
}

#[derive(Debug, Clone)]
pub struct TerminalConfig {
    pub enabled_sources: HashMap<String, bool>,
    pub atuin_db_path: Option<PathBuf>,
    pub history_files: Vec<PathBuf>,
    pub kitty_socket_path: Option<PathBuf>,
    pub recording_output_dir: Option<PathBuf>,
    pub scrollback_capture_enabled: bool,
    pub polling_interval_secs: u64,
    pub batch_size: usize,
    
    // Scanner-specific configuration
    pub scanner_batch_size: usize,
    pub scanner_max_file_size_mb: u64,
}

impl Default for TerminalConfig {
    fn default() -> Self {
        let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("/tmp"));
        
        Self {
            enabled_sources: [
                ("atuin".to_string(), true),
                ("history".to_string(), true),
                ("kitty".to_string(), false), // Disabled by default, requires setup
                ("recording".to_string(), false),
                ("scrollback".to_string(), false),
            ].into_iter().collect(),
            atuin_db_path: Some(home.join(".local/share/atuin/history.db")),
            history_files: vec![
                home.join(".bash_history"),
                home.join(".zsh_history"),
                home.join(".local/share/fish/fish_history"),
            ],
            kitty_socket_path: None, // Auto-detected
            recording_output_dir: Some(home.join(".local/share/sinex/recordings")),
            scrollback_capture_enabled: false,
            polling_interval_secs: 5,
            batch_size: 100,
            scanner_batch_size: 1000,
            scanner_max_file_size_mb: 100,
        }
    }
}

impl TerminalSatellite {
    /// Create a new terminal satellite
    pub fn new() -> Self {
        Self {
            context: None,
            config: TerminalConfig::default(),
            atuin_watcher: None,
            history_watcher: None,
            kitty_watcher: None,
            recording_watcher: None,
            scrollback_watcher: None,
        }
    }

    /// Parse configuration from context
    fn parse_config(&mut self, ctx: &EventSourceContext) -> SatelliteResult<()> {
        // Update config from context source_config
        if let Some(enabled_sources) = ctx.config.get("enabled_sources") {
            if let Ok(sources) = serde_json::from_value::<HashMap<String, bool>>(enabled_sources.clone()) {
                self.config.enabled_sources = sources;
            }
        }

        if let Some(atuin_db_path) = ctx.config.get("atuin_db_path") {
            if let Ok(path) = serde_json::from_value::<PathBuf>(atuin_db_path.clone()) {
                self.config.atuin_db_path = Some(path);
            }
        }

        if let Some(history_files) = ctx.config.get("history_files") {
            if let Ok(files) = serde_json::from_value::<Vec<PathBuf>>(history_files.clone()) {
                self.config.history_files = files;
            }
        }

        if let Some(polling_interval) = ctx.config.get("polling_interval_secs") {
            if let Ok(interval) = serde_json::from_value::<u64>(polling_interval.clone()) {
                self.config.polling_interval_secs = interval;
            }
        }

        if let Some(batch_size) = ctx.config.get("batch_size") {
            if let Ok(size) = serde_json::from_value::<usize>(batch_size.clone()) {
                self.config.batch_size = size;
            }
        }

        Ok(())
    }

    /// Initialize all enabled watchers
    async fn initialize_watchers(&mut self) -> SatelliteResult<()> {
        let _ctx = self.context.as_ref().unwrap();

        // Initialize Atuin watcher
        if self.config.enabled_sources.get("atuin").copied().unwrap_or(false) {
            if let Some(ref atuin_path) = self.config.atuin_db_path {
                if atuin_path.exists() {
                    info!("Initializing Atuin watcher: {}", atuin_path.display());
                    match AtuinWatcher::new(atuin_path.clone()).await {
                        Ok(watcher) => {
                            self.atuin_watcher = Some(watcher);
                            info!("✅ Atuin watcher initialized");
                        }
                        Err(e) => {
                            warn!("Failed to initialize Atuin watcher: {}", e);
                        }
                    }
                } else {
                    warn!("Atuin database not found: {}", atuin_path.display());
                }
            }
        }

        // Initialize History watcher
        if self.config.enabled_sources.get("history").copied().unwrap_or(false) {
            let existing_files: Vec<PathBuf> = self.config.history_files.iter()
                .filter(|f| f.exists())
                .cloned()
                .collect();
            
            if !existing_files.is_empty() {
                info!("Initializing History watcher for {} files", existing_files.len());
                match HistoryWatcher::new(existing_files).await {
                    Ok(watcher) => {
                        self.history_watcher = Some(watcher);
                        info!("✅ History watcher initialized");
                    }
                    Err(e) => {
                        warn!("Failed to initialize History watcher: {}", e);
                    }
                }
            } else {
                warn!("No history files found");
            }
        }

        // Initialize Kitty watcher (if requested and available)
        if self.config.enabled_sources.get("kitty").copied().unwrap_or(false) {
            info!("Initializing Kitty watcher");
            match KittyWatcher::new().await {
                Ok(watcher) => {
                    self.kitty_watcher = Some(watcher);
                    info!("✅ Kitty watcher initialized");
                }
                Err(e) => {
                    warn!("Failed to initialize Kitty watcher: {}", e);
                }
            }
        }

        // Initialize Recording watcher (if requested)
        if self.config.enabled_sources.get("recording").copied().unwrap_or(false) {
            if let Some(ref output_dir) = self.config.recording_output_dir {
                info!("Initializing Recording watcher: {}", output_dir.display());
                match RecordingWatcher::new(output_dir.clone()).await {
                    Ok(watcher) => {
                        self.recording_watcher = Some(watcher);
                        info!("✅ Recording watcher initialized");
                    }
                    Err(e) => {
                        warn!("Failed to initialize Recording watcher: {}", e);
                    }
                }
            }
        }

        // Initialize Scrollback watcher (if requested)
        if self.config.enabled_sources.get("scrollback").copied().unwrap_or(false) {
            info!("Initializing Scrollback watcher");
            match ScrollbackWatcher::new().await {
                Ok(watcher) => {
                    self.scrollback_watcher = Some(watcher);
                    info!("✅ Scrollback watcher initialized");
                }
                Err(e) => {
                    warn!("Failed to initialize Scrollback watcher: {}", e);
                }
            }
        }

        Ok(())
    }
}

impl Default for TerminalSatellite {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EventSource for TerminalSatellite {
    fn source_name(&self) -> &str {
        "terminal"
    }

    async fn initialize(&mut self, ctx: EventSourceContext) -> SatelliteResult<()> {
        info!("Initializing unified terminal satellite");

        // Parse configuration
        self.parse_config(&ctx)?;

        // Store context
        self.context = Some(ctx);

        // Initialize all watchers
        self.initialize_watchers().await?;

        let enabled_count = [
            self.atuin_watcher.is_some(),
            self.history_watcher.is_some(),
            self.kitty_watcher.is_some(),
            self.recording_watcher.is_some(),
            self.scrollback_watcher.is_some(),
        ].iter().filter(|&&x| x).count();

        info!("Terminal satellite initialized with {} active watchers", enabled_count);
        Ok(())
    }

    async fn start_streaming(&mut self) -> SatelliteResult<()> {
        let ctx = self.context.as_ref().ok_or_else(|| {
            SatelliteError::EventSource("Not initialized".to_string())
        })?;

        info!("Starting terminal event streaming");

        // Create channels for coordinating events from different sources
        let (unified_tx, mut unified_rx) = mpsc::unbounded_channel::<RawEvent>();
        let event_sender = ctx.event_sender.clone();

        // Start all watchers concurrently
        let mut handles = vec![];

        // Start Atuin watcher
        if let Some(mut atuin_watcher) = self.atuin_watcher.take() {
            let tx = unified_tx.clone();
            let handle = tokio::spawn(async move {
                if let Err(e) = atuin_watcher.start_streaming(tx).await {
                    error!("Atuin watcher failed: {}", e);
                }
            });
            handles.push(handle);
        }

        // Start History watcher
        if let Some(mut history_watcher) = self.history_watcher.take() {
            let tx = unified_tx.clone();
            let handle = tokio::spawn(async move {
                if let Err(e) = history_watcher.start_streaming(tx).await {
                    error!("History watcher failed: {}", e);
                }
            });
            handles.push(handle);
        }

        // Start Kitty watcher
        if let Some(mut kitty_watcher) = self.kitty_watcher.take() {
            let tx = unified_tx.clone();
            let handle = tokio::spawn(async move {
                if let Err(e) = kitty_watcher.start_streaming(tx).await {
                    error!("Kitty watcher failed: {}", e);
                }
            });
            handles.push(handle);
        }

        // Start Recording watcher
        if let Some(mut recording_watcher) = self.recording_watcher.take() {
            let tx = unified_tx.clone();
            let handle = tokio::spawn(async move {
                if let Err(e) = recording_watcher.start_streaming(tx).await {
                    error!("Recording watcher failed: {}", e);
                }
            });
            handles.push(handle);
        }

        // Start Scrollback watcher
        if let Some(mut scrollback_watcher) = self.scrollback_watcher.take() {
            let tx = unified_tx.clone();
            let handle = tokio::spawn(async move {
                if let Err(e) = scrollback_watcher.start_streaming(tx).await {
                    error!("Scrollback watcher failed: {}", e);
                }
            });
            handles.push(handle);
        }

        // Drop the unified_tx since all workers have their own clones
        drop(unified_tx);

        info!("Started {} terminal event watchers", handles.len());

        // Forward events from unified channel to the main event sender
        while let Some(event) = unified_rx.recv().await {
            debug!("Forwarding terminal event: {} {}", event.source, event.event_type);
            if let Err(e) = event_sender.send(event) {
                error!("Failed to send terminal event: {}", e);
                break;
            }
        }

        info!("Terminal event streaming stopped");
        Ok(())
    }

    async fn shutdown(&mut self) -> SatelliteResult<()> {
        info!("Terminal satellite shutting down");
        // Individual watchers will be dropped and cleaned up automatically
        Ok(())
    }

    // ===== Scanner Mode Support =====
    
    fn supports_scanner(&self) -> bool {
        true // Terminal satellite supports scanner mode for historical data
    }

    async fn run_scanner(&mut self, args: ScannerArgs) -> SatelliteResult<ScanReport> {
        let start_time = Instant::now();
        info!("Starting terminal scanner mode with {} paths", args.paths.len());
        
        let scanner = TerminalScanner::new(self.config.clone());
        let report = scanner.scan_historical_data(args).await?;
        
        let duration = start_time.elapsed();
        info!(
            "Terminal scanner completed: {} events in {:?}", 
            report.events_generated, 
            duration
        );
        
        Ok(report)
    }

    fn scanner_config_schema(&self) -> Option<serde_json::Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "paths": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Paths to scan (Atuin DB, history files, etc.)"
                },
                "time_range": {
                    "type": "array",
                    "items": {"type": "string", "format": "date-time"},
                    "minItems": 2,
                    "maxItems": 2,
                    "description": "Time range [start, end] for historical data"
                },
                "source_types": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "enum": ["atuin", "history", "recording", "all"]
                    },
                    "description": "Terminal source types to scan"
                },
                "dry_run": {
                    "type": "boolean",
                    "description": "Analyze without generating events"
                },
                "max_events": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Maximum events to generate (0 = unlimited)"
                }
            },
            "required": []
        }))
    }

    async fn estimate_scanner_scope(&self, args: &ScannerArgs) -> SatelliteResult<ScannerEstimate> {
        let scanner = TerminalScanner::new(self.config.clone());
        scanner.estimate_scope(args).await
    }
}