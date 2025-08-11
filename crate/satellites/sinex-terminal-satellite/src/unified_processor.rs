//! Unified terminal processor implementing StatefulStreamProcessor
//!
//! This module implements the terminal satellite processor supporting snapshot, historical, and
//! continuous scanning modes for terminal events.

use async_trait::async_trait;
use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sinex_core::db::models::RawEvent;
use sinex_core::types::error::with_context;
use sinex_core::types::events::{
    Event, TerminalCommandHistoricalPayload, TerminalHistoryHistoricalPayload,
    TerminalMonitoringStartedPayload, TerminalSnapshotPayload,
};
use sinex_core::types::validate_path;
use sinex_satellite_sdk::{
    checkpoint::CheckpointManager,
    cli::{
        ActivityEntry, CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry,
        MissingItem, SourceState,
    },
    stream_processor::{
        Checkpoint, ProcessorCapabilities, ProcessorType, ScanArgs, ScanEstimate, ScanReport,
        StatefulStreamProcessor, StreamProcessorContext, TimeHorizon,
    },
    SatelliteResult,
};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{info, warn};
use validator::{Validate, ValidationError};

use crate::{AtuinWatcher, HistoryWatcher, KittyWatcher, RecordingWatcher, ScrollbackWatcher};
// use sinex_core::types::events::constants::{event_types, services}; // already imported above

#[cfg(test)]
mod config_validation_tests;

/// Terminal monitoring configuration
#[derive(Debug, Clone, Serialize, Deserialize, Validate, bon::Builder)]
#[builder(derive(Debug))]
pub struct TerminalConfig {
    pub enabled_sources: HashMap<String, bool>,
    #[validate(custom(
        function = "validate_optional_path",
        message = "Invalid atuin database path"
    ))]
    pub atuin_db_path: Option<Utf8PathBuf>,
    #[validate(custom(
        function = "validate_path_list",
        message = "Invalid history file paths"
    ))]
    pub history_files: Vec<Utf8PathBuf>,
    #[validate(custom(
        function = "validate_optional_path",
        message = "Invalid kitty socket path"
    ))]
    pub kitty_socket_path: Option<Utf8PathBuf>,
    #[validate(custom(
        function = "validate_optional_path",
        message = "Invalid recording output directory"
    ))]
    pub recording_output_dir: Option<Utf8PathBuf>,
    pub scrollback_capture_enabled: bool,
    #[validate(range(
        min = 1,
        max = 3600,
        message = "Polling interval must be between 1 and 3600 seconds"
    ))]
    pub polling_interval_secs: u64,
    #[validate(range(
        min = 1,
        max = 10000,
        message = "Batch size must be between 1 and 10000"
    ))]
    pub batch_size: usize,

    // Scanner-specific configuration
    #[validate(range(
        min = 1,
        max = 100000,
        message = "Scanner batch size must be between 1 and 100000"
    ))]
    pub scanner_batch_size: usize,
    #[validate(range(
        min = 1,
        max = 10240,
        message = "Max file size must be between 1MB and 10GB"
    ))]
    pub scanner_max_file_size_mb: u64,
}

impl Default for TerminalConfig {
    fn default() -> Self {
        let home = dirs::home_dir()
            .and_then(|p| Utf8PathBuf::from_path_buf(p).ok())
            .unwrap_or_else(|| Utf8PathBuf::from("/tmp"));

        Self::builder()
            .enabled_sources(
                [
                    ("atuin".to_owned(), true),
                    ("history".to_owned(), true),
                    ("kitty".to_owned(), false), // Disabled by default, requires setup
                    ("recording".to_owned(), false),
                    ("scrollback".to_string(), false),
                ]
                .into_iter()
                .collect(),
            )
            .atuin_db_path(Some(home.join(".local/share/atuin/history.db")))
            .history_files(vec![
                home.join(".bash_history"),
                home.join(".zsh_history"),
                home.join(".local/share/fish/fish_history"),
            ])
            .kitty_socket_path(None) // Auto-detected
            .recording_output_dir(Some(home.join(".local/share/sinex/recordings")))
            .scrollback_capture_enabled(false)
            .polling_interval_secs(5)
            .batch_size(100)
            .scanner_batch_size(1000)
            .scanner_max_file_size_mb(100)
            .build()
    }
}

impl TerminalConfig {
    /// Validate the configuration and return detailed error messages
    pub fn validate_config(&self) -> Result<(), String> {
        use validator::Validate as ValidateTrait;

        ValidateTrait::validate(self).map_err(|e| {
            sinex_core::types::validation::validation_chains::format_validation_errors(&e)
        })
    }
}

// Custom validation functions for TerminalConfig

/// Validate optional path fields
fn validate_optional_path(path: &Option<Utf8PathBuf>) -> Result<(), ValidationError> {
    if let Some(p) = path {
        validate_single_path(p)?;
    }
    Ok(())
}

/// Validate a list of paths
fn validate_path_list(paths: &[Utf8PathBuf]) -> Result<(), ValidationError> {
    for path in paths {
        validate_single_path(path)?;
    }
    Ok(())
}

/// Validate a single path for security and correctness using comprehensive validation
fn validate_single_path(path: &Utf8PathBuf) -> Result<(), ValidationError> {
    let path_str = path.as_str();

    // Use the comprehensive path validation from sinex-core
    match sinex_core::types::validate_path(path_str) {
        Ok(_) => Ok(()),
        Err(_) => Err(ValidationError::new("invalid_path")),
    }
}

/// Terminal state snapshot for exploration and diagnostics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminalState {
    /// When the snapshot was taken
    pub captured_at: DateTime<Utc>,

    /// Enabled source types
    pub enabled_sources: Vec<String>,

    /// History file status
    pub history_file_status: HashMap<Utf8PathBuf, HistoryFileStatus>,

    /// Atuin database status
    pub atuin_status: Option<AtuinStatus>,

    /// Detected shell information
    pub shell_info: Option<crate::shell_detection::ShellInfo>,

    /// Recent activity summary
    pub recent_activity: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryFileStatus {
    pub exists: bool,
    pub size_bytes: u64,
    pub last_modified: Option<DateTime<Utc>>,
    pub estimated_entries: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtuinStatus {
    pub db_exists: bool,
    pub db_size_bytes: u64,
    pub estimated_entries: u64,
    pub last_entry_timestamp: Option<DateTime<Utc>>,
}

/// Unified terminal processor implementing StatefulStreamProcessor
///
/// Supports snapshot, historical, and continuous scanning modes for terminal events.
pub struct TerminalProcessor {
    /// Current processing context (set during initialization)
    context: Option<StreamProcessorContext>,

    /// Terminal monitoring configuration
    config: TerminalConfig,

    /// Individual watchers (initialized during operation)
    atuin_watcher: Option<AtuinWatcher>,
    history_watcher: Option<HistoryWatcher>,
    kitty_watcher: Option<KittyWatcher>,
    recording_watcher: Option<RecordingWatcher>,
    scrollback_watcher: Option<ScrollbackWatcher>,

    /// Detected shell information
    shell_info: Option<crate::shell_detection::ShellInfo>,

    /// Last captured terminal state for snapshots
    last_state: Option<TerminalState>,

    /// Checkpoint manager for state persistence
    checkpoint_manager: Option<CheckpointManager>,
}

impl TerminalProcessor {
    /// Create a new unified terminal processor
    pub fn new() -> Self {
        Self {
            context: None,
            config: TerminalConfig::default(),
            atuin_watcher: None,
            history_watcher: None,
            kitty_watcher: None,
            recording_watcher: None,
            scrollback_watcher: None,
            shell_info: None,
            last_state: None,
            checkpoint_manager: None,
        }
    }

    /// Create processor with custom configuration
    pub fn with_config(config: TerminalConfig) -> Self {
        Self {
            context: None,
            config,
            atuin_watcher: None,
            history_watcher: None,
            kitty_watcher: None,
            recording_watcher: None,
            scrollback_watcher: None,
            shell_info: None,
            last_state: None,
            checkpoint_manager: None,
        }
    }

    /// Take a snapshot of current terminal state
    #[with_context(
        operation = "take_terminal_snapshot",
        retry_count = 2,
        timeout_ms = 15000,
        enable_metrics
    )]
    async fn take_snapshot(&mut self) -> SatelliteResult<TerminalState> {
        let mut enabled_sources = Vec::with_capacity(8); // Reasonable capacity for config items
        let mut history_file_status = HashMap::new();
        let mut atuin_status = None;

        // Check enabled sources
        for (source, enabled) in &self.config.enabled_sources {
            if *enabled {
                enabled_sources.push(source.clone());
            }
        }

        // Check history files
        for history_file in &self.config.history_files {
            let status = Self::get_file_metadata_and_status(history_file);
            history_file_status.insert(history_file.clone(), status);
        }

        // Check Atuin database
        if let Some(ref atuin_path) = self.config.atuin_db_path {
            atuin_status = Some(Self::get_atuin_status(atuin_path));
        }

        let state = TerminalState {
            captured_at: Utc::now(),
            enabled_sources,
            history_file_status,
            atuin_status,
            shell_info: self.shell_info.clone(),
            recent_activity: vec!["Terminal processor snapshot taken".to_string()],
        };

        self.last_state = Some(state.clone());
        Ok(state)
    }

    /// Initialize watchers based on enabled sources
    #[with_context(
        operation = "initialize_terminal_watchers",
        retry_count = 3,
        timeout_ms = 20000,
        enable_metrics,
        context = "component=watcher_initialization"
    )]
    async fn initialize_watchers(&mut self) -> SatelliteResult<()> {
        // For now, stub implementations - will be implemented properly later

        // Initialize Atuin watcher
        if self
            .config
            .enabled_sources
            .get("atuin")
            .copied()
            .unwrap_or(false)
        {
            if let Some(ref atuin_path) = self.config.atuin_db_path {
                if atuin_path.exists() {
                    info!("Initializing Atuin watcher: {} (stub)", atuin_path.as_str());
                    info!("✅ Atuin watcher initialized (stub)");
                } else {
                    warn!("Atuin database not found: {}", atuin_path.as_str());
                }
            }
        }

        // Initialize History watcher
        if self
            .config
            .enabled_sources
            .get("history")
            .copied()
            .unwrap_or(false)
        {
            let existing_files: Vec<Utf8PathBuf> = self
                .config
                .history_files
                .iter()
                .filter(|f| f.exists())
                .cloned()
                .collect();

            if !existing_files.is_empty() {
                info!(
                    "Initializing History watcher for {} files (stub)",
                    existing_files.len()
                );
                info!("✅ History watcher initialized (stub)");
            } else {
                warn!("No history files found");
            }
        }

        // Initialize Kitty watcher (if requested and available)
        if self
            .config
            .enabled_sources
            .get("kitty")
            .copied()
            .unwrap_or(false)
        {
            info!("Initializing Kitty watcher (stub)");
            info!("✅ Kitty watcher initialized (stub)");
        }

        // Initialize Recording watcher (if requested)
        if self
            .config
            .enabled_sources
            .get("recording")
            .copied()
            .unwrap_or(false)
        {
            if let Some(ref output_dir) = self.config.recording_output_dir {
                info!(
                    "Initializing Recording watcher: {} (stub)",
                    output_dir.as_str()
                );
                info!("✅ Recording watcher initialized (stub)");
            }
        }

        // Initialize Scrollback watcher (if requested)
        if self
            .config
            .enabled_sources
            .get("scrollback")
            .copied()
            .unwrap_or(false)
        {
            info!("Initializing Scrollback watcher (stub)");
            info!("✅ Scrollback watcher initialized (stub)");
        }

        Ok(())
    }

    /// Start continuous terminal monitoring
    #[with_context(
        operation = "start_continuous_terminal_monitoring",
        enable_metrics,
        context = "component=continuous_monitoring"
    )]
    async fn start_continuous_monitoring(
        &mut self,
        _from_checkpoint: Checkpoint,
    ) -> SatelliteResult<()> {
        info!("Starting continuous terminal monitoring");

        // For now, stub implementation - will be implemented properly later
        // This would start the actual watchers and forward events

        if let Some(ref context) = self.context {
            info!("Terminal monitoring context available");

            // Emit monitoring started event with shell info
            let mut monitoring_event: RawEvent = Event::new(TerminalMonitoringStartedPayload {
                enabled_sources: self.config.enabled_sources.clone(),
                start_time: Utc::now(),
            })
            .into();

            // Add shell info to the event payload if available
            if let Some(ref shell_info) = self.shell_info {
                if let serde_json::Value::Object(ref mut map) = monitoring_event.payload {
                    map.insert(
                        "shell_type".to_string(),
                        serde_json::json!(shell_info.shell_type),
                    );
                    map.insert(
                        "shell_version".to_string(),
                        serde_json::json!(shell_info.version),
                    );
                    map.insert(
                        "shell_capabilities".to_string(),
                        serde_json::json!(shell_info.capabilities),
                    );
                }
            }

            context.emit_event(monitoring_event).await?;
        }

        Ok(())
    }

    /// Perform historical scan on terminal sources
    #[with_context(operation = "scan_historical_terminal_data")]
    async fn scan_historical_terminal_data(
        &self,
        _from: &Checkpoint,
        _until: &TimeHorizon,
        _args: &ScanArgs,
        emit_events: bool,
    ) -> SatelliteResult<u64> {
        let mut event_count = 0;

        // This would implement historical scanning of terminal data
        // For now, just provide a placeholder that shows the structure

        if let Some(ref context) = self.context {
            // Example: scan Atuin database for historical entries
            if self
                .config
                .enabled_sources
                .get("atuin")
                .copied()
                .unwrap_or(false)
            {
                if let Some(ref atuin_path) = self.config.atuin_db_path {
                    if atuin_path.exists() && emit_events {
                        // Create a sample historical event
                        let event: RawEvent = Event::new(TerminalCommandHistoricalPayload {
                            source: "atuin".to_string(),
                            db_path: Some(atuin_path.clone().into()),
                            file_path: None,
                            scan_type: "historical".to_string(),
                        })
                        .into();

                        context.emit_event(event).await?;
                        event_count += 1;
                    }
                }
            }

            // Example: scan history files for historical entries
            if self
                .config
                .enabled_sources
                .get("history")
                .copied()
                .unwrap_or(false)
            {
                for history_file in &self.config.history_files {
                    if history_file.exists() && emit_events {
                        let event: RawEvent = Event::new(TerminalHistoryHistoricalPayload {
                            source: "history_file".to_string(),
                            file_path: history_file.clone().into(),
                            scan_type: "historical".to_string(),
                        })
                        .into();

                        context.emit_event(event).await?;
                        event_count += 1;
                    }
                }
            }
        }

        Ok(event_count)
    }

    /// Helper function to parse configuration field values
    fn parse_config_field<T: serde::de::DeserializeOwned>(
        config: &std::collections::HashMap<String, serde_json::Value>,
        key: &str,
    ) -> Option<T> {
        config
            .get(key)
            .and_then(|v| serde_json::from_value::<T>(v.clone()).ok())
    }

    /// Helper function to get file metadata and status
    fn get_file_metadata_and_status(history_file: &Utf8PathBuf) -> HistoryFileStatus {
        // Validate path before file operations to prevent path traversal
        if validate_path(history_file.as_str()).is_err() {
            warn!(
                path = %history_file,
                "Skipping invalid or dangerous history file path"
            );
            return HistoryFileStatus::NonExistent;
        }

        if history_file.exists() {
            let metadata = std::fs::metadata(history_file).ok();
            let size_bytes = metadata.as_ref().map(|m| m.len()).unwrap_or(0);
            let last_modified = metadata
                .and_then(|m| m.modified().ok())
                .map(|t| DateTime::<Utc>::from(t));

            // Estimate entries by counting lines (rough estimate)
            let estimated_entries = if let Ok(content) = std::fs::read_to_string(history_file) {
                content.lines().count() as u64
            } else {
                0
            };

            HistoryFileStatus {
                exists: true,
                size_bytes,
                last_modified,
                estimated_entries,
            }
        } else {
            HistoryFileStatus {
                exists: false,
                size_bytes: 0,
                last_modified: None,
                estimated_entries: 0,
            }
        }
    }

    /// Helper function to get Atuin database status
    fn get_atuin_status(atuin_path: &Utf8PathBuf) -> AtuinStatus {
        if atuin_path.exists() {
            let metadata = std::fs::metadata(atuin_path).ok();
            let db_size_bytes = metadata.map(|m| m.len()).unwrap_or(0);

            // For now, provide a rough estimate
            // In a real implementation, we'd query the SQLite database
            let estimated_entries = db_size_bytes / 100; // Very rough estimate

            AtuinStatus {
                db_exists: true,
                db_size_bytes,
                estimated_entries,
                last_entry_timestamp: None, // TODO: Query actual timestamp
            }
        } else {
            AtuinStatus {
                db_exists: false,
                db_size_bytes: 0,
                estimated_entries: 0,
                last_entry_timestamp: None,
            }
        }
    }

    /// Helper function to estimate Atuin entries
    fn estimate_atuin_entries(
        atuin_db_path: &Option<Utf8PathBuf>,
        warnings: &mut Vec<String>,
    ) -> u64 {
        if let Some(ref atuin_path) = atuin_db_path {
            if atuin_path.exists() {
                // Estimate based on file size (very rough)
                std::fs::metadata(atuin_path)
                    .map(|m| m.len() / 100) // ~100 bytes per entry
                    .unwrap_or(0)
            } else {
                warnings.push("Atuin database not found".to_string());
                0
            }
        } else {
            0
        }
    }

    /// Helper function to estimate history entries from files
    fn estimate_history_entries(history_files: &[Utf8PathBuf]) -> u64 {
        history_files
            .iter()
            .filter_map(|f| {
                if f.exists() {
                    std::fs::read_to_string(f)
                        .map(|content| content.lines().count() as u64)
                        .ok()
                } else {
                    None
                }
            })
            .sum()
    }
}

impl Default for TerminalProcessor {
    fn default() -> Self {
        Self::new()
    }
}

#[sinex_satellite_sdk::auto_satellite_metrics(processor_type = "ingestor", labels = ["source=terminal"])]
#[async_trait]
impl StatefulStreamProcessor for TerminalProcessor {
    type Config = TerminalConfig;

    async fn initialize(
        &mut self,
        ctx: StreamProcessorContext,
        config: Self::Config,
    ) -> SatelliteResult<()> {
        info!(
            processor = self.processor_name(),
            service = %ctx.service_name,
            "Initializing terminal processor"
        );

        // Initialize checkpoint manager
        self.checkpoint_manager = Some(ctx.checkpoint_manager.clone());

        // Parse configuration from processor context
        if let Some(config_json) = ctx.config.get("terminal") {
            match serde_json::from_value::<TerminalConfig>(config_json.clone()) {
                Ok(config) => {
                    self.config = config;
                }
                Err(e) => {
                    warn!("Failed to parse terminal config, using defaults: {}", e);
                }
            }
        }

        // Detect shell environment to enhance configuration
        match crate::shell_detection::detect_current_shell() {
            Ok(shell_info) => {
                info!(
                    "Detected shell: {:?} (version: {:?})",
                    shell_info.shell_type, shell_info.version
                );

                // Add shell-specific history file if not already in config
                if let Some(ref history_path) = shell_info.history_path {
                    if !self.config.history_files.contains(history_path) {
                        info!("Adding detected history file: {}", history_path.as_str());
                        self.config.history_files.push(history_path.clone());
                    }
                }

                // Store shell info for later use
                self.shell_info = Some(shell_info);
            }
            Err(e) => {
                warn!("Failed to detect shell environment: {}", e);
            }
        }

        // Override with individual config values if present
        if let Some(sources) =
            Self::parse_config_field::<HashMap<String, bool>>(&ctx.config, "enabled_sources")
        {
            self.config.enabled_sources = sources;
        }

        if let Some(path) = Self::parse_config_field::<Utf8PathBuf>(&ctx.config, "atuin_db_path") {
            self.config.atuin_db_path = Some(path);
        }

        if let Some(files) =
            Self::parse_config_field::<Vec<Utf8PathBuf>>(&ctx.config, "history_files")
        {
            self.config.history_files = files;
        }

        if let Some(interval) =
            Self::parse_config_field::<u64>(&ctx.config, "polling_interval_secs")
        {
            self.config.polling_interval_secs = interval;
        }

        if let Some(size) = Self::parse_config_field::<usize>(&ctx.config, "batch_size") {
            self.config.batch_size = size;
        }

        info!(
            enabled_sources = ?self.config.enabled_sources,
            atuin_db_path = ?self.config.atuin_db_path,
            history_files = ?self.config.history_files,
            polling_interval_secs = self.config.polling_interval_secs,
            batch_size = self.config.batch_size,
            "Terminal processor configuration"
        );

        self.context = Some(ctx);
        Ok(())
    }

    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        let start_time = std::time::Instant::now();
        let mut events_processed = 0;
        let mut successful_targets = Vec::with_capacity(8);
        let mut failed_targets = Vec::with_capacity(8);
        let mut warnings = Vec::with_capacity(16);

        info!(
            processor = self.processor_name(),
            from = %from.description(),
            until = ?until,
            targets = args.targets.len(),
            dry_run = args.dry_run,
            "Starting terminal scan"
        );

        match until {
            TimeHorizon::Snapshot => {
                // Take current state snapshot
                let _state = self.take_snapshot().await?;

                // Initialize watchers for snapshot capabilities
                if let Err(e) = self.initialize_watchers().await {
                    warnings.push(format!("Failed to initialize some watchers: {}", e));
                }

                // Count available terminal sources
                let active_watchers = [
                    self.atuin_watcher.is_some(),
                    self.history_watcher.is_some(),
                    self.kitty_watcher.is_some(),
                    self.recording_watcher.is_some(),
                    self.scrollback_watcher.is_some(),
                ]
                .iter()
                .filter(|&&x| x)
                .count();

                events_processed = active_watchers as u64;
                successful_targets.push("terminal_state_snapshot".to_string());

                if !args.dry_run {
                    // Emit a snapshot event
                    if let Some(ref context) = self.context {
                        let snapshot_event: RawEvent = Event::new(TerminalSnapshotPayload {
                            active_watchers,
                            enabled_sources: self.config.enabled_sources.clone(),
                            snapshot_time: Utc::now(),
                        })
                        .into();

                        context.emit_event(snapshot_event).await?;
                    }
                }
            }

            TimeHorizon::Historical { .. } => {
                // Historical scan of terminal data
                warnings.push("Historical terminal scanning has limited capabilities".to_string());

                match self
                    .scan_historical_terminal_data(&from, &until, &args, !args.dry_run)
                    .await
                {
                    Ok(count) => {
                        events_processed = count;
                        successful_targets.push("terminal_historical_scan".to_string());
                    }
                    Err(e) => {
                        failed_targets
                            .push(("terminal_historical_scan".to_string(), e.to_string()));
                    }
                }
            }

            TimeHorizon::Continuous => {
                // Initialize watchers for continuous monitoring
                self.initialize_watchers().await?;

                // Start continuous monitoring
                info!("Starting continuous terminal monitoring");
                self.start_continuous_monitoring(from.clone()).await?;
                // Continuous monitoring runs indefinitely
                events_processed = 0; // Can't count events in continuous mode
            }
        }

        let final_checkpoint = Checkpoint::timestamp(Utc::now(), None);

        Ok(ScanReport {
            events_processed,
            duration: start_time.elapsed(),
            final_checkpoint,
            time_range: Some((
                match &from {
                    Checkpoint::Timestamp { timestamp, .. } => *timestamp,
                    _ => Utc::now() - chrono::Duration::hours(1),
                },
                Utc::now(),
            )),
            processor_stats: HashMap::from([
                (
                    "enabled_sources".to_string(),
                    self.config.enabled_sources.len() as u64,
                ),
                (
                    "successful_targets".to_string(),
                    successful_targets.len() as u64,
                ),
                ("failed_targets".to_string(), failed_targets.len() as u64),
            ]),
            successful_targets,
            failed_targets,
            warnings,
        })
    }

    fn processor_name(&self) -> &str {
        "terminal-processor"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Ingestor
    }

    fn capabilities(&self) -> ProcessorCapabilities {
        ProcessorCapabilities {
            supports_continuous: true,
            supports_historical: true,
            supports_snapshot: true,
            supports_interactive: false,
            max_scan_size: Some(10000), // Reasonable limit for terminal events
            supports_concurrent: false,
        }
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        // For terminal monitoring, use timestamp-based checkpoints
        Ok(Checkpoint::timestamp(Utc::now(), None))
    }

    async fn estimate_scan_scope(
        &self,
        _from: &Checkpoint,
        until: &TimeHorizon,
        _args: &ScanArgs,
    ) -> SatelliteResult<ScanEstimate> {
        let mut estimated_events = 0;
        let mut warnings = Vec::new();

        // Estimate based on enabled sources and their potential
        for (source, enabled) in &self.config.enabled_sources {
            if *enabled {
                let source_estimate = match source.as_str() {
                    "atuin" => {
                        Self::estimate_atuin_entries(&self.config.atuin_db_path, &mut warnings)
                    }
                    "history" => Self::estimate_history_entries(&self.config.history_files),
                    _ => 10, // Default estimate for other sources
                };
                estimated_events += source_estimate;
            }
        }

        // Adjust estimate based on time horizon
        let (duration_factor, confidence) = match until {
            TimeHorizon::Snapshot => (0.1, 0.8), // Only current state
            TimeHorizon::Historical { .. } => (1.0, 0.6), // Full history
            TimeHorizon::Continuous => (f64::INFINITY, 0.1), // Unknown duration
        };

        let adjusted_events = (estimated_events as f64 * duration_factor) as u64;

        Ok(ScanEstimate {
            estimated_events: adjusted_events,
            estimated_duration: Duration::from_millis(adjusted_events * 5), // ~5ms per event
            estimated_data_size: adjusted_events * 512,                     // ~512 bytes per event
            estimated_targets: self
                .config
                .enabled_sources
                .values()
                .filter(|&&enabled| enabled)
                .count() as u64,
            warnings,
            confidence,
        })
    }
}

// Implementation of ExplorationProvider for diagnostics
impl ExplorationProvider for TerminalProcessor {
    fn get_source_state(&self) -> color_eyre::eyre::Result<SourceState> {
        let recent_activity = if let Some(ref state) = self.last_state {
            state
                .recent_activity
                .iter()
                .enumerate()
                .map(|(i, desc)| ActivityEntry {
                    timestamp: state.captured_at - chrono::Duration::minutes(i as i64),
                    description: desc.clone(),
                    data: None,
                })
                .collect()
        } else {
            vec![]
        };

        let total_items = self.last_state.as_ref().map(|s| {
            s.history_file_status
                .values()
                .map(|status| status.estimated_entries)
                .sum::<u64>()
                + s.atuin_status
                    .as_ref()
                    .map(|a| a.estimated_entries)
                    .unwrap_or(0)
        });

        Ok(SourceState {
            description: format!(
                "Terminal processor monitoring {} sources",
                self.config
                    .enabled_sources
                    .values()
                    .filter(|&&enabled| enabled)
                    .count()
            ),
            last_updated: self
                .last_state
                .as_ref()
                .map(|s| s.captured_at)
                .unwrap_or_else(Utc::now),
            total_items,
            metadata: HashMap::from([
                (
                    "enabled_sources".to_string(),
                    serde_json::to_value(&self.config.enabled_sources)?,
                ),
                (
                    "atuin_db_path".to_string(),
                    serde_json::to_value(&self.config.atuin_db_path)?,
                ),
                (
                    "history_files".to_string(),
                    serde_json::to_value(&self.config.history_files)?,
                ),
                (
                    "polling_interval_secs".to_string(),
                    serde_json::to_value(self.config.polling_interval_secs)?,
                ),
                (
                    "processor_type".to_string(),
                    serde_json::Value::String("ingestor".to_string()),
                ),
            ]),
            healthy: true,
            recent_activity,
        })
    }

    fn get_ingestion_history(
        &self,
        _limit: u64,
    ) -> color_eyre::eyre::Result<Vec<IngestionHistoryEntry>> {
        // In a real implementation, this would query the database for scan history
        // For now, return empty as this requires database access
        Ok(vec![])
    }

    fn get_coverage_analysis(
        &self,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> color_eyre::eyre::Result<CoverageAnalysis> {
        // In a real implementation, this would compare terminal state with Sinex events
        let (start_time, end_time) = time_range.unwrap_or_else(|| {
            let now = Utc::now();
            let hour_ago = now - chrono::Duration::hours(1);
            (hour_ago, now)
        });

        let source_total = self
            .last_state
            .as_ref()
            .map(|s| {
                s.history_file_status
                    .values()
                    .map(|status| status.estimated_entries)
                    .sum::<u64>()
                    + s.atuin_status
                        .as_ref()
                        .map(|a| a.estimated_entries)
                        .unwrap_or(0)
            })
            .unwrap_or(0);

        Ok(CoverageAnalysis {
            time_range: (start_time, end_time),
            source_total,
            sinex_total: 0, // Would query from database
            coverage_percentage: 0.0,
            missing_count: source_total,
            missing_samples: vec![MissingItem {
                source_id: "terminal".to_string(),
                timestamp: end_time,
                description: "Terminal events not yet ingested into Sinex".to_string(),
                missing_reason: Some("Initial scan required".to_string()),
            }],
            duplicate_count: 0,
            recommendations: vec![
                "Run a full snapshot scan to capture current state".to_string(),
                "Enable continuous monitoring for real-time terminal events".to_string(),
                "Check enabled sources configuration".to_string(),
            ],
        })
    }

    fn export_data(
        &self,
        path: &Utf8PathBuf,
        format: ExportFormat,
    ) -> color_eyre::eyre::Result<()> {
        // Validate export path for security
        validate_path(path.as_str())
            .map_err(|e| color_eyre::eyre::eyre!("Invalid export path '{}': {}", path, e))?;

        if let Some(ref state) = self.last_state {
            let content = match format {
                ExportFormat::Json => serde_json::to_string_pretty(state)?,
                ExportFormat::Csv => {
                    // Simple CSV export
                    let mut csv = "source,enabled,status\n".to_string();
                    for (source, enabled) in &self.config.enabled_sources {
                        csv.push_str(&format!("{},{},configured\n", source, enabled));
                    }
                    csv
                }
                ExportFormat::Raw => format!("{:#?}", state),
            };

            std::fs::write(path, content)?;
        } else {
            // Export configuration if no state available
            let config_data = serde_json::json!({
                "enabled_sources": self.config.enabled_sources,
                "atuin_db_path": self.config.atuin_db_path,
                "history_files": self.config.history_files,
                "polling_interval_secs": self.config.polling_interval_secs,
                "batch_size": self.config.batch_size
            });

            let content = match format {
                ExportFormat::Json => serde_json::to_string_pretty(&config_data)?,
                ExportFormat::Raw => format!("{:#?}", config_data),
                ExportFormat::Csv => "No state data available\n".to_string(),
            };

            std::fs::write(path, content)?;
        }

        Ok(())
    }
}
