//! Unified terminal processor implementing StatefulStreamProcessor
//!
//! This module implements the terminal satellite processor using sensd for source material capture.
//! All terminal data flows through sensd sensors before being converted to events.

use async_trait::async_trait;
use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
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
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};
use validator::{Validate, ValidationError};

use crate::sensd_integration::{SensdIntegrationConfig, SensdTerminalProcessor};

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

    /// sensd integration configuration
    pub sensd_config: SensdIntegrationConfig,
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
            .sensd_config(SensdIntegrationConfig::default())
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
fn validate_single_path(path: &sinex_core::SanitizedPath) -> Result<(), ValidationError> {
    let path_str = path.as_str();

    // Use the comprehensive path validation from sinex-core
    match sinex_core::types::validate_path(path_str) {
        Ok(_) => Ok(()),
        Err(_) => Err(ValidationError::new("invalid_path")),
    }
}

/// Terminal state snapshot for exploration and diagnostics
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
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

    /// sensd job statuses
    pub sensd_jobs: Vec<SensdJobStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
pub struct HistoryFileStatus {
    pub exists: bool,
    pub size_bytes: u64,
    pub last_modified: Option<DateTime<Utc>>,
    pub estimated_entries: u64,
}

impl HistoryFileStatus {
    pub const NonExistent: HistoryFileStatus = HistoryFileStatus {
        exists: false,
        size_bytes: 0,
        last_modified: None,
        estimated_entries: 0,
    };
}

#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
pub struct AtuinStatus {
    pub db_exists: bool,
    pub db_size_bytes: u64,
    pub estimated_entries: u64,
    pub last_entry_timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
pub struct SensdJobStatus {
    pub job_id: String,
    pub sensor_type: String,
    pub target_uri: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub material_id: Option<String>,
}

/// Unified terminal processor implementing StatefulStreamProcessor
///
/// Uses sensd for all terminal data capture instead of direct event creation.
pub struct TerminalProcessor {
    /// Current processing context (set during initialization)
    context: Option<StreamProcessorContext>,

    /// Terminal monitoring configuration
    config: TerminalConfig,

    /// sensd integration processor
    sensd_processor: Option<Arc<SensdTerminalProcessor>>,

    /// Detected shell information
    shell_info: Option<crate::shell_detection::ShellInfo>,

    /// Last captured terminal state for snapshots
    last_state: Option<TerminalState>,

    /// Checkpoint manager for state persistence
    checkpoint_manager: Option<CheckpointManager>,

    /// Event channel for processing events
    event_sender: Option<mpsc::Sender<sinex_core::RawEvent>>,
    event_receiver: Option<mpsc::Receiver<sinex_core::RawEvent>>,
}

impl TerminalProcessor {
    /// Create a new unified terminal processor
    pub fn new() -> Self {
        Self {
            context: None,
            config: TerminalConfig::default(),
            sensd_processor: None,
            shell_info: None,
            last_state: None,
            checkpoint_manager: None,
            event_sender: None,
            event_receiver: None,
        }
    }

    /// Create processor with custom configuration
    pub fn with_config(config: TerminalConfig) -> Self {
        Self {
            context: None,
            config,
            sensd_processor: None,
            shell_info: None,
            last_state: None,
            checkpoint_manager: None,
            event_sender: None,
            event_receiver: None,
        }
    }

    /// Initialize sensd processor and submit terminal monitoring jobs
    async fn initialize_sensd_integration(&mut self) -> SatelliteResult<()> {
        info!("Initializing sensd integration for terminal monitoring");

        // Create event channel for communication between sensd processor and this processor
        let (sender, receiver) = mpsc::channel(1000);
        self.event_sender = Some(sender.clone());
        self.event_receiver = Some(receiver);

        // Create sensd processor
        let sensd_processor = SensdTerminalProcessor::new(self.config.sensd_config.clone(), sender)
            .await
            .map_err(|e| {
                sinex_satellite_sdk::SatelliteError::Processing(format!(
                    "Failed to create sensd processor: {}",
                    e
                ))
            })?;

        let sensd_processor = Arc::new(sensd_processor);

        // Submit monitoring jobs for enabled sources
        if self
            .config
            .enabled_sources
            .get("atuin")
            .copied()
            .unwrap_or(false)
        {
            if let Some(ref atuin_path) = self.config.atuin_db_path {
                if atuin_path.exists() {
                    info!("Submitting Atuin monitoring job: {}", atuin_path.as_str());
                    sensd_processor
                        .submit_atuin_job(atuin_path.as_str())
                        .await
                        .map_err(|e| {
                            sinex_satellite_sdk::SatelliteError::Processing(format!(
                                "Failed to submit Atuin job: {}",
                                e
                            ))
                        })?;
                } else {
                    warn!("Atuin database not found: {}", atuin_path.as_str());
                }
            }
        }

        if self
            .config
            .enabled_sources
            .get("history")
            .copied()
            .unwrap_or(false)
        {
            for history_file in &self.config.history_files {
                if history_file.exists() {
                    info!(
                        "Submitting history file monitoring job: {}",
                        history_file.as_str()
                    );
                    sensd_processor
                        .submit_history_file_job(history_file.as_str())
                        .await
                        .map_err(|e| {
                            sinex_satellite_sdk::SatelliteError::Processing(format!(
                                "Failed to submit history file job: {}",
                                e
                            ))
                        })?;
                }
            }
        }

        if self
            .config
            .enabled_sources
            .get("recording")
            .copied()
            .unwrap_or(false)
        {
            if let Some(ref recordings_dir) = self.config.recording_output_dir {
                info!(
                    "Submitting recording monitoring job: {}",
                    recordings_dir.as_str()
                );
                sensd_processor
                    .submit_recording_job(recordings_dir.as_str())
                    .await
                    .map_err(|e| {
                        sinex_satellite_sdk::SatelliteError::Processing(format!(
                            "Failed to submit recording job: {}",
                            e
                        ))
                    })?;
            }
        }

        if self
            .config
            .enabled_sources
            .get("kitty")
            .copied()
            .unwrap_or(false)
        {
            if let Some(ref socket_path) = self.config.kitty_socket_path {
                info!("Submitting Kitty monitoring job: {}", socket_path.as_str());
                sensd_processor
                    .submit_kitty_job(socket_path.as_str())
                    .await
                    .map_err(|e| {
                        sinex_satellite_sdk::SatelliteError::Processing(format!(
                            "Failed to submit Kitty job: {}",
                            e
                        ))
                    })?;
            }
        }

        self.sensd_processor = Some(sensd_processor);
        info!("sensd integration initialized successfully");
        Ok(())
    }

    /// Start sensd job monitoring
    async fn start_sensd_monitoring(&self) -> SatelliteResult<()> {
        if let Some(ref processor) = self.sensd_processor {
            info!("Starting sensd job monitoring");

            // Start the job monitoring task in background
            let monitor_processor = processor.clone();
            tokio::spawn(async move {
                if let Err(e) = monitor_processor.monitor_jobs().await {
                    warn!("sensd job monitoring error: {}", e);
                }
            });
        }

        Ok(())
    }

    /// Take a snapshot of current terminal state
    async fn take_snapshot(&mut self) -> SatelliteResult<TerminalState> {
        let mut enabled_sources = Vec::with_capacity(8);
        let mut history_file_status = HashMap::new();
        let mut atuin_status = None;
        let sensd_jobs = Vec::new();

        // Check enabled sources
        for (source, enabled) in &self.config.enabled_sources {
            if *enabled {
                enabled_sources.push(source.clone());
            }
        }

        // Check history files
        for history_file in &self.config.history_files {
            let status = Self::get_file_metadata_and_status(history_file).await;
            history_file_status.insert(history_file.clone(), status);
        }

        // Check Atuin database
        if let Some(ref atuin_path) = self.config.atuin_db_path {
            atuin_status = Some(Self::get_atuin_status(atuin_path).await);
        }

        // TODO: Query sensd jobs from database
        // This would require database access to query raw.sensor_jobs table

        let state = TerminalState {
            captured_at: Utc::now(),
            enabled_sources,
            history_file_status,
            atuin_status,
            shell_info: self.shell_info.clone(),
            recent_activity: vec![
                "Terminal processor snapshot taken (sensd-integrated)".to_string()
            ],
            sensd_jobs,
        };

        self.last_state = Some(state.clone());
        Ok(state)
    }

    /// Helper function to get file metadata and status
    async fn get_file_metadata_and_status(history_file: &Utf8PathBuf) -> HistoryFileStatus {
        // Validate path before file operations to prevent path traversal
        if validate_path(history_file.as_str()).is_err() {
            warn!(
                path = %history_file,
                "Skipping invalid or dangerous history file path"
            );
            return HistoryFileStatus::NonExistent;
        }

        if history_file.exists() {
            let metadata = tokio::fs::metadata(history_file).await.ok();
            let size_bytes = metadata.as_ref().map(|m| m.len()).unwrap_or(0);
            let last_modified = metadata
                .and_then(|m| m.modified().ok())
                .map(|t| DateTime::<Utc>::from(t));

            // Estimate entries by counting lines (rough estimate)
            let estimated_entries =
                if let Ok(content) = tokio::fs::read_to_string(history_file).await {
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
    async fn get_atuin_status(atuin_path: &sinex_core::SanitizedPath) -> AtuinStatus {
        if atuin_path.exists() {
            let metadata = tokio::fs::metadata(atuin_path).await.ok();
            let db_size_bytes = metadata.map(|m| m.len()).unwrap_or(0);

            // Query actual data from Atuin SQLite database
            let (estimated_entries, last_entry_timestamp) =
                if let Ok(conn) = rusqlite::Connection::open(atuin_path.as_path()) {
                    // Count entries
                    let count: u64 = conn
                        .query_row("SELECT COUNT(*) FROM history", [], |row| row.get(0))
                        .unwrap_or(0);

                    // Get most recent timestamp
                    let last_timestamp: Option<i64> = conn
                        .query_row("SELECT MAX(timestamp) FROM history", [], |row| row.get(0))
                        .ok();

                    let last_entry = last_timestamp.and_then(|ts| {
                        // Atuin stores timestamps in nanoseconds since epoch
                        let seconds = ts / 1_000_000_000;
                        let nanos = (ts % 1_000_000_000) as u32;
                        DateTime::from_timestamp(seconds, nanos)
                    });

                    (count, last_entry)
                } else {
                    // Fallback to estimate if we can't open the database
                    let estimated_entries = db_size_bytes / 100;
                    (estimated_entries, None)
                };

            AtuinStatus {
                db_exists: true,
                db_size_bytes,
                estimated_entries,
                last_entry_timestamp,
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

    /// Helper function to parse configuration field values
    fn parse_config_field<T: serde::de::DeserializeOwned>(
        config: &std::collections::HashMap<String, serde_json::Value>,
        key: &str,
    ) -> Option<T> {
        config
            .get(key)
            .and_then(|v| serde_json::from_value::<T>(v.clone()).ok())
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
        _config: Self::Config,
    ) -> SatelliteResult<()> {
        info!(
            processor = self.processor_name(),
            service = %ctx.service_name,
            "Initializing terminal processor with sensd integration"
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

        info!(
            enabled_sources = ?self.config.enabled_sources,
            atuin_db_path = ?self.config.atuin_db_path,
            history_files = ?self.config.history_files,
            "Terminal processor configuration"
        );

        // Initialize sensd integration
        self.initialize_sensd_integration().await?;

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
            "Starting terminal scan with sensd integration"
        );

        match until {
            TimeHorizon::Snapshot => {
                // Take current state snapshot
                let _state = self.take_snapshot().await?;
                successful_targets.push("terminal_state_snapshot".to_string());
                events_processed = 1;
            }

            TimeHorizon::Historical { .. } => {
                warnings.push(
                    "Historical scanning delegated to sensd - check sensd job status".to_string(),
                );
                successful_targets.push("sensd_historical_jobs".to_string());
                events_processed = 0; // sensd handles the actual processing
            }

            TimeHorizon::Continuous => {
                // Start continuous monitoring via sensd
                self.start_sensd_monitoring().await?;
                successful_targets.push("sensd_continuous_monitoring".to_string());
                events_processed = 0; // Continuous monitoring doesn't count discrete events
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
        let estimated_events = 100; // sensd handles the actual estimation
        let mut warnings = vec![
            "Event estimation delegated to sensd - actual numbers depend on source material"
                .to_string(),
        ];

        // Adjust estimate based on time horizon
        let (duration_factor, confidence) = match until {
            TimeHorizon::Snapshot => (0.1, 0.8), // Only current state
            TimeHorizon::Historical { .. } => (1.0, 0.3), // sensd handles historical
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
                "Terminal processor with sensd integration monitoring {} sources",
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
                    "sensd_integration".to_string(),
                    serde_json::Value::Bool(true),
                ),
                (
                    "processor_type".to_string(),
                    serde_json::Value::String("sensd-integrated".to_string()),
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
        // In sensd-integrated mode, ingestion history is managed by sensd
        Ok(vec![])
    }

    fn get_coverage_analysis(
        &self,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> color_eyre::eyre::Result<CoverageAnalysis> {
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
            sinex_total: 0, // Would query from database via sensd
            coverage_percentage: 0.0,
            missing_count: source_total,
            missing_samples: vec![MissingItem {
                source_id: "terminal".to_string(),
                timestamp: end_time,
                description: "Terminal events processed via sensd source material".to_string(),
                missing_reason: Some("Check sensd job status for actual processing".to_string()),
            }],
            duplicate_count: 0,
            recommendations: vec![
                "Monitor sensd job status for terminal source processing".to_string(),
                "Check temporal_ledger for source material capture".to_string(),
                "Verify sensd sensors are running for enabled sources".to_string(),
            ],
        })
    }

    fn export_data(
        &self,
        path: &sinex_core::SanitizedPath,
        format: ExportFormat,
    ) -> color_eyre::eyre::Result<()> {
        use sinex_core::types::validate_path;

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
                        csv.push_str(&format!("{},{},sensd-integrated\n", source, enabled));
                    }
                    csv
                }
                ExportFormat::Raw => format!("{:#?}", state),
            };

            std::fs::write(path.as_path(), content)?;
        } else {
            // Export configuration if no state available
            let config_data = serde_json::json!({
                "enabled_sources": self.config.enabled_sources,
                "sensd_integration": true,
                "processor_type": "sensd-integrated"
            });

            let content = match format {
                ExportFormat::Json => serde_json::to_string_pretty(&config_data)?,
                ExportFormat::Raw => format!("{:#?}", config_data),
                ExportFormat::Csv => "No state data available\n".to_string(),
            };

            std::fs::write(path.as_path(), content)?;
        }

        Ok(())
    }
}
