//! Unified desktop processor implementing StatefulStreamProcessor
//!
//! This module implements the desktop satellite processor supporting snapshot, historical, and
//! continuous scanning modes for desktop events.

use async_trait::async_trait;
use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sinex_core::db::models::RawEvent;
use sinex_core::types::events::{
    ClipboardHistoricalPayload, DesktopMonitoringStartedPayload, DesktopSnapshotPayload,
    WindowManagerHistoricalPayload,
};
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

use crate::{ClipboardWatcher, WindowManagerWatcher};

/// Desktop monitoring configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DesktopConfig {
    /// Enable clipboard monitoring
    pub clipboard_enabled: bool,
    /// Enable window manager monitoring  
    pub window_manager_enabled: bool,
    /// Window manager type (currently only "hyprland")
    pub window_manager_type: String,
    /// Clipboard monitoring interval (seconds)
    pub clipboard_poll_interval_secs: u64,
}

impl Default for DesktopConfig {
    fn default() -> Self {
        Self {
            clipboard_enabled: true,
            window_manager_enabled: true,
            window_manager_type: "hyprland".to_string(),
            clipboard_poll_interval_secs: 2,
        }
    }
}

/// Desktop state snapshot for exploration and diagnostics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DesktopState {
    /// When the snapshot was taken
    pub captured_at: DateTime<Utc>,

    /// Enabled source types
    pub enabled_sources: Vec<String>,

    /// Clipboard status
    pub clipboard_status: Option<ClipboardStatus>,

    /// Window manager status
    pub window_manager_status: Option<WindowManagerStatus>,

    /// Recent activity summary
    pub recent_activity: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClipboardStatus {
    pub monitoring_active: bool,
    pub last_clipboard_change: Option<DateTime<Utc>>,
    pub clipboard_content_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowManagerStatus {
    pub wm_type: String,
    pub connection_active: bool,
    pub current_workspace: Option<String>,
    pub active_window: Option<String>,
    pub total_windows: u32,
}

/// Unified desktop processor implementing StatefulStreamProcessor
///
/// Supports snapshot, historical, and continuous scanning modes for desktop events.
pub struct DesktopProcessor {
    /// Current processing context (set during initialization)
    context: Option<StreamProcessorContext>,

    /// Desktop monitoring configuration
    config: DesktopConfig,

    /// Individual watchers (initialized during operation)
    clipboard_watcher: Option<ClipboardWatcher>,
    window_manager_watcher: Option<WindowManagerWatcher>,

    /// Last captured desktop state for snapshots
    last_state: Option<DesktopState>,

    /// Checkpoint manager for state persistence
    checkpoint_manager: Option<CheckpointManager>,
}

impl DesktopProcessor {
    /// Create a new unified desktop processor
    pub fn new() -> Self {
        Self {
            context: None,
            config: DesktopConfig::default(),
            clipboard_watcher: None,
            window_manager_watcher: None,
            last_state: None,
            checkpoint_manager: None,
        }
    }

    /// Create processor with custom configuration
    pub fn with_config(config: DesktopConfig) -> Self {
        Self {
            context: None,
            config,
            clipboard_watcher: None,
            window_manager_watcher: None,
            last_state: None,
            checkpoint_manager: None,
        }
    }

    /// Take a snapshot of current desktop state
    async fn take_snapshot(&mut self) -> SatelliteResult<DesktopState> {
        let mut enabled_sources = Vec::new();
        let mut clipboard_status = None;
        let mut window_manager_status = None;

        // Check enabled sources
        if self.config.clipboard_enabled {
            enabled_sources.push("clipboard".to_string());

            // Try to get clipboard status
            clipboard_status = Some(ClipboardStatus {
                monitoring_active: self.clipboard_watcher.is_some(),
                last_clipboard_change: None,  // Would need to track this
                clipboard_content_hash: None, // Would need to hash current clipboard
            })
            .into();
        }

        if self.config.window_manager_enabled {
            enabled_sources.push("window_manager".to_string());

            // Try to get window manager status
            window_manager_status = Some(WindowManagerStatus {
                wm_type: self.config.window_manager_type.clone(),
                connection_active: self.window_manager_watcher.is_some(),
                current_workspace: None, // Would need to query WM
                active_window: None,     // Would need to query WM
                total_windows: 0,        // Would need to query WM
            })
            .into();
        }

        let state = DesktopState {
            captured_at: Utc::now(),
            enabled_sources,
            clipboard_status,
            window_manager_status,
            recent_activity: vec!["Desktop processor snapshot taken".to_string()],
        };

        self.last_state = Some(state.clone());
        Ok(state)
    }

    /// Initialize watchers based on enabled sources
    async fn initialize_watchers(&mut self) -> SatelliteResult<()> {
        // Initialize clipboard watcher
        if self.config.clipboard_enabled {
            info!("Initializing clipboard watcher");
            // For now, stub implementation - will be implemented properly later
            info!("✅ Clipboard watcher initialized (stub)");
        }

        // Initialize window manager watcher
        if self.config.window_manager_enabled {
            info!(
                "Initializing window manager watcher ({})",
                self.config.window_manager_type
            );
            // For now, stub implementation - will be implemented properly later
            info!("✅ Window manager watcher initialized (stub)");
        }

        Ok(())
    }

    /// Start continuous desktop monitoring
    async fn start_continuous_monitoring(
        &mut self,
        _from_checkpoint: Checkpoint,
    ) -> SatelliteResult<()> {
        info!("Starting continuous desktop monitoring");

        // For now, stub implementation - will be implemented properly later
        // This would start the actual watchers and forward events

        if let Some(ref context) = self.context {
            info!("Desktop monitoring context available");

            // Create a sample event to show the interface works
            let sample_event: RawEvent = Event::from_payload(DesktopMonitoringStartedPayload {
                clipboard_enabled: self.config.clipboard_enabled,
                window_manager_enabled: self.config.window_manager_enabled,
                start_time: Utc::now(),
            })
            .into();

            context.emit_event(sample_event).await?;
        }

        Ok(())
    }

    /// Perform historical scan on desktop sources
    async fn scan_historical_desktop_data(
        &self,
        _from: &Checkpoint,
        _until: &TimeHorizon,
        _args: &ScanArgs,
        emit_events: bool,
    ) -> SatelliteResult<u64> {
        let mut event_count = 0;

        // Desktop sources typically don't have extensive historical data
        // This would implement any available historical scanning

        if let Some(ref context) = self.context {
            // Example: emit historical desktop state events
            if self.config.clipboard_enabled && emit_events {
                let event: RawEvent = Event::from_payload(ClipboardHistoricalPayload {
                    source: "clipboard".to_string(),
                    scan_type: "historical".to_string(),
                    note: "Limited historical data available for desktop events".to_string(),
                })
                .into();

                context.emit_event(event).await?;
                event_count += 1;
            }

            if self.config.window_manager_enabled && emit_events {
                let event: RawEvent = Event::from_payload(WindowManagerHistoricalPayload {
                    source: "window_manager".to_string(),
                    wm_type: self.config.window_manager_type.clone(),
                    scan_type: "historical".to_string(),
                    note: "Limited historical data available for window manager events".to_string(),
                })
                .into();

                context.emit_event(event).await?;
                event_count += 1;
            }
        }

        Ok(event_count)
    }
}

impl Default for DesktopProcessor {
    fn default() -> Self {
        Self::new()
    }
}

#[sinex_satellite_sdk::auto_satellite_metrics(processor_type = "ingestor", labels = ["source=desktop"])]
#[async_trait]
impl StatefulStreamProcessor for DesktopProcessor {
    type Config = DesktopConfig;

    async fn initialize(
        &mut self,
        ctx: StreamProcessorContext,
        config: Self::Config,
    ) -> SatelliteResult<()> {
        info!(
            processor = self.processor_name(),
            service = %ctx.service_name,
            "Initializing desktop processor"
        );

        // Initialize checkpoint manager
        self.checkpoint_manager = Some(ctx.checkpoint_manager.clone());

        // Parse configuration from processor context
        if let Some(config_json) = ctx.config.get("desktop") {
            match serde_json::from_value::<DesktopConfig>(config_json.clone()) {
                Ok(config) => {
                    self.config = config;
                }
                Err(e) => {
                    warn!("Failed to parse desktop config, using defaults: {}", e);
                }
            }
        }

        // Override with individual config values if present
        if let Some(clipboard_enabled_json) = ctx.config.get("clipboard_enabled") {
            if let Ok(enabled) = serde_json::from_value::<bool>(clipboard_enabled_json.clone()) {
                self.config.clipboard_enabled = enabled;
            }
        }

        if let Some(wm_enabled_json) = ctx.config.get("window_manager_enabled") {
            if let Ok(enabled) = serde_json::from_value::<bool>(wm_enabled_json.clone()) {
                self.config.window_manager_enabled = enabled;
            }
        }

        if let Some(wm_type_json) = ctx.config.get("window_manager_type") {
            if let Ok(wm_type) = serde_json::from_value::<String>(wm_type_json.clone()) {
                self.config.window_manager_type = wm_type;
            }
        }

        if let Some(poll_interval_json) = ctx.config.get("clipboard_poll_interval_secs") {
            if let Ok(interval) = serde_json::from_value::<u64>(poll_interval_json.clone()) {
                self.config.clipboard_poll_interval_secs = interval;
            }
        }

        info!(
            clipboard_enabled = self.config.clipboard_enabled,
            window_manager_enabled = self.config.window_manager_enabled,
            window_manager_type = %self.config.window_manager_type,
            clipboard_poll_interval_secs = self.config.clipboard_poll_interval_secs,
            "Desktop processor configuration"
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
        let mut successful_targets = Vec::new();
        let mut failed_targets = Vec::new();
        let mut warnings = Vec::new();

        info!(
            processor = self.processor_name(),
            from = %from.description(),
            until = ?until,
            targets = args.targets.len(),
            dry_run = args.dry_run,
            "Starting desktop scan"
        );

        match until {
            TimeHorizon::Snapshot => {
                // Take current state snapshot
                let _state = self.take_snapshot().await?;

                // Initialize watchers for snapshot capabilities
                if let Err(e) = self.initialize_watchers().await {
                    warnings.push(format!("Failed to initialize some watchers: {}", e));
                }

                // Count available desktop sources
                let active_watchers = [
                    self.clipboard_watcher.is_some(),
                    self.window_manager_watcher.is_some(),
                ]
                .iter()
                .filter(|&&x| x)
                .count();

                events_processed = active_watchers as u64;
                successful_targets.push("desktop_state_snapshot".to_string());

                if !args.dry_run {
                    // Emit a snapshot event
                    if let Some(ref context) = self.context {
                        let snapshot_event: RawEvent =
                            Event::from_payload(DesktopSnapshotPayload {
                                active_watchers,
                                clipboard_enabled: self.config.clipboard_enabled,
                                window_manager_enabled: self.config.window_manager_enabled,
                                snapshot_time: Utc::now(),
                            })
                            .into();

                        context.emit_event(snapshot_event).await?;
                    }
                }
            }

            TimeHorizon::Historical { .. } => {
                // Historical scan of desktop data
                warnings
                    .push("Historical desktop scanning has very limited capabilities".to_string());

                match self
                    .scan_historical_desktop_data(&from, &until, &args, !args.dry_run)
                    .await
                {
                    Ok(count) => {
                        events_processed = count;
                        successful_targets.push("desktop_historical_scan".to_string());
                    }
                    Err(e) => {
                        failed_targets.push(("desktop_historical_scan".to_string(), e.to_string()));
                    }
                }
            }

            TimeHorizon::Continuous => {
                // Initialize watchers for continuous monitoring
                self.initialize_watchers().await?;

                // Start continuous monitoring
                info!("Starting continuous desktop monitoring");
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
                    "clipboard_enabled".to_string(),
                    if self.config.clipboard_enabled { 1 } else { 0 },
                ),
                (
                    "window_manager_enabled".to_string(),
                    if self.config.window_manager_enabled {
                        1
                    } else {
                        0
                    },
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
        "desktop-processor"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Ingestor
    }

    fn capabilities(&self) -> ProcessorCapabilities {
        ProcessorCapabilities {
            supports_continuous: true,
            supports_historical: false, // Very limited historical data
            supports_snapshot: true,
            supports_interactive: false,
            max_scan_size: Some(1000), // Limited number of desktop events
            supports_concurrent: false,
        }
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        // For desktop monitoring, use timestamp-based checkpoints
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

        // Estimate based on enabled sources
        if self.config.clipboard_enabled {
            estimated_events += 10; // Low estimate for clipboard events
        }

        if self.config.window_manager_enabled {
            estimated_events += 50; // Higher estimate for window manager events
        }

        // Adjust estimate based on time horizon
        let (duration_factor, confidence) = match until {
            TimeHorizon::Snapshot => (0.1, 0.9), // Only current state
            TimeHorizon::Historical { .. } => {
                warnings.push("Desktop sources have very limited historical data".to_string());
                (0.1, 0.3) // Very limited historical data
            }
            TimeHorizon::Continuous => (f64::INFINITY, 0.1), // Unknown duration
        };

        let adjusted_events = (estimated_events as f64 * duration_factor) as u64;

        Ok(ScanEstimate {
            estimated_events: adjusted_events,
            estimated_duration: Duration::from_millis(adjusted_events * 10), // ~10ms per event
            estimated_data_size: adjusted_events * 256,                      // ~256 bytes per event
            estimated_targets: 2, // clipboard + window manager
            warnings,
            confidence,
        })
    }
}

// Implementation of ExplorationProvider for diagnostics
impl ExplorationProvider for DesktopProcessor {
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

        let active_sources = [
            self.config.clipboard_enabled,
            self.config.window_manager_enabled,
        ]
        .iter()
        .filter(|&&enabled| enabled)
        .count() as u64;

        Ok(SourceState {
            description: format!("Desktop processor monitoring {} sources", active_sources),
            last_updated: self
                .last_state
                .as_ref()
                .map(|s| s.captured_at)
                .unwrap_or_else(Utc::now),
            total_items: Some(active_sources),
            metadata: HashMap::from([
                (
                    "clipboard_enabled".to_string(),
                    serde_json::to_value(self.config.clipboard_enabled)?,
                ),
                (
                    "window_manager_enabled".to_string(),
                    serde_json::to_value(self.config.window_manager_enabled)?,
                ),
                (
                    "window_manager_type".to_string(),
                    serde_json::to_value(&self.config.window_manager_type)?,
                ),
                (
                    "clipboard_poll_interval_secs".to_string(),
                    serde_json::to_value(self.config.clipboard_poll_interval_secs)?,
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
        // In a real implementation, this would compare desktop state with Sinex events
        let (start_time, end_time) = time_range
            .unwrap_or_else(|| {
                let now = Utc::now();
                let hour_ago = now - chrono::Duration::hours(1);
                (hour_ago, now)
            })
            .into();

        let source_total = [
            self.config.clipboard_enabled,
            self.config.window_manager_enabled,
        ]
        .iter()
        .filter(|&&enabled| enabled)
        .count() as u64;

        Ok(CoverageAnalysis {
            time_range: (start_time, end_time),
            source_total,
            sinex_total: 0, // Would query from database
            coverage_percentage: 0.0,
            missing_count: source_total,
            missing_samples: vec![MissingItem {
                source_id: "desktop".to_string(),
                timestamp: end_time,
                description: "Desktop events not yet ingested into Sinex".to_string(),
                missing_reason: Some("Initial scan required".to_string()),
            }],
            duplicate_count: 0,
            recommendations: vec![
                "Run a snapshot scan to capture current desktop state".to_string(),
                "Enable continuous monitoring for real-time desktop events".to_string(),
                "Check clipboard and window manager configuration".to_string(),
            ],
        })
    }

    fn export_data(
        &self,
        path: &Utf8PathBuf,
        format: ExportFormat,
    ) -> color_eyre::eyre::Result<()> {
        if let Some(ref state) = self.last_state {
            let content = match format {
                ExportFormat::Json => serde_json::to_string_pretty(state)?,
                ExportFormat::Csv => {
                    // Simple CSV export
                    let mut csv = "source,enabled,status\n".to_string();
                    csv.push_str(&format!(
                        "clipboard,{},configured\n",
                        self.config.clipboard_enabled
                    ));
                    csv.push_str(&format!(
                        "window_manager,{},configured\n",
                        self.config.window_manager_enabled
                    ));
                    csv
                }
                ExportFormat::Raw => format!("{:#?}", state),
            };

            std::fs::write(path, content)?;
        } else {
            // Export configuration if no state available
            let config_data = serde_json::json!({
                "clipboard_enabled": self.config.clipboard_enabled,
                "window_manager_enabled": self.config.window_manager_enabled,
                "window_manager_type": self.config.window_manager_type,
                "clipboard_poll_interval_secs": self.config.clipboard_poll_interval_secs
            })
            .into();

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
