//! Unified desktop processor implementing StatefulStreamProcessor
//!
//! This module implements the desktop satellite processor supporting snapshot, historical, and
//! continuous scanning modes for desktop events.

// Use local facade for common types
use crate::common::*;

// Desktop-specific imports for sensd integration
use sinex_core::types::Ulid;
use sqlx::PgPool;

use crate::{window_manager::WindowManagerType, ClipboardWatcher, WindowManagerWatcher};

/// Desktop monitoring configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DesktopConfig {
    /// Enable clipboard monitoring
    pub clipboard_enabled: bool,
    /// Enable window manager monitoring  
    pub window_manager_enabled: bool,
    /// Window manager type (currently only "hyprland")
    pub window_manager_type: WindowManagerType,
    /// Clipboard monitoring interval (seconds)
    pub clipboard_poll_interval_secs: u64,
}

impl Default for DesktopConfig {
    fn default() -> Self {
        Self {
            clipboard_enabled: true,
            window_manager_enabled: true,
            window_manager_type: WindowManagerType::Hyprland,
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

/// Unified desktop processor implementing StatefulStreamProcessor with sensd integration
///
/// This processor captures desktop activity as source material first, then generates
/// events with proper provenance tracking via the sensd pattern.
pub struct DesktopProcessor {
    /// Runtime state captured during initialization
    runtime: Option<ProcessorRuntimeState>,
    /// Desktop monitoring configuration
    config: DesktopConfig,

    /// Individual watchers (initialized during operation)
    clipboard_watcher: Option<ClipboardWatcher>,
    window_manager_watcher: Option<WindowManagerWatcher>,

    /// Last captured desktop state for snapshots
    last_state: Option<DesktopState>,
}

impl DesktopProcessor {
    const MS_PER_EVENT: u64 = 10;
    const BYTES_PER_EVENT: u64 = 256;

    /// Create a new unified desktop processor
    pub fn new() -> Self {
        Self {
            runtime: None,
            config: DesktopConfig::default(),
            clipboard_watcher: None,
            window_manager_watcher: None,
            last_state: None,
        }
    }

    /// Create processor with custom configuration
    pub fn with_config(config: DesktopConfig) -> Self {
        Self {
            runtime: None,
            config,
            clipboard_watcher: None,
            window_manager_watcher: None,
            last_state: None,
        }
    }

    fn runtime(&self) -> SatelliteResult<&ProcessorRuntimeState> {
        self.runtime.as_ref().ok_or_else(|| {
            SatelliteError::General(eyre::eyre!("Desktop runtime handles not initialised"))
        })
    }

    fn db_pool(&self) -> SatelliteResult<&PgPool> {
        Ok(self.runtime()?.db_pool())
    }

    async fn initialise_with_runtime_state(
        &mut self,
        runtime: ProcessorRuntimeState,
        mut config: DesktopConfig,
    ) -> SatelliteResult<()> {
        let service_name = runtime.service_info().service_name().to_string();

        info!(
            processor = self.processor_name(),
            service = %service_name,
            "Initializing desktop processor"
        );

        // Allow overrides from the shared configuration map
        if let Some(context_config) = parse_typed_config::<DesktopConfig, _>("desktop", &runtime) {
            config = context_config;
        }

        if let Some(enabled) = parse_config_value::<bool, _>("clipboard_enabled", &runtime) {
            config.clipboard_enabled = enabled;
        }

        if let Some(enabled) = parse_config_value::<bool, _>("window_manager_enabled", &runtime) {
            config.window_manager_enabled = enabled;
        }

        if let Some(wm_type_str) = parse_config_value::<String, _>("window_manager_type", &runtime)
        {
            if let Ok(wm_type) = wm_type_str.parse::<WindowManagerType>() {
                config.window_manager_type = wm_type;
            } else {
                warn!("Invalid window manager type: {}", wm_type_str);
            }
        }

        if let Some(interval) =
            parse_config_value::<u64, _>("clipboard_poll_interval_secs", &runtime)
        {
            config.clipboard_poll_interval_secs = interval;
        }

        info!(
            clipboard_enabled = config.clipboard_enabled,
            window_manager_enabled = config.window_manager_enabled,
            window_manager_type = %config.window_manager_type,
            clipboard_poll_interval_secs = config.clipboard_poll_interval_secs,
            "Desktop processor configuration"
        );

        self.runtime = Some(runtime);
        self.config = config;
        self.clipboard_watcher = None;
        self.window_manager_watcher = None;
        self.last_state = None;

        Ok(())
    }

    /// Parse configuration value from context with type conversion

    /// Take a snapshot of current desktop state
    #[instrument(skip(self), fields(processor = "desktop"))]
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
                wm_type: self.config.window_manager_type.to_string(),
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
    #[instrument(skip(self), fields(processor = "desktop"))]
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

    /// Start continuous desktop monitoring by submitting jobs to sensd
    #[instrument(skip(self), fields(processor = "desktop", checkpoint = %_from_checkpoint.description()))]
    async fn start_continuous_monitoring(
        &mut self,
        _from_checkpoint: Checkpoint,
    ) -> SatelliteResult<()> {
        info!("Starting continuous desktop monitoring via sensd jobs");

        let db_pool = self.db_pool()?;

        // TODO: Migrate to AcquisitionManager from sinex-satellite-sdk
        // Sensd job submission removed - desktop monitoring needs to be migrated
        // to use AcquisitionManager for material capture

        /* REMOVED - sensd integration
        // Create sensd job submitter
        let submitter = crate::sensd_job_submitter::DesktopSensdSubmitter::new(
            db_pool.clone(),
            "desktop-satellite".to_string(),
        )
        .await
        .map_err(|e| SatelliteError::Processing(e.to_string()))?;

        // Submit job for clipboard monitoring if enabled
        if self.config.clipboard_enabled {
            info!("Submitting clipboard monitoring job to sensd");
            submitter
                .submit_clipboard_job(self.config.clipboard_poll_interval_secs)
                .await
                .map_err(|e| SatelliteError::Processing(e.to_string()))?;
        }

        // Submit job for window manager monitoring if enabled
        if self.config.window_manager_enabled {
            info!("Submitting window manager monitoring job to sensd");
            let socket_path = match self.config.window_manager_type {
                WindowManagerType::Hyprland => {
                    "/tmp/hypr/$HYPRLAND_INSTANCE_SIGNATURE/.socket2.sock"
                }
            };

            submitter
                .submit_window_manager_job(
                    self.config.window_manager_type.as_str(),
                    socket_path,
                )
                .await
                .map_err(|e| SatelliteError::Processing(e.to_string()))?;
        }
        */

        // Store monitoring started event as source material
        let monitoring_data = serde_json::json!({
            "event_type": "monitoring_started",
            "clipboard_enabled": self.config.clipboard_enabled,
            "window_manager_enabled": self.config.window_manager_enabled,
            "start_time": Utc::now().to_rfc3339(),
            "submitted_to_sensd": true
        });

        let _ = self
            .store_desktop_source_material(
                db_pool,
                "desktop_monitoring",
                &monitoring_data.to_string().into_bytes(),
                monitoring_data,
            )
            .await;

        Ok(())
    }

    /// Perform historical scan on desktop sources
    #[instrument(skip(self), fields(processor = "desktop", from = %_from.description(), emit_events))]
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

        if emit_events {
            let db_pool = self.db_pool()?;

            if self.config.clipboard_enabled {
                let scan_data = serde_json::json!({
                    "event_type": "historical_scan_attempt",
                    "source": "clipboard",
                    "scan_type": "historical",
                    "scan_time": Utc::now().to_rfc3339(),
                    "note": "Limited historical data available for desktop events"
                });

                let _ = self
                    .store_desktop_source_material(
                        db_pool,
                        "desktop_monitoring",
                        &scan_data.to_string().into_bytes(),
                        scan_data,
                    )
                    .await;
                event_count += 1;
            }

            if self.config.window_manager_enabled {
                let scan_data = serde_json::json!({
                    "event_type": "historical_scan_attempt",
                    "source": "window_manager",
                    "wm_type": self.config.window_manager_type.to_string(),
                    "scan_type": "historical",
                    "scan_time": Utc::now().to_rfc3339(),
                    "note": "Limited historical data available for window manager events"
                });

                let _ = self
                    .store_desktop_source_material(
                        db_pool,
                        "desktop_monitoring",
                        &scan_data.to_string().into_bytes(),
                        scan_data,
                    )
                    .await;
                event_count += 1;
            }
        }

        Ok(event_count)
    }

    /// Store desktop data as source material using sensd pattern
    async fn store_desktop_source_material(
        &self,
        db_pool: &PgPool,
        source_identifier: &str,
        data: &[u8],
        metadata: serde_json::Value,
    ) -> SatelliteResult<Option<Ulid>> {
        let material_id = Ulid::new();
        let acquired_at = Utc::now();

        // Store in source material registry
        let result = sqlx::query!(
            r#"
            INSERT INTO raw.source_material_registry (
                id,
                source_identifier, 
                staged_at,
                material_kind,
                timing_info_type,
                metadata,
                status,
                staged_by
            )
            VALUES ($1::ulid, $2, $3, $4, $5, $6, $7, $8)
            "#,
            material_id as Ulid, // $1 - id
            source_identifier,   // $2 - source_identifier
            acquired_at,         // $3 - staged_at
            "desktop_snapshot",  // $4 - material_kind
            "realtime",          // $5 - timing_info_type
            metadata,            // $6 - metadata
            "completed",         // $7 - status
            "desktop-monitor"    // $8 - staged_by
        )
        .execute(db_pool)
        .await;

        match result {
            Ok(_) => {
                // Create temporal ledger entry
                let _ = sqlx::query!(
                    r#"
                    INSERT INTO raw.temporal_ledger (
                        id,
                        source_material_id,
                        offset_start,
                        offset_end,
                        offset_kind,
                        ts_capture,
                        precision,
                        clock,
                        source_type
                    )
                    VALUES ($1::ulid, $2::ulid, 0, $3, 'byte', $4, 'millisecond', 'wall', 'realtime_capture')
                    "#,
                    Ulid::new() as Ulid,      // id
                    material_id as Ulid,      // source_material_id
                    data.len() as i64,        // offset_end
                    acquired_at               // ts_capture
                )
                .execute(db_pool)
                .await;

                Ok(Some(material_id))
            }
            Err(e) => {
                error!(error = %e, "Failed to store desktop source material");
                Ok(None)
            }
        }
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

    #[instrument(skip(self, init), fields(processor = "desktop", service = %init.service_info().service_name()))]
    async fn initialize(
        &mut self,
        init: ProcessorInitContext<Self::Config>,
    ) -> SatelliteResult<()> {
        let (config, runtime) = init.into_runtime();
        self.initialise_with_runtime_state(runtime, config).await
    }

    #[instrument(skip(self), fields(processor = "desktop", from = %from.description(), dry_run = args.dry_run, targets_count = args.targets.len()))]
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
                    warn!(error = %e, "Failed to initialize some watchers for snapshot");
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
                    // Store snapshot data as source material
                    let db_pool = self.db_pool()?;
                    let mut enabled_sources = Vec::new();
                    if self.config.clipboard_enabled {
                        enabled_sources.push("clipboard");
                    }
                    if self.config.window_manager_enabled {
                        enabled_sources.push("window_manager");
                    }

                    let snapshot_data = serde_json::json!({
                        "snapshot_type": "desktop_state",
                        "enabled_sources": enabled_sources,
                        "source_count": active_watchers,
                        "clipboard_enabled": self.config.clipboard_enabled,
                        "window_manager_enabled": self.config.window_manager_enabled,
                        "snapshot_time": Utc::now().to_rfc3339()
                    });

                    let _ = self
                        .store_desktop_source_material(
                            db_pool,
                            "desktop_snapshot",
                            &snapshot_data.to_string().into_bytes(),
                            snapshot_data,
                        )
                        .await;
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
                        error!(error = %e, "Historical desktop scan failed");
                        failed_targets.push(("desktop_historical_scan".to_string(), e.to_string()));
                    }
                }
            }

            TimeHorizon::Continuous => {
                // Initialize watchers for continuous monitoring
                debug!("Initializing watchers for continuous monitoring");
                self.initialize_watchers().await.map_err(|e| {
                    error!(error = %e, "Failed to initialize watchers for continuous monitoring");
                    e
                })?;

                // Start continuous monitoring
                info!("Starting continuous desktop monitoring");
                self.start_continuous_monitoring(from.clone())
                    .await
                    .map_err(|e| {
                        error!(error = %e, "Failed to start continuous monitoring");
                        e
                    })?;
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
            processor_stats: [
                (
                    "clipboard_enabled",
                    if self.config.clipboard_enabled { 1 } else { 0 },
                ),
                (
                    "window_manager_enabled",
                    if self.config.window_manager_enabled {
                        1
                    } else {
                        0
                    },
                ),
                ("successful_targets", successful_targets.len() as u64),
                ("failed_targets", failed_targets.len() as u64),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
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
            manages_own_continuous_loop: false,
        }
    }

    #[instrument(skip(self), fields(processor = "desktop"))]
    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        // For desktop monitoring, use timestamp-based checkpoints
        Ok(Checkpoint::timestamp(Utc::now(), None))
    }

    #[instrument(skip(self), fields(processor = "desktop", from = %_from.description()))]
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
            estimated_duration: Duration::from_millis(adjusted_events * Self::MS_PER_EVENT),
            estimated_data_size: adjusted_events * Self::BYTES_PER_EVENT,
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
            metadata: [
                (
                    "clipboard_enabled",
                    serde_json::to_value(self.config.clipboard_enabled)?,
                ),
                (
                    "window_manager_enabled",
                    serde_json::to_value(self.config.window_manager_enabled)?,
                ),
                (
                    "window_manager_type",
                    serde_json::to_value(self.config.window_manager_type.to_string())?,
                ),
                (
                    "clipboard_poll_interval_secs",
                    serde_json::to_value(self.config.clipboard_poll_interval_secs)?,
                ),
                (
                    "processor_type",
                    serde_json::Value::String("ingestor".to_string()),
                ),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
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
        path: &sinex_core::SanitizedPath,
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

            std::fs::write(path.as_str(), content)?;
        } else {
            // Export configuration if no state available
            let config_data = serde_json::json!({
                "clipboard_enabled": self.config.clipboard_enabled,
                "window_manager_enabled": self.config.window_manager_enabled,
                "window_manager_type": self.config.window_manager_type,
                "clipboard_poll_interval_secs": self.config.clipboard_poll_interval_secs
            });

            let content = match format {
                ExportFormat::Json => serde_json::to_string_pretty(&config_data)?,
                ExportFormat::Raw => format!("{:#?}", config_data),
                ExportFormat::Csv => "No state data available\n".to_string(),
            };

            std::fs::write(path.as_str(), content)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_satellite_sdk::stream_processor::{
        Checkpoint, ProcessorInitContext, ScanArgs, TimeHorizon,
    };
    use sinex_test_utils::{satellite_runtime::TestRuntimeBuilder, sinex_test, TestContext};

    #[sinex_test]
    async fn desktop_processor_emits_clipboard_events(ctx: TestContext) -> color_eyre::Result<()> {
        let runtime = TestRuntimeBuilder::new(&ctx, "desktop-processor")
            .build()
            .await?;
        let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();
        let init_ctx = ProcessorInitContext::new(
            DesktopConfig::default(),
            raw_config,
            service_info,
            handles,
            work_dir,
        );

        let mut processor = DesktopProcessor::new();
        processor.initialize(init_ctx).await?;

        let report = processor
            .scan(Checkpoint::None, TimeHorizon::Snapshot, ScanArgs::default())
            .await?;

        assert!(
            report.events_processed > 0,
            "Desktop snapshot scans should emit clipboard/window events"
        );

        Ok(())
    }
}
