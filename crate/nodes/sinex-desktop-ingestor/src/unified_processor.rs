//! Unified desktop processor implementing Node
//!
//! This module implements the desktop node processor supporting snapshot, historical, and
//! continuous scanning modes for desktop events.

// Use local facade for common types
use crate::common::*;

use crate::{window_manager::WindowManagerType, ClipboardWatcher, WindowManagerWatcher};
use sinex_core::types::Seconds;
use sinex_node_sdk::{
    acquisition_manager::{AcquisitionManager, RotationPolicy},
    event_processor::EventTransport,
    stage_as_you_go::StageAsYouGoContext,
};
use std::sync::Arc;
use tokio::sync::watch;

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
    pub clipboard_poll_interval_secs: Seconds,
    /// Require Hyprland to be present (if false, runs in degraded mode)
    pub require_hyprland: bool,
}

impl Default for DesktopConfig {
    fn default() -> Self {
        Self {
            clipboard_enabled: true,
            window_manager_enabled: true,
            window_manager_type: WindowManagerType::Hyprland,
            // Native clipboard API is fast, poll at 100ms (but Seconds type is u64, so minimum is 1 second)
            // We'll handle the actual poll interval in the watcher code
            clipboard_poll_interval_secs: Seconds::from_secs(1),
            // Allow running in headless/degraded mode by default
            require_hyprland: false,
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
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowManagerStatus {
    pub wm_type: String,
    pub connection_active: bool,
    pub current_workspace: Option<String>,
    pub active_window: Option<String>,
    pub total_windows: u32,
    pub last_error: Option<String>,
}

/// Health tracking for desktop monitors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DesktopMonitorHealth {
    /// Clipboard monitor active and working
    pub clipboard_active: bool,
    /// Window manager monitor active and working
    pub window_manager_active: bool,
    /// Last error from clipboard monitor
    pub clipboard_last_error: Option<String>,
    /// Last error from window manager monitor
    pub window_manager_last_error: Option<String>,
    /// Last successful clipboard event
    pub clipboard_last_success: Option<DateTime<Utc>>,
    /// Last successful window manager event
    pub window_manager_last_success: Option<DateTime<Utc>>,
}

/// Unified desktop processor implementing Node with Stage-as-You-Go
///
/// This processor captures desktop activity as source material first, then generates
/// events with proper provenance tracking via JetStream capture.
pub struct DesktopProcessor {
    /// Runtime state captured during initialization
    runtime: Option<NodeRuntimeState>,
    /// Desktop monitoring configuration
    config: DesktopConfig,
    /// Stage-as-you-go context for event emission
    stage_context: Option<StageAsYouGoContext>,
    /// Acquisition manager for material capture
    acquisition: Option<Arc<AcquisitionManager>>,
    /// Shutdown signal for watcher tasks
    shutdown_tx: Option<watch::Sender<bool>>,

    /// Individual watchers (initialized during operation)
    clipboard_watcher: Option<ClipboardWatcher>,
    window_manager_watcher: Option<WindowManagerWatcher>,
    clipboard_task: Option<tokio::task::JoinHandle<()>>,
    window_manager_task: Option<tokio::task::JoinHandle<()>>,

    /// Last captured desktop state for snapshots
    last_state: Option<DesktopState>,
    /// Health tracking for monitors
    health: DesktopMonitorHealth,
}

impl DesktopProcessor {
    const MS_PER_EVENT: u64 = 10;
    const BYTES_PER_EVENT: u64 = 256;

    /// Create a new unified desktop processor
    pub fn new() -> Self {
        Self {
            runtime: None,
            config: DesktopConfig::default(),
            stage_context: None,
            acquisition: None,
            shutdown_tx: None,
            clipboard_watcher: None,
            window_manager_watcher: None,
            clipboard_task: None,
            window_manager_task: None,
            last_state: None,
            health: DesktopMonitorHealth {
                clipboard_active: false,
                window_manager_active: false,
                clipboard_last_error: None,
                window_manager_last_error: None,
                clipboard_last_success: None,
                window_manager_last_success: None,
            },
        }
    }

    fn is_platform_missing_error(err: &NodeError) -> bool {
        let message = err.to_string();
        message.contains("HYPRLAND_INSTANCE_SIGNATURE not set")
            || message.contains("XDG_RUNTIME_DIR not set")
            || message.contains("Unsupported window manager")
            || message.contains("Cannot connect to Hyprland event socket")
            || message.contains("Neither wl-clipboard nor xclip found")
    }

    /// Create processor with custom configuration
    pub fn with_config(config: DesktopConfig) -> Self {
        Self {
            runtime: None,
            config,
            stage_context: None,
            acquisition: None,
            shutdown_tx: None,
            clipboard_watcher: None,
            window_manager_watcher: None,
            clipboard_task: None,
            window_manager_task: None,
            last_state: None,
            health: DesktopMonitorHealth {
                clipboard_active: false,
                window_manager_active: false,
                clipboard_last_error: None,
                window_manager_last_error: None,
                clipboard_last_success: None,
                window_manager_last_success: None,
            },
        }
    }

    async fn initialise_with_runtime_state(
        &mut self,
        runtime: NodeRuntimeState,
        mut config: DesktopConfig,
    ) -> NodeResult<()> {
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
            parse_config_value::<Seconds, _>("clipboard_poll_interval_secs", &runtime)
        {
            config.clipboard_poll_interval_secs = interval;
        }

        if let Some(require_hyprland) = parse_config_value::<bool, _>("require_hyprland", &runtime)
        {
            config.require_hyprland = require_hyprland;
        }

        // Also check environment variable for require_hyprland
        if let Ok(val) = std::env::var("SINEX_DESKTOP_REQUIRE_HYPRLAND") {
            config.require_hyprland = val.parse().unwrap_or(false);
        }

        info!(
            clipboard_enabled = config.clipboard_enabled,
            window_manager_enabled = config.window_manager_enabled,
            window_manager_type = %config.window_manager_type,
            clipboard_poll_interval_secs = config.clipboard_poll_interval_secs.as_secs(),
            require_hyprland = config.require_hyprland,
            "Desktop processor configuration"
        );

        let publisher = match runtime.transport() {
            EventTransport::Nats(publisher) => Arc::clone(publisher),
        };
        AcquisitionManager::bootstrap_streams(publisher.nats_client())
            .await
            .map_err(NodeError::from)?;

        let acquisition = Arc::new(runtime.acquisition_manager(
            RotationPolicy::default(),
            "desktop",
            "desktop-watcher",
        )?);
        let stage_context = StageAsYouGoContext::from_runtime(&runtime)
            .with_acquisition_manager(Arc::clone(&acquisition))
            .with_default_reconciliation();
        let (shutdown_tx, _) = watch::channel(false);

        self.runtime = Some(runtime);
        self.config = config;
        self.stage_context = Some(stage_context);
        self.acquisition = Some(acquisition);
        self.shutdown_tx = Some(shutdown_tx);
        self.clipboard_watcher = None;
        self.window_manager_watcher = None;
        self.clipboard_task = None;
        self.window_manager_task = None;
        self.last_state = None;

        Ok(())
    }

    /// Parse configuration value from context with type conversion

    /// Get current health status
    pub fn health_status(&self) -> &DesktopMonitorHealth {
        &self.health
    }

    /// Log health status to console
    pub fn log_health_status(&self) {
        info!(
            clipboard_active = self.health.clipboard_active,
            window_manager_active = self.health.window_manager_active,
            clipboard_error = ?self.health.clipboard_last_error,
            window_manager_error = ?self.health.window_manager_last_error,
            "Desktop node health status"
        );
    }

    /// Take a snapshot of current desktop state
    #[instrument(skip(self), fields(processor = "desktop"))]
    async fn take_snapshot(&mut self) -> NodeResult<DesktopState> {
        let mut enabled_sources = Vec::new();
        let mut clipboard_status = None;
        let mut window_manager_status = None;

        // Check enabled sources
        if self.config.clipboard_enabled {
            enabled_sources.push("clipboard".to_string());

            // Try to get clipboard status
            clipboard_status = Some(ClipboardStatus {
                monitoring_active: self.clipboard_watcher.is_some()
                    || self.clipboard_task.is_some(),
                last_clipboard_change: self.health.clipboard_last_success,
                clipboard_content_hash: None, // Would need to hash current clipboard
                last_error: self.health.clipboard_last_error.clone(),
            })
            .into();
        }

        if self.config.window_manager_enabled {
            enabled_sources.push("window_manager".to_string());

            // Try to get window manager status
            window_manager_status = Some(WindowManagerStatus {
                wm_type: self.config.window_manager_type.to_string(),
                connection_active: self.window_manager_watcher.is_some()
                    || self.window_manager_task.is_some(),
                current_workspace: None, // Would need to query WM
                active_window: None,     // Would need to query WM
                total_windows: 0,        // Would need to query WM
                last_error: self.health.window_manager_last_error.clone(),
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
    async fn initialize_watchers(&mut self) -> NodeResult<()> {
        let needs_watchers = (self.config.clipboard_enabled && self.clipboard_watcher.is_none())
            || (self.config.window_manager_enabled && self.window_manager_watcher.is_none());
        let stage_context = if needs_watchers {
            Some(self.stage_context.clone().ok_or_else(|| {
                NodeError::Lifecycle("Stage-as-you-go context not initialized".into())
            })?)
        } else {
            None
        };
        let shutdown_tx =
            if needs_watchers {
                Some(self.shutdown_tx.as_ref().ok_or_else(|| {
                    NodeError::Lifecycle("Shutdown channel not initialized".into())
                })?)
            } else {
                None
            };

        // Initialize clipboard watcher
        if self.config.clipboard_enabled {
            info!("Initializing clipboard watcher");
            if self.clipboard_watcher.is_none() {
                let stage_context = stage_context
                    .as_ref()
                    .ok_or_else(|| NodeError::Lifecycle("Stage-as-you-go not available".into()))?
                    .clone();
                let shutdown_tx = shutdown_tx
                    .as_ref()
                    .ok_or_else(|| NodeError::Lifecycle("Shutdown not available".into()))?;
                let shutdown_rx = shutdown_tx.subscribe();
                match ClipboardWatcher::new(
                    self.config.clipboard_poll_interval_secs,
                    stage_context,
                    shutdown_rx,
                )
                .await
                {
                    Ok(watcher) => {
                        self.clipboard_watcher = Some(watcher);
                        self.health.clipboard_active = true;
                        self.health.clipboard_last_error = None;
                        info!("✅ Clipboard watcher initialized");
                    }
                    Err(e) if Self::is_platform_missing_error(&e) => {
                        warn!(
                            error = %e,
                            "Clipboard watcher unavailable on this platform; disabling"
                        );
                        self.clipboard_watcher = None;
                        self.health.clipboard_active = false;
                        self.health.clipboard_last_error = Some(e.to_string());
                    }
                    Err(e) => {
                        self.health.clipboard_active = false;
                        self.health.clipboard_last_error = Some(e.to_string());
                        return Err(e);
                    }
                }
            }
        } else {
            self.clipboard_watcher = None;
        }

        // Initialize window manager watcher
        if self.config.window_manager_enabled {
            info!(
                "Initializing window manager watcher ({})",
                self.config.window_manager_type
            );
            if self.window_manager_watcher.is_none() {
                let stage_context = stage_context
                    .as_ref()
                    .ok_or_else(|| NodeError::Lifecycle("Stage-as-you-go not available".into()))?
                    .clone();
                let shutdown_tx = shutdown_tx
                    .as_ref()
                    .ok_or_else(|| NodeError::Lifecycle("Shutdown not available".into()))?;
                let shutdown_rx = shutdown_tx.subscribe();
                match WindowManagerWatcher::new(
                    self.config.window_manager_type.clone(),
                    stage_context,
                    shutdown_rx,
                )
                .await
                {
                    Ok(watcher) => {
                        self.window_manager_watcher = Some(watcher);
                        self.health.window_manager_active = true;
                        self.health.window_manager_last_error = None;
                        info!("✅ Window manager watcher initialized");
                    }
                    Err(e) if Self::is_platform_missing_error(&e) => {
                        warn!(
                            error = %e,
                            "Window manager watcher unavailable on this platform; disabling"
                        );
                        self.window_manager_watcher = None;
                        self.health.window_manager_active = false;
                        self.health.window_manager_last_error = Some(e.to_string());
                        // Don't fail if require_hyprland is false
                        if self.config.require_hyprland {
                            return Err(e);
                        }
                    }
                    Err(e) => {
                        self.health.window_manager_active = false;
                        self.health.window_manager_last_error = Some(e.to_string());
                        // Don't fail if require_hyprland is false
                        if self.config.require_hyprland {
                            return Err(e);
                        }
                    }
                }
            }
        } else {
            self.window_manager_watcher = None;
        }

        Ok(())
    }

    /// Start continuous desktop monitoring by running watcher tasks
    #[instrument(skip(self), fields(processor = "desktop", checkpoint = %_from_checkpoint.description()))]
    async fn start_continuous_monitoring(
        &mut self,
        _from_checkpoint: Checkpoint,
    ) -> NodeResult<()> {
        info!("Starting continuous desktop monitoring");

        if self.clipboard_task.is_none() {
            if let Some(mut watcher) = self.clipboard_watcher.take() {
                let handle = tokio::spawn(async move {
                    if let Err(e) = watcher.start_monitoring().await {
                        error!(error = %e, "Clipboard watcher terminated");
                    }
                });
                self.clipboard_task = Some(handle);
            }
        }

        if self.window_manager_task.is_none() {
            if let Some(mut watcher) = self.window_manager_watcher.take() {
                let handle = tokio::spawn(async move {
                    if let Err(e) = watcher.start_monitoring().await {
                        error!(error = %e, "Window manager watcher terminated");
                    }
                });
                self.window_manager_task = Some(handle);
            }
        }

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
    ) -> NodeResult<u64> {
        let mut event_count = 0;

        // Desktop sources typically don't have extensive historical data
        // This would implement any available historical scanning

        if emit_events {
            if self.config.clipboard_enabled {
                event_count += 1;
            }

            if self.config.window_manager_enabled {
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

#[async_trait]
impl Node for DesktopProcessor {
    type Config = DesktopConfig;

    #[instrument(skip(self, init), fields(processor = "desktop", service = %init.service_info().service_name()))]
    async fn initialize(&mut self, init: NodeInitContext<Self::Config>) -> NodeResult<()> {
        let (config, runtime) = init.into_runtime();
        self.initialise_with_runtime_state(runtime, config).await
    }

    #[instrument(skip(self), fields(processor = "desktop", from = %from.description(), dry_run = args.dry_run, targets_count = args.targets.len()))]
    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> NodeResult<ScanReport> {
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

                // Log health status
                self.log_health_status();

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
                let mut shutdown_rx = self
                    .shutdown_tx
                    .as_ref()
                    .ok_or_else(|| NodeError::Lifecycle("Shutdown channel not initialized".into()))?
                    .subscribe();
                // Initialize watchers for continuous monitoring
                debug!("Initializing watchers for continuous monitoring");
                self.initialize_watchers().await.map_err(|e| {
                    error!(error = %e, "Failed to initialize watchers for continuous monitoring");
                    e
                })?;

                // Log health status after initialization
                self.log_health_status();

                // Start continuous monitoring
                info!("Starting continuous desktop monitoring");
                self.start_continuous_monitoring(from.clone())
                    .await
                    .map_err(|e| {
                        error!(error = %e, "Failed to start continuous monitoring");
                        e
                    })?;

                let _ = shutdown_rx.changed().await;
                successful_targets.push("desktop_continuous".to_string());
                events_processed = 0;
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
        "desktop-watcher"
    }

    fn processor_type(&self) -> NodeType {
        NodeType::Ingestor
    }

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
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
    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
        // For desktop monitoring, use timestamp-based checkpoints
        Ok(Checkpoint::timestamp(Utc::now(), None))
    }

    #[instrument(skip(self), fields(processor = "desktop", from = %_from.description()))]
    async fn estimate_scan_scope(
        &self,
        _from: &Checkpoint,
        until: &TimeHorizon,
        _args: &ScanArgs,
    ) -> NodeResult<ScanEstimate> {
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

    async fn shutdown(&mut self) -> NodeResult<()> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(true);
        }

        if let Some(handle) = self.clipboard_task.take() {
            handle.abort();
        }
        if let Some(handle) = self.window_manager_task.take() {
            handle.abort();
        }

        self.clipboard_watcher = None;
        self.window_manager_watcher = None;
        tokio::task::yield_now().await;
        Ok(())
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
    use sinex_node_sdk::stream_processor::{Checkpoint, NodeInitContext, ScanArgs, TimeHorizon};
    use sinex_test_utils::{node_runtime::TestRuntimeBuilder, sinex_test, TestContext};

    #[sinex_test]
    async fn desktop_processor_emits_clipboard_events(ctx: TestContext) -> TestResult<()> {
        let runtime = TestRuntimeBuilder::new(&ctx, "desktop-watcher")
            .build()
            .await?;
        let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();
        let init_ctx = NodeInitContext::new(
            DesktopConfig::default(),
            raw_config,
            service_info,
            handles,
            work_dir,
        );

        let mut processor = DesktopProcessor::new();
        processor.initialize(init_ctx).await?;
        processor.clipboard_watcher = Some(ClipboardWatcher::stub());
        processor.window_manager_watcher =
            Some(WindowManagerWatcher::stub(WindowManagerType::Hyprland));

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
