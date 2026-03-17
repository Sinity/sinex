//! Unified desktop node implementing Node
//!
//! This module implements the desktop node node supporting snapshot, historical, and
//! continuous scanning modes for desktop events.

// Use local facade for common types
use crate::common::{
    ActivityEntry, Checkpoint, CoverageAnalysis, Deserialize, HashMap, IngestionHistoryEntry,
    NodeCapabilities, NodeResult, NodeRuntimeState, ScanArgs, ScanReport, Serialize, SinexError,
    SourceState, TimeHorizon, error, info, instrument, parse_config_value, parse_typed_config,
    warn,
};

use crate::{ClipboardWatcher, WindowManagerWatcher, window_manager::WindowManagerType};
use sinex_node_sdk::{
    EventTransport,
    acquisition_manager::{AcquisitionManager, RotationPolicy},
    ingestor_node::IngestorNode,
    nats_publisher::NatsPublisher,
    stage_as_you_go::StageAsYouGoContext,
    watcher_handle::WatcherHandle,
};
use sinex_primitives::{Seconds, Timestamp};
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
    pub captured_at: Timestamp,

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
    pub last_clipboard_change: Option<Timestamp>,
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
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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
    pub clipboard_last_success: Option<Timestamp>,
    /// Last successful window manager event
    pub window_manager_last_success: Option<Timestamp>,
}

/// Persistent state for `IngestorNode`
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DesktopPersistentState {
    pub health: DesktopMonitorHealth,
    pub last_state: Option<DesktopState>,
}

/// Unified desktop node implementing Node with Stage-as-You-Go
///
/// This node captures desktop activity as source material first, then generates
/// events with proper provenance tracking via `JetStream` capture.
pub struct DesktopNode {
    /// Runtime state captured during initialization
    runtime: Option<NodeRuntimeState>,
    /// Configuration
    config: DesktopConfig,
    /// Stage-as-you-go context for event emission
    stage_context: Option<StageAsYouGoContext>,
    /// Acquisition manager for material capture
    acquisition: Option<Arc<AcquisitionManager>>,

    /// Watcher handles
    // We store the Watcher instance inside the handle's material context until started
    clipboard_watcher: Option<WatcherHandle<ClipboardWatcher>>,
    window_manager_watcher: Option<WatcherHandle<WindowManagerWatcher>>,
}

impl DesktopNode {
    const _MS_PER_EVENT: u64 = 10;
    const _BYTES_PER_EVENT: u64 = 256;

    /// Create a new unified desktop node
    #[must_use]
    pub fn new() -> Self {
        Self {
            runtime: None,
            config: DesktopConfig::default(),
            stage_context: None,
            acquisition: None,
            clipboard_watcher: None,
            window_manager_watcher: None,
        }
    }

    fn is_platform_missing_error(err: &SinexError) -> bool {
        err.context_map()
            .get("error_class")
            .is_some_and(|class| class.starts_with("desktop_platform_"))
    }

    /// Take a snapshot of current desktop state
    #[instrument(skip(self), fields(node = "desktop"))]
    async fn take_snapshot(&self, health: &DesktopMonitorHealth) -> NodeResult<DesktopState> {
        let mut enabled_sources = Vec::new();
        let mut clipboard_status = None;
        let mut window_manager_status = None;

        // Check enabled sources
        if self.config.clipboard_enabled {
            enabled_sources.push("clipboard".to_string());

            // Try to get clipboard status
            clipboard_status = Some(ClipboardStatus {
                monitoring_active: self
                    .clipboard_watcher
                    .as_ref()
                    .is_some_and(sinex_node_sdk::WatcherHandle::is_active),
                last_clipboard_change: health.clipboard_last_success,
                clipboard_content_hash: None, // Would need to hash current clipboard
                last_error: health.clipboard_last_error.clone(),
            });
        }

        if self.config.window_manager_enabled {
            enabled_sources.push("window_manager".to_string());

            // Try to get window manager status
            window_manager_status = Some(WindowManagerStatus {
                wm_type: self.config.window_manager_type.to_string(),
                connection_active: self
                    .window_manager_watcher
                    .as_ref()
                    .is_some_and(sinex_node_sdk::WatcherHandle::is_active),
                current_workspace: None, // Would need to query WM
                active_window: None,     // Would need to query WM
                total_windows: 0,        // Would need to query WM
                last_error: health.window_manager_last_error.clone(),
            });
        }

        let state = DesktopState {
            captured_at: Timestamp::now(),
            enabled_sources,
            clipboard_status,
            window_manager_status,
            recent_activity: vec!["Desktop node snapshot taken".to_string()],
        };

        Ok(state)
    }

    async fn initialize_watcher_handles(&mut self) -> NodeResult<()> {
        if self.config.clipboard_enabled && self.clipboard_watcher.is_none() {
            // Create initialized handle
            let handle = WatcherHandle::initialized("clipboard");
            self.clipboard_watcher = Some(handle);
        }

        if self.config.window_manager_enabled && self.window_manager_watcher.is_none() {
            let handle = WatcherHandle::initialized("window_manager");
            self.window_manager_watcher = Some(handle);
        }
        Ok(())
    }
}

impl Default for DesktopNode {
    fn default() -> Self {
        Self::new()
    }
}

impl IngestorNode for DesktopNode {
    type Config = DesktopConfig;
    type State = DesktopPersistentState;

    fn name(&self) -> &'static str {
        "desktop-watcher"
    }

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            supports_continuous: true,
            supports_snapshot: true,
            supports_interactive: false,
            max_scan_size: Some(1000),
            supports_concurrent: false,
            manages_own_continuous_loop: true,
            ..NodeCapabilities::default()
        }
    }

    #[instrument(skip(self, runtime, _state), fields(node = "desktop"))]
    async fn initialize(
        &mut self,
        mut config: Self::Config,
        runtime: &NodeRuntimeState,
        _state: &mut Self::State,
    ) -> NodeResult<()> {
        let service_name = runtime.service_info().service_name().to_string();

        info!(
            node = self.name(),
            service = %service_name,
            "Initializing desktop node"
        );

        // Apply config overrides logic
        if let Some(context_config) = parse_typed_config::<DesktopConfig, _>("desktop", runtime) {
            config = context_config;
        }

        if let Some(enabled) = parse_config_value::<bool, _>("clipboard_enabled", runtime) {
            config.clipboard_enabled = enabled;
        }
        if let Some(enabled) = parse_config_value::<bool, _>("window_manager_enabled", runtime) {
            config.window_manager_enabled = enabled;
        }
        if let Some(wm_type_str) = parse_config_value::<String, _>("window_manager_type", runtime) {
            if let Ok(wm_type) = wm_type_str.parse::<WindowManagerType>() {
                config.window_manager_type = wm_type;
            } else {
                warn!("Invalid window manager type: {}", wm_type_str);
            }
        }
        if let Some(interval) =
            parse_config_value::<Seconds, _>("clipboard_poll_interval_secs", runtime)
        {
            config.clipboard_poll_interval_secs = interval;
        }
        if let Some(require_hyprland) = parse_config_value::<bool, _>("require_hyprland", runtime) {
            config.require_hyprland = require_hyprland;
        }
        if let Ok(val) = std::env::var("SINEX_DESKTOP_REQUIRE_HYPRLAND") {
            config.require_hyprland = val.parse().unwrap_or(false);
        }

        info!(
            clipboard_enabled = config.clipboard_enabled,
            window_manager_enabled = config.window_manager_enabled,
            window_manager_type = %config.window_manager_type,
            clipboard_poll_interval_secs = config.clipboard_poll_interval_secs.as_secs(),
            require_hyprland = config.require_hyprland,
            "Desktop node configuration"
        );

        let publisher: Arc<NatsPublisher> = match runtime.transport() {
            EventTransport::Nats(publisher) => Arc::clone(publisher),
        };
        AcquisitionManager::bootstrap_streams(publisher.nats_client()).await?;

        let acquisition = Arc::new(runtime.acquisition_manager(
            RotationPolicy::default(),
            "desktop",
            "desktop-watcher",
        )?);
        let stage_context = StageAsYouGoContext::from_runtime(runtime)
            .with_acquisition_manager(Arc::clone(&acquisition))
            .with_default_reconciliation();

        self.runtime = Some(runtime.clone());
        self.config = config;
        self.stage_context = Some(stage_context);
        self.acquisition = Some(acquisition);

        self.initialize_watcher_handles().await?;

        Ok(())
    }

    #[instrument(skip(self, state), fields(node = "desktop"))]
    async fn scan_snapshot(
        &mut self,
        state: &mut Self::State,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        let start_time = std::time::Instant::now();

        let snapshot = self.take_snapshot(&state.health).await?;
        state.last_state = Some(snapshot.clone());

        let report = ScanReport {
            events_processed: snapshot.enabled_sources.len() as u64,
            duration: start_time.elapsed(),
            final_checkpoint: Checkpoint::timestamp(Timestamp::now(), None),
            time_range: Some((Timestamp::now(), Timestamp::now())),
            node_stats: HashMap::new(),
            successful_targets: vec!["desktop_snapshot".to_string()],
            failed_targets: vec![],
            warnings: vec![],
        };
        Ok(report)
    }

    #[instrument(skip(self, state), fields(node = "desktop"))]
    async fn scan_historical(
        &mut self,
        state: &mut Self::State,
        from: Checkpoint,
        _until: TimeHorizon,
        args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        // Historical scan for desktop: re-capture current desktop state from checkpoint.
        // Uses the same snapshot mechanism as scan_snapshot — desktop state is ephemeral,
        // so historical means "capture what's there now, knowing we're replaying from checkpoint."
        info!(
            checkpoint = ?from,
            replay = args.replay.is_some(),
            "Starting desktop historical scan"
        );
        let start_time = std::time::Instant::now();

        let snapshot = self.take_snapshot(&state.health).await?;
        state.last_state = Some(snapshot.clone());

        Ok(ScanReport {
            events_processed: snapshot.enabled_sources.len() as u64,
            duration: start_time.elapsed(),
            final_checkpoint: Checkpoint::timestamp(Timestamp::now(), None),
            time_range: Some((Timestamp::now(), Timestamp::now())),
            node_stats: HashMap::new(),
            successful_targets: vec!["desktop_historical".to_string()],
            failed_targets: vec![],
            warnings: Vec::new(),
        })
    }

    #[instrument(skip(self, state, shutdown_rx), fields(node = "desktop"))]
    async fn run_continuous(
        &mut self,
        state: &mut Self::State,
        _from: Checkpoint,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> NodeResult<ScanReport> {
        info!("Starting continuous desktop monitoring");
        let start_time = std::time::Instant::now();

        // Ensure handles are initialized
        self.initialize_watcher_handles().await?;

        let stage_context = self
            .stage_context
            .as_ref()
            .ok_or_else(|| SinexError::lifecycle("Stage context not initialized"))?;

        // Start Clipboard Watcher
        if self.config.clipboard_enabled
            && let Some(handle) = &mut self.clipboard_watcher
            && !handle.is_active()
        {
            // Create actual watcher
            let watcher_shutdown_rx = shutdown_rx.clone(); // Clone for this watcher

            // We need to create the watcher task.
            // The trick is WatcherHandle expects us to give it the task.
            // But we also need to keep the Watcher object alive if it has state?
            // Verify WatcherHandle design: it holds material.
            // ClipboardWatcher holds state.

            match ClipboardWatcher::new(
                self.config.clipboard_poll_interval_secs,
                stage_context.clone(),
                watcher_shutdown_rx,
            )
            .await
            {
                Ok(mut watcher) => {
                    let task = tokio::spawn(async move {
                        if let Err(e) = watcher.start_monitoring().await {
                            error!("Clipboard monitoring failed: {}", e);
                        }
                    });
                    let _ = handle.start(task, None);
                    state.health.clipboard_active = true;
                }
                Err(e) => {
                    if !Self::is_platform_missing_error(&e) || self.config.require_hyprland {
                        error!("Failed to initialize clipboard watcher: {}", e);
                        state.health.clipboard_active = false;
                        state.health.clipboard_last_error = Some(e.to_string());
                    } else {
                        warn!("Clipboard watcher skipped: {}", e);
                    }
                }
            }
        }

        // Start Window Manager Watcher
        if self.config.window_manager_enabled
            && let Some(handle) = &mut self.window_manager_watcher
            && !handle.is_active()
        {
            let watcher_shutdown_rx = shutdown_rx.clone();

            match WindowManagerWatcher::new(
                self.config.window_manager_type.clone(),
                stage_context.clone(),
                watcher_shutdown_rx,
            )
            .await
            {
                Ok(mut watcher) => {
                    let task = tokio::spawn(async move {
                        if let Err(e) = watcher.start_monitoring().await {
                            error!("Window manager monitoring failed: {}", e);
                        }
                    });
                    let _ = handle.start(task, None);
                    state.health.window_manager_active = true;
                }
                Err(e) => {
                    if !Self::is_platform_missing_error(&e) || self.config.require_hyprland {
                        error!("Failed to initialize window manager watcher: {}", e);
                        state.health.window_manager_active = false;
                        state.health.window_manager_last_error = Some(e.to_string());
                    } else {
                        warn!("Window manager watcher skipped: {}", e);
                    }
                }
            }
        }

        // Wait for shutdown
        let _ = shutdown_rx.changed().await;

        // Cleanup handled by Drop of WatcherHandles when DesktopNode is dropped?
        // IngestorNode doesn't drop self immediately, shutdown is called.

        Ok(ScanReport {
            events_processed: 0,
            duration: start_time.elapsed(),
            final_checkpoint: Checkpoint::timestamp(Timestamp::now(), None),
            time_range: Some((Timestamp::now(), Timestamp::now())),
            node_stats: HashMap::new(),
            successful_targets: vec!["desktop_continuous".to_string()],
            failed_targets: vec![],
            warnings: vec![],
        })
    }

    async fn shutdown(&mut self, _state: &Self::State) -> NodeResult<()> {
        if let Some(handle) = self.clipboard_watcher.take() {
            handle.shutdown().await;
        }
        if let Some(handle) = self.window_manager_watcher.take() {
            handle.shutdown().await;
        }
        Ok(())
    }

    // Impl ExplorationProvider via IngestorNode interface override
    fn get_source_state(&self, state: &Self::State) -> NodeResult<SourceState> {
        let recent_activity = if let Some(ref s) = state.last_state {
            s.recent_activity
                .iter()
                .enumerate()
                .map(|(i, desc)| ActivityEntry {
                    timestamp: s.captured_at - time::Duration::minutes(i as i64),
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
            description: "Desktop Source".to_string(),
            last_updated: state
                .last_state
                .as_ref()
                .map_or_else(Timestamp::now, |s| s.captured_at),
            total_items: None,
            healthy: state.health.clipboard_active
                || state.health.window_manager_active
                || active_sources == 0,
            recent_activity,
            metadata: HashMap::new(),
            is_connected: true,
            lag_seconds: None,
        })
    }

    fn get_ingestion_history(
        &self,
        _state: &Self::State,
        _limit: u64,
    ) -> NodeResult<Vec<IngestionHistoryEntry>> {
        // Desktop node doesn't maintain granular ingestion history yet
        Ok(vec![])
    }

    fn get_coverage_analysis(
        &self,
        _state: &Self::State,
        _time_range: Option<(sinex_primitives::Timestamp, sinex_primitives::Timestamp)>,
    ) -> NodeResult<CoverageAnalysis> {
        Ok(CoverageAnalysis {
            time_range: (
                sinex_primitives::Timestamp::now(),
                sinex_primitives::Timestamp::now(),
            ),
            source_total: 0,
            sinex_total: 0,
            coverage_percentage: 100.0,
            missing_count: 0,
            duplicate_count: 0,
            missing_samples: vec![],
            recommendations: vec![],
        })
    }
}

// End of file
