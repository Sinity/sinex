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
    nats_publisher::NatsPublisher,
    simple_ingestor::SimpleIngestor,
    stage_as_you_go::StageAsYouGoContext,
    watcher_handle::WatcherHandle,
    EventTransport,
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
    pub clipboard_last_success: Option<DateTime<Utc>>,
    /// Last successful window manager event
    pub window_manager_last_success: Option<DateTime<Utc>>,
}

/// Persistent state for SimpleIngestor
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DesktopPersistentState {
    pub health: DesktopMonitorHealth,
    pub last_state: Option<DesktopState>,
}

/// Unified desktop processor implementing Node with Stage-as-You-Go
///
/// This processor captures desktop activity as source material first, then generates
/// events with proper provenance tracking via JetStream capture.
pub struct DesktopProcessor {
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

impl DesktopProcessor {
    const _MS_PER_EVENT: u64 = 10;
    const _BYTES_PER_EVENT: u64 = 256;

    /// Create a new unified desktop processor
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

    fn is_platform_missing_error(err: &NodeError) -> bool {
        let message = err.to_string();
        message.contains("HYPRLAND_INSTANCE_SIGNATURE not set")
            || message.contains("XDG_RUNTIME_DIR not set")
            || message.contains("Unsupported window manager")
            || message.contains("Cannot connect to Hyprland event socket")
            || message.contains("Neither wl-clipboard nor xclip found")
    }

    /// Take a snapshot of current desktop state
    #[instrument(skip(self), fields(processor = "desktop"))]
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
                    .map(|h| h.is_active())
                    .unwrap_or(false),
                last_clipboard_change: health.clipboard_last_success,
                clipboard_content_hash: None, // Would need to hash current clipboard
                last_error: health.clipboard_last_error.clone(),
            })
            .into();
        }

        if self.config.window_manager_enabled {
            enabled_sources.push("window_manager".to_string());

            // Try to get window manager status
            window_manager_status = Some(WindowManagerStatus {
                wm_type: self.config.window_manager_type.to_string(),
                connection_active: self
                    .window_manager_watcher
                    .as_ref()
                    .map(|h| h.is_active())
                    .unwrap_or(false),
                current_workspace: None, // Would need to query WM
                active_window: None,     // Would need to query WM
                total_windows: 0,        // Would need to query WM
                last_error: health.window_manager_last_error.clone(),
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

impl Default for DesktopProcessor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SimpleIngestor for DesktopProcessor {
    type Config = DesktopConfig;
    type State = DesktopPersistentState;

    fn name(&self) -> &str {
        "desktop-watcher"
    }

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            supports_continuous: true,
            supports_historical: false, // Very limited historical data
            supports_snapshot: true,
            supports_interactive: false,
            max_scan_size: Some(1000), // Limited number of desktop events
            supports_concurrent: false,
            manages_own_continuous_loop: true,
        }
    }

    #[instrument(skip(self, runtime, _state), fields(processor = "desktop"))]
    async fn initialize(
        &mut self,
        mut config: Self::Config,
        runtime: &NodeRuntimeState,
        _state: &mut Self::State,
    ) -> NodeResult<()> {
        let service_name = runtime.service_info().service_name().to_string();

        info!(
            processor = self.name(),
            service = %service_name,
            "Initializing desktop processor"
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
            "Desktop processor configuration"
        );

        let publisher: Arc<NatsPublisher> = match runtime.transport() {
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

    #[instrument(skip(self, state), fields(processor = "desktop"))]
    async fn scan_snapshot(&self, state: &Self::State, _args: ScanArgs) -> NodeResult<ScanReport> {
        let start_time = std::time::Instant::now();

        // Use state.health for reporting
        let snapshot = self.take_snapshot(&state.health).await?;

        // Note: SimpleIngestor doesn't pass &mut state to scan_snapshot, so we can't update last_state in persistent state?
        // Wait, SimpleIngestor trait definition: `async fn scan_snapshot(&self, state: &Self::State, ...)`
        // It takes immutable state! That's a limitation for snapshotting.
        // But internal watchers need to be initialized?
        // If we want to support snapshots that might init watchers, we might need interior mutability or just transient watchers.

        let report = ScanReport {
            events_processed: snapshot.enabled_sources.len() as u64,
            duration: start_time.elapsed(),
            final_checkpoint: Checkpoint::timestamp(Utc::now(), None),
            time_range: Some((Utc::now(), Utc::now())),
            processor_stats: HashMap::new(),
            successful_targets: vec!["desktop_snapshot".to_string()],
            failed_targets: vec![],
            warnings: vec![],
        };
        Ok(report)
    }

    #[instrument(skip(self, _state), fields(processor = "desktop"))]
    async fn scan_historical(
        &mut self,
        _state: &mut Self::State,
        _from: Checkpoint,
        _until: TimeHorizon,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::from_secs(0),
            final_checkpoint: Checkpoint::None,
            time_range: None,
            processor_stats: HashMap::new(),
            successful_targets: vec![],
            failed_targets: vec![],
            warnings: vec!["Historical scan not supported".to_string()],
        })
    }

    #[instrument(skip(self, state, shutdown_rx), fields(processor = "desktop"))]
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
            .ok_or_else(|| NodeError::Lifecycle("Stage context not initialized".into()))?;

        // Start Clipboard Watcher
        if self.config.clipboard_enabled {
            if let Some(handle) = &mut self.clipboard_watcher {
                if !handle.is_active() {
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
                            handle.start(task, None);
                            state.health.clipboard_active = true;
                        }
                        Err(e) => {
                            if !Self::is_platform_missing_error(&e) || self.config.require_hyprland
                            {
                                error!("Failed to initialize clipboard watcher: {}", e);
                                state.health.clipboard_active = false;
                                state.health.clipboard_last_error = Some(e.to_string());
                            } else {
                                warn!("Clipboard watcher skipped: {}", e);
                            }
                        }
                    }
                }
            }
        }

        // Start Window Manager Watcher
        if self.config.window_manager_enabled {
            if let Some(handle) = &mut self.window_manager_watcher {
                if !handle.is_active() {
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
                            handle.start(task, None);
                            state.health.window_manager_active = true;
                        }
                        Err(e) => {
                            if !Self::is_platform_missing_error(&e) || self.config.require_hyprland
                            {
                                error!("Failed to initialize window manager watcher: {}", e);
                                state.health.window_manager_active = false;
                                state.health.window_manager_last_error = Some(e.to_string());
                            } else {
                                warn!("Window manager watcher skipped: {}", e);
                            }
                        }
                    }
                }
            }
        }

        // Wait for shutdown
        let _ = shutdown_rx.changed().await;

        // Cleanup handled by Drop of WatcherHandles when DesktopProcessor is dropped?
        // SimpleIngestor doesn't drop self immediately, shutdown is called.

        Ok(ScanReport {
            events_processed: 0,
            duration: start_time.elapsed(),
            final_checkpoint: Checkpoint::timestamp(Utc::now(), None),
            time_range: Some((Utc::now(), Utc::now())),
            processor_stats: HashMap::new(),
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

    // Impl ExplorationProvider via SimpleIngestor interface override
    fn get_source_state(&self, state: &Self::State) -> NodeResult<SourceState> {
        let recent_activity = if let Some(ref s) = state.last_state {
            s.recent_activity
                .iter()
                .enumerate()
                .map(|(i, desc)| ActivityEntry {
                    timestamp: s.captured_at - chrono::Duration::minutes(i as i64),
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
                .map(|s| s.captured_at)
                .unwrap_or_else(Utc::now),
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
        // Desktop processor doesn't maintain granular ingestion history yet
        Ok(vec![])
    }

    fn get_coverage_analysis(
        &self,
        _state: &Self::State,
        _time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> NodeResult<CoverageAnalysis> {
        Ok(CoverageAnalysis {
            time_range: (Utc::now(), Utc::now()),
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
