#![doc = include_str!("../docs/unified_processor.md")]

//! Unified system processor implementing `Node`.

// Use local facade for common types
use crate::common::*;
use sinex_node_sdk::error_helpers::{parse_config_value, parse_typed_config};
use sinex_node_sdk::stream_processor::{
    EventEmitter, ProcessorInitContext, ProcessorRuntimeState,
};

// System-specific event payloads
use serde_json::json;
use sinex_core::db::models::Event;
use sinex_core::types::events::{SystemMonitoringStartedPayload, SystemSnapshotPayload};
use sinex_core::types::Seconds;
use sinex_core::JsonValue;

use crate::{DbusWatcher, UnifiedJournalWatcher, UdevWatcher, WatcherMaterialContext};
use sinex_node_sdk::acquisition_manager::{AcquisitionManager, RotationPolicy};
use sinex_node_sdk::event_processor::EventTransport;
use std::sync::Arc;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::warn;

// Import the existing SystemConfig from the parent module
pub use crate::SystemConfig;

/// System state snapshot for exploration and diagnostics
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
pub struct SystemState {
    /// When the snapshot was taken
    pub captured_at: DateTime<Utc>,

    /// Enabled source types
    pub enabled_sources: Vec<String>,

    /// D-Bus status
    pub dbus_status: Option<DbusStatus>,

    /// Journal status
    pub journal_status: Option<JournalStatus>,

    /// udev status
    pub udev_status: Option<UdevStatus>,

    /// systemd status
    pub systemd_status: Option<SystemdStatus>,

    /// Recent activity summary
    pub recent_activity: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbusStatus {
    pub buses_monitored: Vec<String>,
    pub connection_active: bool,
    pub recent_signal_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalStatus {
    pub following_active: bool,
    pub cursor_position: Option<String>,
    pub recent_entry_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdevStatus {
    pub monitoring_active: bool,
    pub recent_device_events: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemdStatus {
    pub monitoring_active: bool,
    pub units_tracked: u32,
    pub recent_state_changes: u32,
}

/// Snapshot describing which watchers are currently wired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatcherSnapshot {
    pub dbus_ready: bool,
    pub journal_ready: bool,
    pub udev_ready: bool,
    pub systemd_ready: bool,
}

impl WatcherSnapshot {
    pub fn all_ready(&self) -> bool {
        self.dbus_ready && self.journal_ready && self.udev_ready && self.systemd_ready
    }
}

/// Capacity for watcher → emitter channels; we prefer bounded buffers to avoid unbounded growth.
const WATCHER_CHANNEL_CAPACITY: usize = 1024;

#[derive(Debug)]
enum WatcherState {
    Initialized,
    Running {
        task: JoinHandle<()>,
        forwarder: Option<JoinHandle<()>>,
    },
}

#[derive(Debug)]
struct WatcherHandle {
    _name: &'static str,
    state: WatcherState,
    material: Option<WatcherMaterialContext>,
}

impl WatcherHandle {
    fn initialized(name: &'static str) -> Self {
        Self {
            _name: name,
            state: WatcherState::Initialized,
            material: None,
        }
    }

    fn running(
        name: &'static str,
        task: JoinHandle<()>,
        forwarder: Option<JoinHandle<()>>,
        material: Option<WatcherMaterialContext>,
    ) -> Self {
        Self {
            _name: name,
            state: WatcherState::Running { task, forwarder },
            material,
        }
    }

    fn is_active(&self) -> bool {
        match &self.state {
            WatcherState::Running { task, .. } => !task.is_finished(),
            WatcherState::Initialized => false,
        }
    }

    fn take_material(&mut self) -> Option<WatcherMaterialContext> {
        self.material.take()
    }
}

impl Drop for WatcherHandle {
    fn drop(&mut self) {
        match &self.state {
            WatcherState::Running { task, forwarder } => {
                task.abort();
                if let Some(handle) = forwarder {
                    handle.abort();
                }
            }
            WatcherState::Initialized => {}
        }
    }
}

/// Unified system processor implementing Node
///
/// Supports snapshot, historical, and continuous scanning modes for system events.
pub struct SystemProcessor {
    runtime: Option<ProcessorRuntimeState>,

    /// System monitoring configuration
    config: SystemConfig,

    /// Stage-as-you-go acquisition manager for system streams
    acquisition: Option<Arc<AcquisitionManager>>,
    /// Processor-level material context for internal events
    processor_material: Option<WatcherMaterialContext>,

    /// Individual watchers (initialized during operation)
    dbus_watcher: Option<WatcherHandle>,
    unified_journal_watcher: Option<WatcherHandle>,
    udev_watcher: Option<WatcherHandle>,

    /// Last captured system state for snapshots
    last_state: Option<SystemState>,
}

impl SystemProcessor {
    /// Create a new unified system processor
    pub fn new() -> Self {
        Self {
            runtime: None,
            config: SystemConfig::default(),
            acquisition: None,
            processor_material: None,
            dbus_watcher: None,
            unified_journal_watcher: None,
            udev_watcher: None,
            last_state: None,
        }
    }

    /// Create processor with custom configuration
    pub fn with_config(config: SystemConfig) -> Self {
        Self {
            runtime: None,
            config,
            acquisition: None,
            processor_material: None,
            dbus_watcher: None,
            unified_journal_watcher: None,
            udev_watcher: None,
            last_state: None,
        }
    }

    fn runtime(&self) -> NodeResult<&ProcessorRuntimeState> {
        self.runtime.as_ref().ok_or_else(|| {
            NodeError::Lifecycle("Processor runtime not initialized".to_string())
        })
    }

    fn emitter(&self) -> NodeResult<&EventEmitter> {
        Ok(self.runtime()?.event_emitter())
    }

    fn emitter_clone(&self) -> NodeResult<EventEmitter> {
        Ok(self.runtime()?.event_emitter().clone())
    }

    fn acquisition(&self) -> NodeResult<Arc<AcquisitionManager>> {
        self.acquisition.clone().ok_or_else(|| {
            NodeError::Lifecycle("System processor acquisition not initialized".to_string())
        })
    }

    fn processor_material(&self) -> NodeResult<&WatcherMaterialContext> {
        self.processor_material.as_ref().ok_or_else(|| {
            NodeError::Lifecycle("System processor material not initialized".to_string())
        })
    }

    async fn new_watcher_material(
        &self,
        watcher: &'static str,
    ) -> NodeResult<WatcherMaterialContext> {
        let acquisition = self.acquisition()?;
        let source_identifier = format!("system.{}", watcher);
        let metadata = json!({
            "watcher": watcher,
            "processor": self.processor_name(),
        });
        WatcherMaterialContext::new(acquisition, &source_identifier, metadata).await
    }

    fn apply_config_overrides(config: &mut SystemConfig, runtime: &ProcessorRuntimeState) {
        if let Some(overrides) = parse_typed_config::<SystemConfig, _>("system", runtime) {
            *config = overrides;
        }

        if let Some(enabled) = parse_config_value::<bool, _>("dbus_enabled", runtime) {
            config.dbus_enabled = enabled;
        }

        if let Some(enabled) = parse_config_value::<bool, _>("journal_enabled", runtime) {
            config.journal_enabled = enabled;
        }

        if let Some(enabled) = parse_config_value::<bool, _>("udev_enabled", runtime) {
            config.udev_enabled = enabled;
        }

        if let Some(enabled) = parse_config_value::<bool, _>("systemd_enabled", runtime) {
            config.systemd_enabled = enabled;
        }

        if let Some(buses) = parse_config_value::<String, _>("dbus_buses", runtime) {
            config.dbus_buses = buses;
        }

        if let Some(timeout) = parse_config_value::<Seconds, _>("journal_timeout_secs", runtime) {
            config.journal_timeout_secs = timeout;
        }
    }

    /// Take a snapshot of current system state
    #[instrument(skip(self), fields(processor = "system"))]
    async fn take_snapshot(&mut self) -> NodeResult<SystemState> {
        let mut enabled_sources = Vec::new();
        let mut dbus_status = None;
        let mut journal_status = None;
        let mut udev_status = None;
        let mut systemd_status = None;

        // Check enabled sources
        if self.config.dbus_enabled {
            enabled_sources.push("dbus".to_string());
            dbus_status = Some(DbusStatus {
                buses_monitored: self
                    .config
                    .dbus_buses
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect(),
                connection_active: self.dbus_watcher.is_some(),
                recent_signal_count: 0, // Would need to track this
            });
        }

        if self.config.journal_enabled {
            enabled_sources.push("journal".to_string());
            journal_status = Some(JournalStatus {
                following_active: self.unified_journal_watcher.is_some(),
                cursor_position: None, // Would need to track this
                recent_entry_count: 0, // Would need to track this
            });
        }

        if self.config.udev_enabled {
            enabled_sources.push("udev".to_string());
            udev_status = Some(UdevStatus {
                monitoring_active: self.udev_watcher.is_some(),
                recent_device_events: 0, // Would need to track this
            });
        }

        if self.config.systemd_enabled {
            enabled_sources.push("systemd".to_string());
            systemd_status = Some(SystemdStatus {
                monitoring_active: self.unified_journal_watcher.is_some(),
                units_tracked: 0,        // Would need to query systemd
                recent_state_changes: 0, // Would need to track this
            });
        }

        let state = SystemState {
            captured_at: Utc::now(),
            enabled_sources,
            dbus_status,
            journal_status,
            udev_status,
            systemd_status,
            recent_activity: vec!["System processor snapshot taken".to_string()],
        };

        self.last_state = Some(state.clone());
        Ok(state)
    }

    /// Expose watcher readiness for tests and diagnostics.
    pub fn watcher_snapshot(&self) -> WatcherSnapshot {
        let unified_ready = self.unified_journal_watcher.is_some();
        WatcherSnapshot {
            dbus_ready: self.dbus_watcher.is_some(),
            journal_ready: unified_ready,
            udev_ready: self.udev_watcher.is_some(),
            systemd_ready: unified_ready,
        }
    }

    /// Initialize watcher metadata (actual streaming starts during continuous scans).
    async fn initialize_watchers(&mut self) -> NodeResult<()> {
        if self.config.dbus_enabled {
            if self.dbus_watcher.is_none() {
                info!(
                    "Preparing D-Bus watcher (buses: {})",
                    self.config.dbus_buses
                );
                self.dbus_watcher = Some(WatcherHandle::initialized("dbus"));
            }
        } else {
            self.dbus_watcher = None;
        }

        if self.config.journal_enabled || self.config.systemd_enabled {
            if self.unified_journal_watcher.is_none() {
                info!("Preparing unified journal watcher (journal: {}, systemd: {})",
                      self.config.journal_enabled, self.config.systemd_enabled);
                self.unified_journal_watcher = Some(WatcherHandle::initialized("unified_journal"));
            }
        } else {
            self.unified_journal_watcher = None;
        }

        if self.config.udev_enabled {
            if self.udev_watcher.is_none() {
                info!("Preparing udev watcher");
                self.udev_watcher = Some(WatcherHandle::initialized("udev"));
            }
        } else {
            self.udev_watcher = None;
        }

        Ok(())
    }

    /// Abort and drop any active watcher handles.
    async fn shutdown_watchers(&mut self) {
        if let Some(handle) = self.dbus_watcher.take() {
            self.finalize_watcher_handle(handle).await;
        }
        if let Some(handle) = self.unified_journal_watcher.take() {
            self.finalize_watcher_handle(handle).await;
        }
        if let Some(handle) = self.udev_watcher.take() {
            self.finalize_watcher_handle(handle).await;
        }

        if let Some(material) = self.processor_material.take() {
            if let Err(err) = material.finalize("system-watcher shutdown").await {
                warn!(error = %err, "Failed to finalize system processor material");
            }
        }
    }

    /// Public shutdown hook used by tests and the runtime when tearing down the processor.
    pub async fn shutdown(&mut self) -> NodeResult<()> {
        self.shutdown_watchers().await;
        tokio::task::yield_now().await;
        Ok(())
    }

    async fn finalize_watcher_handle(&self, mut handle: WatcherHandle) {
        if let Some(material) = handle.take_material() {
            if let Err(err) = material.finalize("system-watcher shutdown").await {
                warn!(error = %err, "Failed to finalize system watcher material");
            }
        }
        drop(handle);
    }

    /// Start continuous system monitoring
    async fn start_continuous_monitoring(
        &mut self,
        _from_checkpoint: Checkpoint,
    ) -> NodeResult<()> {
        info!("Starting continuous system monitoring");

        self.start_dbus_stream().await?;
        self.start_unified_journal_stream().await?;
        self.start_udev_stream().await?;
        self.emit_monitoring_started_event().await?;

        Ok(())
    }

    async fn emit_monitoring_started_event(&self) -> NodeResult<()> {
        let emitter = self.emitter()?;
        let material = self.processor_material()?;

        let mut event = Event::new(
            SystemMonitoringStartedPayload {
                dbus_enabled: self.config.dbus_enabled,
                journal_enabled: self.config.journal_enabled,
                udev_enabled: self.config.udev_enabled,
                systemd_enabled: self.config.systemd_enabled,
                start_time: Utc::now(),
            },
            material.initial_provenance(),
        )
        .to_json_event()?;

        material.decorate_event(&mut event).await?;
        emitter.emit(event).await?;
        Ok(())
    }

    async fn start_dbus_stream(&mut self) -> NodeResult<()> {
        if !self.config.dbus_enabled {
            return Ok(());
        }

        if self
            .dbus_watcher
            .as_ref()
            .map(|handle| handle.is_active())
            .unwrap_or(false)
        {
            return Ok(());
        }

        if let Some(handle) = self.dbus_watcher.take() {
            self.finalize_watcher_handle(handle).await;
        }

        let material = self.new_watcher_material("dbus").await?;
        let handle = self.spawn_dbus_task(material).await?;
        self.dbus_watcher = Some(handle);

        Ok(())
    }

    async fn start_unified_journal_stream(&mut self) -> NodeResult<()> {
        if !self.config.journal_enabled && !self.config.systemd_enabled {
            return Ok(());
        }

        if self
            .unified_journal_watcher
            .as_ref()
            .map(|handle| handle.is_active())
            .unwrap_or(false)
        {
            return Ok(());
        }

        if let Some(handle) = self.unified_journal_watcher.take() {
            self.finalize_watcher_handle(handle).await;
        }

        let material = self.new_watcher_material("unified_journal").await?;
        let handle = self.spawn_unified_journal_task(material).await?;
        self.unified_journal_watcher = Some(handle);

        Ok(())
    }

    async fn start_udev_stream(&mut self) -> NodeResult<()> {
        if !self.config.udev_enabled {
            return Ok(());
        }

        if self
            .udev_watcher
            .as_ref()
            .map(|handle| handle.is_active())
            .unwrap_or(false)
        {
            return Ok(());
        }

        if let Some(handle) = self.udev_watcher.take() {
            self.finalize_watcher_handle(handle).await;
        }

        let material = self.new_watcher_material("udev").await?;
        let handle = self.spawn_udev_task(material).await?;
        self.udev_watcher = Some(handle);

        Ok(())
    }


    async fn spawn_dbus_task(
        &self,
        material: WatcherMaterialContext,
    ) -> NodeResult<WatcherHandle> {
        let emitter = self.emitter_clone()?;
        let (tx, rx) = mpsc::channel(WATCHER_CHANNEL_CAPACITY);
        let forwarder = spawn_forwarder("system.dbus.signal", rx, emitter);
        let mut watcher = DbusWatcher::new(self.config.dbus_config.clone()).await?;
        let watcher_material = material.clone();
        let task = tokio::spawn(async move {
            if let Err(err) = watcher.start_streaming(tx, watcher_material).await {
                warn!(error = %err, "D-Bus watcher terminated");
            }
        });
        Ok(WatcherHandle::running(
            "dbus",
            task,
            Some(forwarder),
            Some(material),
        ))
    }

    async fn spawn_unified_journal_task(
        &self,
        material: WatcherMaterialContext,
    ) -> NodeResult<WatcherHandle> {
        let emitter = self.emitter_clone()?;

        // Create two channels: one for journal events, one for systemd events
        let (journal_tx, journal_rx) = mpsc::channel(WATCHER_CHANNEL_CAPACITY);
        let (systemd_tx, systemd_rx) = mpsc::channel(WATCHER_CHANNEL_CAPACITY);

        // Create forwarders for both channels
        let journal_forwarder = spawn_forwarder("system.journal.entry", journal_rx, emitter.clone());
        let systemd_forwarder = spawn_forwarder("system.systemd.unit_state", systemd_rx, emitter);

        let mut watcher = UnifiedJournalWatcher::new(
            self.config.journal_config.clone(),
            self.config.systemd_enabled,
        ).await?;

        let watcher_material = material.clone();
        let systemd_tx_opt = if self.config.systemd_enabled {
            Some(systemd_tx)
        } else {
            None
        };

        let task = tokio::spawn(async move {
            if let Err(err) = watcher.start_streaming(journal_tx, systemd_tx_opt, watcher_material).await {
                warn!(error = %err, "Unified journal watcher terminated");
            }
        });

        // Spawn a task to join both forwarders
        let combined_forwarder = tokio::spawn(async move {
            let _ = tokio::join!(journal_forwarder, systemd_forwarder);
        });

        Ok(WatcherHandle::running(
            "unified_journal",
            task,
            Some(combined_forwarder),
            Some(material),
        ))
    }

    async fn spawn_udev_task(
        &self,
        material: WatcherMaterialContext,
    ) -> NodeResult<WatcherHandle> {
        let emitter = self.emitter_clone()?;
        let (tx, rx) = mpsc::channel(WATCHER_CHANNEL_CAPACITY);
        let forwarder = spawn_forwarder("system.udev.device", rx, emitter);
        let mut watcher = UdevWatcher::new(true).await?;
        let watcher_material = material.clone();
        let task = tokio::spawn(async move {
            if let Err(err) = watcher.start_streaming(tx, watcher_material).await {
                warn!(error = %err, "udev watcher terminated");
            }
        });
        Ok(WatcherHandle::running(
            "udev",
            task,
            Some(forwarder),
            Some(material),
        ))
    }


    /// Perform historical scan on system sources
    async fn scan_historical_system_data(
        &self,
        _from: &Checkpoint,
        _until: &TimeHorizon,
        _args: &ScanArgs,
        emit_events: bool,
    ) -> NodeResult<u64> {
        if !self.config.journal_enabled || !emit_events {
            return Ok(0);
        }

        let emitter = self.emitter_clone()?;

        // Create two channels: one for journal events, one for systemd events
        let (journal_tx, journal_rx) = mpsc::channel(WATCHER_CHANNEL_CAPACITY);
        let (systemd_tx, systemd_rx) = mpsc::channel(WATCHER_CHANNEL_CAPACITY);

        // Create forwarders for both channels
        let journal_forwarder = spawn_forwarder("system.journal.entry", journal_rx, emitter.clone());
        let systemd_forwarder = spawn_forwarder("system.systemd.unit_state", systemd_rx, emitter);

        let material = self.new_watcher_material("unified-journal-historical").await?;
        let mut watcher = UnifiedJournalWatcher::new(
            self.config.journal_config.clone(),
            self.config.systemd_enabled,
        ).await?;

        let systemd_tx_opt = if self.config.systemd_enabled {
            Some(systemd_tx)
        } else {
            None
        };

        let count = match watcher.import_historical(&journal_tx, &systemd_tx_opt, &material).await {
            Ok(count) => count,
            Err(err) => {
                let _ = material.finalize("system-unified-journal historical scan").await;
                return Err(err);
            }
        };

        drop(journal_tx);
        drop(systemd_tx_opt);

        if let Err(err) = tokio::join!(journal_forwarder, systemd_forwarder).0 {
            warn!(error = %err, "Historical journal forwarder task failed");
        }

        material.finalize("system-unified-journal historical scan").await?;

        Ok(count)
    }
}

impl Default for SystemProcessor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Node for SystemProcessor {
    type Config = SystemConfig;

    #[instrument(skip(self, init), fields(processor = "system", service = %init.service_info().service_name()))]
    async fn initialize(
        &mut self,
        init: ProcessorInitContext<Self::Config>,
    ) -> NodeResult<()> {
        let (mut config, runtime) = init.into_runtime();
        Self::apply_config_overrides(&mut config, &runtime);
        self.config = config;

        let publisher = match runtime.transport() {
            EventTransport::Nats(publisher) => Arc::clone(publisher),
        };
        AcquisitionManager::bootstrap_streams(publisher.nats_client())
            .await
            .map_err(NodeError::from)?;
        let acquisition = Arc::new(runtime.acquisition_manager(
            RotationPolicy::default(),
            "system",
            "system-watcher",
        )?);
        let processor_material = WatcherMaterialContext::new(
            Arc::clone(&acquisition),
            "system.processor",
            json!({
                "watcher": "processor",
                "processor": self.processor_name(),
            }),
        )
        .await?;

        self.runtime = Some(runtime);
        self.acquisition = Some(acquisition);
        self.processor_material = Some(processor_material);

        info!(
            dbus_enabled = self.config.dbus_enabled,
            journal_enabled = self.config.journal_enabled,
            udev_enabled = self.config.udev_enabled,
            systemd_enabled = self.config.systemd_enabled,
            dbus_buses = %self.config.dbus_buses,
            journal_timeout_secs = self.config.journal_timeout_secs.as_secs(),
            "System processor configuration"
        );

        Ok(())
    }

    #[instrument(skip(self), fields(processor = "system", from = %from.description(), dry_run = args.dry_run, targets_count = args.targets.len()))]
    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        let start_time = std::time::Instant::now();
        let mut successful_targets = Vec::new();
        let mut failed_targets = Vec::new();
        let mut warnings = Vec::new();

        info!(
            processor = self.processor_name(),
            from = %from.description(),
            until = ?until,
            targets = args.targets.len(),
            dry_run = args.dry_run,
            "Starting system scan"
        );

        let events_processed = match until {
            TimeHorizon::Snapshot => {
                self.scan_snapshot(&args, &mut successful_targets, &mut warnings)
                    .await?
            }
            TimeHorizon::Historical { .. } => {
                self.scan_historical(
                    &from,
                    &until,
                    &args,
                    &mut successful_targets,
                    &mut failed_targets,
                    &mut warnings,
                )
                .await?
            }
            TimeHorizon::Continuous => {
                self.scan_continuous(from.clone()).await?;
                0
            }
        };

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
            processor_stats: self.build_scan_stats(successful_targets.len(), failed_targets.len()),
            successful_targets,
            failed_targets,
            warnings,
        })
    }

    fn processor_name(&self) -> &str {
        "system-watcher"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Ingestor
    }

    fn capabilities(&self) -> ProcessorCapabilities {
        ProcessorCapabilities {
            supports_continuous: true,
            supports_historical: self.config.journal_enabled,
            supports_snapshot: true,
            supports_interactive: false,
            max_scan_size: Some(10000), // Reasonable limit for system events
            supports_concurrent: false,
            manages_own_continuous_loop: false,
        }
    }

    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
        // For system monitoring, use timestamp-based checkpoints
        Ok(Checkpoint::timestamp(Utc::now(), None))
    }

    async fn estimate_scan_scope(
        &self,
        _from: &Checkpoint,
        until: &TimeHorizon,
        _args: &ScanArgs,
    ) -> NodeResult<ScanEstimate> {
        let mut estimated_events = match until {
            TimeHorizon::Historical { .. } => {
                if self.config.journal_enabled {
                    200
                } else {
                    0
                }
            }
            _ => 0,
        };
        let warnings = Vec::new();

        if !matches!(until, TimeHorizon::Historical { .. }) {
            // Estimate based on enabled sources
            if self.config.dbus_enabled {
                estimated_events += 100; // D-Bus can be very active
            }

            if self.config.journal_enabled {
                estimated_events += 200; // Journal is typically very active
            }

            if self.config.udev_enabled {
                estimated_events += 20; // udev events are less frequent
            }

            if self.config.systemd_enabled {
                estimated_events += 50; // systemd state changes
            }
        }

        // Adjust estimate based on time horizon
        let (duration_factor, confidence) = match until {
            TimeHorizon::Snapshot => (0.1, 0.9), // Only current state
            TimeHorizon::Historical { .. } => (0.5, 0.6), // Some historical data available
            TimeHorizon::Continuous => (f64::INFINITY, 0.1), // Unknown duration
        };

        let adjusted_events = (estimated_events as f64 * duration_factor) as u64;

        Ok(ScanEstimate {
            estimated_events: adjusted_events,
            estimated_duration: Duration::from_millis(adjusted_events * 2), // ~2ms per event
            estimated_data_size: adjusted_events * 1024,                    // ~1KB per event
            estimated_targets: 4, // dbus + journal + udev + systemd
            warnings,
            confidence,
        })
    }

    #[instrument(skip(self), fields(processor = "system"))]
    async fn shutdown(&mut self) -> NodeResult<()> {
        SystemProcessor::shutdown(self).await
    }
}

impl SystemProcessor {
    async fn scan_snapshot(
        &mut self,
        args: &ScanArgs,
        successful_targets: &mut Vec<String>,
        warnings: &mut Vec<String>,
    ) -> NodeResult<u64> {
        let _state = self.take_snapshot().await?;

        if let Err(e) = self.initialize_watchers().await {
            warnings.push(format!("Failed to initialize some watchers: {}", e));
        }

        let active_watchers = self.active_watchers_count();
        successful_targets.push("system_state_snapshot".to_string());

        if !args.dry_run {
            let emitter = self.emitter()?;
            let material = self.processor_material()?;

            let mut snapshot_event = Event::new(
                SystemSnapshotPayload {
                    active_watchers,
                    dbus_enabled: self.config.dbus_enabled,
                    journal_enabled: self.config.journal_enabled,
                    udev_enabled: self.config.udev_enabled,
                    systemd_enabled: self.config.systemd_enabled,
                    snapshot_time: Utc::now(),
                },
                material.initial_provenance(),
            )
            .to_json_event()?;

            material.decorate_event(&mut snapshot_event).await?;
            emitter.emit(snapshot_event).await?;
        }

        Ok(active_watchers as u64)
    }

    async fn scan_historical(
        &mut self,
        from: &Checkpoint,
        until: &TimeHorizon,
        args: &ScanArgs,
        successful_targets: &mut Vec<String>,
        failed_targets: &mut Vec<(String, String)>,
        warnings: &mut Vec<String>,
    ) -> NodeResult<u64> {
        warnings.push("Historical system scans only import journal entries".to_string());

        match self
            .scan_historical_system_data(from, until, args, !args.dry_run)
            .await
        {
            Ok(count) => {
                successful_targets.push("system_historical_scan".to_string());
                Ok(count)
            }
            Err(e) => {
                failed_targets.push(("system_historical_scan".to_string(), e.to_string()));
                Ok(0)
            }
        }
    }

    async fn scan_continuous(&mut self, from: Checkpoint) -> NodeResult<()> {
        self.initialize_watchers().await?;

        info!("Starting continuous system monitoring");
        self.start_continuous_monitoring(from).await?;

        let mut shutdown = Box::pin(self.runtime()?.lifecycle_manager().shutdown_future());

        loop {
            self.supervise_watchers().await?;

            tokio::select! {
                _ = &mut shutdown => {
                    info!("Shutdown requested; stopping system watchers");
                    self.shutdown_watchers().await;
                    break;
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {}
            }
        }

        Ok(())
    }

    fn build_scan_stats(
        &self,
        successful_targets: usize,
        failed_targets: usize,
    ) -> HashMap<String, u64> {
        let mut stats = HashMap::with_capacity(6);
        stats.insert(
            "dbus_enabled".to_string(),
            if self.config.dbus_enabled { 1 } else { 0 },
        );
        stats.insert(
            "journal_enabled".to_string(),
            if self.config.journal_enabled { 1 } else { 0 },
        );
        stats.insert(
            "udev_enabled".to_string(),
            if self.config.udev_enabled { 1 } else { 0 },
        );
        stats.insert(
            "systemd_enabled".to_string(),
            if self.config.systemd_enabled { 1 } else { 0 },
        );
        stats.insert("successful_targets".to_string(), successful_targets as u64);
        stats.insert("failed_targets".to_string(), failed_targets as u64);
        stats
    }

    fn active_watchers_count(&self) -> usize {
        [
            self.dbus_watcher.is_some(),
            self.unified_journal_watcher.is_some(),
            self.udev_watcher.is_some(),
        ]
        .iter()
        .filter(|&&x| x)
        .count()
    }

    fn collect_restart_needed(&self) -> Vec<&'static str> {
        let mut restart_needed = Vec::new();

        if let Some(w) = &self.dbus_watcher {
            if !w.is_active() {
                warn!("D-Bus watcher is inactive, scheduling restart");
                restart_needed.push("dbus");
            }
        } else if self.config.dbus_enabled {
            restart_needed.push("dbus");
        }

        if let Some(w) = &self.unified_journal_watcher {
            if !w.is_active() {
                warn!("Unified journal watcher is inactive, scheduling restart");
                restart_needed.push("unified_journal");
            }
        } else if self.config.journal_enabled || self.config.systemd_enabled {
            restart_needed.push("unified_journal");
        }

        if let Some(w) = &self.udev_watcher {
            if !w.is_active() {
                warn!("Udev watcher is inactive, scheduling restart");
                restart_needed.push("udev");
            }
        } else if self.config.udev_enabled {
            restart_needed.push("udev");
        }

        restart_needed
    }

    async fn restart_watcher(&mut self, source: &'static str) -> NodeResult<()> {
        info!("Restarting watcher: {}", source);
        match source {
            "dbus" => {
                if let Err(e) = self.start_dbus_stream().await {
                    warn!("Failed to restart D-Bus stream: {}", e);
                    self.dbus_watcher = None; // Reset so next check tries again
                }
            }
            "unified_journal" => {
                if let Err(e) = self.start_unified_journal_stream().await {
                    warn!("Failed to restart unified journal stream: {}", e);
                    self.unified_journal_watcher = None;
                }
            }
            "udev" => {
                if let Err(e) = self.start_udev_stream().await {
                    warn!("Failed to restart udev stream: {}", e);
                    self.udev_watcher = None;
                }
            }
            _ => {}
        }

        Ok(())
    }

    async fn supervise_watchers_with<F>(&mut self, mut restart: F) -> NodeResult<()>
    where
        F: for<'a> FnMut(
            &'a mut Self,
            &'static str,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = NodeResult<()>> + Send + 'a>,
        >,
    {
        let restart_needed = self.collect_restart_needed();

        for source in restart_needed {
            restart(self, source).await?;
        }

        Ok(())
    }

    /// Check watchers and restart any that have failed
    async fn supervise_watchers(&mut self) -> NodeResult<()> {
        self.supervise_watchers_with(|this, source| Box::pin(this.restart_watcher(source)))
            .await
    }
}

// Implementation of ExplorationProvider for diagnostics
impl ExplorationProvider for SystemProcessor {
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
            self.config.dbus_enabled,
            self.config.journal_enabled,
            self.config.udev_enabled,
            self.config.systemd_enabled,
        ]
        .iter()
        .filter(|&&enabled| enabled)
        .count() as u64;

        Ok(SourceState {
            description: format!("System processor monitoring {} sources", active_sources),
            last_updated: self
                .last_state
                .as_ref()
                .map(|s| s.captured_at)
                .unwrap_or_else(Utc::now),
            total_items: Some(active_sources),
            metadata: {
                let mut metadata = HashMap::with_capacity(7);
                metadata.insert(
                    "dbus_enabled".to_string(),
                    serde_json::to_value(self.config.dbus_enabled)?,
                );
                metadata.insert(
                    "journal_enabled".to_string(),
                    serde_json::to_value(self.config.journal_enabled)?,
                );
                metadata.insert(
                    "udev_enabled".to_string(),
                    serde_json::to_value(self.config.udev_enabled)?,
                );
                metadata.insert(
                    "systemd_enabled".to_string(),
                    serde_json::to_value(self.config.systemd_enabled)?,
                );
                metadata.insert(
                    "dbus_buses".to_string(),
                    serde_json::to_value(&self.config.dbus_buses)?,
                );
                metadata.insert(
                    "journal_timeout_secs".to_string(),
                    serde_json::to_value(self.config.journal_timeout_secs)?,
                );
                metadata.insert(
                    "processor_type".to_string(),
                    serde_json::Value::String("ingestor".to_string()),
                );
                metadata
            },
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
        // In a real implementation, this would compare system state with Sinex events
        let (start_time, end_time) = time_range.unwrap_or_else(|| {
            let now = Utc::now();
            let hour_ago = now - chrono::Duration::hours(1);
            (hour_ago, now)
        });

        let source_total = [
            self.config.dbus_enabled,
            self.config.journal_enabled,
            self.config.udev_enabled,
            self.config.systemd_enabled,
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
                source_id: "system".to_string(),
                timestamp: end_time,
                description: "System events not yet ingested into Sinex".to_string(),
                missing_reason: Some("Initial scan required".to_string()),
            }],
            duplicate_count: 0,
            recommendations: vec![
                "Run a snapshot scan to capture current system state".to_string(),
                "Enable continuous monitoring for real-time system events".to_string(),
                "Check system source configuration (D-Bus, journal, udev, systemd)".to_string(),
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
                    csv.push_str(&format!("dbus,{},configured\n", self.config.dbus_enabled));
                    csv.push_str(&format!(
                        "journal,{},configured\n",
                        self.config.journal_enabled
                    ));
                    csv.push_str(&format!("udev,{},configured\n", self.config.udev_enabled));
                    csv.push_str(&format!(
                        "systemd,{},configured\n",
                        self.config.systemd_enabled
                    ));
                    csv
                }
                ExportFormat::Raw => format!("{:#?}", state),
            };

            std::fs::write(path.as_str(), content)?;
        } else {
            // Export configuration if no state available
            let config_data = serde_json::json!({
                "dbus_enabled": self.config.dbus_enabled,
                "journal_enabled": self.config.journal_enabled,
                "udev_enabled": self.config.udev_enabled,
                "systemd_enabled": self.config.systemd_enabled,
                "dbus_buses": self.config.dbus_buses,
                "journal_timeout_secs": self.config.journal_timeout_secs
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

fn spawn_forwarder(
    watcher: &'static str,
    mut rx: mpsc::Receiver<Event<JsonValue>>,
    emitter: EventEmitter,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            if emitter.dry_run() {
                continue;
            }
            if let Err(err) = emitter.emit(event).await {
                warn!(watcher = watcher, error = %err, "Failed to forward watcher event");
                break;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::{sinex_test, TestResult};
    use std::sync::Arc;

    fn enabled_config() -> SystemConfig {
        SystemConfig {
            dbus_enabled: true,
            journal_enabled: true,
            udev_enabled: true,
            systemd_enabled: true,
            ..SystemConfig::default()
        }
    }

    #[sinex_test]
    async fn system_processor_initializes_all_watchers_when_enabled() -> TestResult<()> {
        let mut processor = SystemProcessor::with_config(enabled_config());

        processor
            .initialize_watchers()
            .await
            .expect("watcher initialization should succeed when sources enabled");

        assert!(
            processor.dbus_watcher.is_some(),
            "D-Bus watcher should be instantiated once initialization succeeds"
        );
        assert!(
            processor.unified_journal_watcher.is_some(),
            "Unified journal watcher should be instantiated once initialization succeeds"
        );
        assert!(
            processor.udev_watcher.is_some(),
            "Udev watcher should be instantiated once initialization succeeds"
        );
        Ok(())
    }
    #[sinex_test]
    async fn system_processor_shutdown_aborts_watcher_tasks() -> TestResult<()> {
        use std::sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        };

        struct CancelFlag(Arc<AtomicBool>);

        impl Drop for CancelFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let cancelled = Arc::new(AtomicBool::new(false));
        let handle_cancelled = cancelled.clone();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _guard = CancelFlag(handle_cancelled);
            let _ = ready_tx.send(());
            std::future::pending::<()>().await;
        });

        let _ = ready_rx.await;

        let mut processor = SystemProcessor::with_config(enabled_config());
        processor.dbus_watcher = Some(WatcherHandle::running("dbus-fixture", task, None, None));

        processor.shutdown().await?;

        assert!(processor.dbus_watcher.is_none());
        assert!(
            cancelled.load(Ordering::SeqCst),
            "shutdown should abort watcher tasks"
        );
        Ok(())
    }

    #[sinex_test]
    async fn system_processor_supervises_inactive_watchers() -> TestResult<()> {
        async fn finished_task() -> JoinHandle<()> {
            let handle = tokio::spawn(async {});
            tokio::task::yield_now().await;
            handle
        }

        let mut processor = SystemProcessor::with_config(enabled_config());
        processor.dbus_watcher = Some(WatcherHandle::running(
            "dbus-fixture",
            finished_task().await,
            None,
            None,
        ));
        processor.unified_journal_watcher = None;
        processor.udev_watcher = Some(WatcherHandle::running(
            "udev-fixture",
            finished_task().await,
            None,
            None,
        ));

        let restart_log = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let log_handle = restart_log.clone();

        processor
            .supervise_watchers_with(|_processor, source| {
                let log_handle = log_handle.clone();
                Box::pin(async move {
                    log_handle
                        .lock()
                        .expect("restart log lock should be healthy")
                        .push(source.to_string());
                    Ok(())
                })
            })
            .await?;

        let mut restarted = restart_log
            .lock()
            .expect("restart log lock should be healthy")
            .clone();
        restarted.sort();

        // Unified journal watcher handles both journal and systemd
        // After sorting: "dbus" < "udev" < "unified_journal"
        assert_eq!(restarted, vec!["dbus", "udev", "unified_journal"]);
        Ok(())
    }
}
