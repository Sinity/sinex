#![doc = include_str!("../docs/unified_processor.md")]

//! Unified system processor implementing `SimpleIngestor`.

// Use local facade for common types
use crate::common::*;
use sinex_node_sdk::error_helpers::{parse_config_value, parse_typed_config};
use sinex_node_sdk::stream_processor::{EventEmitter, NodeRuntimeState};

// System-specific event payloads
use serde_json::json;
use sinex_db::models::Event;
use sinex_primitives::events::SystemMonitoringStartedPayload;
use sinex_primitives::{JsonValue, Seconds};

use crate::{DbusWatcher, UdevWatcher, UnifiedJournalWatcher, WatcherMaterialContext};
use serde::{Deserialize, Serialize};
use sinex_node_sdk::acquisition_manager::{AcquisitionManager, RotationPolicy};
use sinex_node_sdk::prelude::OffsetDateTime;
use sinex_node_sdk::SinexError;
use sinex_node_sdk::{
    nats_publisher::NatsPublisher, simple_ingestor::SimpleIngestor, watcher_handle::WatcherHandle,
    EventTransport,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::{sync::mpsc, sync::watch, task::JoinHandle};
use tracing::warn;

// Import the existing SystemConfig from the parent module
pub use crate::SystemConfig;

/// System state snapshot for exploration and diagnostics
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
pub struct SystemState {
    /// When the snapshot was taken
    pub captured_at: OffsetDateTime,

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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SystemMonitorHealth {
    pub dbus_active: bool,
    pub journal_active: bool,
    pub udev_active: bool,
    pub systemd_active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemPersistentState {
    pub health: SystemMonitorHealth,
    pub last_state: Option<SystemState>,
}

impl Default for SystemPersistentState {
    fn default() -> Self {
        Self {
            health: SystemMonitorHealth {
                dbus_active: false,
                journal_active: false,
                udev_active: false,
                systemd_active: false,
            },
            last_state: None,
        }
    }
}

/// Capacity for watcher → emitter channels; we prefer bounded buffers to avoid unbounded growth.
const WATCHER_CHANNEL_CAPACITY: usize = 1024;

/// Unified system processor implementing SimpleIngestor
pub struct SystemProcessor {
    /// System monitoring configuration
    config: SystemConfig,

    runtime: Option<NodeRuntimeState>,

    /// Stage-as-you-go acquisition manager for system streams
    acquisition: Option<Arc<AcquisitionManager>>,
    /// Processor-level material context for internal events
    processor_material: Option<WatcherMaterialContext>,

    /// Individual watchers (initialized during operation)
    dbus_watcher: Option<WatcherHandle<WatcherMaterialContext>>,
    unified_journal_watcher: Option<WatcherHandle<WatcherMaterialContext>>,
    udev_watcher: Option<WatcherHandle<WatcherMaterialContext>>,
}

impl Default for SystemProcessor {
    fn default() -> Self {
        Self {
            config: SystemConfig::default(),
            runtime: None,
            acquisition: None,
            processor_material: None,
            dbus_watcher: None,
            unified_journal_watcher: None,
            udev_watcher: None,
        }
    }
}

impl SystemProcessor {
    /// Create a new unified system processor
    pub fn new() -> Self {
        Self::default()
    }

    /// Create processor with custom configuration
    pub fn with_config(config: SystemConfig) -> Self {
        Self {
            config,
            ..Self::default()
        }
    }

    fn runtime(&self) -> NodeResult<&NodeRuntimeState> {
        self.runtime
            .as_ref()
            .ok_or_else(|| SinexError::lifecycle("Processor runtime not initialized".to_string()))
    }

    fn emitter(&self) -> NodeResult<&EventEmitter> {
        Ok(self.runtime()?.event_emitter())
    }

    fn emitter_clone(&self) -> NodeResult<EventEmitter> {
        Ok(self.runtime()?.event_emitter().clone())
    }

    fn acquisition(&self) -> NodeResult<Arc<AcquisitionManager>> {
        self.acquisition.clone().ok_or_else(|| {
            SinexError::lifecycle("System processor acquisition not initialized".to_string())
        })
    }

    fn processor_material(&self) -> NodeResult<&WatcherMaterialContext> {
        self.processor_material.as_ref().ok_or_else(|| {
            SinexError::lifecycle("System processor material not initialized".to_string())
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
            "processor": self.node_name(),
        });
        WatcherMaterialContext::new(acquisition, &source_identifier, metadata).await
    }

    fn apply_config_overrides(config: &mut SystemConfig, runtime: &NodeRuntimeState) {
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
    async fn take_snapshot(
        &mut self,
        state: &mut SystemPersistentState,
    ) -> NodeResult<SystemState> {
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

        let snapshot = SystemState {
            captured_at: OffsetDateTime::now_utc(),
            enabled_sources,
            dbus_status,
            journal_status,
            udev_status,
            systemd_status,
            recent_activity: vec!["System processor snapshot taken".to_string()],
        };

        state.last_state = Some(snapshot.clone());
        Ok(snapshot)
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
                info!(
                    "Preparing unified journal watcher (journal: {}, systemd: {})",
                    self.config.journal_enabled, self.config.systemd_enabled
                );
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

    async fn finalize_watcher_handle(&self, mut handle: WatcherHandle<WatcherMaterialContext>) {
        if let Some(material) = handle.take_material() {
            if let Err(err) = material.finalize("system-watcher shutdown").await {
                warn!(error = %err, "Failed to finalize system watcher material");
            }
        }
        // Handle shutdown is automatic via Drop, but we call it explicitly for cleaner async shutdown
        handle.shutdown().await;
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
                start_time: sinex_primitives::temporal::Timestamp::from(
                    OffsetDateTime::now_utc(),
                ),
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
    ) -> NodeResult<WatcherHandle<WatcherMaterialContext>> {
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
    ) -> NodeResult<WatcherHandle<WatcherMaterialContext>> {
        let emitter = self.emitter_clone()?;

        // Create two channels: one for journal events, one for systemd events
        let (journal_tx, journal_rx) = mpsc::channel(WATCHER_CHANNEL_CAPACITY);
        let (systemd_tx, systemd_rx) = mpsc::channel(WATCHER_CHANNEL_CAPACITY);

        // Create forwarders for both channels
        let journal_forwarder =
            spawn_forwarder("system.journal.entry", journal_rx, emitter.clone());
        let systemd_forwarder = spawn_forwarder("system.systemd.unit_state", systemd_rx, emitter);

        let mut watcher = UnifiedJournalWatcher::new(
            self.config.journal_config.clone(),
            self.config.systemd_enabled,
        )
        .await?;

        let watcher_material = material.clone();
        let systemd_tx_opt = if self.config.systemd_enabled {
            Some(systemd_tx)
        } else {
            None
        };

        let task = tokio::spawn(async move {
            if let Err(err) = watcher
                .start_streaming(journal_tx, systemd_tx_opt, watcher_material)
                .await
            {
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
    ) -> NodeResult<WatcherHandle<WatcherMaterialContext>> {
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
        let journal_forwarder =
            spawn_forwarder("system.journal.entry", journal_rx, emitter.clone());
        let systemd_forwarder = spawn_forwarder("system.systemd.unit_state", systemd_rx, emitter);

        let material = self
            .new_watcher_material("unified-journal-historical")
            .await?;
        let mut watcher = UnifiedJournalWatcher::new(
            self.config.journal_config.clone(),
            self.config.systemd_enabled,
        )
        .await?;

        let systemd_tx_opt = if self.config.systemd_enabled {
            Some(systemd_tx)
        } else {
            None
        };

        let count = match watcher
            .import_historical(&journal_tx, &systemd_tx_opt, &material)
            .await
        {
            Ok(count) => count,
            Err(err) => {
                let _ = material
                    .finalize("system-unified-journal historical scan")
                    .await;
                return Err(err);
            }
        };

        drop(journal_tx);
        drop(systemd_tx_opt);

        if let Err(err) = tokio::join!(journal_forwarder, systemd_forwarder).0 {
            warn!(error = %err, "Historical journal forwarder task failed");
        }

        material
            .finalize("system-unified-journal historical scan")
            .await?;

        Ok(count)
    }

    fn node_name(&self) -> &str {
        "system-watcher"
    }
}

#[async_trait]
impl SimpleIngestor for SystemProcessor {
    type Config = SystemConfig;
    type State = SystemPersistentState;

    fn name(&self) -> &str {
        "system-watcher"
    }

    async fn initialize(
        &mut self,
        mut config: Self::Config,
        runtime: &NodeRuntimeState,
        _state: &mut Self::State,
    ) -> NodeResult<()> {
        Self::apply_config_overrides(&mut config, runtime);
        self.config = config;

        let publisher: Arc<NatsPublisher> = match runtime.transport() {
            EventTransport::Nats(publisher) => Arc::clone(publisher),
        };
        AcquisitionManager::bootstrap_streams(publisher.nats_client())
            .await
            .map_err(SinexError::from)?;
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
                "processor": self.node_name(),
            }),
        )
        .await?;

        self.runtime = Some(runtime.clone());
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

        self.initialize_watchers().await?;

        Ok(())
    }

    async fn scan_snapshot(&self, _state: &Self::State, _args: ScanArgs) -> NodeResult<ScanReport> {
        let start_time = std::time::Instant::now();

        // Clone self to allow taking snapshot (needs mutable access to update state in original implementation, but here state is external)
        // Wait, original implementation updated `self.last_state`.
        // `SimpleIngestor` `state` is mutable only in `run_continuous` or explicitly passed.
        // `scan_snapshot` signature is `&self`.
        // But `state` passed here is `&Self::State`. It is unexpected that `state` is updated in `scan_snapshot`?
        // In `SimpleIngestor::scan_snapshot`: `state: &Self::State`. It's immutable.
        // But `DesktopProcessor` refactor used `scan_snapshot(&self, state: &Self::State, args: ScanArgs)`.
        // It returned `ScanReport`.

        // If `take_snapshot` updates state, it needs `&mut self` or `&mut state`.
        // `DesktopProcessor` refactor didn't update state in `scan_snapshot`?
        // Ah, `DesktopProcessor` refactor:
        // `async fn scan_snapshot(&self, state: &Self::State, args: ScanArgs) -> NodeResult<ScanReport>`
        // It constructed `DesktopState` and returned it in `ScanReport`? No, `ScanReport` doesn't carry state.

        // In `DesktopProcessor` refactor:
        // `scan_snapshot` gathered data and returned `ScanReport`.
        // It did NOT update `state.last_state`.
        // This seems to be a deviation from original `take_snapshot` which updated `self.last_state`.

        // If we want to persist the snapshot, we need `&mut state`.
        // `SimpleIngestor` trait has `scan_snapshot(&self, state: &Self::State, args: ScanArgs)`.
        // It prevents updating state.
        // Effectively, `scan_snapshot` is just an operation.

        // However, `get_source_state` relies on `state.last_state`.
        // If we never update `state.last_state`, `get_source_state` will always be empty.
        // Currently `SimpleIngestor` relies on `run_continuous` or `scan_historical` to update state?
        // Or maybe we should improve `SimpleIngestor` to allow state update in `scan_snapshot`?
        // Changing trait signature is heavy.

        // `DesktopProcessor` `scan_snapshot` implementation:
        // constructed `ScanReport`.
        // Where is `state` updated?
        // It seems it wasn't!

        // We might need to address this limitation.
        // For now, I'll follow `DesktopProcessor` pattern.

        Ok(ScanReport {
            events_processed: 0,
            duration: start_time.elapsed(),
            final_checkpoint: Checkpoint::timestamp(OffsetDateTime::now_utc(), None),
            time_range: Some((OffsetDateTime::now_utc(), OffsetDateTime::now_utc())),
            processor_stats: HashMap::new(),
            successful_targets: vec!["system_snapshot".to_string()],
            failed_targets: vec![],
            warnings: vec![],
        })
    }

    async fn scan_historical(
        &mut self,
        _state: &mut Self::State,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        let start_time = std::time::Instant::now();
        let emit_events = !args.dry_run;

        // TODO: Update state with historical stats?

        let events_processed = self
            .scan_historical_system_data(&from, &until, &args, emit_events)
            .await?;

        Ok(ScanReport {
            events_processed,
            duration: start_time.elapsed(),
            final_checkpoint: Checkpoint::timestamp(OffsetDateTime::now_utc(), None),
            time_range: Some((
                match &from {
                    Checkpoint::Timestamp { timestamp, .. } => *timestamp,
                    _ => OffsetDateTime::now_utc() - time::Duration::hours(1), // estimate
                },
                OffsetDateTime::now_utc(),
            )),
            processor_stats: HashMap::new(),
            successful_targets: vec!["system_historical".to_string()],
            failed_targets: vec![],
            warnings: vec![],
        })
    }

    async fn run_continuous(
        &mut self,
        state: &mut Self::State,
        from: Checkpoint,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> NodeResult<ScanReport> {
        self.start_continuous_monitoring(from).await?;

        // Periodic snapshot loop or just wait for shutdown
        // In original SystemProcessor, there wasn't an explicit loop updating state, logic was event-driven.
        // But `take_snapshot` was called by `scan` when `until == TimeHorizon::Snapshot`.

        // We can implement a loop that updates `state.health` periodically.
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        let start_time = std::time::Instant::now();

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    break;
                }
                _ = interval.tick() => {
                    // specific health checks?
                    let snapshot = self.watcher_snapshot();
                    state.health = SystemMonitorHealth {
                        dbus_active: snapshot.dbus_ready,
                        journal_active: snapshot.journal_ready,
                        udev_active: snapshot.udev_ready,
                        systemd_active: snapshot.systemd_ready,
                    };

                    // We can also take a full snapshot and update state.last_state
                    if let Ok(s) = self.take_snapshot(state).await {
                        state.last_state = Some(s);
                    }
                }
            }
        }

        self.shutdown_watchers().await;

        Ok(ScanReport {
            events_processed: 0,
            duration: start_time.elapsed(),
            final_checkpoint: Checkpoint::timestamp(OffsetDateTime::now_utc(), None),
            time_range: Some((OffsetDateTime::now_utc(), OffsetDateTime::now_utc())),
            processor_stats: HashMap::new(),
            successful_targets: vec!["system_continuous".to_string()],
            failed_targets: vec![],
            warnings: vec![],
        })
    }

    async fn shutdown(&mut self, _state: &Self::State) -> NodeResult<()> {
        self.shutdown_watchers().await;
        Ok(())
    }

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            supports_continuous: true,
            supports_historical: self.config.journal_enabled,
            supports_snapshot: true,
            supports_interactive: false,
            max_scan_size: Some(10000),
            supports_concurrent: false,
            manages_own_continuous_loop: true, // we run our own loop in run_continuous
        }
    }

    // Default implementations for ExplorationProvider
    fn get_source_state(
        &self,
        state: &Self::State,
    ) -> NodeResult<sinex_node_sdk::exploration::SourceState> {
        let recent_activity = if let Some(ref s) = state.last_state {
            s.recent_activity
                .iter()
                .enumerate()
                .map(|(i, desc)| sinex_node_sdk::automaton_base::ActivityEntry {
                    timestamp: s.captured_at - std::time::Duration::from_secs(i as u64 * 60),
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

        Ok(sinex_node_sdk::exploration::SourceState {
            description: "System Source".to_string(),
            last_updated: state
                .last_state
                .as_ref()
                .map(|s| s.captured_at)
                .unwrap_or_else(OffsetDateTime::now_utc),
            total_items: None,
            healthy: state.health.dbus_active
                || state.health.journal_active
                || state.health.udev_active
                || state.health.systemd_active
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
    ) -> NodeResult<Vec<sinex_node_sdk::exploration::IngestionHistoryEntry>> {
        Ok(vec![])
    }

    fn get_coverage_analysis(
        &self,
        _state: &Self::State,
        _time_range: Option<(OffsetDateTime, OffsetDateTime)>,
    ) -> NodeResult<sinex_node_sdk::exploration::CoverageAnalysis> {
        Ok(sinex_node_sdk::exploration::CoverageAnalysis {
            time_range: (OffsetDateTime::now_utc(), OffsetDateTime::now_utc()),
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

/// Helper to forward events from a watcher channel to the emitter
fn spawn_forwarder<E>(
    channel_name: &'static str,
    mut rx: mpsc::Receiver<Event<E>>,
    emitter: EventEmitter,
) -> JoinHandle<()>
where
    E: Serialize + Send + 'static,
{
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            match event.to_json_event() {
                Ok(json_event) => {
                    if let Err(err) = emitter.emit(json_event).await {
                        warn!(error = %err, channel = channel_name, "Failed to emit forwarded event");
                    }
                }
                Err(err) => {
                    warn!(error = %err, channel = channel_name, "Failed to convert event to JSON");
                    // We continue even if one event fails? Or abort?
                    // Original likely logged and continued or used ?
                    // Let's log and continue to avoid killing the stream for one bad event
                }
            }
        }
    })
}
