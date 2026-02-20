#![doc = include_str!("../docs/unified_processor.md")]

//! Unified system processor implementing `SimpleIngestor`.

// Use local facade for common types
use crate::common::{
    async_trait, info, instrument, Checkpoint, NodeCapabilities, NodeResult, ScanArgs, ScanReport,
    TimeHorizon,
};
use sinex_node_sdk::error_helpers::{parse_config_value, parse_typed_config};
use sinex_node_sdk::stream_processor::{EventEmitter, NodeRuntimeState};

// System-specific event payloads
use serde_json::json;
use sinex_db::models::Event;
use sinex_primitives::events::SystemMonitoringStartedPayload;
use sinex_primitives::{Seconds, Timestamp};

use crate::material_context::RealWatcherMaterialContext;
use crate::watcher_factory::{RealWatcherFactory, WatcherFactory};
use crate::{UnifiedJournalWatcher, WatcherMaterialContext};
use serde::{Deserialize, Serialize};
use sinex_node_sdk::acquisition_manager::{AcquisitionManager, RotationPolicy};
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
    pub captured_at: Timestamp,

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
    #[must_use]
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

/// Per-watcher channel capacities, tuned to their event rate characteristics.
/// D-Bus can burst 1000+ events/sec during app launches; journal/systemd are lower throughput.
const DBUS_CHANNEL_CAPACITY: usize = 8192;
const JOURNAL_CHANNEL_CAPACITY: usize = 2048;
const SYSTEMD_CHANNEL_CAPACITY: usize = 512;
const UDEV_CHANNEL_CAPACITY: usize = 2048;

/// Unified system processor implementing `SimpleIngestor`
pub struct SystemProcessor {
    /// System monitoring configuration
    config: SystemConfig,

    /// Watcher factory for creating system watchers (real or mock)
    factory: Box<dyn WatcherFactory>,

    runtime: Option<NodeRuntimeState>,

    /// Stage-as-you-go acquisition manager for system streams
    acquisition: Option<Arc<AcquisitionManager>>,
    /// Processor-level material context for internal events
    processor_material: Option<WatcherMaterialContext>,

    /// Individual watchers (initialized during operation)
    dbus_watcher: Option<WatcherHandle<WatcherMaterialContext>>,
    unified_journal_watcher: Option<WatcherHandle<WatcherMaterialContext>>,
    udev_watcher: Option<WatcherHandle<WatcherMaterialContext>>,

    /// Optional emitter override for testing (avoids full runtime setup)
    emitter_override: Option<EventEmitter>,
}

impl Default for SystemProcessor {
    fn default() -> Self {
        Self {
            config: SystemConfig::default(),
            factory: Box::new(RealWatcherFactory),
            runtime: None,
            acquisition: None,
            processor_material: None,
            dbus_watcher: None,
            unified_journal_watcher: None,
            udev_watcher: None,
            emitter_override: None,
        }
    }
}

impl SystemProcessor {
    /// Create a new unified system processor
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new processor with a custom factory (for testing)
    #[must_use]
    pub fn new_with_factory(factory: Box<dyn WatcherFactory>) -> Self {
        Self {
            factory,
            ..Self::default()
        }
    }

    /// Create processor with custom configuration
    #[must_use]
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
        if let Some(e) = &self.emitter_override {
            return Ok(e);
        }
        Ok(self.runtime()?.event_emitter())
    }

    fn emitter_clone(&self) -> NodeResult<EventEmitter> {
        if let Some(e) = &self.emitter_override {
            return Ok(e.clone());
        }
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
        watcher: &str, // removed static lifetime requirement
    ) -> NodeResult<WatcherMaterialContext> {
        // Fallback for tests: if acquisition not present, assume mocking via processor_material
        if self.acquisition.is_none() {
            if let Some(ref m) = self.processor_material {
                return Ok(m.clone());
            }
        }

        let acquisition = self.acquisition()?;
        let source_identifier = format!("system.{watcher}");
        let metadata = json!({
            "watcher": watcher,
            "processor": self.node_name(),
        });
        let context =
            RealWatcherMaterialContext::new(acquisition, &source_identifier, metadata).await?;
        Ok(Arc::new(context))
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
                recent_signal_count: self
                    .dbus_watcher
                    .as_ref()
                    .and_then(|w| w.material())
                    .map(|m| m.event_count() as u32)
                    .unwrap_or(0),
            });
        }

        if self.config.journal_enabled {
            enabled_sources.push("journal".to_string());
            journal_status = Some(JournalStatus {
                following_active: self.unified_journal_watcher.is_some(),
                cursor_position: None, // Would need to track this
                recent_entry_count: self
                    .unified_journal_watcher
                    .as_ref()
                    .and_then(|w| w.material())
                    .map(|m| m.event_count() as u32)
                    .unwrap_or(0),
            });
        }

        if self.config.udev_enabled {
            enabled_sources.push("udev".to_string());
            udev_status = Some(UdevStatus {
                monitoring_active: self.udev_watcher.is_some(),
                recent_device_events: self
                    .udev_watcher
                    .as_ref()
                    .and_then(|w| w.material())
                    .map(|m| m.event_count() as u32)
                    .unwrap_or(0),
            });
        }

        if self.config.systemd_enabled {
            enabled_sources.push("systemd".to_string());
            systemd_status = Some(SystemdStatus {
                monitoring_active: self.unified_journal_watcher.is_some(),
                units_tracked: 0,        // Would need to query systemd
                recent_state_changes: 0, // Systemd events are mixed in journal watcher, hard to separate without filter
            });
        }

        let snapshot = SystemState {
            captured_at: Timestamp::now(),
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
    #[must_use]
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
                start_time: Timestamp::now(),
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
            .is_some_and(sinex_node_sdk::WatcherHandle::is_active)
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
            .is_some_and(sinex_node_sdk::WatcherHandle::is_active)
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
            .is_some_and(sinex_node_sdk::WatcherHandle::is_active)
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
        let (tx, rx) = mpsc::channel(DBUS_CHANNEL_CAPACITY);
        let forwarder = spawn_forwarder("system.dbus.signal", rx, emitter);
        let mut watcher = self
            .factory
            .create_dbus_watcher(self.config.dbus_config.clone())
            .await?;
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

        let (journal_tx, journal_rx) = mpsc::channel(JOURNAL_CHANNEL_CAPACITY);
        let (systemd_tx, systemd_rx) = mpsc::channel(SYSTEMD_CHANNEL_CAPACITY);

        let journal_forwarder =
            spawn_forwarder("system.journal.entry", journal_rx, emitter.clone());
        let systemd_forwarder = spawn_forwarder("system.systemd.unit_state", systemd_rx, emitter);

        let mut watcher = self
            .factory
            .create_journal_watcher(
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
                .start_streaming_with_systemd(journal_tx, systemd_tx_opt, watcher_material)
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
        let (tx, rx) = mpsc::channel(UDEV_CHANNEL_CAPACITY);
        let forwarder = spawn_forwarder("system.udev.device", rx, emitter);
        let mut watcher = self.factory.create_udev_watcher(true).await?;
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

        let (journal_tx, journal_rx) = mpsc::channel(JOURNAL_CHANNEL_CAPACITY);
        let (systemd_tx, systemd_rx) = mpsc::channel(SYSTEMD_CHANNEL_CAPACITY);

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

        let (journal_result, systemd_result) = tokio::join!(journal_forwarder, systemd_forwarder);
        if let Err(err) = journal_result {
            warn!(error = %err, "Historical journal forwarder task failed");
        }
        if let Err(err) = systemd_result {
            warn!(error = %err, "Historical systemd forwarder task failed");
        }

        material
            .finalize("system-unified-journal historical scan")
            .await?;

        Ok(count)
    }

    fn node_name(&self) -> &'static str {
        "system-watcher"
    }

    async fn ensure_watchers_running(&mut self) -> NodeResult<()> {
        self.start_dbus_stream().await?;
        self.start_unified_journal_stream().await?;
        self.start_udev_stream().await?;
        Ok(())
    }
}

#[async_trait]
impl SimpleIngestor for SystemProcessor {
    type Config = SystemConfig;
    type State = SystemPersistentState;

    fn name(&self) -> &'static str {
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
        AcquisitionManager::bootstrap_streams(publisher.nats_client()).await?;
        let acquisition = Arc::new(runtime.acquisition_manager(
            RotationPolicy::default(),
            "system",
            "system-watcher",
        )?);

        let processor_material_real = RealWatcherMaterialContext::new(
            Arc::clone(&acquisition),
            "system.processor",
            json!({
                "watcher": "processor",
                "processor": self.node_name(),
            }),
        )
        .await?;
        let processor_material: WatcherMaterialContext = Arc::new(processor_material_real);

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

    async fn scan_snapshot(
        &mut self,
        state: &mut Self::State,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        let start_time = std::time::Instant::now();

        let snapshot = self.take_snapshot(state).await?;
        let source_count = snapshot.enabled_sources.len() as u64;

        Ok(ScanReport {
            events_processed: source_count,
            duration: start_time.elapsed(),
            final_checkpoint: Checkpoint::timestamp(Timestamp::now(), None),
            time_range: Some((Timestamp::now(), Timestamp::now())),
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

        let events_processed = self
            .scan_historical_system_data(&from, &until, &args, emit_events)
            .await?;

        Ok(ScanReport {
            events_processed,
            duration: start_time.elapsed(),
            final_checkpoint: Checkpoint::timestamp(Timestamp::now(), None),
            time_range: Some((
                match &from {
                    Checkpoint::Timestamp { timestamp, .. } => *timestamp,
                    _ => Timestamp::now() - time::Duration::hours(1), // estimate
                },
                Timestamp::now(),
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

        // Periodic snapshot loop: updates `state.health` every 30 seconds.
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        let start_time = std::time::Instant::now();

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    break;
                }
                _ = interval.tick() => {
                    // Check and restart watchers if needed
                    if let Err(e) = self.ensure_watchers_running().await {
                        warn!(error = %e, "Failed to ensure watchers are running");
                    }

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
            final_checkpoint: Checkpoint::timestamp(Timestamp::now(), None),
            time_range: Some((Timestamp::now(), Timestamp::now())),
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
                    timestamp: s.captured_at - time::Duration::seconds(i as i64 * 60),
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
                .map_or_else(Timestamp::now, |s| s.captured_at),
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
        _time_range: Option<(sinex_primitives::Timestamp, sinex_primitives::Timestamp)>,
    ) -> NodeResult<sinex_node_sdk::exploration::CoverageAnalysis> {
        Ok(sinex_node_sdk::exploration::CoverageAnalysis {
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

#[cfg(test)]
impl SystemProcessor {
    pub fn set_emitter_override(&mut self, emitter: EventEmitter) {
        self.emitter_override = Some(emitter);
    }

    pub fn set_material_override(&mut self, material: WatcherMaterialContext) {
        self.processor_material = Some(material);
    }

    pub async fn simulate_watcher_failure(&mut self, watcher_type: &str) {
        match watcher_type {
            "dbus" => {
                if let Some(h) = self.dbus_watcher.take() {
                    let _ = h.shutdown().await;
                }
            }
            "unified_journal" => {
                if let Some(h) = self.unified_journal_watcher.take() {
                    let _ = h.shutdown().await;
                }
            }
            "udev" => {
                if let Some(h) = self.udev_watcher.take() {
                    let _ = h.shutdown().await;
                }
            }
            _ => panic!("Unknown watcher: {}", watcher_type),
        }
    }

    pub fn is_dbus_watcher_active(&self) -> bool {
        self.dbus_watcher.as_ref().map_or(false, |w| w.is_active())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::material_context::MaterialContext;
    use crate::watcher_factory::{JournalWatcherTrait, SystemWatcher};
    use sinex_db::models::{OffsetKind, Provenance};
    use sinex_primitives::JsonValue;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[derive(Debug)]
    struct MockMaterialContext;

    #[async_trait::async_trait]
    impl MaterialContext for MockMaterialContext {
        fn initial_provenance(&self) -> Provenance {
            Provenance::Material {
                id: sinex_primitives::Id::new(),
                anchor_byte: 0,
                offset_start: None,
                offset_end: None,
                offset_kind: OffsetKind::Byte,
            }
        }
        async fn decorate_event(&self, _event: &mut Event<JsonValue>) -> NodeResult<()> {
            Ok(())
        }
        async fn finalize(&self, _reason: &str) -> NodeResult<()> {
            Ok(())
        }
        fn event_count(&self) -> u64 {
            0
        }
    }

    struct MockWatcher;
    #[async_trait::async_trait]
    impl SystemWatcher for MockWatcher {
        async fn start_streaming(
            &mut self,
            _tx: mpsc::Sender<Event<JsonValue>>,
            _m: WatcherMaterialContext,
        ) -> NodeResult<()> {
            // Keep running to simulate active watcher
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(())
        }
    }

    struct MockFactory {
        dbus_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl WatcherFactory for MockFactory {
        async fn create_dbus_watcher(
            &self,
            _config: crate::payloads::DbusConfig,
        ) -> NodeResult<Box<dyn SystemWatcher>> {
            self.dbus_count.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(MockWatcher))
        }
        async fn create_journal_watcher(
            &self,
            _config: crate::payloads::JournalConfig,
            _sys: bool,
        ) -> NodeResult<Box<dyn JournalWatcherTrait>> {
            unimplemented!()
        }
        async fn create_udev_watcher(&self, _poll: bool) -> NodeResult<Box<dyn SystemWatcher>> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_watcher_resilience() {
        let dbus_count = Arc::new(AtomicUsize::new(0));
        let mut processor = SystemProcessor::new_with_factory(Box::new(MockFactory {
            dbus_count: dbus_count.clone(),
        }));

        // Setup emitter
        let (tx, _rx) = mpsc::channel(100);
        let emitter = EventEmitter::new(tx, true);
        processor.set_emitter_override(emitter);

        // Setup material
        processor.set_material_override(Arc::new(MockMaterialContext));

        // Enable DBus
        processor.config.dbus_enabled = true;
        processor.config.journal_enabled = false;
        processor.config.udev_enabled = false;
        processor.config.systemd_enabled = false;

        // Step 1: Ensure running (creates first watcher)
        processor.ensure_watchers_running().await.unwrap();
        assert_eq!(dbus_count.load(Ordering::SeqCst), 1);
        assert!(processor.is_dbus_watcher_active());

        // Step 2: Simulate failure
        processor.simulate_watcher_failure("dbus").await;
        // Verify it is gone/inactive
        assert!(!processor.is_dbus_watcher_active());

        // Step 3: Ensure running (recreates watcher)
        processor.ensure_watchers_running().await.unwrap();
        assert_eq!(dbus_count.load(Ordering::SeqCst), 2);
        assert!(processor.is_dbus_watcher_active());
    }
}
