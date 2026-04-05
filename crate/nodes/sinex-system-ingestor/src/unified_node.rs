#![doc = include_str!("../docs/unified_node.md")]

//! Unified system node implementing `IngestorNode`.

// Use local facade for common types
use crate::common::{
    Checkpoint, NodeCapabilities, NodeResult, ScanArgs, ScanReport, TimeHorizon, info, instrument,
};
use sinex_node_sdk::error_helpers::{ConfigAccessor, parse_config_value, parse_typed_config};
use sinex_node_sdk::runtime::stream::{EventEmitter, NodeRuntimeState};

// System-specific event payloads
use serde_json::json;
use sinex_db::models::Event;
use sinex_primitives::events::SystemMonitoringStartedPayload;
use sinex_primitives::{Seconds, Timestamp};

use crate::material_context::RealWatcherMaterialContext;
use crate::watcher_factory::{RealWatcherFactory, WatcherFactory};
use crate::{UnifiedJournalWatcher, WatcherMaterialContext};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sinex_node_sdk::SinexError;
use sinex_node_sdk::acquisition_manager::{AcquisitionManager, RotationPolicy};
use sinex_node_sdk::{
    EventTransport,
    ingestor_node::IngestorNode,
    nats_publisher::NatsPublisher,
    watcher_handle::{WatcherHandle, WatcherHealth},
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::{
    sync::mpsc,
    sync::watch,
    task::{JoinError, JoinHandle},
};
use tracing::warn;

// Import the existing SystemConfig from the parent module
use crate::DbusBusScope;
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

fn forwarder_join_result(
    task_name: &str,
    result: Result<NodeResult<()>, JoinError>,
) -> NodeResult<()> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(SinexError::processing("system forwarder task failed")
            .with_context("task", task_name.to_string())
            .with_std_error(&err)),
        Err(err) if err.is_cancelled() => {
            warn!(task = task_name, "System forwarder task was cancelled");
            Ok(())
        }
        Err(err) => {
            let error = if err.is_panic() {
                SinexError::processing("system forwarder task failed")
                    .with_context("task", task_name.to_string())
                    .with_context("panic", panic_payload_message(err.into_panic()))
            } else {
                SinexError::processing("system forwarder task failed")
                    .with_context("task", task_name.to_string())
                    .with_context("join_error", err.to_string())
            };
            Err(error)
        }
    }
}

fn collapse_forwarder_errors(mut errors: Vec<SinexError>) -> NodeResult<()> {
    if errors.is_empty() {
        return Ok(());
    }

    let mut error = errors.remove(0);
    for (index, extra) in errors.into_iter().enumerate() {
        error = error.with_context(
            format!("additional_forwarder_error_{}", index + 1),
            extra.to_string(),
        );
    }
    Err(error)
}

fn panic_payload_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_string()
    } else {
        "non-string panic payload".to_string()
    }
}

fn checkpoint_timestamp(checkpoint: &Checkpoint) -> Option<Timestamp> {
    match checkpoint {
        Checkpoint::Timestamp { timestamp, .. } => Some(*timestamp),
        _ => None,
    }
}

async fn finalize_material_after_scan_error(
    material: &WatcherMaterialContext,
    reason: &str,
    original_error: &SinexError,
) -> NodeResult<()> {
    material.finalize(reason).await.map_err(|finalize_error| {
        SinexError::processing(
            "unified journal historical scan failed and material finalization also failed",
        )
        .with_context("scan_error", original_error.to_string())
        .with_context("finalize_error", finalize_error.to_string())
    })?;
    Err(original_error.clone())
}

/// Unified system node implementing `IngestorNode`
pub struct SystemNode {
    /// System monitoring configuration
    config: SystemConfig,

    /// Watcher factory for creating system watchers (real or mock)
    factory: Box<dyn WatcherFactory>,

    runtime: Option<NodeRuntimeState>,

    /// Stage-as-you-go acquisition manager for system streams
    acquisition: Option<Arc<AcquisitionManager>>,
    /// Node-level material context for internal events
    node_material: Option<WatcherMaterialContext>,

    /// Individual watchers (initialized during operation)
    dbus_watcher: Option<WatcherHandle<WatcherMaterialContext>>,
    unified_journal_watcher: Option<WatcherHandle<WatcherMaterialContext>>,
    udev_watcher: Option<WatcherHandle<WatcherMaterialContext>>,

    /// Optional emitter override for testing (avoids full runtime setup)
    emitter_override: Option<EventEmitter>,
}

impl Default for SystemNode {
    fn default() -> Self {
        Self {
            config: SystemConfig::default(),
            factory: Box::new(RealWatcherFactory),
            runtime: None,
            acquisition: None,
            node_material: None,
            dbus_watcher: None,
            unified_journal_watcher: None,
            udev_watcher: None,
            emitter_override: None,
        }
    }
}

impl SystemNode {
    fn watcher_last_error<M>(handle: Option<&WatcherHandle<M>>) -> Option<String> {
        handle.and_then(|watcher| watcher.health().last_error)
    }

    fn collapse_shutdown_errors(mut errors: Vec<(String, SinexError)>) -> NodeResult<()> {
        if errors.is_empty() {
            return Ok(());
        }

        let (step, error) = errors.remove(0);
        let mut combined = error.with_context("shutdown_step", step);
        for (index, (step, extra)) in errors.into_iter().enumerate() {
            combined = combined
                .with_context(format!("additional_shutdown_step_{}", index + 1), step)
                .with_context(
                    format!("additional_shutdown_error_{}", index + 1),
                    extra.to_string(),
                );
        }
        Err(combined)
    }

    /// Create a new unified system node
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new node with a custom factory (for testing)
    #[must_use]
    pub fn new_with_factory(factory: Box<dyn WatcherFactory>) -> Self {
        Self {
            factory,
            ..Self::default()
        }
    }

    /// Create node with custom configuration
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
            .ok_or_else(|| SinexError::lifecycle("Node runtime not initialized".to_string()))
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

    fn dlq_publisher(&self) -> Option<Arc<NatsPublisher>> {
        self.runtime
            .as_ref()
            .map(|runtime| match runtime.transport() {
                EventTransport::Nats(publisher) => Arc::clone(publisher),
            })
    }

    fn acquisition(&self) -> NodeResult<Arc<AcquisitionManager>> {
        self.acquisition.clone().ok_or_else(|| {
            SinexError::lifecycle("System node acquisition not initialized".to_string())
        })
    }

    fn node_material(&self) -> NodeResult<&WatcherMaterialContext> {
        self.node_material.as_ref().ok_or_else(|| {
            SinexError::lifecycle("System node material not initialized".to_string())
        })
    }

    async fn new_watcher_material(
        &self,
        watcher: &str, // removed static lifetime requirement
    ) -> NodeResult<WatcherMaterialContext> {
        // Fallback for tests: if acquisition not present, assume mocking via node_material
        if self.acquisition.is_none()
            && let Some(ref m) = self.node_material
        {
            return Ok(m.clone());
        }

        let acquisition = self.acquisition()?;
        let source_identifier = format!("system.{watcher}");
        let metadata = json!({
            "watcher": watcher,
            "node": self.node_name(),
        });
        let context =
            RealWatcherMaterialContext::new(acquisition, &source_identifier, metadata).await?;
        Ok(Arc::new(context))
    }

    fn dbus_connected(&self) -> bool {
        self.config.dbus_enabled
            && self
                .dbus_watcher
                .as_ref()
                .is_some_and(WatcherHandle::is_active)
    }

    fn journal_connected(&self) -> bool {
        self.config.journal_enabled
            && self
                .unified_journal_watcher
                .as_ref()
                .is_some_and(WatcherHandle::is_active)
    }

    fn udev_connected(&self) -> bool {
        self.config.udev_enabled
            && self
                .udev_watcher
                .as_ref()
                .is_some_and(WatcherHandle::is_active)
    }

    fn systemd_connected(&self) -> bool {
        self.config.systemd_enabled
            && self
                .unified_journal_watcher
                .as_ref()
                .is_some_and(WatcherHandle::is_active)
    }

    fn apply_config_overrides<S: ConfigAccessor>(
        config: &mut SystemConfig,
        source: &S,
    ) -> NodeResult<()> {
        if let Some(overrides) = parse_typed_config::<SystemConfig, _>("system", source)? {
            *config = overrides;
        }

        if let Some(enabled) = parse_config_value::<bool, _>("dbus_enabled", source)? {
            config.dbus_enabled = enabled;
        }

        if let Some(enabled) = parse_config_value::<bool, _>("journal_enabled", source)? {
            config.journal_enabled = enabled;
        }

        if let Some(enabled) = parse_config_value::<bool, _>("udev_enabled", source)? {
            config.udev_enabled = enabled;
        }

        if let Some(enabled) = parse_config_value::<bool, _>("systemd_enabled", source)? {
            config.systemd_enabled = enabled;
        }

        if let Some(buses) = parse_config_value::<DbusBusScope, _>("dbus_buses", source)? {
            config.dbus_buses = buses;
        }

        if let Some(timeout) = parse_config_value::<Seconds, _>("journal_timeout_secs", source)? {
            config.journal_timeout_secs = timeout;
        }

        Ok(())
    }

    /// Take a snapshot of current system state
    #[instrument(skip(self), fields(node = "system"))]
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
                    .bus_names()
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect(),
                connection_active: self.dbus_connected(),
                recent_signal_count: self
                    .dbus_watcher
                    .as_ref()
                    .and_then(|w| w.material())
                    .map_or(0, |m| m.event_count() as u32),
            });
        }

        if self.config.journal_enabled {
            enabled_sources.push("journal".to_string());
            journal_status = Some(JournalStatus {
                following_active: self.journal_connected(),
                cursor_position: None, // Would need to track this
                recent_entry_count: self
                    .unified_journal_watcher
                    .as_ref()
                    .and_then(|w| w.material())
                    .map_or(0, |m| m.event_count() as u32),
            });
        }

        if self.config.udev_enabled {
            enabled_sources.push("udev".to_string());
            udev_status = Some(UdevStatus {
                monitoring_active: self.udev_connected(),
                recent_device_events: self
                    .udev_watcher
                    .as_ref()
                    .and_then(|w| w.material())
                    .map_or(0, |m| m.event_count() as u32),
            });
        }

        if self.config.systemd_enabled {
            enabled_sources.push("systemd".to_string());
            systemd_status = Some(SystemdStatus {
                monitoring_active: self.systemd_connected(),
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
            recent_activity: vec!["System node snapshot taken".to_string()],
        };

        state.last_state = Some(snapshot.clone());
        Ok(snapshot)
    }

    /// Expose watcher readiness for tests and diagnostics.
    #[must_use]
    pub fn watcher_snapshot(&self) -> WatcherSnapshot {
        WatcherSnapshot {
            dbus_ready: self.dbus_connected(),
            journal_ready: self.journal_connected(),
            udev_ready: self.udev_connected(),
            systemd_ready: self.systemd_connected(),
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
    async fn shutdown_watchers(&mut self) -> NodeResult<()> {
        let mut shutdown_errors = Vec::new();
        if let Some(handle) = self.dbus_watcher.take() {
            if let Err(error) = self.finalize_watcher_handle(handle).await {
                shutdown_errors.push(("dbus watcher".to_string(), error));
            }
        }
        if let Some(handle) = self.unified_journal_watcher.take() {
            if let Err(error) = self.finalize_watcher_handle(handle).await {
                shutdown_errors.push(("unified journal watcher".to_string(), error));
            }
        }
        if let Some(handle) = self.udev_watcher.take() {
            if let Err(error) = self.finalize_watcher_handle(handle).await {
                shutdown_errors.push(("udev watcher".to_string(), error));
            }
        }

        if let Some(material) = self.node_material.take()
            && let Err(error) = material.finalize("system-watcher shutdown").await
        {
            shutdown_errors.push(("system node material".to_string(), error));
        }

        Self::collapse_shutdown_errors(shutdown_errors)
    }

    async fn finalize_watcher_handle(
        &self,
        mut handle: WatcherHandle<WatcherMaterialContext>,
    ) -> NodeResult<()> {
        let mut shutdown_errors = Vec::new();
        if let Some(material) = handle.take_material()
            && let Err(error) = material.finalize("system-watcher shutdown").await
        {
            shutdown_errors.push(("watcher material".to_string(), error));
        }
        if let Err(error) = handle.shutdown().await {
            shutdown_errors.push(("watcher task".to_string(), error));
        }
        Self::collapse_shutdown_errors(shutdown_errors)
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
        let material = self.node_material()?;

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
            self.finalize_watcher_handle(handle).await?;
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
            self.finalize_watcher_handle(handle).await?;
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
            self.finalize_watcher_handle(handle).await?;
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
        let handle = WatcherHandle::initialized("dbus").with_material(material.clone());
        let health = handle.health_tracker();
        let emitter = self.emitter_clone()?;
        let (tx, rx) = mpsc::channel(DBUS_CHANNEL_CAPACITY);
        let forwarder =
            spawn_forwarder("system.dbus.signal", rx, emitter, Some(Arc::clone(&health)));
        let mut watcher = self
            .factory
            .create_dbus_watcher(self.config.dbus_config.clone())
            .await?;
        let watcher_material = material.clone();
        let task = tokio::spawn(async move {
            if let Err(err) = watcher.start_streaming(tx, watcher_material).await {
                health.write().last_error = Some(err.to_string());
                warn!(error = %err, "D-Bus watcher terminated");
            }
        });
        let mut handle = handle;
        handle.start(task, Some(forwarder))?;
        Ok(handle)
    }

    async fn spawn_unified_journal_task(
        &self,
        material: WatcherMaterialContext,
    ) -> NodeResult<WatcherHandle<WatcherMaterialContext>> {
        let handle = WatcherHandle::initialized("unified_journal").with_material(material.clone());
        let health = handle.health_tracker();
        let emitter = self.emitter_clone()?;

        let (journal_tx, journal_rx) = mpsc::channel(JOURNAL_CHANNEL_CAPACITY);
        let (systemd_tx, systemd_rx) = mpsc::channel(SYSTEMD_CHANNEL_CAPACITY);

        let journal_forwarder = spawn_forwarder(
            "system.journal.entry",
            journal_rx,
            emitter.clone(),
            Some(Arc::clone(&health)),
        );
        let systemd_forwarder = spawn_forwarder(
            "system.systemd.unit_state",
            systemd_rx,
            emitter,
            Some(Arc::clone(&health)),
        );

        let mut watcher = self
            .factory
            .create_journal_watcher(
                self.config.journal_config.clone(),
                self.config.systemd_enabled,
                self.dlq_publisher(),
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
                health.write().last_error = Some(err.to_string());
                warn!(error = %err, "Unified journal watcher terminated");
            }
        });

        // Spawn a task to join both forwarders
        let combined_forwarder = tokio::spawn(async move {
            let (journal_result, systemd_result) =
                tokio::join!(journal_forwarder, systemd_forwarder);
            let mut forwarder_errors = Vec::new();
            if let Err(error) = forwarder_join_result("journal", journal_result) {
                forwarder_errors.push(error);
            }
            if let Err(error) = forwarder_join_result("systemd", systemd_result) {
                forwarder_errors.push(error);
            }
            collapse_forwarder_errors(forwarder_errors)
        });

        let mut handle = handle;
        handle.start(task, Some(combined_forwarder))?;
        Ok(handle)
    }

    async fn spawn_udev_task(
        &self,
        material: WatcherMaterialContext,
    ) -> NodeResult<WatcherHandle<WatcherMaterialContext>> {
        let handle = WatcherHandle::initialized("udev").with_material(material.clone());
        let health = handle.health_tracker();
        let emitter = self.emitter_clone()?;
        let (tx, rx) = mpsc::channel(UDEV_CHANNEL_CAPACITY);
        let forwarder =
            spawn_forwarder("system.udev.device", rx, emitter, Some(Arc::clone(&health)));
        let mut watcher = self.factory.create_udev_watcher(true).await?;
        let watcher_material = material.clone();
        let task = tokio::spawn(async move {
            if let Err(err) = watcher.start_streaming(tx, watcher_material).await {
                health.write().last_error = Some(err.to_string());
                warn!(error = %err, "udev watcher terminated");
            }
        });
        let mut handle = handle;
        handle.start(task, Some(forwarder))?;
        Ok(handle)
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
            spawn_forwarder("system.journal.entry", journal_rx, emitter.clone(), None);
        let systemd_forwarder =
            spawn_forwarder("system.systemd.unit_state", systemd_rx, emitter, None);

        let material = self
            .new_watcher_material("unified-journal-historical")
            .await?;
        let mut watcher = UnifiedJournalWatcher::new(
            self.config.journal_config.clone(),
            self.config.systemd_enabled,
            self.dlq_publisher(),
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
                finalize_material_after_scan_error(
                    &material,
                    "system-unified-journal historical scan",
                    &err,
                )
                .await?;
                return Err(err);
            }
        };

        drop(journal_tx);
        drop(systemd_tx_opt);

        let (journal_result, systemd_result) = tokio::join!(journal_forwarder, systemd_forwarder);
        let mut forwarder_errors = Vec::new();
        if let Err(error) = forwarder_join_result("historical journal", journal_result) {
            forwarder_errors.push(error);
        }
        if let Err(error) = forwarder_join_result("historical systemd", systemd_result) {
            forwarder_errors.push(error);
        }

        if let Err(error) = material
            .finalize("system-unified-journal historical scan")
            .await
        {
            forwarder_errors.push(error);
        }

        collapse_forwarder_errors(forwarder_errors)?;

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

    fn record_snapshot_result(
        state: &mut SystemPersistentState,
        warnings: &mut Vec<String>,
        snapshot_result: NodeResult<SystemState>,
    ) {
        match snapshot_result {
            Ok(snapshot) => state.last_state = Some(snapshot),
            Err(error) => {
                let warning = format!("system snapshot refresh failed: {error}");
                warn!("{warning}");
                if !warnings.iter().any(|existing| existing == &warning) {
                    warnings.push(warning);
                }
            }
        }
    }
}

impl IngestorNode for SystemNode {
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
        Self::apply_config_overrides(&mut config, runtime)?;
        self.config = config;

        let publisher: Arc<NatsPublisher> = match runtime.transport() {
            EventTransport::Nats(publisher) => Arc::clone(publisher),
        };
        AcquisitionManager::bootstrap_streams(publisher.nats_client()).await?;
        let acquisition =
            Arc::new(runtime.acquisition_manager(RotationPolicy::default(), "system")?);

        let node_material_real = RealWatcherMaterialContext::new(
            Arc::clone(&acquisition),
            "system.node",
            json!({
                "watcher": "node",
                "node": self.node_name(),
            }),
        )
        .await?;
        let node_material: WatcherMaterialContext = Arc::new(node_material_real);

        self.runtime = Some(runtime.clone());
        self.acquisition = Some(acquisition);
        self.node_material = Some(node_material);

        info!(
            dbus_enabled = self.config.dbus_enabled,
            journal_enabled = self.config.journal_enabled,
            udev_enabled = self.config.udev_enabled,
            systemd_enabled = self.config.systemd_enabled,
            dbus_buses = %self.config.dbus_buses,
            journal_timeout_secs = self.config.journal_timeout_secs.as_secs(),
            "System node configuration"
        );

        self.initialize_watchers().await?;

        Ok(())
    }

    async fn scan_snapshot(
        &mut self,
        state: &mut Self::State,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        let started_at = Timestamp::now();
        let start_time = std::time::Instant::now();

        let snapshot = self.take_snapshot(state).await?;
        let source_count = snapshot.enabled_sources.len() as u64;
        let finished_at = Timestamp::now();

        Ok(ScanReport {
            events_processed: source_count,
            duration: start_time.elapsed(),
            final_checkpoint: Checkpoint::timestamp(finished_at, None),
            time_range: Some((started_at, finished_at)),
            node_stats: HashMap::new(),
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
        let final_timestamp = until.end_time().unwrap_or_else(Timestamp::now);

        let events_processed = self
            .scan_historical_system_data(&from, &until, &args, emit_events)
            .await?;

        Ok(ScanReport {
            events_processed,
            duration: start_time.elapsed(),
            final_checkpoint: Checkpoint::timestamp(final_timestamp, None),
            time_range: checkpoint_timestamp(&from).map(|started_at| (started_at, final_timestamp)),
            node_stats: HashMap::new(),
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
        let started_at = Timestamp::now();
        self.start_continuous_monitoring(from).await?;

        // Periodic snapshot loop: updates `state.health` every 30 seconds.
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        let start_time = std::time::Instant::now();
        let mut warnings = Vec::new();

        loop {
            tokio::select! {
                shutdown_result = shutdown_rx.changed() => {
                    if shutdown_result.is_err() {
                        let warning =
                            "system continuous monitoring shutdown channel dropped before explicit shutdown";
                        warn!("{warning}");
                        warnings.push(warning.to_string());
                    }
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

                    let snapshot_result = self.take_snapshot(state).await;
                    Self::record_snapshot_result(state, &mut warnings, snapshot_result);
                }
            }
        }

        self.shutdown_watchers().await?;
        let finished_at = Timestamp::now();

        Ok(ScanReport {
            events_processed: 0,
            duration: start_time.elapsed(),
            final_checkpoint: Checkpoint::timestamp(finished_at, None),
            time_range: Some((started_at, finished_at)),
            node_stats: HashMap::new(),
            successful_targets: vec!["system_continuous".to_string()],
            failed_targets: vec![],
            warnings,
        })
    }

    async fn shutdown(&mut self, _state: &Self::State) -> NodeResult<()> {
        self.shutdown_watchers().await
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
        let connected_sources = [
            self.dbus_connected(),
            self.journal_connected(),
            self.udev_connected(),
            self.systemd_connected(),
        ]
        .iter()
        .filter(|&&active| active)
        .count() as u64;
        let healthy = active_sources > 0 && connected_sources == active_sources;
        let is_connected = active_sources > 0 && connected_sources > 0;
        let mut metadata = HashMap::new();
        metadata.insert("enabled_sources".to_string(), json!(active_sources));
        metadata.insert("connected_sources".to_string(), json!(connected_sources));
        metadata.insert(
            "watcher_health".to_string(),
            json!({
                "dbus_active": self.dbus_connected(),
                "journal_active": self.journal_connected(),
                "udev_active": self.udev_connected(),
                "systemd_active": self.systemd_connected(),
                "dbus_error": Self::watcher_last_error(self.dbus_watcher.as_ref()),
                "journal_error": Self::watcher_last_error(self.unified_journal_watcher.as_ref()),
                "udev_error": Self::watcher_last_error(self.udev_watcher.as_ref()),
                "systemd_error": Self::watcher_last_error(self.unified_journal_watcher.as_ref()),
            }),
        );
        let description = if active_sources == 0 {
            "System Source (all watchers disabled)".to_string()
        } else if connected_sources == 0 {
            format!("System Source ({active_sources} enabled watcher(s), none connected)")
        } else if connected_sources < active_sources {
            format!(
                "System Source ({connected_sources}/{active_sources} watcher(s) connected, degraded)"
            )
        } else {
            format!("System Source ({connected_sources}/{active_sources} watcher(s) connected)")
        };

        Ok(sinex_node_sdk::exploration::SourceState {
            description,
            last_updated: state.last_state.as_ref().map(|s| s.captured_at),
            total_items: None,
            healthy,
            recent_activity,
            metadata,
            is_connected,
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
        sinex_node_sdk::exploration::coverage_analysis_unavailable(
            "coverage analysis is not implemented for system watcher sources",
        )
    }
}

/// Helper to forward events from a watcher channel to the emitter
fn record_forwarder_health_error(health: &Arc<RwLock<WatcherHealth>>, error: &SinexError) {
    health.write().last_error = Some(error.to_string());
}

fn spawn_forwarder<E>(
    channel_name: &'static str,
    mut rx: mpsc::Receiver<Event<E>>,
    emitter: EventEmitter,
    health: Option<Arc<RwLock<WatcherHealth>>>,
) -> JoinHandle<NodeResult<()>>
where
    E: Serialize + Send + 'static,
{
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            match event.to_json_event() {
                Ok(json_event) => {
                    if let Err(err) = emitter.emit(json_event).await {
                        let error = SinexError::processing("Failed to emit forwarded event")
                            .with_context("channel", channel_name.to_string())
                            .with_source(err);
                        if let Some(health) = &health {
                            record_forwarder_health_error(health, &error);
                            warn!(error = %error, channel = channel_name, "System forwarder terminated");
                        }
                        return Err(error);
                    }
                }
                Err(err) => {
                    let error = SinexError::processing("Failed to convert forwarded event to JSON")
                        .with_context("channel", channel_name.to_string())
                        .with_source(err);
                    if let Some(health) = &health {
                        record_forwarder_health_error(health, &error);
                        warn!(error = %error, channel = channel_name, "System forwarder terminated");
                    }
                    return Err(error);
                }
            }
        }

        Ok(())
    })
}

#[cfg(test)]
impl SystemNode {
    pub fn set_emitter_override(&mut self, emitter: EventEmitter) {
        self.emitter_override = Some(emitter);
    }

    pub fn set_material_override(&mut self, material: WatcherMaterialContext) {
        self.node_material = Some(material);
    }

    pub async fn simulate_watcher_failure(&mut self, watcher_type: &str) {
        match watcher_type {
            "dbus" => {
                if let Some(h) = self.dbus_watcher.take() {
                    match h.shutdown().await {
                        Ok(()) => {}
                        Err(error)
                            if error
                                .to_string()
                                .contains("Watcher task exceeded shutdown grace period and was aborted") => {}
                        Err(error) => {
                            panic!("dbus watcher shutdown should stay explicit in tests: {error}");
                        }
                    }
                }
            }
            "unified_journal" => {
                if let Some(h) = self.unified_journal_watcher.take() {
                    match h.shutdown().await {
                        Ok(()) => {}
                        Err(error)
                            if error
                                .to_string()
                                .contains("Watcher task exceeded shutdown grace period and was aborted") => {}
                        Err(error) => {
                            panic!(
                                "journal watcher shutdown should stay explicit in tests: {error}"
                            );
                        }
                    }
                }
            }
            "udev" => {
                if let Some(h) = self.udev_watcher.take() {
                    match h.shutdown().await {
                        Ok(()) => {}
                        Err(error)
                            if error
                                .to_string()
                                .contains("Watcher task exceeded shutdown grace period and was aborted") => {}
                        Err(error) => {
                            panic!("udev watcher shutdown should stay explicit in tests: {error}");
                        }
                    }
                }
            }
            _ => panic!("Unknown watcher: {watcher_type}"),
        }
    }

    #[must_use]
    pub fn is_dbus_watcher_active(&self) -> bool {
        self.dbus_watcher
            .as_ref()
            .is_some_and(sinex_node_sdk::WatcherHandle::is_active)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::material_context::MaterialContext;
    use crate::watcher_factory::{JournalWatcherTrait, SystemWatcher};
    use serde_json::json;
    use sinex_db::models::{OffsetKind, Provenance};
    use sinex_primitives::JsonValue;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use xtask::sandbox::prelude::*;

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

    #[derive(Debug)]
    struct FailingFinalizeMaterialContext;

    #[async_trait::async_trait]
    impl MaterialContext for FailingFinalizeMaterialContext {
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
            Err(SinexError::processing("finalize exploded"))
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
            _dlq_publisher: Option<Arc<NatsPublisher>>,
        ) -> NodeResult<Box<dyn JournalWatcherTrait>> {
            Err(SinexError::unknown(
                "mock: journal watcher not supported in this test",
            ))
        }
        async fn create_udev_watcher(&self, _poll: bool) -> NodeResult<Box<dyn SystemWatcher>> {
            Err(SinexError::unknown(
                "mock: udev watcher not supported in this test",
            ))
        }
    }

    #[sinex_test]
    async fn system_node_reports_coverage_analysis_unavailable() -> TestResult<()> {
        let node = SystemNode::new();
        let error =
            IngestorNode::get_coverage_analysis(&node, &SystemPersistentState::default(), None)
                .expect_err("system node should not fabricate coverage analysis");
        assert!(error.to_string().contains("not implemented"));
        Ok(())
    }

    #[sinex_test]
    async fn system_source_state_is_disconnected_when_enabled_watchers_are_inactive()
    -> TestResult<()> {
        let node = SystemNode::new();
        let source = IngestorNode::get_source_state(&node, &SystemPersistentState::default())?;

        assert!(!source.is_connected);
        assert!(!source.healthy);
        assert!(
            source.description.contains("none connected"),
            "unexpected description: {}",
            source.description
        );
        assert_eq!(
            source
                .metadata
                .get("enabled_sources")
                .and_then(serde_json::Value::as_u64),
            Some(4)
        );
        assert_eq!(
            source
                .metadata
                .get("connected_sources")
                .and_then(serde_json::Value::as_u64),
            Some(0)
        );
        assert_eq!(source.last_updated, None);
        Ok(())
    }

    #[sinex_test]
    async fn system_source_state_ignores_stale_persisted_watcher_flags() -> TestResult<()> {
        let node = SystemNode::new();
        let state = SystemPersistentState {
            health: SystemMonitorHealth {
                dbus_active: true,
                journal_active: true,
                udev_active: true,
                systemd_active: true,
            },
            ..SystemPersistentState::default()
        };

        let source = IngestorNode::get_source_state(&node, &state)?;

        assert!(!source.is_connected);
        assert!(!source.healthy);
        assert_eq!(
            source
                .metadata
                .get("connected_sources")
                .and_then(serde_json::Value::as_u64),
            Some(0)
        );
        assert_eq!(
            source.metadata.get("watcher_health"),
            Some(&json!({
                "dbus_active": false,
                "journal_active": false,
                "udev_active": false,
                "systemd_active": false,
                "dbus_error": null,
                "journal_error": null,
                "udev_error": null,
                "systemd_error": null,
            }))
        );
        Ok(())
    }

    #[sinex_test]
    async fn watcher_snapshot_requires_live_tasks() -> TestResult<()> {
        let mut node = SystemNode::new();
        node.dbus_watcher = Some(WatcherHandle::initialized("dbus"));
        node.unified_journal_watcher = Some(WatcherHandle::initialized("unified_journal"));
        node.udev_watcher = Some(WatcherHandle::initialized("udev"));

        let snapshot = node.watcher_snapshot();

        assert_eq!(
            snapshot,
            WatcherSnapshot {
                dbus_ready: false,
                journal_ready: false,
                udev_ready: false,
                systemd_ready: false,
            }
        );
        Ok(())
    }

    #[sinex_test]
    async fn system_source_state_reports_live_watcher_handles() -> TestResult<()> {
        let mut node = SystemNode::new();
        let task = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        node.dbus_watcher = Some(WatcherHandle::running("dbus", task, None, None));

        let source = IngestorNode::get_source_state(&node, &SystemPersistentState::default())?;

        assert!(source.is_connected);
        assert!(!source.healthy);
        assert!(source.description.contains("degraded"));
        assert_eq!(
            source
                .metadata
                .get("connected_sources")
                .and_then(serde_json::Value::as_u64),
            Some(1)
        );
        assert_eq!(
            source.metadata.get("watcher_health"),
            Some(&json!({
                "dbus_active": true,
                "journal_active": false,
                "udev_active": false,
                "systemd_active": false,
                "dbus_error": null,
                "journal_error": null,
                "udev_error": null,
                "systemd_error": null,
            }))
        );
        Ok(())
    }

    #[sinex_test]
    async fn system_source_state_is_healthy_when_all_enabled_watchers_are_connected()
    -> TestResult<()> {
        let mut node = SystemNode::with_config(SystemConfig {
            dbus_enabled: true,
            journal_enabled: false,
            udev_enabled: false,
            systemd_enabled: false,
            ..SystemConfig::default()
        });
        let task = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        node.dbus_watcher = Some(WatcherHandle::running("dbus", task, None, None));

        let source = IngestorNode::get_source_state(&node, &SystemPersistentState::default())?;

        assert!(source.is_connected);
        assert!(source.healthy);
        assert!(source.description.contains("1/1 watcher(s) connected"));
        Ok(())
    }

    #[sinex_test]
    async fn system_source_state_marks_disabled_configuration_unhealthy() -> TestResult<()> {
        let node = SystemNode::with_config(SystemConfig {
            dbus_enabled: false,
            journal_enabled: false,
            udev_enabled: false,
            systemd_enabled: false,
            ..SystemConfig::default()
        });

        let source = IngestorNode::get_source_state(&node, &SystemPersistentState::default())?;

        assert!(!source.is_connected);
        assert!(!source.healthy);
        assert!(source.description.contains("all watchers disabled"));
        assert_eq!(
            source
                .metadata
                .get("enabled_sources")
                .and_then(serde_json::Value::as_u64),
            Some(0)
        );
        assert_eq!(
            source
                .metadata
                .get("connected_sources")
                .and_then(serde_json::Value::as_u64),
            Some(0)
        );
        Ok(())
    }

    #[sinex_test]
    async fn system_source_state_surfaces_watcher_errors() -> TestResult<()> {
        let mut node = SystemNode::new();
        let dbus = WatcherHandle::<WatcherMaterialContext>::initialized("dbus");
        dbus.record_error("dbus stream failed".to_string());
        node.dbus_watcher = Some(dbus);

        let source = IngestorNode::get_source_state(&node, &SystemPersistentState::default())?;

        assert_eq!(
            source.metadata.get("watcher_health"),
            Some(&json!({
                "dbus_active": false,
                "journal_active": false,
                "udev_active": false,
                "systemd_active": false,
                "dbus_error": "dbus stream failed",
                "journal_error": null,
                "udev_error": null,
                "systemd_error": null,
            }))
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_watcher_resilience() -> TestResult<()> {
        let dbus_count = Arc::new(AtomicUsize::new(0));
        let mut node = SystemNode::new_with_factory(Box::new(MockFactory {
            dbus_count: dbus_count.clone(),
        }));

        // Setup emitter
        let (tx, _rx) = mpsc::channel(100);
        let emitter = EventEmitter::new(tx, true);
        node.set_emitter_override(emitter);

        // Setup material
        node.set_material_override(Arc::new(MockMaterialContext));

        // Enable DBus
        node.config.dbus_enabled = true;
        node.config.journal_enabled = false;
        node.config.udev_enabled = false;
        node.config.systemd_enabled = false;

        // Step 1: Ensure running (creates first watcher)
        node.ensure_watchers_running().await.unwrap();
        assert_eq!(dbus_count.load(Ordering::SeqCst), 1);
        assert!(node.is_dbus_watcher_active());

        // Step 2: Simulate failure
        node.simulate_watcher_failure("dbus").await;
        // Verify it is gone/inactive
        assert!(!node.is_dbus_watcher_active());

        // Step 3: Ensure running (recreates watcher)
        node.ensure_watchers_running().await.unwrap();
        assert_eq!(dbus_count.load(Ordering::SeqCst), 2);
        assert!(node.is_dbus_watcher_active());

        Ok(())
    }

    #[sinex_test]
    async fn run_continuous_warns_when_shutdown_sender_drops() -> TestResult<()> {
        let mut node = SystemNode::with_config(SystemConfig {
            dbus_enabled: false,
            journal_enabled: false,
            udev_enabled: false,
            systemd_enabled: false,
            ..SystemConfig::default()
        });

        let (tx, _rx) = mpsc::channel(16);
        node.set_emitter_override(EventEmitter::new(tx, true));
        node.set_material_override(Arc::new(MockMaterialContext));

        let state = SystemPersistentState::default();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(async move {
            let mut node = node;
            let mut state = state;
            node.run_continuous(&mut state, Checkpoint::None, shutdown_rx)
                .await
        });

        tokio::task::yield_now().await;
        drop(shutdown_tx);

        let report = task.await??;
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("shutdown channel dropped")),
            "expected shutdown channel drop warning, got: {:?}",
            report.warnings
        );
        Ok(())
    }

    #[sinex_test]
    async fn system_historical_report_uses_requested_time_bounds() -> TestResult<()> {
        let mut node = SystemNode::with_config(SystemConfig {
            dbus_enabled: false,
            journal_enabled: false,
            udev_enabled: false,
            systemd_enabled: false,
            ..SystemConfig::default()
        });

        let from_ts = Timestamp::now() - time::Duration::minutes(10);
        let until_ts = from_ts + time::Duration::minutes(5);
        let report = node
            .scan_historical(
                &mut SystemPersistentState::default(),
                Checkpoint::timestamp(from_ts, None),
                TimeHorizon::Historical { end_time: until_ts },
                ScanArgs::default(),
            )
            .await?;

        assert_eq!(
            report.final_checkpoint,
            Checkpoint::timestamp(until_ts, None)
        );
        assert_eq!(report.time_range, Some((from_ts, until_ts)));
        Ok(())
    }

    #[sinex_test]
    async fn run_continuous_reports_elapsed_time_window() -> TestResult<()> {
        let mut node = SystemNode::with_config(SystemConfig {
            dbus_enabled: false,
            journal_enabled: false,
            udev_enabled: false,
            systemd_enabled: false,
            ..SystemConfig::default()
        });

        let (tx, _rx) = mpsc::channel(16);
        node.set_emitter_override(EventEmitter::new(tx, true));
        node.set_material_override(Arc::new(MockMaterialContext));

        let state = SystemPersistentState::default();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(async move {
            let mut node = node;
            let mut state = state;
            node.run_continuous(&mut state, Checkpoint::None, shutdown_rx)
                .await
        });

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        drop(shutdown_tx);

        let report = task.await??;
        let (started_at, finished_at) = report
            .time_range
            .expect("continuous report should include an elapsed time window");
        assert!(finished_at > started_at);
        assert_eq!(
            report.final_checkpoint,
            Checkpoint::timestamp(finished_at, None)
        );
        Ok(())
    }

    #[sinex_test]
    async fn system_config_overrides_reject_invalid_bool_types() -> TestResult<()> {
        let mut config = SystemConfig::default();
        let overrides = HashMap::from([("dbus_enabled".to_string(), json!("yes"))]);

        let error = SystemNode::apply_config_overrides(&mut config, &overrides)
            .expect_err("invalid override types should fail honestly");
        let message = error.to_string();

        assert!(message.contains("dbus_enabled"));
        assert!(message.contains("bool"));
        Ok(())
    }

    #[sinex_test]
    async fn record_snapshot_result_preserves_successful_snapshot() -> TestResult<()> {
        let mut state = SystemPersistentState::default();
        let mut warnings = Vec::new();
        let snapshot = SystemState {
            captured_at: Timestamp::now(),
            enabled_sources: vec!["dbus".to_string()],
            dbus_status: None,
            journal_status: None,
            udev_status: None,
            systemd_status: None,
            recent_activity: vec!["ok".to_string()],
        };

        SystemNode::record_snapshot_result(&mut state, &mut warnings, Ok(snapshot.clone()));

        assert_eq!(
            state.last_state.as_ref().map(|s| &s.enabled_sources),
            Some(&snapshot.enabled_sources)
        );
        assert!(warnings.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn record_snapshot_result_surfaces_failures_once() -> TestResult<()> {
        let mut state = SystemPersistentState::default();
        let mut warnings = Vec::new();
        let error = SinexError::processing("snapshot exploded");

        SystemNode::record_snapshot_result(&mut state, &mut warnings, Err(error.clone()));
        SystemNode::record_snapshot_result(&mut state, &mut warnings, Err(error));

        assert!(state.last_state.is_none());
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("snapshot exploded"));
        Ok(())
    }

    #[sinex_test]
    async fn finalize_material_after_scan_error_preserves_both_failures() -> TestResult<()> {
        let material: WatcherMaterialContext = Arc::new(FailingFinalizeMaterialContext);
        let error = finalize_material_after_scan_error(
            &material,
            "system-unified-journal historical scan",
            &SinexError::processing("historical scan exploded"),
        )
        .await
        .expect_err("double failure should be surfaced honestly");
        let message = error.to_string();

        assert!(message.contains("historical scan failed"));
        assert!(message.contains("historical scan exploded"));
        assert!(message.contains("finalize exploded"));
        Ok(())
    }

    #[sinex_test]
    async fn forwarder_join_result_accepts_clean_exit() -> TestResult<()> {
        let handle = tokio::spawn(async { Ok::<(), SinexError>(()) });
        forwarder_join_result("journal", handle.await)?;
        Ok(())
    }

    #[sinex_test]
    async fn forwarder_join_result_accepts_cancelled_task() -> TestResult<()> {
        let handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok::<(), SinexError>(())
        });
        handle.abort();
        forwarder_join_result("journal", handle.await)?;
        Ok(())
    }

    #[sinex_test]
    async fn forwarder_join_result_rejects_panicked_task() -> TestResult<()> {
        let handle = tokio::spawn(async {
            panic!("journal forwarder panic");
        });
        let error = forwarder_join_result("journal", handle.await)
            .expect_err("panicked forwarders must fail honestly");
        let message = format!("{error:#}");
        assert!(message.contains("system forwarder task failed"));
        assert!(message.contains("journal"));
        assert!(message.contains("journal forwarder panic"));
        Ok(())
    }

    #[sinex_test]
    async fn spawn_forwarder_rejects_emission_failures() -> TestResult<()> {
        let (emitter_tx, emitter_rx) = mpsc::channel(1);
        drop(emitter_rx);
        let emitter = EventEmitter::new(emitter_tx, false);

        let (tx, rx) = mpsc::channel(1);
        let handle = spawn_forwarder("system.test.forwarder", rx, emitter, None);

        let event = DynamicPayload::new(
            "system.test",
            "system.test.forwarded",
            serde_json::json!({ "ok": true }),
        )
        .from_material(Id::<SourceMaterial>::new())
        .build()?;

        tx.send(event)
            .await
            .map_err(|error| color_eyre::eyre::eyre!(error.to_string()))?;
        drop(tx);

        let error = forwarder_join_result("system.test.forwarder", handle.await)
            .expect_err("forwarder emission failures must fail honestly");
        let message = format!("{error:#}");
        assert!(message.contains("system forwarder task failed"));
        assert!(message.contains("system.test.forwarder"));
        assert!(message.contains("Failed to emit forwarded event"));
        assert!(message.contains("Event channel closed"));
        Ok(())
    }

    #[sinex_test]
    async fn spawn_forwarder_records_emission_failures_in_health() -> TestResult<()> {
        let (emitter_tx, emitter_rx) = mpsc::channel(1);
        drop(emitter_rx);
        let emitter = EventEmitter::new(emitter_tx, false);

        let health = Arc::new(RwLock::new(WatcherHealth::default()));
        let (tx, rx) = mpsc::channel(1);
        let handle = spawn_forwarder(
            "system.test.forwarder",
            rx,
            emitter,
            Some(Arc::clone(&health)),
        );

        let event = DynamicPayload::new(
            "system.test",
            "system.test.forwarded",
            serde_json::json!({ "ok": true }),
        )
        .from_material(Id::<SourceMaterial>::new())
        .build()?;

        tx.send(event)
            .await
            .map_err(|error| color_eyre::eyre::eyre!(error.to_string()))?;
        drop(tx);

        let error = forwarder_join_result("system.test.forwarder", handle.await)
            .expect_err("forwarder failure should still surface through join results");
        let watcher_health = health.read();
        let recorded = watcher_health
            .last_error
            .as_deref()
            .expect("forwarder failure should be recorded in watcher health");
        assert!(recorded.contains("Failed to emit forwarded event"));
        assert!(recorded.contains("system.test.forwarder"));
        let message = format!("{error:#}");
        assert!(message.contains("system forwarder task failed"));
        assert!(message.contains("system.test.forwarder"));
        Ok(())
    }

    #[sinex_test]
    async fn spawn_forwarder_stamps_missing_ids_before_emit() -> TestResult<()> {
        let (emitter_tx, mut emitter_rx) = mpsc::channel(1);
        let emitter = EventEmitter::new(emitter_tx, false);

        let (tx, rx) = mpsc::channel(1);
        let handle = spawn_forwarder("system.test.forwarder", rx, emitter, None);

        let event = sinex_primitives::events::Event::new_json(
            "system.test",
            "system.test.forwarded",
            serde_json::json!({ "ok": true }),
            sinex_primitives::events::Provenance::from_material(Id::<SourceMaterial>::new(), 0, None, None),
        );
        assert!(event.id.is_none());

        tx.send(event)
            .await
            .map_err(|error| color_eyre::eyre::eyre!(error.to_string()))?;
        drop(tx);

        let emitted = emitter_rx
            .recv()
            .await
            .ok_or_else(|| color_eyre::eyre::eyre!("missing forwarded event"))?;
        assert!(emitted.id.is_some());

        forwarder_join_result("system.test.forwarder", handle.await)?;
        Ok(())
    }
}
