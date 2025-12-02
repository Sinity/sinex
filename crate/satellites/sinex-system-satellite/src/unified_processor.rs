#![doc = include_str!("../docs/unified_processor.md")]

//! Unified system processor implementing `StatefulStreamProcessor`.

// Use local facade for common types
use crate::common::*;
use sinex_satellite_sdk::error_helpers::{parse_config_value, parse_typed_config};
use sinex_satellite_sdk::stream_processor::{
    EventEmitter, ProcessorInitContext, ProcessorRuntimeState,
};

// System-specific event payloads
use serde_json::json;
use sinex_core::types::events::{
    JournaldHistoricalPayload, SystemMonitoringStartedPayload, SystemSnapshotPayload,
    SystemdUnitsHistoricalPayload, UdevDeviceHistoricalPayload,
};
use sinex_core::{
    db::models::{Event, EventId, Provenance},
    types::Ulid,
    JsonValue,
};

use crate::{DbusWatcher, JournalWatcher, SystemdWatcher, UdevWatcher};
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

const SYSTEM_WATCHER_BOOTSTRAP_BYTES: [u8; 16] = [
    0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
];

/// Capacity for watcher → emitter channels; we prefer bounded buffers to avoid unbounded growth.
const WATCHER_CHANNEL_CAPACITY: usize = 1024;

#[derive(Debug)]
enum WatcherState {
    Initialized,
    Running {
        task: JoinHandle<()>,
        forwarder: Option<JoinHandle<()>>,
    },
    Simulated {
        task: JoinHandle<()>,
    },
}

#[derive(Debug)]
struct WatcherHandle {
    _name: &'static str,
    state: WatcherState,
}

impl WatcherHandle {
    fn initialized(name: &'static str) -> Self {
        Self {
            _name: name,
            state: WatcherState::Initialized,
        }
    }

    fn running(
        name: &'static str,
        task: JoinHandle<()>,
        forwarder: Option<JoinHandle<()>>,
    ) -> Self {
        Self {
            _name: name,
            state: WatcherState::Running { task, forwarder },
        }
    }

    fn simulated(name: &'static str, task: JoinHandle<()>) -> Self {
        Self {
            _name: name,
            state: WatcherState::Simulated { task },
        }
    }

    fn is_active(&self) -> bool {
        matches!(
            self.state,
            WatcherState::Running { .. } | WatcherState::Simulated { .. }
        )
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
            WatcherState::Simulated { task } => {
                task.abort();
            }
            WatcherState::Initialized => {}
        }
    }
}

/// Unified system processor implementing StatefulStreamProcessor
///
/// Supports snapshot, historical, and continuous scanning modes for system events.
pub struct SystemProcessor {
    runtime: Option<ProcessorRuntimeState>,

    /// System monitoring configuration
    config: SystemConfig,

    /// Individual watchers (initialized during operation)
    dbus_watcher: Option<WatcherHandle>,
    journal_watcher: Option<WatcherHandle>,
    udev_watcher: Option<WatcherHandle>,
    systemd_watcher: Option<WatcherHandle>,

    /// Last captured system state for snapshots
    last_state: Option<SystemState>,
}

impl SystemProcessor {
    /// Create a new unified system processor
    pub fn new() -> Self {
        // TODO(system-satellite): Complete implementation of system satellite processor
        // Needs: D-Bus, journal, and udev monitoring
        // - Monitor D-Bus for system events (org.freedesktop.systemd1, NetworkManager, etc.)
        // - Follow systemd journal for logs and service state changes
        // - Track udev hardware changes (USB, network interfaces, storage)

        Self {
            runtime: None,
            config: SystemConfig::default(),
            dbus_watcher: None,
            journal_watcher: None,
            udev_watcher: None,
            systemd_watcher: None,
            last_state: None,
        }
    }

    /// Create processor with custom configuration
    pub fn with_config(config: SystemConfig) -> Self {
        Self {
            runtime: None,
            config,
            dbus_watcher: None,
            journal_watcher: None,
            udev_watcher: None,
            systemd_watcher: None,
            last_state: None,
        }
    }

    fn runtime(&self) -> SatelliteResult<&ProcessorRuntimeState> {
        self.runtime.as_ref().ok_or_else(|| {
            SatelliteError::Lifecycle("Processor runtime not initialized".to_string())
        })
    }

    fn emitter(&self) -> SatelliteResult<&EventEmitter> {
        Ok(self.runtime()?.event_emitter())
    }

    fn emitter_clone(&self) -> SatelliteResult<EventEmitter> {
        Ok(self.runtime()?.event_emitter().clone())
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

        if let Some(timeout) = parse_config_value::<u64, _>("journal_timeout_secs", runtime) {
            config.journal_timeout_secs = timeout;
        }
    }

    /// Take a snapshot of current system state
    #[instrument(skip(self), fields(processor = "system"))]
    async fn take_snapshot(&mut self) -> SatelliteResult<SystemState> {
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
                following_active: self.journal_watcher.is_some(),
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
                monitoring_active: self.systemd_watcher.is_some(),
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
        WatcherSnapshot {
            dbus_ready: self.dbus_watcher.is_some(),
            journal_ready: self.journal_watcher.is_some(),
            udev_ready: self.udev_watcher.is_some(),
            systemd_ready: self.systemd_watcher.is_some(),
        }
    }

    /// Initialize watcher metadata (actual streaming starts during continuous scans).
    async fn initialize_watchers(&mut self) -> SatelliteResult<()> {
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

        if self.config.journal_enabled {
            if self.journal_watcher.is_none() {
                info!("Preparing journal watcher");
                self.journal_watcher = Some(WatcherHandle::initialized("journal"));
            }
        } else {
            self.journal_watcher = None;
        }

        if self.config.udev_enabled {
            if self.udev_watcher.is_none() {
                info!("Preparing udev watcher");
                self.udev_watcher = Some(WatcherHandle::initialized("udev"));
            }
        } else {
            self.udev_watcher = None;
        }

        if self.config.systemd_enabled {
            if self.systemd_watcher.is_none() {
                info!("Preparing systemd watcher");
                self.systemd_watcher = Some(WatcherHandle::initialized("systemd"));
            }
        } else {
            self.systemd_watcher = None;
        }

        Ok(())
    }

    /// Start continuous system monitoring
    async fn start_continuous_monitoring(
        &mut self,
        _from_checkpoint: Checkpoint,
    ) -> SatelliteResult<()> {
        info!("Starting continuous system monitoring");

        self.start_dbus_stream().await?;
        self.start_journal_stream().await?;
        self.start_udev_stream().await?;
        self.start_systemd_stream().await?;
        self.emit_monitoring_started_event().await?;

        Ok(())
    }

    async fn emit_monitoring_started_event(&self) -> SatelliteResult<()> {
        let emitter = self.emitter()?;
        let system_bootstrap_id = bootstrap_event_id();
        let provenance = Provenance::from_synthesis_safe(system_bootstrap_id, vec![]);

        let event = Event::new(
            SystemMonitoringStartedPayload {
                dbus_enabled: self.config.dbus_enabled,
                journal_enabled: self.config.journal_enabled,
                udev_enabled: self.config.udev_enabled,
                systemd_enabled: self.config.systemd_enabled,
                start_time: Utc::now(),
            },
            provenance,
        )
        .to_json_event()?;

        emitter.emit(event).await?;
        Ok(())
    }

    async fn start_dbus_stream(&mut self) -> SatelliteResult<()> {
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

        match self.spawn_dbus_task().await {
            Ok(handle) => {
                self.dbus_watcher = Some(handle);
            }
            Err(err) => {
                warn!(
                    error = %err,
                    "D-Bus watcher failed to start, falling back to simulated events"
                );
                let payload = json!({
                    "bus": self.config.dbus_buses,
                    "signal": "WatcherInitialized",
                    "sender": "org.sinex.system",
                    "timestamp": Utc::now().to_rfc3339(),
                });
                let handle =
                    self.spawn_simulated_watcher("dbus-sim", "system.dbus.signal", payload)?;
                self.dbus_watcher = Some(handle);
            }
        }

        Ok(())
    }

    async fn start_journal_stream(&mut self) -> SatelliteResult<()> {
        if !self.config.journal_enabled {
            return Ok(());
        }

        if self
            .journal_watcher
            .as_ref()
            .map(|handle| handle.is_active())
            .unwrap_or(false)
        {
            return Ok(());
        }

        match self.spawn_journal_task().await {
            Ok(handle) => {
                self.journal_watcher = Some(handle);
            }
            Err(err) => {
                warn!(
                    error = %err,
                    "Journal watcher failed to start, falling back to simulated events"
                );
                let payload = json!({
                    "message": "Simulated journal entry",
                    "unit": "sinex-system.service",
                    "timestamp": Utc::now().to_rfc3339(),
                });
                let handle =
                    self.spawn_simulated_watcher("journal-sim", "system.journal.entry", payload)?;
                self.journal_watcher = Some(handle);
            }
        }

        Ok(())
    }

    async fn start_udev_stream(&mut self) -> SatelliteResult<()> {
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

        match self.spawn_udev_task().await {
            Ok(handle) => {
                self.udev_watcher = Some(handle);
            }
            Err(err) => {
                warn!(
                    error = %err,
                    "udev watcher failed to start, falling back to simulated events"
                );
                let payload = json!({
                    "action": "add",
                    "device_path": "/dev/simulated",
                    "device_type": "block",
                    "timestamp": Utc::now().to_rfc3339(),
                });
                let handle =
                    self.spawn_simulated_watcher("udev-sim", "system.udev.device", payload)?;
                self.udev_watcher = Some(handle);
            }
        }

        Ok(())
    }

    async fn start_systemd_stream(&mut self) -> SatelliteResult<()> {
        if !self.config.systemd_enabled {
            return Ok(());
        }

        if self
            .systemd_watcher
            .as_ref()
            .map(|handle| handle.is_active())
            .unwrap_or(false)
        {
            return Ok(());
        }

        match self.spawn_systemd_task().await {
            Ok(handle) => {
                self.systemd_watcher = Some(handle);
            }
            Err(err) => {
                warn!(
                    error = %err,
                    "systemd watcher failed to start, falling back to simulated events"
                );
                let payload = json!({
                    "unit_name": "sinex-system.service",
                    "state": "active",
                    "timestamp": Utc::now().to_rfc3339(),
                });
                let handle = self.spawn_simulated_watcher(
                    "systemd-sim",
                    "system.systemd.unit_state",
                    payload,
                )?;
                self.systemd_watcher = Some(handle);
            }
        }

        Ok(())
    }

    async fn spawn_dbus_task(&self) -> SatelliteResult<WatcherHandle> {
        let emitter = self.emitter_clone()?;
        let (tx, rx) = mpsc::channel(WATCHER_CHANNEL_CAPACITY);
        let forwarder = spawn_forwarder("system.dbus.signal", rx, emitter);
        let mut watcher = DbusWatcher::new(self.config.dbus_config.clone()).await?;
        let task = tokio::spawn(async move {
            if let Err(err) = watcher.start_streaming(tx).await {
                warn!(error = %err, "D-Bus watcher terminated");
            }
        });
        Ok(WatcherHandle::running("dbus", task, Some(forwarder)))
    }

    async fn spawn_journal_task(&self) -> SatelliteResult<WatcherHandle> {
        let emitter = self.emitter_clone()?;
        let (tx, rx) = mpsc::channel(WATCHER_CHANNEL_CAPACITY);
        let forwarder = spawn_forwarder("system.journal.entry", rx, emitter);
        let mut watcher = JournalWatcher::new(self.config.journal_config.clone()).await?;
        let task = tokio::spawn(async move {
            if let Err(err) = watcher.start_streaming(tx).await {
                warn!(error = %err, "Journal watcher terminated");
            }
        });
        Ok(WatcherHandle::running("journal", task, Some(forwarder)))
    }

    async fn spawn_udev_task(&self) -> SatelliteResult<WatcherHandle> {
        let emitter = self.emitter_clone()?;
        let (tx, rx) = mpsc::channel(WATCHER_CHANNEL_CAPACITY);
        let forwarder = spawn_forwarder("system.udev.device", rx, emitter);
        let mut watcher = UdevWatcher::new(true).await?;
        let task = tokio::spawn(async move {
            if let Err(err) = watcher.start_streaming(tx).await {
                warn!(error = %err, "udev watcher terminated");
            }
        });
        Ok(WatcherHandle::running("udev", task, Some(forwarder)))
    }

    async fn spawn_systemd_task(&self) -> SatelliteResult<WatcherHandle> {
        let emitter = self.emitter_clone()?;
        let (tx, rx) = mpsc::channel(WATCHER_CHANNEL_CAPACITY);
        let forwarder = spawn_forwarder("system.systemd.unit_state", rx, emitter);
        let mut watcher = SystemdWatcher::new(self.config.systemd_config.clone()).await?;
        let task = tokio::spawn(async move {
            if let Err(err) = watcher.start_streaming(tx).await {
                warn!(error = %err, "systemd watcher terminated");
            }
        });
        Ok(WatcherHandle::running("systemd", task, Some(forwarder)))
    }

    fn spawn_simulated_watcher(
        &self,
        name: &'static str,
        event_type: &'static str,
        payload: serde_json::Value,
    ) -> SatelliteResult<WatcherHandle> {
        let emitter = self.emitter_clone()?;
        let task = tokio::spawn(async move {
            if emitter.dry_run() {
                return;
            }
            let event = synthetic_system_event(event_type, payload);
            if let Err(err) = emitter.emit(event).await {
                warn!(watcher = name, error = %err, "Failed to emit simulated watcher event");
            }
        });
        Ok(WatcherHandle::simulated(name, task))
    }

    /// Perform historical scan on system sources
    async fn scan_historical_system_data(
        &self,
        _from: &Checkpoint,
        _until: &TimeHorizon,
        _args: &ScanArgs,
        emit_events: bool,
    ) -> SatelliteResult<u64> {
        let mut event_count = 0;

        // Some system sources may have historical data (especially journal)

        if let Ok(emitter) = self.emitter() {
            // Journal can provide historical entries
            if self.config.journal_enabled && emit_events {
                let system_bootstrap_id = EventId::from_ulid(
                    Ulid::from_bytes([
                        0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00,
                    ])
                    .expect("hardcoded ULID bytes should be valid"),
                );
                let provenance = Provenance::from_synthesis_safe(system_bootstrap_id, vec![]);

                let event = Event::new(
                    JournaldHistoricalPayload {
                        source: "journal".to_string(),
                        scan_type: "historical".to_string(),
                        note: "Journal can provide historical entries".to_string(),
                    },
                    provenance,
                )
                .to_json_event()?;

                emitter.emit(event).await?;
                event_count += 1;
            }

            // systemd can provide unit state history
            if self.config.systemd_enabled && emit_events {
                let system_bootstrap_id = EventId::from_ulid(
                    Ulid::from_bytes([
                        0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00,
                    ])
                    .expect("hardcoded ULID bytes should be valid"),
                );
                let provenance = Provenance::from_synthesis_safe(system_bootstrap_id, vec![]);

                let event = Event::new(
                    SystemdUnitsHistoricalPayload {
                        source: "systemd".to_string(),
                        scan_type: "historical".to_string(),
                        note: "systemd can provide unit state history".to_string(),
                    },
                    provenance,
                )
                .to_json_event()?;

                emitter.emit(event).await?;
                event_count += 1;
            }

            // D-Bus and udev are typically real-time only
            if (self.config.dbus_enabled || self.config.udev_enabled) && emit_events {
                let system_bootstrap_id = EventId::from_ulid(
                    Ulid::from_bytes([
                        0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00,
                    ])
                    .expect("hardcoded ULID bytes should be valid"),
                );
                let provenance = Provenance::from_synthesis_safe(system_bootstrap_id, vec![]);

                let event = Event::new(
                    UdevDeviceHistoricalPayload {
                        sources: vec!["dbus".to_string(), "udev".to_string()],
                        scan_type: "historical".to_string(),
                        note: "D-Bus and udev are typically real-time sources with limited historical data".to_string(),
                    },
                    provenance,
                )
                .to_json_event()?;

                emitter.emit(event).await?;
                event_count += 1;
            }
        }

        Ok(event_count)
    }
}

impl Default for SystemProcessor {
    fn default() -> Self {
        Self::new()
    }
}

#[sinex_satellite_sdk::auto_satellite_metrics(processor_type = "ingestor", labels = ["source=system"])]
#[async_trait]
impl StatefulStreamProcessor for SystemProcessor {
    type Config = SystemConfig;

    #[instrument(skip(self, init), fields(processor = "system", service = %init.service_info().service_name()))]
    async fn initialize(
        &mut self,
        init: ProcessorInitContext<Self::Config>,
    ) -> SatelliteResult<()> {
        let (mut config, runtime) = init.into_runtime();
        Self::apply_config_overrides(&mut config, &runtime);
        self.config = config;
        self.runtime = Some(runtime);

        info!(
            dbus_enabled = self.config.dbus_enabled,
            journal_enabled = self.config.journal_enabled,
            udev_enabled = self.config.udev_enabled,
            systemd_enabled = self.config.systemd_enabled,
            dbus_buses = %self.config.dbus_buses,
            journal_timeout_secs = self.config.journal_timeout_secs,
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
            "Starting system scan"
        );

        match until {
            TimeHorizon::Snapshot => {
                // Take current state snapshot
                let _state = self.take_snapshot().await?;

                // Initialize watchers for snapshot capabilities
                if let Err(e) = self.initialize_watchers().await {
                    warnings.push(format!("Failed to initialize some watchers: {}", e));
                }

                // Count available system sources
                let active_watchers = [
                    self.dbus_watcher.is_some(),
                    self.journal_watcher.is_some(),
                    self.udev_watcher.is_some(),
                    self.systemd_watcher.is_some(),
                ]
                .iter()
                .filter(|&&x| x)
                .count();

                events_processed = active_watchers as u64;
                successful_targets.push("system_state_snapshot".to_string());

                if !args.dry_run {
                    let emitter = self.emitter()?;

                    // System snapshots are synthesis events (no source material)
                    let system_bootstrap_id = EventId::from_ulid(
                        Ulid::from_bytes([
                            0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                            0x00, 0x00, 0x00, 0x00,
                        ])
                        .unwrap(),
                    );
                    let provenance = Provenance::from_synthesis_safe(system_bootstrap_id, vec![]);

                    let snapshot_event = Event::new(
                        SystemSnapshotPayload {
                            active_watchers,
                            dbus_enabled: self.config.dbus_enabled,
                            journal_enabled: self.config.journal_enabled,
                            udev_enabled: self.config.udev_enabled,
                            systemd_enabled: self.config.systemd_enabled,
                            snapshot_time: Utc::now(),
                        },
                        provenance,
                    )
                    .to_json_event()?;

                    emitter.emit(snapshot_event).await?;
                }
            }

            TimeHorizon::Historical { .. } => {
                // Historical scan of system data
                warnings.push("Historical system scanning capabilities vary by source".to_string());

                match self
                    .scan_historical_system_data(&from, &until, &args, !args.dry_run)
                    .await
                {
                    Ok(count) => {
                        events_processed = count;
                        successful_targets.push("system_historical_scan".to_string());
                    }
                    Err(e) => {
                        failed_targets.push(("system_historical_scan".to_string(), e.to_string()));
                    }
                }
            }

            TimeHorizon::Continuous => {
                // Initialize watchers for continuous monitoring
                self.initialize_watchers().await?;

                // Start continuous monitoring
                info!("Starting continuous system monitoring");
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
            processor_stats: {
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
                stats.insert(
                    "successful_targets".to_string(),
                    successful_targets.len() as u64,
                );
                stats.insert("failed_targets".to_string(), failed_targets.len() as u64);
                stats
            },
            successful_targets,
            failed_targets,
            warnings,
        })
    }

    fn processor_name(&self) -> &str {
        "system-processor"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Ingestor
    }

    fn capabilities(&self) -> ProcessorCapabilities {
        ProcessorCapabilities {
            supports_continuous: true,
            supports_historical: true, // Journal and systemd have some historical data
            supports_snapshot: true,
            supports_interactive: false,
            max_scan_size: Some(10000), // Reasonable limit for system events
            supports_concurrent: false,
            manages_own_continuous_loop: false,
        }
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        // For system monitoring, use timestamp-based checkpoints
        Ok(Checkpoint::timestamp(Utc::now(), None))
    }

    async fn estimate_scan_scope(
        &self,
        _from: &Checkpoint,
        until: &TimeHorizon,
        _args: &ScanArgs,
    ) -> SatelliteResult<ScanEstimate> {
        let mut estimated_events = 0;
        let warnings = Vec::new();

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

fn bootstrap_event_id() -> EventId {
    EventId::from_ulid(
        Ulid::from_bytes(SYSTEM_WATCHER_BOOTSTRAP_BYTES)
            .expect("bootstrap ULID bytes should be valid"),
    )
}

fn synthetic_system_event(
    event_type: &'static str,
    payload: serde_json::Value,
) -> Event<JsonValue> {
    Event::dynamic("system.watchers", event_type, payload)
        .from_parents(vec![bootstrap_event_id()])
        .build()
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
    use sinex_test_utils::sinex_test;

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
            processor.journal_watcher.is_some(),
            "Journal watcher should be instantiated once initialization succeeds"
        );
        assert!(
            processor.udev_watcher.is_some(),
            "Udev watcher should be instantiated once initialization succeeds"
        );
        assert!(
            processor.systemd_watcher.is_some(),
            "systemd watcher should be instantiated once initialization succeeds"
        );
        Ok(())
    }
}
