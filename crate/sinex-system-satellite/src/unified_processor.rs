//! Unified system processor implementing StatefulStreamProcessor
//!
//! This module implements the system satellite processor supporting snapshot, historical, and
//! continuous scanning modes for system events.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_events::{services, EventFactory};
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
use std::path::PathBuf;
use std::time::Duration;
use tracing::{info, warn};

use crate::{EnhancedDbusWatcher, EnhancedJournalWatcher, SystemdWatcher, UdevWatcher};

// Import the existing SystemConfig from the parent module
pub use crate::SystemConfig;

/// System state snapshot for exploration and diagnostics
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Unified system processor implementing StatefulStreamProcessor
///
/// Supports snapshot, historical, and continuous scanning modes for system events.
pub struct SystemProcessor {
    /// Current processing context (set during initialization)
    context: Option<StreamProcessorContext>,

    /// System monitoring configuration
    config: SystemConfig,

    /// Individual watchers (initialized during operation)
    dbus_watcher: Option<EnhancedDbusWatcher>,
    journal_watcher: Option<EnhancedJournalWatcher>,
    udev_watcher: Option<UdevWatcher>,
    systemd_watcher: Option<SystemdWatcher>,

    /// Last captured system state for snapshots
    last_state: Option<SystemState>,

    /// Checkpoint manager for state persistence
    checkpoint_manager: Option<CheckpointManager>,
}

impl SystemProcessor {
    /// Create a new unified system processor
    pub fn new() -> Self {
        Self {
            context: None,
            config: SystemConfig::default(),
            dbus_watcher: None,
            journal_watcher: None,
            udev_watcher: None,
            systemd_watcher: None,
            last_state: None,
            checkpoint_manager: None,
        }
    }

    /// Create processor with custom configuration
    pub fn with_config(config: SystemConfig) -> Self {
        Self {
            context: None,
            config,
            dbus_watcher: None,
            journal_watcher: None,
            udev_watcher: None,
            systemd_watcher: None,
            last_state: None,
            checkpoint_manager: None,
        }
    }

    /// Take a snapshot of current system state
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

    /// Initialize watchers based on enabled sources
    async fn initialize_watchers(&mut self) -> SatelliteResult<()> {
        // For now, stub implementations - will be implemented properly later

        // Initialize D-Bus watcher
        if self.config.dbus_enabled {
            info!(
                "Initializing enhanced D-Bus watcher for buses: {} (stub)",
                self.config.dbus_buses
            );
            info!("✅ Enhanced D-Bus watcher initialized (stub)");
        }

        // Initialize Journal watcher
        if self.config.journal_enabled {
            info!("Initializing enhanced journal watcher (stub)");
            info!("✅ Enhanced journal watcher initialized (stub)");
        }

        // Initialize udev watcher
        if self.config.udev_enabled {
            info!("Initializing udev watcher (stub)");
            info!("✅ udev watcher initialized (stub)");
        }

        // Initialize systemd watcher
        if self.config.systemd_enabled {
            info!("Initializing systemd watcher (stub)");
            info!("✅ systemd watcher initialized (stub)");
        }

        Ok(())
    }

    /// Start continuous system monitoring
    async fn start_continuous_monitoring(
        &mut self,
        _from_checkpoint: Checkpoint,
    ) -> SatelliteResult<()> {
        info!("Starting continuous system monitoring");

        // For now, stub implementation - will be implemented properly later
        // This would start the actual watchers and forward events

        if let Some(ref context) = self.context {
            info!("System monitoring context available");

            // Create a sample event to show the interface works
            let factory = EventFactory::new(services::SYSTEM_SATELLITE);
            let sample_event = factory.create_event(
                "system.monitoring_started",
                json!({
                    "dbus_enabled": self.config.dbus_enabled,
                    "journal_enabled": self.config.journal_enabled,
                    "udev_enabled": self.config.udev_enabled,
                    "systemd_enabled": self.config.systemd_enabled,
                    "start_time": Utc::now()
                }),
            );

            context.emit_event(sample_event).await?;
        }

        Ok(())
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

        if let Some(ref context) = self.context {
            // Journal can provide historical entries
            if self.config.journal_enabled && emit_events {
                let factory = EventFactory::new(services::SYSTEM_SATELLITE);
                let event = factory.create_event(
                    "journal.historical",
                    json!({
                        "source": "journal",
                        "scan_type": "historical",
                        "note": "Journal can provide historical entries"
                    }),
                );

                context.emit_event(event).await?;
                event_count += 1;
            }

            // systemd can provide unit state history
            if self.config.systemd_enabled && emit_events {
                let factory = EventFactory::new(services::SYSTEM_SATELLITE);
                let event = factory.create_event(
                    "systemd.historical",
                    json!({
                        "source": "systemd",
                        "scan_type": "historical",
                        "note": "systemd can provide unit state history"
                    }),
                );

                context.emit_event(event).await?;
                event_count += 1;
            }

            // D-Bus and udev are typically real-time only
            if (self.config.dbus_enabled || self.config.udev_enabled) && emit_events {
                let factory = EventFactory::new(services::SYSTEM_SATELLITE);
                let event = factory.create_event(
                    "realtime_sources.historical", 
                    json!({
                        "sources": ["dbus", "udev"],
                        "scan_type": "historical",
                        "note": "D-Bus and udev are typically real-time sources with limited historical data"
                    })
                );

                context.emit_event(event).await?;
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

#[sinex_macros::auto_satellite_metrics(processor_type = "ingestor", labels = ["source=system"])]
#[async_trait]
impl StatefulStreamProcessor for SystemProcessor {
    async fn initialize(&mut self, ctx: StreamProcessorContext) -> SatelliteResult<()> {
        info!(
            processor = self.processor_name(),
            service = %ctx.service_name,
            "Initializing system processor"
        );

        // Initialize checkpoint manager
        self.checkpoint_manager = Some(ctx.checkpoint_manager.clone());

        // Parse configuration from processor context
        if let Some(config_json) = ctx.config.get("system") {
            match serde_json::from_value::<SystemConfig>(config_json.clone()) {
                Ok(config) => {
                    self.config = config;
                }
                Err(e) => {
                    warn!("Failed to parse system config, using defaults: {}", e);
                }
            }
        }

        // Override with individual config values if present
        if let Some(dbus_enabled_json) = ctx.config.get("dbus_enabled") {
            if let Ok(enabled) = serde_json::from_value::<bool>(dbus_enabled_json.clone()) {
                self.config.dbus_enabled = enabled;
            }
        }

        if let Some(journal_enabled_json) = ctx.config.get("journal_enabled") {
            if let Ok(enabled) = serde_json::from_value::<bool>(journal_enabled_json.clone()) {
                self.config.journal_enabled = enabled;
            }
        }

        if let Some(udev_enabled_json) = ctx.config.get("udev_enabled") {
            if let Ok(enabled) = serde_json::from_value::<bool>(udev_enabled_json.clone()) {
                self.config.udev_enabled = enabled;
            }
        }

        if let Some(systemd_enabled_json) = ctx.config.get("systemd_enabled") {
            if let Ok(enabled) = serde_json::from_value::<bool>(systemd_enabled_json.clone()) {
                self.config.systemd_enabled = enabled;
            }
        }

        if let Some(dbus_buses_json) = ctx.config.get("dbus_buses") {
            if let Ok(buses) = serde_json::from_value::<String>(dbus_buses_json.clone()) {
                self.config.dbus_buses = buses;
            }
        }

        if let Some(journal_timeout_json) = ctx.config.get("journal_timeout_secs") {
            if let Ok(timeout) = serde_json::from_value::<u64>(journal_timeout_json.clone()) {
                self.config.journal_timeout_secs = timeout;
            }
        }

        info!(
            dbus_enabled = self.config.dbus_enabled,
            journal_enabled = self.config.journal_enabled,
            udev_enabled = self.config.udev_enabled,
            systemd_enabled = self.config.systemd_enabled,
            dbus_buses = %self.config.dbus_buses,
            journal_timeout_secs = self.config.journal_timeout_secs,
            "System processor configuration"
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
                    // Emit a snapshot event
                    if let Some(ref context) = self.context {
                        let factory = EventFactory::new(services::SYSTEM_SATELLITE);
                        let snapshot_event = factory.create_event(
                            "system.snapshot",
                            json!({
                                "active_watchers": active_watchers,
                                "dbus_enabled": self.config.dbus_enabled,
                                "journal_enabled": self.config.journal_enabled,
                                "udev_enabled": self.config.udev_enabled,
                                "systemd_enabled": self.config.systemd_enabled,
                                "snapshot_time": Utc::now()
                            }),
                        );

                        context.emit_event(snapshot_event).await?;
                    }
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
            processor_stats: HashMap::from([
                (
                    "dbus_enabled".to_string(),
                    if self.config.dbus_enabled { 1 } else { 0 },
                ),
                (
                    "journal_enabled".to_string(),
                    if self.config.journal_enabled { 1 } else { 0 },
                ),
                (
                    "udev_enabled".to_string(),
                    if self.config.udev_enabled { 1 } else { 0 },
                ),
                (
                    "systemd_enabled".to_string(),
                    if self.config.systemd_enabled { 1 } else { 0 },
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
    fn get_source_state(&self) -> Result<SourceState, Box<dyn std::error::Error>> {
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
            metadata: HashMap::from([
                (
                    "dbus_enabled".to_string(),
                    serde_json::to_value(self.config.dbus_enabled)?,
                ),
                (
                    "journal_enabled".to_string(),
                    serde_json::to_value(self.config.journal_enabled)?,
                ),
                (
                    "udev_enabled".to_string(),
                    serde_json::to_value(self.config.udev_enabled)?,
                ),
                (
                    "systemd_enabled".to_string(),
                    serde_json::to_value(self.config.systemd_enabled)?,
                ),
                (
                    "dbus_buses".to_string(),
                    serde_json::to_value(&self.config.dbus_buses)?,
                ),
                (
                    "journal_timeout_secs".to_string(),
                    serde_json::to_value(self.config.journal_timeout_secs)?,
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
    ) -> Result<Vec<IngestionHistoryEntry>, Box<dyn std::error::Error>> {
        // In a real implementation, this would query the database for scan history
        // For now, return empty as this requires database access
        Ok(vec![])
    }

    fn get_coverage_analysis(
        &self,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> Result<CoverageAnalysis, Box<dyn std::error::Error>> {
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
        path: &PathBuf,
        format: ExportFormat,
    ) -> Result<(), Box<dyn std::error::Error>> {
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

            std::fs::write(path, content)?;
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

            std::fs::write(path, content)?;
        }

        Ok(())
    }
}
