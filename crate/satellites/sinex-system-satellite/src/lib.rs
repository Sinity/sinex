#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../../../lib/sinex-satellite-sdk/docs/overview.md")]
#![doc = include_str!("../../../../docs/current/architecture/SystemOperations_And_Integrity_Architecture.md")]

//! Unified system satellite that coordinates D-Bus, journal, udev, and systemd signals.

mod dbus_watcher;
mod journal_watcher;
mod payloads;
mod systemd_watcher;
mod udev_watcher;

// Modern systemd/journald integration using nix crate
pub mod systemd_integration;

// New unified processor module
pub mod unified_processor;

// Local facade module to reduce import verbosity
mod common {
    // Core types facade

    pub use sinex_processor_runtime::{
        ActivityEntry, CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry,
        MissingItem, SourceState,
    };
    // SDK facade for common processor types
    pub use sinex_satellite_sdk::{
        stream_processor::{
            Checkpoint, ProcessorCapabilities, ProcessorType, ScanArgs, ScanEstimate, ScanReport,
            StatefulStreamProcessor, TimeHorizon,
        },
        SatelliteError, SatelliteResult,
    };

    // External dependencies
    pub use {
        async_trait::async_trait,
        chrono::{DateTime, Utc},
        serde::{Deserialize, Serialize},
        std::{collections::HashMap, time::Duration},
        tracing::{info, instrument},
    };
}

pub use dbus_watcher::DbusWatcher;
pub use journal_watcher::JournalWatcher;
pub use payloads::*;
pub use systemd_watcher::{SystemdConfig, SystemdWatcher};
pub use udev_watcher::UdevWatcher;

// Re-export the new unified processor as the primary interface
pub use unified_processor::{
    DbusStatus, JournalStatus, SystemProcessor, SystemState, SystemdStatus, UdevStatus,
    WatcherSnapshot,
};

/// Configuration for system satellite
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SystemConfig {
    /// Enable D-Bus monitoring
    pub dbus_enabled: bool,
    /// Enable systemd journal monitoring
    pub journal_enabled: bool,
    /// Enable udev hardware monitoring
    pub udev_enabled: bool,
    /// Enable systemd unit monitoring
    pub systemd_enabled: bool,
    /// D-Bus buses to monitor ("session", "system", or "both")
    pub dbus_buses: String,
    /// Journal follow timeout in seconds
    pub journal_timeout_secs: u64,
    /// systemd configuration
    pub systemd_config: SystemdConfig,
    /// D-Bus configuration
    pub dbus_config: DbusConfig,
    /// Journal configuration
    pub journal_config: JournalConfig,
}

impl Default for SystemConfig {
    fn default() -> Self {
        Self {
            dbus_enabled: true,
            journal_enabled: true,
            udev_enabled: true,
            systemd_enabled: true,
            dbus_buses: "both".to_string(),
            journal_timeout_secs: 5,
            systemd_config: SystemdConfig::default(),
            dbus_config: DbusConfig::default(),
            journal_config: JournalConfig::default(),
        }
    }
}
