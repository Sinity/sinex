#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]
#![doc = include_str!("../../../../docs/current/architecture/SystemOperations_And_Integrity_Architecture.md")]

//! Unified system node that coordinates D-Bus, journal, udev, and systemd signals.

mod dbus_watcher;
mod material_context;
mod payloads;
mod udev_watcher;
mod unified_journal_watcher;
mod watcher_lifecycle;

// Modern systemd/journald integration using nix crate
pub mod systemd_integration;

// New unified processor module
pub mod unified_processor;

use sinex_primitives::Seconds;

// Local facade module to reduce import verbosity
mod common {
    // Core types facade

    // SDK facade for common processor types
    pub use sinex_node_sdk::{
        stream_processor::{Checkpoint, NodeCapabilities, ScanArgs, ScanReport, TimeHorizon},
        NodeResult,
    };

    // External dependencies

    pub(crate) use {
        async_trait::async_trait,
        tracing::{info, instrument},
    };
}

pub use dbus_watcher::DbusWatcher;
pub(crate) use material_context::WatcherMaterialContext;
pub use payloads::*;
pub use udev_watcher::UdevWatcher;
pub use unified_journal_watcher::UnifiedJournalWatcher;
pub use watcher_lifecycle::{WatcherHealth, WatcherLifecycle};

// Re-export the new unified processor as the primary interface
pub use unified_processor::{
    DbusStatus, JournalStatus, SystemProcessor, SystemState, SystemdStatus, UdevStatus,
    WatcherSnapshot,
};

/// Configuration for system node
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
    pub journal_timeout_secs: Seconds,
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
            journal_timeout_secs: Seconds::from_secs(5),
            systemd_config: SystemdConfig::default(),
            dbus_config: DbusConfig::default(),
            journal_config: JournalConfig::default(),
        }
    }
}
