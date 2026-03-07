#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]
#![doc = include_str!("../../../../docs/current/architecture/SystemOperations_And_Integrity_Architecture.md")]

//! Unified system node that coordinates D-Bus, journal, udev, and systemd signals.

mod dbus_watcher;
mod material_context;
mod payloads;
mod udev_watcher;
pub mod unified_journal_watcher;
pub mod watcher_factory;
pub mod watcher_lifecycle;

pub mod systemd_integration;
pub mod unified_node;

use sinex_primitives::Seconds;
use std::fmt;

// Local facade module to reduce import verbosity
mod common {
    // Core types facade

    // SDK facade for common node types
    pub use sinex_node_sdk::{
        NodeResult,
        runtime::stream::{Checkpoint, NodeCapabilities, ScanArgs, ScanReport, TimeHorizon},
    };

    // External dependencies

    pub(crate) use tracing::{info, instrument};
}

pub use dbus_watcher::DbusWatcher;
pub(crate) use material_context::WatcherMaterialContext;
pub use payloads::*;
pub use udev_watcher::UdevWatcher;
pub use unified_journal_watcher::UnifiedJournalWatcher;
pub use watcher_lifecycle::{WatcherActivitySnapshot, WatcherLifecycle};

pub use unified_node::{
    DbusStatus, JournalStatus, SystemNode, SystemState, SystemdStatus, UdevStatus, WatcherSnapshot,
};

/// Which D-Bus buses the system node monitors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum DbusBusScope {
    /// Monitor only the session D-Bus (user scope)
    Session,
    /// Monitor only the system D-Bus (system-wide)
    System,
    /// Monitor both session and system D-Bus
    #[default]
    Both,
}

impl DbusBusScope {
    /// Canonical string representation (matches the serialized form).
    #[must_use] 
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::System => "system",
            Self::Both => "both",
        }
    }

    /// Enumerate the individual bus names this scope covers.
    #[must_use] 
    pub fn bus_names(self) -> &'static [&'static str] {
        match self {
            Self::Session => &["session"],
            Self::System => &["system"],
            Self::Both => &["session", "system"],
        }
    }
}


impl fmt::Display for DbusBusScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

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
    /// D-Bus buses to monitor.
    pub dbus_buses: DbusBusScope,
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
            dbus_buses: DbusBusScope::Both,
            journal_timeout_secs: Seconds::from_secs(5),
            systemd_config: SystemdConfig::default(),
            dbus_config: DbusConfig::default(),
            journal_config: JournalConfig::default(),
        }
    }
}
