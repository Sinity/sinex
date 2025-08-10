//! Unified System Satellite
//!
//! Coordinates multiple system event sources:
//! - D-Bus events (signals, method calls, notifications)
//! - systemd Journal events
//! - udev hardware events  
//! - systemd unit state changes
//!
//! This module provides the unified StatefulStreamProcessor architecture from Part 16.

mod dbus_watcher;
mod journal_watcher;
mod payloads;
mod systemd_watcher;
mod udev_watcher;

// New unified processor module
pub mod unified_processor;

pub use dbus_watcher::DbusWatcher;
pub use journal_watcher::JournalWatcher;
pub use payloads::*;
pub use systemd_watcher::{SystemdConfig, SystemdWatcher};
pub use udev_watcher::UdevWatcher;

// Re-export for convenience
pub use sinex_core::db::models::RawEvent;

// Re-export the new unified processor as the primary interface
pub use unified_processor::{
    DbusStatus, JournalStatus, SystemProcessor, SystemState, SystemdStatus, UdevStatus,
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

/// Error types for system satellite
#[derive(Debug, thiserror::Error)]
pub enum SystemSatelliteError {
    #[error("Processing error: {0}")]
    Processing(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

impl From<SystemSatelliteError> for sinex_satellite_sdk::SatelliteError {
    fn from(err: SystemSatelliteError) -> Self {
        sinex_satellite_sdk::SatelliteError::Processing(err.to_string())
    }
}
