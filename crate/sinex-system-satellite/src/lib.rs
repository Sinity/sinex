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
mod enhanced_dbus_watcher;
mod journal_watcher;
mod enhanced_journal_watcher;
mod udev_watcher;
mod systemd_watcher;
mod payloads;

// New unified processor module
pub mod unified_processor;

pub use dbus_watcher::DbusWatcher;
pub use enhanced_dbus_watcher::EnhancedDbusWatcher;
pub use journal_watcher::JournalWatcher;
pub use enhanced_journal_watcher::EnhancedJournalWatcher;
pub use udev_watcher::UdevWatcher;
pub use systemd_watcher::{SystemdWatcher, SystemdConfig};
pub use payloads::*;

// Re-export for convenience
pub use sinex_core::RawEvent;

// Re-export the new unified processor as the primary interface
pub use unified_processor::{SystemProcessor, SystemState, DbusStatus, JournalStatus, UdevStatus, SystemdStatus};

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
    /// Enhanced D-Bus configuration
    pub dbus_config: DbusConfig,
    /// Enhanced journal configuration
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
    #[error("Event source error: {0}")]
    EventSource(String),
    
    #[error("Configuration error: {0}")]
    Configuration(String),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

impl From<SystemSatelliteError> for sinex_satellite_sdk::SatelliteError {
    fn from(err: SystemSatelliteError) -> Self {
        sinex_satellite_sdk::SatelliteError::EventSource(err.to_string())
    }
}