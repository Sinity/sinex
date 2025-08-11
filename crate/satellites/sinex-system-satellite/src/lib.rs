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

/// Error types for system satellite with rich context support
#[derive(Debug, thiserror::Error)]
pub enum SystemSatelliteError {
    #[error("Processing error: {0}")]
    Processing(ErrorDetails),

    #[error("Configuration error: {0}")]
    Configuration(ErrorDetails),

    #[error("IO error: {0}")]
    Io(ErrorDetails),

    #[error("JSON error: {0}")]
    Json(ErrorDetails),

    #[error(transparent)]
    #[from]
    StdIo(#[from] std::io::Error),

    #[error(transparent)]
    #[from]
    SerdeJson(#[from] serde_json::Error),
}

/// Detailed error information with context and source chain
#[derive(Debug, Clone)]
pub struct ErrorDetails {
    /// The primary error message
    pub message: String,
    /// Additional context as key-value pairs
    pub context: indexmap::IndexMap<String, String>,
    /// Chain of source errors
    pub sources: Vec<String>,
}

impl std::fmt::Display for ErrorDetails {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)?;

        if !self.context.is_empty() {
            write!(f, " (")?;
            for (i, (k, v)) in self.context.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}: {}", k, v)?;
            }
            write!(f, ")")?;
        }

        if !self.sources.is_empty() {
            write!(f, "\nCaused by:")?;
            for (i, source) in self.sources.iter().enumerate() {
                write!(f, "\n  {}: {}", i + 1, source)?;
            }
        }

        Ok(())
    }
}

impl ErrorDetails {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            context: indexmap::IndexMap::new(),
            sources: Vec::new(),
        }
    }

    pub fn with_context(mut self, key: impl Into<String>, value: impl ToString) -> Self {
        self.context.insert(key.into(), value.to_string());
        self
    }

    pub fn with_source(mut self, source: impl ToString) -> Self {
        self.sources.push(source.to_string());
        self
    }
}

impl SystemSatelliteError {
    /// Create a new processing error
    pub fn processing(msg: impl Into<String>) -> Self {
        SystemSatelliteError::Processing(ErrorDetails::new(msg))
    }

    /// Create a new configuration error
    pub fn configuration(msg: impl Into<String>) -> Self {
        SystemSatelliteError::Configuration(ErrorDetails::new(msg))
    }

    /// Create a new IO error
    pub fn io(msg: impl Into<String>) -> Self {
        SystemSatelliteError::Io(ErrorDetails::new(msg))
    }

    /// Create a new JSON error
    pub fn json(msg: impl Into<String>) -> Self {
        SystemSatelliteError::Json(ErrorDetails::new(msg))
    }

    /// Add context key-value pair
    pub fn with_context(mut self, key: impl Into<String>, value: impl ToString) -> Self {
        let details = match &mut self {
            SystemSatelliteError::Processing(d)
            | SystemSatelliteError::Configuration(d)
            | SystemSatelliteError::Io(d)
            | SystemSatelliteError::Json(d) => d,
            // For transparent errors, we can't add context directly
            SystemSatelliteError::StdIo(_) | SystemSatelliteError::SerdeJson(_) => return self,
        };
        details.context.insert(key.into(), value.to_string());
        self
    }

    /// Add operation context
    pub fn with_operation(self, operation: impl ToString) -> Self {
        self.with_context("operation", operation)
    }

    /// Add ID context
    pub fn with_id(self, id_type: &str, id: impl ToString) -> Self {
        self.with_context(id_type, id)
    }

    /// Add source error to the chain
    pub fn with_source(mut self, source: impl ToString) -> Self {
        let details = match &mut self {
            SystemSatelliteError::Processing(d)
            | SystemSatelliteError::Configuration(d)
            | SystemSatelliteError::Io(d)
            | SystemSatelliteError::Json(d) => d,
            // For transparent errors, we can't add source context directly
            SystemSatelliteError::StdIo(_) | SystemSatelliteError::SerdeJson(_) => return self,
        };
        details.sources.push(source.to_string());
        self
    }

    /// Add path context
    pub fn with_path(self, path: impl AsRef<std::path::Path>) -> Self {
        self.with_context("path", path.as_ref().display().to_string())
    }

    /// Add duration context
    pub fn with_duration(self, duration: std::time::Duration) -> Self {
        self.with_context("duration_ms", duration.as_millis())
    }
}

impl From<SystemSatelliteError> for sinex_satellite_sdk::SatelliteError {
    fn from(err: SystemSatelliteError) -> Self {
        sinex_satellite_sdk::SatelliteError::Processing(err.to_string())
    }
}
