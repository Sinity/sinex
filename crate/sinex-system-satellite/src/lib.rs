//! Unified System Satellite
//!
//! Coordinates multiple system event sources:
//! - D-Bus events (signals, method calls, notifications)
//! - systemd Journal events

use async_trait::async_trait;
use sinex_satellite_sdk::{
    EventSource, EventSourceContext, SatelliteResult, SatelliteError,
    ScannerArgs, ScanReport, ScannerEstimate, VersionInfo
};
use std::time::Instant;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

mod dbus_watcher;
mod enhanced_dbus_watcher;
mod journal_watcher;
mod enhanced_journal_watcher;
mod udev_watcher;
mod systemd_watcher;
mod payloads;

pub use dbus_watcher::DbusWatcher;
pub use enhanced_dbus_watcher::EnhancedDbusWatcher;
pub use journal_watcher::JournalWatcher;
pub use enhanced_journal_watcher::EnhancedJournalWatcher;
pub use udev_watcher::UdevWatcher;
pub use systemd_watcher::{SystemdWatcher, SystemdConfig};
pub use payloads::*;

// Re-export for convenience
pub use sinex_core::RawEvent;

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

/// Unified system satellite
pub struct SystemSatellite {
    context: Option<EventSourceContext>,
    config: SystemConfig,
    dbus_watcher: Option<EnhancedDbusWatcher>,
    journal_watcher: Option<EnhancedJournalWatcher>,
    udev_watcher: Option<UdevWatcher>,
    systemd_watcher: Option<SystemdWatcher>,
}

impl SystemSatellite {
    /// Create new system satellite
    pub fn new() -> Self {
        Self {
            context: None,
            config: SystemConfig::default(),
            dbus_watcher: None,
            journal_watcher: None,
            udev_watcher: None,
            systemd_watcher: None,
        }
    }

    /// Create with specific configuration
    pub fn with_config(config: SystemConfig) -> Self {
        Self {
            context: None,
            config,
            dbus_watcher: None,
            journal_watcher: None,
            udev_watcher: None,
            systemd_watcher: None,
        }
    }
}

#[async_trait]
impl EventSource for SystemSatellite {
    fn source_name(&self) -> &str {
        "system"
    }

    async fn initialize(&mut self, context: EventSourceContext) -> SatelliteResult<()> {
        info!("Initializing system satellite");

        // Store context for later use
        self.context = Some(context);

        // Parse configuration from context if available
        if let Ok(config_str) = std::env::var("SINEX_SYSTEM_CONFIG") {
            if let Ok(config) = serde_json::from_str::<SystemConfig>(&config_str) {
                self.config = config;
            }
        }

        // Initialize enhanced D-Bus watcher if enabled
        if self.config.dbus_enabled {
            match EnhancedDbusWatcher::new(self.config.dbus_config.clone()).await {
                Ok(watcher) => {
                    self.dbus_watcher = Some(watcher);
                    info!("Enhanced D-Bus watcher initialized");
                }
                Err(e) => {
                    error!("Failed to initialize enhanced D-Bus watcher: {}", e);
                    return Err(SatelliteError::EventSource(format!(
                        "Failed to initialize enhanced D-Bus watcher: {}", e
                    )));
                }
            }
        }

        // Initialize enhanced journal watcher if enabled
        if self.config.journal_enabled {
            match EnhancedJournalWatcher::new(self.config.journal_config.clone()).await {
                Ok(watcher) => {
                    self.journal_watcher = Some(watcher);
                    info!("Enhanced journal watcher initialized");
                }
                Err(e) => {
                    error!("Failed to initialize enhanced journal watcher: {}", e);
                    return Err(SatelliteError::EventSource(format!(
                        "Failed to initialize enhanced journal watcher: {}", e
                    )));
                }
            }
        }

        // Initialize udev watcher if enabled
        if self.config.udev_enabled {
            match UdevWatcher::new(true).await { // Monitor hotplug events
                Ok(watcher) => {
                    self.udev_watcher = Some(watcher);
                    info!("udev watcher initialized");
                }
                Err(e) => {
                    error!("Failed to initialize udev watcher: {}", e);
                    return Err(SatelliteError::EventSource(format!(
                        "Failed to initialize udev watcher: {}", e
                    )));
                }
            }
        }

        // Initialize systemd watcher if enabled
        if self.config.systemd_enabled {
            match SystemdWatcher::new(self.config.systemd_config.clone()).await {
                Ok(watcher) => {
                    self.systemd_watcher = Some(watcher);
                    info!("systemd watcher initialized");
                }
                Err(e) => {
                    error!("Failed to initialize systemd watcher: {}", e);
                    return Err(SatelliteError::EventSource(format!(
                        "Failed to initialize systemd watcher: {}", e
                    )));
                }
            }
        }

        info!("System satellite initialization completed");
        Ok(())
    }

    async fn start_streaming(&mut self) -> SatelliteResult<()> {
        info!("Starting system event streaming");

        let mut tasks: Vec<JoinHandle<SatelliteResult<()>>> = Vec::new();

        // Get event sender from context
        let context = self.context.as_ref().ok_or_else(|| {
            SatelliteError::Lifecycle("EventSource not initialized".to_string())
        })?;
        let tx = context.event_sender.clone();

        // Start enhanced D-Bus watcher
        if let Some(mut dbus_watcher) = self.dbus_watcher.take() {
            let tx_dbus = tx.clone();
            let handle = tokio::spawn(async move {
                dbus_watcher.start_streaming(tx_dbus).await
            });
            tasks.push(handle);
        }

        // Start enhanced journal watcher
        if let Some(mut journal_watcher) = self.journal_watcher.take() {
            let tx_journal = tx.clone();
            let handle = tokio::spawn(async move {
                journal_watcher.start_streaming(tx_journal).await
            });
            tasks.push(handle);
        }

        // Start udev watcher
        if let Some(mut udev_watcher) = self.udev_watcher.take() {
            let tx_udev = tx.clone();
            let handle = tokio::spawn(async move {
                udev_watcher.start_streaming(tx_udev).await
            });
            tasks.push(handle);
        }

        // Start systemd watcher
        if let Some(mut systemd_watcher) = self.systemd_watcher.take() {
            let tx_systemd = tx.clone();
            let handle = tokio::spawn(async move {
                systemd_watcher.start_streaming(tx_systemd).await
            });
            tasks.push(handle);
        }

        if tasks.is_empty() {
            warn!("No system watchers enabled, satellite will not produce events");
            // Keep the satellite running but idle
            tokio::time::sleep(tokio::time::Duration::from_secs(u64::MAX)).await;
            return Ok(());
        }

        // Wait for any task to complete (or fail)
        let (_result, _index, _remaining) = futures::future::select_all(tasks).await;

        // If we get here, one of the watchers has stopped
        error!("System watcher stopped unexpectedly");
        Ok(())
    }

    async fn shutdown(&mut self) -> SatelliteResult<()> {
        info!("Shutting down system satellite");
        
        // Watchers will be dropped when the satellite is dropped
        // No explicit cleanup needed for now
        
        Ok(())
    }

    // ===== Dual-Mode Extensions =====
    
    fn supports_scanner(&self) -> bool {
        true // System satellite supports historical scanning
    }
    
    async fn run_scanner(&mut self, args: ScannerArgs) -> SatelliteResult<ScanReport> {
        let start_time = Instant::now();
        info!("Starting system scanner mode");
        
        let mut events_generated = 0u64;
        let mut source_stats = std::collections::HashMap::new();
        let mut processed_paths = Vec::new();
        let mut failed_paths = Vec::new();

        // Get event sender from context
        let context = self.context.as_ref().ok_or_else(|| {
            SatelliteError::Lifecycle("EventSource not initialized".to_string())
        })?;
        let tx = &context.event_sender;

        // Scanner mode for D-Bus: Scan existing D-Bus configuration and generate discovery events
        if self.config.dbus_enabled {
            info!("Scanning D-Bus configuration and services");
            
            match self.scan_dbus_services(&args, tx).await {
                Ok(count) => {
                    events_generated += count;
                    source_stats.insert("dbus_services_scanned".to_string(), count);
                    processed_paths.push("dbus://session".to_string());
                    processed_paths.push("dbus://system".to_string());
                }
                Err(e) => {
                    error!("Failed to scan D-Bus services: {}", e);
                    failed_paths.push(("dbus://".to_string(), e.to_string()));
                }
            }
        }

        // Scanner mode for Journal: Scan existing journal entries within time range
        if self.config.journal_enabled {
            info!("Scanning systemd journal entries");
            
            let journal_paths = if args.paths.is_empty() {
                vec!["/var/log/journal".to_string(), "/run/log/journal".to_string()]
            } else {
                args.paths.iter()
                    .filter(|p| p.contains("journal"))
                    .cloned()
                    .collect()
            };

            for path in journal_paths {
                match self.scan_journal_entries(&args, &path, tx).await {
                    Ok(count) => {
                        events_generated += count;
                        source_stats.insert(format!("journal_entries_{}", path.replace('/', "_")), count);
                        processed_paths.push(path);
                    }
                    Err(e) => {
                        error!("Failed to scan journal path {}: {}", path, e);
                        failed_paths.push((path, e.to_string()));
                    }
                }
            }
        }

        // Scanner mode for systemd: Scan current unit states and generate historical state events
        if self.config.systemd_enabled {
            info!("Scanning systemd unit states");
            
            match self.scan_systemd_units(&args, tx).await {
                Ok(count) => {
                    events_generated += count;
                    source_stats.insert("systemd_units_scanned".to_string(), count);
                    processed_paths.push("systemd://units".to_string());
                }
                Err(e) => {
                    error!("Failed to scan systemd units: {}", e);
                    failed_paths.push(("systemd://units".to_string(), e.to_string()));
                }
            }
        }

        // Scanner mode for udev: Scan current device state
        if self.config.udev_enabled {
            info!("Scanning current udev device state");
            
            match self.scan_udev_devices(&args, tx).await {
                Ok(count) => {
                    events_generated += count;
                    source_stats.insert("udev_devices_scanned".to_string(), count);
                    processed_paths.push("udev://devices".to_string());
                }
                Err(e) => {
                    error!("Failed to scan udev devices: {}", e);
                    failed_paths.push(("udev://devices".to_string(), e.to_string()));
                }
            }
        }

        let duration = start_time.elapsed();
        
        info!(
            events_generated,
            duration_ms = duration.as_millis(),
            "System scanner mode completed"
        );

        Ok(ScanReport {
            events_generated,
            duration,
            blob_id: None, // No blob storage for system events
            time_range: args.time_range,
            content_hash: None,
            source_stats,
            version_info: VersionInfo {
                git_revision: std::env::var("VERGEN_GIT_SHA").unwrap_or_else(|_| "unknown".to_string()),
                binary_hash: "".to_string(), // TODO: Calculate binary hash
                component_version: format!("sinex-system-satellite-{}", env!("CARGO_PKG_VERSION")),
                scan_timestamp: chrono::Utc::now(),
            },
            processed_paths,
            failed_paths,
        })
    }

    async fn estimate_scanner_scope(&self, args: &ScannerArgs) -> SatelliteResult<ScannerEstimate> {
        let mut estimated_events = 0u64;
        let mut estimated_paths = 0u64;
        let mut warnings = Vec::new();

        // Estimate D-Bus services
        if self.config.dbus_enabled {
            estimated_events += 50; // Typical number of D-Bus services
            estimated_paths += 2; // session and system buses
        }

        // Estimate journal entries (this could be very large)
        if self.config.journal_enabled {
            let journal_estimate = if let Some((start, end)) = args.time_range {
                let duration_hours = (end - start).num_hours();
                std::cmp::min(duration_hours as u64 * 100, 10000) // ~100 entries per hour, capped at 10k
            } else {
                warnings.push("No time range specified for journal scan - could generate many events".to_string());
                1000 // Default estimate without time range
            };
            estimated_events += journal_estimate;
            estimated_paths += 2; // /var/log/journal and /run/log/journal
        }

        // Estimate systemd units
        if self.config.systemd_enabled {
            estimated_events += 200; // Typical number of systemd units
            estimated_paths += 1;
        }

        // Estimate udev devices
        if self.config.udev_enabled {
            estimated_events += 100; // Typical number of devices
            estimated_paths += 1;
        }

        Ok(ScannerEstimate {
            estimated_events,
            estimated_duration: std::time::Duration::from_secs(estimated_events / 100), // ~100 events/sec
            estimated_data_size: estimated_events * 1024, // ~1KB per event
            estimated_paths,
            warnings,
        })
    }
}

impl SystemSatellite {
    // ===== Scanner Helper Methods =====

    /// Scan D-Bus services and generate discovery events
    async fn scan_dbus_services(
        &self,
        _args: &ScannerArgs,
        tx: &tokio::sync::mpsc::UnboundedSender<sinex_core::RawEvent>,
    ) -> SatelliteResult<u64> {
        let mut events_generated = 0u64;
        
        // Use dbus-send to list services on session and system buses
        for bus_type in &["session", "system"] {
            match tokio::process::Command::new("dbus-send")
                .args(&[
                    &format!("--{}", bus_type),
                    "--dest=org.freedesktop.DBus",
                    "--type=method_call",
                    "--print-reply",
                    "/org/freedesktop/DBus",
                    "org.freedesktop.DBus.ListNames"
                ])
                .output()
                .await
            {
                Ok(output) => {
                    let output_str = String::from_utf8_lossy(&output.stdout);
                    for line in output_str.lines() {
                        if line.contains("string \"") {
                            if let Some(service_name) = line.split("string \"").nth(1)
                                .and_then(|s| s.split('"').next()) {
                                
                                let payload = serde_json::json!({
                                    "bus_type": bus_type,
                                    "service_name": service_name,
                                    "discovery_method": "scanner",
                                    "timestamp": chrono::Utc::now().to_rfc3339(),
                                });

                                let event = sinex_events::RawEventBuilder::new(
                                    sinex_core::sources::DBUS,
                                    "service.discovered",
                                    payload,
                                )
                                .with_host("localhost")
                                .build();

                                if tx.send(event).is_ok() {
                                    events_generated += 1;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to list D-Bus services on {} bus: {}", bus_type, e);
                }
            }
        }

        Ok(events_generated)
    }

    /// Scan journal entries within time range
    async fn scan_journal_entries(
        &self,
        args: &ScannerArgs,
        _path: &str,
        tx: &tokio::sync::mpsc::UnboundedSender<sinex_core::RawEvent>,
    ) -> SatelliteResult<u64> {
        let mut events_generated = 0u64;
        let mut cmd = tokio::process::Command::new("journalctl");
        
        cmd.args(&["--output=json", "--no-hostname", "--lines=1000"]);
        
        // Add time range if specified
        if let Some((start, end)) = args.time_range {
            cmd.arg("--since").arg(start.format("%Y-%m-%d %H:%M:%S").to_string());
            cmd.arg("--until").arg(end.format("%Y-%m-%d %H:%M:%S").to_string());
        }

        match cmd.output().await {
            Ok(output) => {
                let output_str = String::from_utf8_lossy(&output.stdout);
                for line in output_str.lines() {
                    if line.trim().is_empty() {
                        continue;
                    }
                    
                    match serde_json::from_str::<serde_json::Value>(line) {
                        Ok(entry) => {
                            let payload = serde_json::json!({
                                "message": entry["MESSAGE"].as_str().unwrap_or(""),
                                "unit": entry["_SYSTEMD_UNIT"].as_str(),
                                "pid": entry["_PID"].as_str(),
                                "uid": entry["_UID"].as_str(),
                                "cursor": entry["__CURSOR"].as_str().unwrap_or("unknown"),
                                "realtime_timestamp": entry["__REALTIME_TIMESTAMP"].as_str(),
                                "discovery_method": "scanner",
                                "timestamp": chrono::Utc::now().to_rfc3339(),
                            });

                            let event = sinex_events::RawEventBuilder::new(
                                sinex_core::sources::JOURNALD,
                                "entry.historical",
                                payload,
                            )
                            .with_host("localhost")
                            .build();

                            if tx.send(event).is_ok() {
                                events_generated += 1;
                            }

                            // Respect max_events limit
                            if args.max_events > 0 && events_generated >= args.max_events {
                                break;
                            }
                        }
                        Err(e) => {
                            debug!("Failed to parse journal entry: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                return Err(SatelliteError::EventSource(format!("Failed to run journalctl: {}", e)));
            }
        }

        Ok(events_generated)
    }

    /// Scan systemd units and generate state events
    async fn scan_systemd_units(
        &self,
        _args: &ScannerArgs,
        tx: &tokio::sync::mpsc::UnboundedSender<sinex_core::RawEvent>,
    ) -> SatelliteResult<u64> {
        let mut events_generated = 0u64;

        match tokio::process::Command::new("systemctl")
            .args(&["list-units", "--all", "--no-pager", "--output=json"])
            .output()
            .await
        {
            Ok(output) => {
                let output_str = String::from_utf8_lossy(&output.stdout);
                if let Ok(units) = serde_json::from_str::<serde_json::Value>(&output_str) {
                    if let Some(units_array) = units.as_array() {
                        for unit in units_array {
                            let unit_name = unit["unit"].as_str().unwrap_or("unknown");
                            let load_state = unit["load"].as_str().unwrap_or("unknown");
                            let active_state = unit["active"].as_str().unwrap_or("unknown");
                            let sub_state = unit["sub"].as_str().unwrap_or("unknown");

                            let payload = serde_json::json!({
                                "unit_name": unit_name,
                                "load_state": load_state,
                                "active_state": active_state,
                                "sub_state": sub_state,
                                "discovery_method": "scanner",
                                "timestamp": chrono::Utc::now().to_rfc3339(),
                            });

                            let event = sinex_events::RawEventBuilder::new(
                                sinex_core::sources::SYSTEMD,
                                "unit.discovered",
                                payload,
                            )
                            .with_host("localhost")
                            .build();

                            if tx.send(event).is_ok() {
                                events_generated += 1;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                return Err(SatelliteError::EventSource(format!("Failed to list systemd units: {}", e)));
            }
        }

        Ok(events_generated)
    }

    /// Scan udev devices and generate discovery events
    async fn scan_udev_devices(
        &self,
        _args: &ScannerArgs,
        tx: &tokio::sync::mpsc::UnboundedSender<sinex_core::RawEvent>,
    ) -> SatelliteResult<u64> {
        let mut events_generated = 0u64;

        // For now, use sysfs traversal instead of libudev since it's disabled
        match std::fs::read_dir("/sys/class") {
            Ok(entries) => {
                for entry in entries.flatten() {
                    let class_name = entry.file_name().to_string_lossy().to_string();
                    
                    // Focus on interesting device classes
                    if !["net", "block", "input", "usb", "sound"].contains(&class_name.as_str()) {
                        continue;
                    }

                    if let Ok(class_entries) = std::fs::read_dir(entry.path()) {
                        for device_entry in class_entries.flatten() {
                            let device_name = device_entry.file_name().to_string_lossy().to_string();
                            let device_path = device_entry.path().to_string_lossy().to_string();

                            let payload = serde_json::json!({
                                "device_class": class_name,
                                "device_name": device_name,
                                "device_path": device_path,
                                "discovery_method": "scanner",
                                "timestamp": chrono::Utc::now().to_rfc3339(),
                            });

                            let event = sinex_events::RawEventBuilder::new(
                                sinex_core::sources::UDEV,
                                "device.discovered",
                                payload,
                            )
                            .with_host("localhost")
                            .build();

                            if tx.send(event).is_ok() {
                                events_generated += 1;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                return Err(SatelliteError::EventSource(format!("Failed to scan /sys/class: {}", e)));
            }
        }

        Ok(events_generated)
    }
}

impl Default for SystemSatellite {
    fn default() -> Self {
        Self::new()
    }
}