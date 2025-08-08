//! systemd watcher
//!
//! Monitors systemd services, timers, and unit state changes

use sinex_core::db::models::Event;

use sinex_satellite_sdk::SatelliteResult;
use sinex_core::types::events::{
    SystemdTimerTriggeredPayload, SystemdUnitFailedPayload, SystemdUnitReloadedPayload,
    SystemdUnitStartedPayload, SystemdUnitStartingPayload, SystemdUnitStateChangedPayload,
    SystemdUnitStatusPayload, SystemdUnitStoppedPayload, SystemdUnitStoppingPayload,
};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

/// systemd watcher configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SystemdConfig {
    /// Monitor service state changes
    pub monitor_services: bool,
    /// Monitor timer state changes
    pub monitor_timers: bool,
    /// Monitor all unit types
    pub monitor_all_units: bool,
    /// systemctl monitor timeout in seconds
    pub monitor_timeout_secs: u64,
}

impl Default for SystemdConfig {
    fn default() -> Self {
        Self {
            monitor_services: true,
            monitor_timers: true,
            monitor_all_units: false, // Start conservative
            monitor_timeout_secs: 5,
        }
    }
}

/// systemd watcher
pub struct SystemdWatcher {
    config: SystemdConfig,
}

impl SystemdWatcher {
    /// Create new systemd watcher
    pub async fn new(config: SystemdConfig) -> SatelliteResult<Self> {
        let watcher = Self { config };

        info!("systemd watcher initialized");
        Ok(watcher)
    }

    /// Parse systemd unit status line
    fn parse_unit_status(&self, line: &str) -> Option<Event> {
        // Example systemd monitor output format:
        // "● service.service - Description"
        // "  Active: active (running) since ..."
        // "  Process: 1234 ExecStart=/usr/bin/service (code=exited, status=0/SUCCESS)"

        if line.trim().is_empty() {
            return None;
        }

        // Look for unit status lines that start with ●
        if line.starts_with("● ") {
            let parts: Vec<&str> = line[2..].splitn(2, " - ").collect();
            if parts.len() >= 2 {
                let unit_name = parts[0].trim();
                let description = parts[1].trim();

                // Determine unit type
                let unit_type = if unit_name.ends_with(".service") {
                    "service"
                } else if unit_name.ends_with(".timer") {
                    "timer"
                } else if unit_name.ends_with(".socket") {
                    "socket"
                } else if unit_name.ends_with(".target") {
                    "target"
                } else if unit_name.ends_with(".mount") {
                    "mount"
                } else {
                    "other"
                };

                return Some(Event::from_payload(SystemdUnitStatusPayload {
                    unit_name: unit_name.to_string(),
                    unit_type: unit_type.to_string(),
                    description: description.to_string(),
                    action: "status_check".to_string(),
                    timestamp: chrono::Utc::now().to_rfc3339(),
                }));
            }
        }

        // Look for Active: lines
        if line.trim().starts_with("Active: ") {
            let status_part = line.trim().strip_prefix("Active: ").unwrap_or("");
            let status = status_part.split(' ').next().unwrap_or("unknown");

            return match status {
                "active" => Some(Event::from_payload(SystemdUnitStartedPayload {
                    unit_name: "unknown".to_string(), // Will be filled by journal monitoring
                    unit_type: "unknown".to_string(),
                    main_pid: None,
                    active_state: status.to_string(),
                    sub_state: status_part.to_string(),
                })),
                "inactive" => Some(Event::from_payload(SystemdUnitStoppedPayload {
                    unit_name: "unknown".to_string(),
                    unit_type: "unknown".to_string(),
                    exit_code: None,
                    active_state: status.to_string(),
                    sub_state: status_part.to_string(),
                })),
                "failed" => Some(Event::from_payload(SystemdUnitFailedPayload {
                    unit_name: "unknown".to_string(),
                    message: status_part.to_string(),
                    cursor: "unknown".to_string(),
                    pid: None,
                    uid: None,
                    timestamp: chrono::Utc::now().to_rfc3339(),
                    journal_timestamp: None,
                })),
                "activating" => Some(Event::from_payload(SystemdUnitStartingPayload {
                    status: status.to_string(),
                    status_detail: status_part.to_string(),
                    timestamp: chrono::Utc::now().to_rfc3339(),
                })),
                "deactivating" => Some(Event::from_payload(SystemdUnitStoppingPayload {
                    status: status.to_string(),
                    status_detail: status_part.to_string(),
                    timestamp: chrono::Utc::now().to_rfc3339(),
                })),
                _ => Some(Event::from_payload(SystemdUnitStateChangedPayload {
                    status: status.to_string(),
                    status_detail: status_part.to_string(),
                    timestamp: chrono::Utc::now().to_rfc3339(),
                })),
            };
        }

        None
    }

    /// Get current systemd unit status
    async fn get_unit_status(&self, tx: &mpsc::UnboundedSender<Event>) -> SatelliteResult<()> {
        info!("Checking systemd unit status");

        let mut args = vec!["status"];

        // Add filters based on configuration
        if self.config.monitor_services && !self.config.monitor_all_units {
            args.push("--type=service");
        } else if self.config.monitor_timers && !self.config.monitor_all_units {
            args.push("--type=timer");
        }

        args.extend_from_slice(&["--no-pager", "--full", "--lines=0"]);

        let mut child = Command::new("systemctl")
            .args(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                sinex_satellite_sdk::SatelliteError::Processing(format!(
                    "Failed to start systemctl: {}",
                    e
                ))
            })?;

        let stdout = child.stdout.take().ok_or_else(|| {
            sinex_satellite_sdk::SatelliteError::Processing(
                "Failed to get systemctl stdout".to_string(),
            )
        })?;

        let reader = BufReader::new(stdout);
        let mut lines = reader.lines();

        // Read lines with timeout
        while let Ok(Ok(Some(line))) = timeout(
            Duration::from_secs(self.config.monitor_timeout_secs),
            lines.next_line(),
        )
        .await
        {
            if let Some(event) = self.parse_unit_status(&line) {
                if tx.send(event).is_err() {
                    warn!("Event channel closed");
                    break;
                }
            }
        }

        // Kill the child process if it's still running
        if let Err(e) = child.kill().await {
            warn!("Failed to kill systemctl process: {}", e);
        }

        Ok(())
    }

    /// Monitor systemd journal for unit state changes
    async fn monitor_systemd_journal(
        &self,
        tx: mpsc::UnboundedSender<Event>,
    ) -> SatelliteResult<()> {
        info!("Starting systemd journal monitoring for unit changes");

        loop {
            // Start journalctl to follow systemd messages
            let mut child = Command::new("journalctl")
                .args([
                    "--follow",
                    "--output=json",
                    "--lines=0",
                    "_SYSTEMD_UNIT=*", // Filter for systemd unit messages
                    "--no-hostname",
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|e| {
                    sinex_satellite_sdk::SatelliteError::Processing(format!(
                        "Failed to start journalctl: {}",
                        e
                    ))
                })?;

            let stdout = child.stdout.take().ok_or_else(|| {
                sinex_satellite_sdk::SatelliteError::Processing(
                    "Failed to get journalctl stdout".to_string(),
                )
            })?;

            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();

            info!("systemd journal monitoring started");

            // Read lines with timeout
            loop {
                match timeout(
                    Duration::from_secs(self.config.monitor_timeout_secs),
                    lines.next_line(),
                )
                .await
                {
                    Ok(Ok(Some(line))) => {
                        if let Some(event) = self.parse_systemd_journal_entry(&line) {
                            if tx.send(event).is_err() {
                                warn!("Event channel closed");
                                break;
                            }
                        }
                    }
                    Ok(Ok(None)) => {
                        warn!("systemd journal stream ended unexpectedly");
                        break;
                    }
                    Ok(Err(e)) => {
                        error!("Error reading systemd journal line: {}", e);
                        break;
                    }
                    Err(_) => {
                        // Timeout - this is normal, continue
                        debug!("systemd journal read timeout, continuing...");
                        continue;
                    }
                }
            }

            // Kill the child process if it's still running
            if let Err(e) = child.kill().await {
                warn!("Failed to kill journalctl process: {}", e);
            }

            // Wait a bit before restarting
            tokio::time::sleep(Duration::from_secs(5)).await;
            info!("Restarting systemd journal monitoring");
        }
    }

    /// Parse systemd journal entry for unit state changes
    fn parse_systemd_journal_entry(&self, line: &str) -> Option<Event> {
        match serde_json::from_str::<serde_json::Value>(line) {
            Ok(entry) => {
                let message = entry["MESSAGE"].as_str().unwrap_or("");
                let unit_name = entry["_SYSTEMD_UNIT"].as_str();
                let cursor = entry["__CURSOR"].as_str().unwrap_or("unknown");

                // Look for systemd state change messages
                if message.contains("Started ") {
                    let unit_type = unit_name
                        .map(|n| {
                            if n.ends_with(".service") {
                                "service"
                            } else if n.ends_with(".timer") {
                                "timer"
                            } else if n.ends_with(".socket") {
                                "socket"
                            } else {
                                "unknown"
                            }
                        })
                        .unwrap_or("unknown");

                    Some(Event::from_payload(SystemdUnitStartedPayload {
                        unit_name: unit_name.unwrap_or("unknown").to_string(),
                        unit_type: unit_type.to_string(),
                        main_pid: entry["_PID"].as_str().and_then(|s| s.parse().ok()),
                        active_state: "active".to_string(),
                        sub_state: "running".to_string(),
                    }))
                } else if message.contains("Stopped ") {
                    let unit_type = unit_name
                        .map(|n| {
                            if n.ends_with(".service") {
                                "service"
                            } else if n.ends_with(".timer") {
                                "timer"
                            } else if n.ends_with(".socket") {
                                "socket"
                            } else {
                                "unknown"
                            }
                        })
                        .unwrap_or("unknown");

                    Some(Event::from_payload(SystemdUnitStoppedPayload {
                        unit_name: unit_name.unwrap_or("unknown").to_string(),
                        unit_type: unit_type.to_string(),
                        exit_code: None,
                        active_state: "inactive".to_string(),
                        sub_state: "dead".to_string(),
                    }))
                } else if message.contains("Failed ") {
                    Some(Event::from_payload(SystemdUnitFailedPayload {
                        unit_name: unit_name.unwrap_or("unknown").to_string(),
                        message: message.to_string(),
                        cursor: cursor.to_string(),
                        pid: entry["_PID"].as_str().map(String::from),
                        uid: entry["_UID"].as_str().map(String::from),
                        timestamp: chrono::Utc::now().to_rfc3339(),
                        journal_timestamp: entry["__REALTIME_TIMESTAMP"].as_str().map(String::from),
                    }))
                } else if message.contains("Reloaded ") {
                    Some(Event::from_payload(SystemdUnitReloadedPayload {
                        unit_name: unit_name.map(String::from),
                        message: message.to_string(),
                        cursor: cursor.to_string(),
                        pid: entry["_PID"].as_str().map(String::from),
                        uid: entry["_UID"].as_str().map(String::from),
                        timestamp: chrono::Utc::now().to_rfc3339(),
                        journal_timestamp: entry["__REALTIME_TIMESTAMP"].as_str().map(String::from),
                    }))
                } else if message.contains("Triggered ") {
                    Some(Event::from_payload(SystemdTimerTriggeredPayload {
                        unit_name: unit_name.map(String::from),
                        message: message.to_string(),
                        cursor: cursor.to_string(),
                        pid: entry["_PID"].as_str().map(String::from),
                        uid: entry["_UID"].as_str().map(String::from),
                        timestamp: chrono::Utc::now().to_rfc3339(),
                        journal_timestamp: entry["__REALTIME_TIMESTAMP"].as_str().map(String::from),
                    }))
                } else {
                    None // Not a state change we care about
                }
            }
            Err(e) => {
                debug!("Failed to parse systemd journal entry: {}", e);
                None
            }
        }
    }

    /// Start streaming events
    pub async fn start_streaming(
        &mut self,
        tx: mpsc::UnboundedSender<Event>,
    ) -> SatelliteResult<()> {
        info!("Starting systemd event streaming");

        // Start with a status check to capture current state
        if let Err(e) = self.get_unit_status(&tx).await {
            warn!("Failed to get initial systemd status: {}", e);
        }

        // Then monitor journal for ongoing changes
        self.monitor_systemd_journal(tx).await
    }
}
