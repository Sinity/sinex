//! D-Bus watcher
//!
//! Monitors D-Bus signals using external dbus-monitor command

use serde_json::json;
use sinex_events::{EventFactory, RawEvent};
use sinex_satellite_sdk::SatelliteResult;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use sinex_events::constants::{sources};

/// D-Bus watcher
pub struct DbusWatcher {
    buses: String,
}

impl DbusWatcher {
    /// Create new D-Bus watcher
    pub async fn new(buses: String) -> SatelliteResult<Self> {
        info!("D-Bus watcher initialized for buses: {}", buses);
        Ok(Self { buses })
    }

    /// Parse dbus-monitor output and create events
    fn parse_dbus_monitor_line(&self, line: &str, bus_type: &str) -> Option<RawEvent> {
        // dbus-monitor output format varies, look for signal patterns
        if line.contains("signal") || line.contains("method call") {
            // Extract basic information from dbus-monitor output
            let payload = json!({
                "bus": bus_type,
                "raw_line": line.trim(),
                "timestamp": chrono::Utc::now().to_rfc3339(),
            });

            // Determine event type based on content
            let event_type = if line.contains("signal") {
                "signal.received"
            } else if line.contains("method call") {
                "method.called"
            } else {
                "message.received"
            };

            Some(
                {
                    let factory = EventFactory::new(sinex_events::sources::DBUS);
                    factory.create_event(event_type, payload)
                }
            )
        } else {
            None
        }
    }

    /// Monitor D-Bus session bus
    async fn monitor_session_bus(
        &self,
        tx: mpsc::UnboundedSender<RawEvent>,
    ) -> SatelliteResult<()> {
        info!("Starting D-Bus session bus monitoring via dbus-monitor");

        loop {
            let mut child = Command::new("dbus-monitor")
                .args(["--session"])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|e| {
                    sinex_satellite_sdk::SatelliteError::Processing(format!(
                        "Failed to start dbus-monitor: {}",
                        e
                    ))
                })?;

            let stdout = child.stdout.take().ok_or_else(|| {
                sinex_satellite_sdk::SatelliteError::Processing(
                    "Failed to get dbus-monitor stdout".to_string(),
                )
            })?;

            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();

            info!("Session bus monitoring started");

            // Read lines with timeout
            loop {
                match tokio::time::timeout(Duration::from_secs(60), lines.next_line()).await {
                    Ok(Ok(Some(line))) => {
                        if let Some(event) = self.parse_dbus_monitor_line(&line, "session") {
                            if tx.send(event).is_err() {
                                warn!("Event channel closed");
                                break;
                            }
                        }
                    }
                    Ok(Ok(None)) => {
                        warn!("D-Bus monitor session stream ended");
                        break;
                    }
                    Ok(Err(e)) => {
                        error!("Error reading D-Bus monitor session line: {}", e);
                        break;
                    }
                    Err(_) => {
                        // Timeout - this is normal for low activity periods
                        debug!("D-Bus monitor session timeout, continuing...");
                        continue;
                    }
                }
            }

            // Kill the child process if still running
            if let Err(e) = child.kill().await {
                warn!("Failed to kill dbus-monitor process: {}", e);
            }

            // Wait a bit before restarting
            tokio::time::sleep(Duration::from_secs(5)).await;
            info!("Restarting D-Bus session monitoring");
        }
    }

    /// Monitor D-Bus system bus
    async fn monitor_system_bus(&self, tx: mpsc::UnboundedSender<RawEvent>) -> SatelliteResult<()> {
        info!("Starting D-Bus system bus monitoring via dbus-monitor");

        loop {
            let mut child = Command::new("dbus-monitor")
                .args(["--system"])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|e| {
                    sinex_satellite_sdk::SatelliteError::Processing(format!(
                        "Failed to start dbus-monitor: {}",
                        e
                    ))
                })?;

            let stdout = child.stdout.take().ok_or_else(|| {
                sinex_satellite_sdk::SatelliteError::Processing(
                    "Failed to get dbus-monitor stdout".to_string(),
                )
            })?;

            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();

            info!("System bus monitoring started");

            // Read lines with timeout
            loop {
                match tokio::time::timeout(Duration::from_secs(60), lines.next_line()).await {
                    Ok(Ok(Some(line))) => {
                        if let Some(event) = self.parse_dbus_monitor_line(&line, "system") {
                            if tx.send(event).is_err() {
                                warn!("Event channel closed");
                                break;
                            }
                        }
                    }
                    Ok(Ok(None)) => {
                        warn!("D-Bus monitor system stream ended");
                        break;
                    }
                    Ok(Err(e)) => {
                        error!("Error reading D-Bus monitor system line: {}", e);
                        break;
                    }
                    Err(_) => {
                        // Timeout - this is normal for low activity periods
                        debug!("D-Bus monitor system timeout, continuing...");
                        continue;
                    }
                }
            }

            // Kill the child process if still running
            if let Err(e) = child.kill().await {
                warn!("Failed to kill dbus-monitor process: {}", e);
            }

            // Wait a bit before restarting
            tokio::time::sleep(Duration::from_secs(5)).await;
            info!("Restarting D-Bus system monitoring");
        }
    }

    /// Start streaming events
    pub async fn start_streaming(
        &mut self,
        tx: mpsc::UnboundedSender<RawEvent>,
    ) -> SatelliteResult<()> {
        info!("Starting D-Bus event streaming for buses: {}", self.buses);

        match self.buses.as_str() {
            "session" => self.monitor_session_bus(tx).await,
            "system" => self.monitor_system_bus(tx).await,
            "both" => {
                // Monitor both buses concurrently
                let tx_session = tx.clone();
                let tx_system = tx;

                let session_task = {
                    let buses = self.buses.clone();
                    tokio::spawn(async move {
                        let watcher = DbusWatcher { buses };
                        watcher.monitor_session_bus(tx_session).await
                    })
                };

                let system_task = {
                    let buses = self.buses.clone();
                    tokio::spawn(async move {
                        let watcher = DbusWatcher { buses };
                        watcher.monitor_system_bus(tx_system).await
                    })
                };

                // Wait for either task to complete
                tokio::select! {
                    result = session_task => {
                        match result {
                            Ok(r) => r,
                            Err(e) => Err(sinex_satellite_sdk::SatelliteError::Processing(format!("Session task failed: {}", e))),
                        }
                    }
                    result = system_task => {
                        match result {
                            Ok(r) => r,
                            Err(e) => Err(sinex_satellite_sdk::SatelliteError::Processing(format!("System task failed: {}", e))),
                        }
                    }
                }
            }
            _ => Err(sinex_satellite_sdk::SatelliteError::Processing(format!(
                "Invalid D-Bus buses configuration: {}",
                self.buses
            ))),
        }
    }
}
