#![doc = include_str!("../docs/systemd_watcher.md")]

//! systemd watcher module.

use crate::WatcherMaterialContext;
use sinex_core::types::utils::wait_helpers::retry_with_exponential_backoff;
use sinex_core::types::Seconds;
use sinex_core::{Event, JsonValue};

use sinex_core::types::events::{
    SystemdTimerTriggeredPayload, SystemdUnitFailedPayload, SystemdUnitReloadedPayload,
    SystemdUnitStartedPayload, SystemdUnitStartingPayload, SystemdUnitStateChangedPayload,
    SystemdUnitStatusPayload, SystemdUnitStoppedPayload, SystemdUnitStoppingPayload,
};
use sinex_node_sdk::NodeResult;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

/// SystemD unit types
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SystemdUnitType {
    Service,
    Timer,
    Socket,
    Target,
    Mount,
    Other,
}

impl SystemdUnitType {
    /// Determine unit type from unit name
    pub fn from_unit_name(unit_name: &str) -> Self {
        if unit_name.ends_with(".service") {
            Self::Service
        } else if unit_name.ends_with(".timer") {
            Self::Timer
        } else if unit_name.ends_with(".socket") {
            Self::Socket
        } else if unit_name.ends_with(".target") {
            Self::Target
        } else if unit_name.ends_with(".mount") {
            Self::Mount
        } else {
            Self::Other
        }
    }
}

impl std::fmt::Display for SystemdUnitType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Service => write!(f, "service"),
            Self::Timer => write!(f, "timer"),
            Self::Socket => write!(f, "socket"),
            Self::Target => write!(f, "target"),
            Self::Mount => write!(f, "mount"),
            Self::Other => write!(f, "other"),
        }
    }
}

/// SystemD unit states
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SystemdUnitState {
    Active,
    Inactive,
    Failed,
    Activating,
    Deactivating,
    Unknown,
}

impl SystemdUnitState {
    /// Parse unit state from systemctl output
    pub fn from_str(s: &str) -> Self {
        match s {
            "active" => Self::Active,
            "inactive" => Self::Inactive,
            "failed" => Self::Failed,
            "activating" => Self::Activating,
            "deactivating" => Self::Deactivating,
            _ => Self::Unknown,
        }
    }
}

impl std::fmt::Display for SystemdUnitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Inactive => write!(f, "inactive"),
            Self::Failed => write!(f, "failed"),
            Self::Activating => write!(f, "activating"),
            Self::Deactivating => write!(f, "deactivating"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

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
    pub monitor_timeout_secs: Seconds,
}

impl Default for SystemdConfig {
    fn default() -> Self {
        Self {
            monitor_services: true,
            monitor_timers: true,
            monitor_all_units: false, // Start conservative
            monitor_timeout_secs: Seconds::from_secs(5),
        }
    }
}

/// systemd watcher
pub struct SystemdWatcher {
    config: SystemdConfig,
}

impl SystemdWatcher {
    /// Create new systemd watcher
    pub async fn new(config: SystemdConfig) -> NodeResult<Self> {
        let watcher = Self { config };

        info!("systemd watcher initialized");
        Ok(watcher)
    }

    /// Parse systemd unit status line
    fn parse_unit_status(
        &self,
        line: &str,
        material: &WatcherMaterialContext,
        current_unit: &mut Option<(String, SystemdUnitType)>,
    ) -> Option<Event<JsonValue>> {
        // Example systemd monitor output format:
        // "● service.service - Description"
        // "  Active: active (running) since ..."
        // "  Process: 1234 ExecStart=/usr/bin/service (code=exited, status=0/SUCCESS)"

        if line.trim().is_empty() {
            return None;
        }

        // Look for unit status lines that start with ●
        if let Some(rest) = line.strip_prefix("● ") {
            let parts: Vec<&str> = rest.splitn(2, " - ").collect();
            if parts.len() >= 2 {
                let unit_name = parts[0].trim();
                let description = parts[1].trim();

                // Determine unit type
                let unit_type = SystemdUnitType::from_unit_name(unit_name);

                // Update state
                *current_unit = Some((unit_name.to_string(), unit_type));

                return Some(
                    Event::new(
                        SystemdUnitStatusPayload {
                            unit_name: unit_name.to_string(),
                            unit_type: unit_type.to_string(),
                            description: description.to_string(),
                            action: "status_check".to_string(),
                            timestamp: chrono::Utc::now().to_rfc3339(),
                        },
                        material.initial_provenance(),
                    )
                    .to_json_event()
                    .ok()?,
                );
            }
        }

        // Look for Active: lines
        if line.trim().starts_with("Active: ") {
            let status_part = line.trim().strip_prefix("Active: ").unwrap_or("");
            let status_str = status_part.split(' ').next().unwrap_or("unknown");
            let status = SystemdUnitState::from_str(status_str);

            let (unit_name, unit_type_str) = current_unit
                .as_ref()
                .map(|(n, t)| (n.clone(), t.to_string()))
                .unwrap_or_else(|| ("unknown".to_string(), "unknown".to_string()));

            return match status {
                SystemdUnitState::Active => Some(
                    Event::new(
                        SystemdUnitStartedPayload {
                            unit_name,
                            unit_type: unit_type_str,
                            main_pid: None,
                            active_state: status.to_string(),
                            sub_state: status_part.to_string(),
                        },
                        material.initial_provenance(),
                    )
                    .to_json_event()
                    .ok()?,
                ),
                SystemdUnitState::Inactive => Some(
                    Event::new(
                        SystemdUnitStoppedPayload {
                            unit_name,
                            unit_type: unit_type_str,
                            exit_code: None,
                            active_state: status.to_string(),
                            sub_state: status_part.to_string(),
                        },
                        material.initial_provenance(),
                    )
                    .to_json_event()
                    .ok()?,
                ),
                SystemdUnitState::Failed => Some(
                    Event::new(
                        SystemdUnitFailedPayload {
                            unit_name,
                            message: status_part.to_string(),
                            cursor: "unknown".to_string(),
                            pid: None,
                            uid: None,
                            timestamp: chrono::Utc::now().to_rfc3339(),
                            journal_timestamp: None,
                        },
                        material.initial_provenance(),
                    )
                    .to_json_event()
                    .ok()?,
                ),
                SystemdUnitState::Activating => Some(
                    Event::new(
                        SystemdUnitStartingPayload {
                            status: status.to_string(),
                            status_detail: status_part.to_string(),
                            timestamp: chrono::Utc::now().to_rfc3339(),
                        },
                        material.initial_provenance(),
                    )
                    .to_json_event()
                    .ok()?,
                ),
                SystemdUnitState::Deactivating => Some(
                    Event::new(
                        SystemdUnitStoppingPayload {
                            status: status.to_string(),
                            status_detail: status_part.to_string(),
                            timestamp: chrono::Utc::now().to_rfc3339(),
                        },
                        material.initial_provenance(),
                    )
                    .to_json_event()
                    .ok()?,
                ),
                SystemdUnitState::Unknown => Some(
                    Event::new(
                        SystemdUnitStateChangedPayload {
                            status: status.to_string(),
                            status_detail: status_part.to_string(),
                            timestamp: chrono::Utc::now().to_rfc3339(),
                        },
                        material.initial_provenance(),
                    )
                    .to_json_event()
                    .ok()?,
                ),
            };
        }

        None
    }

    /// Get current systemd unit status
    async fn get_unit_status(
        &self,
        tx: &mpsc::Sender<Event<JsonValue>>,
        material: &WatcherMaterialContext,
    ) -> NodeResult<()> {
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
                sinex_node_sdk::NodeError::Processing(format!("Failed to start systemctl: {}", e))
            })?;

        let stdout = child.stdout.take().ok_or_else(|| {
            sinex_node_sdk::NodeError::Processing("Failed to get systemctl stdout".to_string())
        })?;

        let reader = BufReader::new(stdout);
        let mut lines = reader.lines();
        let mut current_unit = None;

        // Read lines with timeout
        while let Ok(Ok(Some(line))) = timeout(
            Duration::from_secs(self.config.monitor_timeout_secs.as_secs()),
            lines.next_line(),
        )
        .await
        {
            if let Some(event) = self.parse_unit_status(&line, material, &mut current_unit) {
                if let Err(err) = Self::send_event(tx, event, "systemd_status", material).await {
                    warn!("Failed to stage systemd status event: {}", err);
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
        tx: mpsc::Sender<Event<JsonValue>>,
        material: &WatcherMaterialContext,
    ) -> NodeResult<()> {
        info!("Starting systemd journal monitoring for unit changes");

        const SYSTEMD_FOLLOW_CHANNEL_CAPACITY: usize = 1024;
        let (bounded_tx, mut bounded_rx) =
            mpsc::channel::<Event<JsonValue>>(SYSTEMD_FOLLOW_CHANNEL_CAPACITY);

        // Forwarder to the original sender keeps memory bounded while avoiding unbounded buffering.
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            while let Some(event) = bounded_rx.recv().await {
                if let Err(err) = tx_clone.send(event).await {
                    warn!("Systemd watcher: event channel closed: {}", err);
                    break;
                }
            }
        });

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
                    sinex_node_sdk::NodeError::Processing(format!(
                        "Failed to start journalctl: {}",
                        e
                    ))
                })?;

            let stdout = child.stdout.take().ok_or_else(|| {
                sinex_node_sdk::NodeError::Processing("Failed to get journalctl stdout".to_string())
            })?;

            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();

            info!("systemd journal monitoring started");

            // Read lines with timeout
            loop {
                match timeout(
                    Duration::from_secs(self.config.monitor_timeout_secs.as_secs()),
                    lines.next_line(),
                )
                .await
                {
                    Ok(Ok(Some(line))) => {
                        if let Some(mut event) = self.parse_systemd_journal_entry(&line, material) {
                            if let Err(err) = material.decorate_event(&mut event).await {
                                warn!("Failed to stage systemd journal event: {}", err);
                                continue;
                            }
                            if let Err(mpsc::error::TrySendError::Full(_)) =
                                bounded_tx.try_send(event)
                            {
                                debug!("Systemd watcher channel full; dropping oldest event");
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

            // Use exponential backoff for reconnection
            let retry_result = retry_with_exponential_backoff(
                "systemd_journal_restart",
                Duration::from_secs(1),
                5,    // Max 5 retries
                true, // With jitter
                || async {
                    // Just a delay before retry
                    Ok::<(), &str>(())
                },
            )
            .await;

            if let Err(e) = retry_result {
                error!("Failed to restart systemd monitoring after retries: {}", e);
            }

            info!("Restarting systemd journal monitoring");
        }
    }

    async fn send_event(
        tx: &mpsc::Sender<Event<JsonValue>>,
        mut event: Event<JsonValue>,
        context: &str,
        material: &WatcherMaterialContext,
    ) -> NodeResult<()> {
        material.decorate_event(&mut event).await?;
        if let Err(err) = tx.send(event).await {
            warn!("Event channel closed while sending {}: {}", context, err);
        }
        Ok(())
    }

    /// Parse systemd journal entry for unit state changes
    fn parse_systemd_journal_entry(
        &self,
        line: &str,
        material: &WatcherMaterialContext,
    ) -> Option<Event<JsonValue>> {
        match serde_json::from_str::<serde_json::Value>(line) {
            Ok(entry) => {
                let message = entry["MESSAGE"].as_str().unwrap_or("");
                let unit_name = entry["_SYSTEMD_UNIT"].as_str();
                let cursor = entry["__CURSOR"].as_str().unwrap_or("unknown");

                // Look for systemd state change messages
                if message.contains("Started ") {
                    let unit_type = unit_name
                        .map(SystemdUnitType::from_unit_name)
                        .unwrap_or(SystemdUnitType::Other);

                    Some(
                        Event::new(
                            SystemdUnitStartedPayload {
                                unit_name: unit_name.unwrap_or("unknown").to_string(),
                                unit_type: unit_type.to_string(),
                                main_pid: entry["_PID"].as_str().and_then(|s| s.parse().ok()),
                                active_state: SystemdUnitState::Active.to_string(),
                                sub_state: "running".to_string(),
                            },
                            material.initial_provenance(),
                        )
                        .to_json_event()
                        .ok()?,
                    )
                } else if message.contains("Stopped ") {
                    let unit_type = unit_name
                        .map(SystemdUnitType::from_unit_name)
                        .unwrap_or(SystemdUnitType::Other);

                    Some(
                        Event::new(
                            SystemdUnitStoppedPayload {
                                unit_name: unit_name.unwrap_or("unknown").to_string(),
                                unit_type: unit_type.to_string(),
                                exit_code: None,
                                active_state: SystemdUnitState::Inactive.to_string(),
                                sub_state: "dead".to_string(),
                            },
                            material.initial_provenance(),
                        )
                        .to_json_event()
                        .ok()?,
                    )
                } else if message.contains("Failed ") {
                    Some(
                        Event::new(
                            SystemdUnitFailedPayload {
                                unit_name: unit_name.unwrap_or("unknown").to_string(),
                                message: message.to_string(),
                                cursor: cursor.to_string(),
                                pid: entry["_PID"].as_str().map(String::from),
                                uid: entry["_UID"].as_str().map(String::from),
                                timestamp: chrono::Utc::now().to_rfc3339(),
                                journal_timestamp: entry["__REALTIME_TIMESTAMP"]
                                    .as_str()
                                    .map(String::from),
                            },
                            material.initial_provenance(),
                        )
                        .to_json_event()
                        .ok()?,
                    )
                } else if message.contains("Reloaded ") {
                    Some(
                        Event::new(
                            SystemdUnitReloadedPayload {
                                unit_name: unit_name.map(String::from),
                                message: message.to_string(),
                                cursor: cursor.to_string(),
                                pid: entry["_PID"].as_str().map(String::from),
                                uid: entry["_UID"].as_str().map(String::from),
                                timestamp: chrono::Utc::now().to_rfc3339(),
                                journal_timestamp: entry["__REALTIME_TIMESTAMP"]
                                    .as_str()
                                    .map(String::from),
                            },
                            material.initial_provenance(),
                        )
                        .to_json_event()
                        .ok()?,
                    )
                } else if message.contains("Triggered ") {
                    Some(
                        Event::new(
                            SystemdTimerTriggeredPayload {
                                unit_name: unit_name.map(String::from),
                                message: message.to_string(),
                                cursor: cursor.to_string(),
                                pid: entry["_PID"].as_str().map(String::from),
                                uid: entry["_UID"].as_str().map(String::from),
                                timestamp: chrono::Utc::now().to_rfc3339(),
                                journal_timestamp: entry["__REALTIME_TIMESTAMP"]
                                    .as_str()
                                    .map(String::from),
                            },
                            material.initial_provenance(),
                        )
                        .to_json_event()
                        .ok()?,
                    )
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
    pub(crate) async fn start_streaming(
        &mut self,
        tx: mpsc::Sender<Event<JsonValue>>,
        material: WatcherMaterialContext,
    ) -> NodeResult<()> {
        info!("Starting systemd event streaming");

        // Start with a status check to capture current state
        if let Err(e) = self.get_unit_status(&tx, &material).await {
            warn!("Failed to get initial systemd status: {}", e);
        }

        // Then monitor journal for ongoing changes
        self.monitor_systemd_journal(tx, &material).await
    }
}
