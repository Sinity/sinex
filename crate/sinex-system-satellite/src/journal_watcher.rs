//! systemd Journal watcher
//!
//! Monitors systemd journal entries in real-time

use serde_json::json;
use sinex_core::RawEvent;
use sinex_events::RawEventBuilder;
use sinex_satellite_sdk::SatelliteResult;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

/// Journal watcher
pub struct JournalWatcher {
    timeout_secs: u64,
}

impl JournalWatcher {
    /// Create new journal watcher
    pub async fn new(timeout_secs: u64) -> SatelliteResult<Self> {
        let watcher = Self { timeout_secs };

        info!("Journal watcher initialized with {}s timeout", timeout_secs);
        Ok(watcher)
    }

    /// Parse journal JSON line into structured event
    fn parse_journal_entry(&self, line: &str) -> Option<RawEvent> {
        match serde_json::from_str::<serde_json::Value>(line) {
            Ok(entry) => {
                // Extract key fields from journal entry
                let timestamp_us = entry["__REALTIME_TIMESTAMP"]
                    .as_str()
                    .and_then(|ts| ts.parse::<i64>().ok())
                    .unwrap_or(0);

                let cursor = entry["__CURSOR"]
                    .as_str()
                    .unwrap_or("unknown")
                    .to_string();

                let message = entry["MESSAGE"]
                    .as_str()
                    .unwrap_or("")
                    .to_string();

                let unit = entry["_SYSTEMD_UNIT"]
                    .as_str()
                    .map(|s| s.to_string());

                let syslog_identifier = entry["SYSLOG_IDENTIFIER"]
                    .as_str()
                    .map(|s| s.to_string());

                let pid = entry["_PID"]
                    .as_str()
                    .and_then(|p| p.parse::<u32>().ok());

                let uid = entry["_UID"]
                    .as_str()
                    .and_then(|u| u.parse::<u32>().ok());

                let gid = entry["_GID"]
                    .as_str()
                    .and_then(|g| g.parse::<u32>().ok());

                let cmdline = entry["_CMDLINE"]
                    .as_str()
                    .map(|s| s.to_string());

                let exe = entry["_EXE"]
                    .as_str()
                    .map(|s| s.to_string());

                let priority = entry["PRIORITY"]
                    .as_str()
                    .and_then(|p| p.parse::<u8>().ok());

                let hostname = entry["_HOSTNAME"]
                    .as_str()
                    .map(|s| s.to_string());

                // Convert timestamp from microseconds to RFC3339
                let timestamp = if timestamp_us > 0 {
                    chrono::DateTime::from_timestamp_micros(timestamp_us)
                        .map(|dt| dt.to_rfc3339())
                        .unwrap_or_else(|| chrono::Utc::now().to_rfc3339())
                } else {
                    chrono::Utc::now().to_rfc3339()
                };

                let payload = json!({
                    "cursor": cursor,
                    "timestamp_us": timestamp_us,
                    "timestamp": timestamp,
                    "hostname": hostname,
                    "unit": unit,
                    "syslog_identifier": syslog_identifier,
                    "pid": pid,
                    "uid": uid,
                    "gid": gid,
                    "cmdline": cmdline,
                    "exe": exe,
                    "priority": priority,
                    "message": message,
                    "raw_entry": entry,
                });

                Some(RawEventBuilder::new(sinex_core::sources::JOURNALD, "entry.written", payload)
                    .with_host("localhost")
                    .build())
            }
            Err(e) => {
                debug!("Failed to parse journal entry: {}", e);
                None
            }
        }
    }

    /// Follow the journal and emit events
    async fn follow_journal(&self, tx: mpsc::UnboundedSender<RawEvent>) -> SatelliteResult<()> {
        info!("Starting journal following");

        loop {
            // Start journalctl process to follow journal entries
            let mut child = Command::new("journalctl")
                .args([
                    "--follow", 
                    "--output=json",
                    "--lines=0", // Start from now, don't dump existing entries
                    "--no-hostname"
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|e| {
                    sinex_satellite_sdk::SatelliteError::EventSource(format!("Failed to start journalctl: {}", e))
                })?;

            let stdout = child.stdout.take().ok_or_else(|| {
                sinex_satellite_sdk::SatelliteError::EventSource("Failed to get journalctl stdout".to_string())
            })?;

            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();

            info!("Journal following started");

            // Read lines with timeout
            loop {
                match timeout(Duration::from_secs(self.timeout_secs), lines.next_line()).await {
                    Ok(Ok(Some(line))) => {
                        if let Some(event) = self.parse_journal_entry(&line) {
                            if tx.send(event).is_err() {
                                warn!("Event channel closed");
                                break;
                            }
                        }
                    }
                    Ok(Ok(None)) => {
                        warn!("Journal stream ended unexpectedly");
                        break;
                    }
                    Ok(Err(e)) => {
                        error!("Error reading journal line: {}", e);
                        break;
                    }
                    Err(_) => {
                        // Timeout - this is normal, continue
                        debug!("Journal read timeout, continuing...");
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
            info!("Restarting journal following");
        }
    }

    /// Start streaming events
    pub async fn start_streaming(&mut self, tx: mpsc::UnboundedSender<RawEvent>) -> SatelliteResult<()> {
        info!("Starting journal event streaming");

        self.follow_journal(tx).await
    }
}