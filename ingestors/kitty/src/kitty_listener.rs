use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sinex_shared::{
    agent_events::*, create_error_event, create_heartbeat_event, event_types::{self, RawEventBuilder}, sources,
    AgentMetrics, AgentStatus, DatabaseService, DlqManager, ErrorSeverity,
    RetryConfig, retry_db_operation,
};
use sinex_db::models::RawEvent;
use std::collections::HashMap;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error, info, warn};

use crate::config::KittyConfig;

/// Command execution event payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandExecutedPayload {
    pub command_string: String,
    pub cwd: String,
    pub exit_code: i32,
    pub ts_start_orig: DateTime<Utc>,
    pub ts_end_orig: DateTime<Utc>,
}

/// Kitty window information
#[derive(Debug, Clone)]
struct KittyWindow {
    id: u32,
    #[allow(dead_code)]
    pid: u32,
    cwd: String,
    #[allow(dead_code)]
    title: String,
}

/// Kitty terminal listener
pub struct KittyListener {
    config: KittyConfig,
    db: Arc<DatabaseService>,
    dlq: Arc<DlqManager>,
    metrics: Arc<Mutex<AgentMetrics>>,
    retry_config: RetryConfig,
    last_command_times: Arc<Mutex<HashMap<u32, DateTime<Utc>>>>,
}

impl KittyListener {
    pub fn new(config: KittyConfig, db: Arc<DatabaseService>) -> Result<Self> {
        let dlq = Arc::new(DlqManager::new("kitty-ingestor")?);
        let metrics = Arc::new(Mutex::new(AgentMetrics::new(
            "kitty-ingestor",
            env!("CARGO_PKG_VERSION"),
        )));
        
        let retry_config = RetryConfig {
            max_retries: config.max_retries,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(config.retry_delay_secs),
            exponential_base: 2,
        };

        Ok(Self {
            config,
            db,
            dlq,
            metrics,
            retry_config,
            last_command_times: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Start the Kitty listener
    pub async fn start(self) -> Result<()> {
        info!("Starting Kitty terminal listener");

        let (event_tx, mut event_rx) = mpsc::channel(1000);
        
        // Spawn database writer task
        let db = Arc::clone(&self.db);
        let dlq = Arc::clone(&self.dlq);
        let retry_config = self.retry_config.clone();
        let metrics = Arc::clone(&self.metrics);
        
        let db_writer = tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                Self::process_event_with_retry(
                    &db,
                    &dlq,
                    &retry_config,
                    &metrics,
                    event,
                )
                .await;
            }
        });

        // Spawn heartbeat task
        let heartbeat_tx = event_tx.clone();
        let metrics_clone = Arc::clone(&self.metrics);
        let heartbeat_interval = self.config.heartbeat_interval_secs;
        
        let heartbeat_task = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(heartbeat_interval));
            loop {
                interval.tick().await;
                let heartbeat = {
                    let metrics = metrics_clone.lock().unwrap();
                    metrics.create_heartbeat(AgentStatus::Running)
                };
                let event = create_heartbeat_event(heartbeat);
                if heartbeat_tx.send(event).await.is_err() {
                    break;
                }
            }
        });

        // Spawn command polling task
        let command_tx = event_tx.clone();
        let socket_path = self.config.socket_path.clone();
        let polling_interval = self.config.polling_interval_secs;
        let last_command_times = Arc::clone(&self.last_command_times);
        
        let polling_task = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(polling_interval));
            loop {
                interval.tick().await;
                
                if let Err(e) = Self::poll_kitty_commands(&command_tx, &socket_path, &last_command_times).await {
                    error!("Error polling Kitty commands: {}", e);
                    
                    // Send error event
                    let error = AgentError {
                        agent_name: "kitty-ingestor".to_string(),
                        error_message: format!("Failed to poll Kitty commands: {}", e),
                        error_context: "command_polling".to_string(),
                        severity: ErrorSeverity::Warning,
                        original_event_id_if_related: None,
                    };
                    let _ = command_tx.send(create_error_event(error)).await;
                }
            }
        });

        // Wait for tasks
        tokio::try_join!(db_writer, heartbeat_task, polling_task)?;

        Ok(())
    }

    /// Poll Kitty for new commands
    async fn poll_kitty_commands(
        tx: &mpsc::Sender<RawEvent>,
        socket_pattern: &str,
        last_command_times: &Arc<Mutex<HashMap<u32, DateTime<Utc>>>>,
    ) -> Result<()> {
        // Find Kitty sockets
        let sockets = Self::find_kitty_sockets(socket_pattern)?;
        
        if sockets.is_empty() {
            debug!("No Kitty sockets found");
            return Ok(());
        }

        for socket in sockets {
            // Get list of windows
            let windows = Self::get_kitty_windows(&socket)?;
            
            for window in windows {
                // Get command history for this window
                if let Ok(commands) = Self::get_window_commands(&socket, window.id) {
                    let now = Utc::now();
                    let last_time = {
                        let times = last_command_times.lock().unwrap();
                        times.get(&window.id).cloned().unwrap_or(now - chrono::Duration::hours(1))
                    };

                    for cmd in &commands {
                        if cmd.ts_end_orig > last_time {
                            let payload = CommandExecutedPayload {
                                command_string: cmd.command_string.clone(),
                                cwd: window.cwd.clone(),
                                exit_code: cmd.exit_code,
                                ts_start_orig: cmd.ts_start_orig,
                                ts_end_orig: cmd.ts_end_orig,
                            };

                            let event = RawEventBuilder::new(
                                sources::TERMINAL_KITTY,
                                event_types::event_types::terminal::COMMAND_EXECUTED,
                                serde_json::to_value(payload)?,
                            )
                            .with_orig_timestamp(cmd.ts_end_orig)
                            .build();

                            tx.send(event).await?;
                        }
                    }

                    // Update last command time
                    if let Some(last_cmd) = commands.last() {
                        let mut times = last_command_times.lock().unwrap();
                        times.insert(window.id, last_cmd.ts_end_orig);
                    }
                }
            }
        }

        Ok(())
    }

    /// Find Kitty sockets matching the pattern
    fn find_kitty_sockets(pattern: &str) -> Result<Vec<String>> {
        let output = Command::new("ls")
            .arg("-1")
            .arg(pattern)
            .output()
            .context("Failed to list Kitty sockets")?;

        if !output.status.success() {
            return Ok(Vec::new());
        }

        let sockets = String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(|s| s.to_string())
            .collect();

        Ok(sockets)
    }

    /// Get list of windows from Kitty
    fn get_kitty_windows(socket: &str) -> Result<Vec<KittyWindow>> {
        let output = Command::new("kitty")
            .arg("@")
            .arg("--to")
            .arg(format!("unix:{}", socket))
            .arg("ls")
            .output()
            .context("Failed to list Kitty windows")?;

        if !output.status.success() {
            return Err(anyhow::anyhow!("Failed to get Kitty windows"));
        }

        // Parse JSON output
        let data: serde_json::Value = serde_json::from_slice(&output.stdout)
            .context("Failed to parse Kitty ls output")?;

        let mut windows = Vec::new();
        
        if let Some(os_windows) = data.as_array() {
            for os_window in os_windows {
                if let Some(tabs) = os_window["tabs"].as_array() {
                    for tab in tabs {
                        if let Some(wins) = tab["windows"].as_array() {
                            for win in wins {
                                if let (Some(id), Some(pid), Some(cwd), Some(title)) = (
                                    win["id"].as_u64(),
                                    win["pid"].as_u64(),
                                    win["cwd"].as_str(),
                                    win["title"].as_str(),
                                ) {
                                    windows.push(KittyWindow {
                                        id: id as u32,
                                        pid: pid as u32,
                                        cwd: cwd.to_string(),
                                        title: title.to_string(),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(windows)
    }

    /// Get command history for a window (mock implementation)
    fn get_window_commands(_socket: &str, _window_id: u32) -> Result<Vec<CommandExecutedPayload>> {
        // Note: Kitty doesn't directly expose command history via remote control
        // This is a simplified implementation that could be enhanced with:
        // 1. Shell integration markers
        // 2. Terminal scrollback parsing
        // 3. Integration with shell history files
        
        // For now, we'll use a placeholder that could be extended
        warn!("Command history extraction from Kitty is limited - consider shell integration");
        
        // In a real implementation, you might:
        // 1. Use Kitty's scrollback_lines command to get recent output
        // 2. Parse shell prompt patterns
        // 3. Integrate with shell history files based on PID
        
        Ok(Vec::new())
    }

    /// Process event with retry logic
    async fn process_event_with_retry(
        db: &Arc<DatabaseService>,
        dlq: &Arc<DlqManager>,
        retry_config: &RetryConfig,
        metrics: &Arc<Mutex<AgentMetrics>>,
        event: RawEvent,
    ) {
        let result = retry_db_operation(retry_config, || async {
            db.insert_event(&event).await.map_err(|e| e.into())
        })
        .await;

        match result {
            Ok(_) => {
                metrics.lock().unwrap().increment_processed();
                debug!("Successfully inserted event: {} {}", event.source, event.event_type);
            }
            Err(e) => {
                error!("Failed to insert event after retries: {}", e);
                
                // Write to DLQ
                match dlq.write_event(event.clone(), e.to_string(), retry_config.max_retries).await {
                    Ok(dlq_path) => {
                        metrics.lock().unwrap().increment_dlq();
                        
                        // Try to emit DLQ notification
                        let dlq_event = dlq.create_dlq_notification(&event, dlq_path, e.to_string());
                        
                        if let Err(e2) = db.insert_event(&dlq_event).await {
                            // Critical failure - can't even write DLQ notifications
                            let _ = dlq.log_critical_failure(&format!(
                                "Failed to emit DLQ notification: {} (original error: {})",
                                e2, e
                            ));
                        }
                    }
                    Err(dlq_err) => {
                        // Can't even write to DLQ
                        let _ = dlq.log_critical_failure(&format!(
                            "Failed to write to DLQ: {} (original error: {})",
                            dlq_err, e
                        ));
                    }
                }
            }
        }
    }
}