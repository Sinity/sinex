use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sinex_shared::{RawEventBuilder, sources};
use sinex_db::models::RawEvent;
use std::collections::{HashMap, VecDeque};
use std::env;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error, info};

use crate::config::{HyprlandConfig, WindowAugmentation, WorkspaceTracking};

/// Hyprland instance info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyprlandInstance {
    pub instance: String,
    pub time: i64,
    pub pid: i32,
    pub wl_socket: String,
}

/// Cache entry for hyprctl results
struct CacheEntry {
    data: Value,
    timestamp: Instant,
}

/// Focus history entry
#[derive(Debug, Clone)]
struct FocusHistoryEntry {
    timestamp: DateTime<Utc>,
    window_address: String,
    window_data: Option<Value>,
}

/// Hyprland ingestor that watches for window manager events
pub struct HyprlandIngestor {
    config: HyprlandConfig,
    socket_path: PathBuf,
    hyprctl_cache: Arc<Mutex<HashMap<String, CacheEntry>>>,
    focus_history: Arc<Mutex<VecDeque<FocusHistoryEntry>>>,
    last_descriptions_emit: Arc<Mutex<DateTime<Utc>>>,
    last_config_reload: Arc<Mutex<DateTime<Utc>>>,
}

impl HyprlandIngestor {
    pub fn new(config: HyprlandConfig) -> Result<Self> {
        // Get Hyprland instance signature
        let hyprland_instance_sig = env::var("HYPRLAND_INSTANCE_SIGNATURE")
            .context("HYPRLAND_INSTANCE_SIGNATURE not set. Is Hyprland running?")?;
        
        // Build socket path
        let xdg_runtime = env::var("XDG_RUNTIME_DIR")
            .context("XDG_RUNTIME_DIR not set")?;
        let socket_path = PathBuf::from(xdg_runtime)
            .join("hypr")
            .join(&hyprland_instance_sig)
            .join(".socket2.sock");

        Ok(Self {
            config,
            socket_path,
            hyprctl_cache: Arc::new(Mutex::new(HashMap::new())),
            focus_history: Arc::new(Mutex::new(VecDeque::new())),
            last_descriptions_emit: Arc::new(Mutex::new(Utc::now() - chrono::Duration::hours(5))),
            last_config_reload: Arc::new(Mutex::new(Utc::now())),
        })
    }

    /// Watch Hyprland events and send them through the provided channel
    pub async fn watch(&mut self, event_tx: mpsc::Sender<RawEvent>) -> Result<()> {
        info!(
            socket_path = %self.socket_path.display(),
            window_augmentation = ?self.config.window_augmentation,
            workspace_tracking = ?self.config.workspace_tracking,
            state_snapshot_interval_secs = self.config.state_snapshot_interval_secs,
            "Starting Hyprland event watcher with socket2 capture"
        );

        // Spawn state snapshot task
        let snapshot_handle = self.spawn_snapshot_task(event_tx.clone());

        // Spawn cache cleanup task
        let cache_cleanup_handle = self.spawn_cache_cleanup_task();

        // Start socket listener
        let socket_result = self.listen_socket_events(event_tx).await;

        // Cancel background tasks
        snapshot_handle.abort();
        cache_cleanup_handle.abort();

        socket_result
    }

    /// Listen to socket2 events
    async fn listen_socket_events(&self, event_tx: mpsc::Sender<RawEvent>) -> Result<()> {
        loop {
            match UnixStream::connect(&self.socket_path).await {
                Ok(stream) => {
                    info!("Connected to Hyprland socket2");
                    if let Err(e) = self.process_event_stream(stream, &event_tx).await {
                        error!("Event stream processing error: {}", e);
                        // Reconnect after a short delay
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
                Err(e) => {
                    error!("Failed to connect to Hyprland socket: {}", e);
                    // Retry connection after delay
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    /// Process the event stream from socket2
    async fn process_event_stream(&self, stream: UnixStream, event_tx: &mpsc::Sender<RawEvent>) -> Result<()> {
        let reader = BufReader::new(stream);
        let mut lines = reader.lines();

        while let Some(line) = lines.next_line().await? {
            // socket2 format: event_name>>event_data
            if let Some((event_name, event_data)) = line.split_once(">>") {
                debug!("Raw event: {} >> {}", event_name, event_data);

                // Skip ignored events
                if self.config.ignore_events.contains(&event_name.to_string()) {
                    continue;
                }

                // Create event payload
                let payload = self.create_event_payload(event_name, event_data).await?;

                // Build raw event
                let raw_event = RawEventBuilder::new(
                    sources::HYPRLAND,
                    event_name,
                    payload,
                )
                .with_orig_timestamp(Utc::now())
                .build();

                // Send event
                event_tx.send(raw_event).await?;

                // Update focus history if applicable
                if event_name == "activewindow" || event_name == "activewindowv2" {
                    self.update_focus_history(&line);
                }
            }
        }

        Ok(())
    }

    /// Create event payload with optional augmentation
    async fn create_event_payload(&self, event_name: &str, event_data: &str) -> Result<Value> {
        let mut payload = self.parse_event_data(event_name, event_data);

        // Add augmentation based on event type
        match event_name {
            "activewindow" | "activewindowv2" | "openwindow" | "movewindow" | "movewindowv2" => {
                if self.config.window_augmentation != WindowAugmentation::None {
                    self.augment_window_event(&mut payload).await;
                }
            }
            "workspace" | "workspacev2" | "createworkspace" | "createworkspacev2" => {
                if self.config.workspace_tracking != WorkspaceTracking::Events {
                    self.augment_workspace_event(&mut payload).await;
                }
            }
            _ => {}
        }

        Ok(payload)
    }

    /// Parse event data based on event type
    fn parse_event_data(&self, event_name: &str, event_data: &str) -> Value {
        match event_name {
            // Window events with comma-separated data
            "activewindow" => {
                let parts: Vec<&str> = event_data.splitn(2, ',').collect();
                json!({
                    "window_class": parts.get(0).unwrap_or(&"").to_string(),
                    "window_title": parts.get(1).unwrap_or(&"").to_string(),
                })
            }
            "activewindowv2" => {
                json!({
                    "window_address": event_data.to_string(),
                })
            }
            "openwindow" => {
                let parts: Vec<&str> = event_data.splitn(4, ',').collect();
                json!({
                    "window_address": parts.get(0).unwrap_or(&"").to_string(),
                    "workspace_id": parts.get(1).unwrap_or(&"").to_string(),
                    "window_class": parts.get(2).unwrap_or(&"").to_string(),
                    "window_title": parts.get(3).unwrap_or(&"").to_string(),
                })
            }
            // Workspace events
            "workspace" | "createworkspace" | "destroyworkspace" => {
                json!({
                    "workspace_name": event_data.to_string(),
                })
            }
            "workspacev2" | "createworkspacev2" | "destroyworkspacev2" => {
                let parts: Vec<&str> = event_data.splitn(2, ',').collect();
                json!({
                    "workspace_id": parts.get(0).unwrap_or(&"").to_string(),
                    "workspace_name": parts.get(1).unwrap_or(&"").to_string(),
                })
            }
            // Default: just pass the raw data
            _ => {
                json!({
                    "raw_data": event_data.to_string(),
                })
            }
        }
    }

    /// Augment window event with additional data
    async fn augment_window_event(&self, payload: &mut Value) {
        // Implementation would fetch additional window data from hyprctl
        // This is simplified for now
        if self.config.window_augmentation == WindowAugmentation::Full {
            if let Some(address) = payload.get("window_address").and_then(|v| v.as_str()) {
                if let Ok(window_data) = self.get_window_data(address).await {
                    payload["augmented_data"] = window_data;
                }
            }
        }
    }

    /// Augment workspace event with additional data
    async fn augment_workspace_event(&self, payload: &mut Value) {
        // Implementation would fetch additional workspace data from hyprctl
        // This is simplified for now
        if self.config.workspace_tracking == WorkspaceTracking::WithState {
            if let Ok(workspace_data) = self.get_workspace_data().await {
                payload["augmented_data"] = workspace_data;
            }
        }
    }

    /// Get window data from hyprctl (cached)
    async fn get_window_data(&self, address: &str) -> Result<Value> {
        self.get_hyprctl_data("clients", Some(address)).await
    }

    /// Get workspace data from hyprctl (cached)
    async fn get_workspace_data(&self) -> Result<Value> {
        self.get_hyprctl_data("workspaces", None).await
    }

    /// Get data from hyprctl with caching
    async fn get_hyprctl_data(&self, command: &str, filter: Option<&str>) -> Result<Value> {
        let cache_key = format!("{}:{}", command, filter.unwrap_or(""));
        
        // Check cache
        {
            let cache = self.hyprctl_cache.lock().unwrap();
            if let Some(entry) = cache.get(&cache_key) {
                if entry.timestamp.elapsed() < Duration::from_secs(5) {
                    return Ok(entry.data.clone());
                }
            }
        }

        // Execute hyprctl
        let output = Command::new("hyprctl")
            .arg(command)
            .arg("-j")
            .output()
            .context("Failed to execute hyprctl")?;

        if !output.status.success() {
            return Err(anyhow::anyhow!("hyprctl failed: {}", String::from_utf8_lossy(&output.stderr)));
        }

        let data: Value = serde_json::from_slice(&output.stdout)
            .context("Failed to parse hyprctl output")?;

        // Update cache
        {
            let mut cache = self.hyprctl_cache.lock().unwrap();
            cache.insert(cache_key, CacheEntry {
                data: data.clone(),
                timestamp: Instant::now(),
            });
        }

        Ok(data)
    }

    /// Update focus history
    fn update_focus_history(&self, event_line: &str) {
        if !self.config.track_focus_history {
            return;
        }

        if let Some((_, event_data)) = event_line.split_once(">>") {
            let window_address = if event_line.starts_with("activewindowv2") {
                event_data.to_string()
            } else {
                // Extract from other formats if needed
                String::new()
            };

            if !window_address.is_empty() {
                let mut history = self.focus_history.lock().unwrap();
                history.push_front(FocusHistoryEntry {
                    timestamp: Utc::now(),
                    window_address,
                    window_data: None,
                });

                // Keep only last 100 entries
                if history.len() > 100 {
                    history.pop_back();
                }
            }
        }
    }

    /// Spawn state snapshot task
    fn spawn_snapshot_task(&self, event_tx: mpsc::Sender<RawEvent>) -> tokio::task::JoinHandle<()> {
        let interval_secs = self.config.state_snapshot_interval_secs;
        let config = self.config.clone();
        
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(interval_secs));
            
            loop {
                interval.tick().await;
                
                // Create state snapshot event
                let snapshot = match Self::create_state_snapshot(&config).await {
                    Ok(data) => data,
                    Err(e) => {
                        error!("Failed to create state snapshot: {}", e);
                        continue;
                    }
                };

                let event = RawEventBuilder::new(
                    sources::HYPRLAND,
                    "state_snapshot",
                    snapshot,
                )
                .build();

                if event_tx.send(event).await.is_err() {
                    break;
                }
            }
        })
    }

    /// Create a state snapshot
    async fn create_state_snapshot(_config: &HyprlandConfig) -> Result<Value> {
        // Execute hyprctl commands to get full state
        let mut snapshot = json!({
            "timestamp": Utc::now(),
            "type": "state_snapshot"
        });

        // Get monitors
        if let Ok(output) = Command::new("hyprctl").arg("monitors").arg("-j").output() {
            if output.status.success() {
                if let Ok(monitors) = serde_json::from_slice::<Value>(&output.stdout) {
                    snapshot["monitors"] = monitors;
                }
            }
        }

        // Get workspaces
        if let Ok(output) = Command::new("hyprctl").arg("workspaces").arg("-j").output() {
            if output.status.success() {
                if let Ok(workspaces) = serde_json::from_slice::<Value>(&output.stdout) {
                    snapshot["workspaces"] = workspaces;
                }
            }
        }

        // Get clients
        if let Ok(output) = Command::new("hyprctl").arg("clients").arg("-j").output() {
            if output.status.success() {
                if let Ok(clients) = serde_json::from_slice::<Value>(&output.stdout) {
                    snapshot["clients"] = clients;
                }
            }
        }

        Ok(snapshot)
    }

    /// Spawn cache cleanup task
    fn spawn_cache_cleanup_task(&self) -> tokio::task::JoinHandle<()> {
        let cache = Arc::clone(&self.hyprctl_cache);
        
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(60));
            
            loop {
                interval.tick().await;
                
                let mut cache_guard = cache.lock().unwrap();
                cache_guard.retain(|_, entry| {
                    entry.timestamp.elapsed() < Duration::from_secs(30)
                });
            }
        })
    }
}

/// Create a startup event
pub fn create_startup_event(agent_name: &str, version: &str) -> RawEvent {
    RawEventBuilder::new(
        sources::SINEX,
        "agent.startup",
        json!({
            "agent_name": agent_name,
            "version": version,
            "timestamp": Utc::now(),
        }),
    )
    .build()
}

/// Create a shutdown event
pub fn create_shutdown_event(agent_name: &str, reason: &str) -> RawEvent {
    RawEventBuilder::new(
        sources::SINEX,
        "agent.shutdown",
        json!({
            "agent_name": agent_name,
            "reason": reason,
            "timestamp": Utc::now(),
        }),
    )
    .build()
}

// SimpleIngestor implementation for use with IngestorRuntime
use async_trait::async_trait;
use sinex_shared::SimpleIngestor;

#[async_trait]
impl SimpleIngestor for HyprlandIngestor {
    fn name() -> &'static str {
        "hyprland-ingestor"
    }
    
    fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
    
    async fn capture_events(&mut self, event_tx: mpsc::Sender<RawEvent>) -> Result<()> {
        // Send startup event
        let startup_event = create_startup_event(Self::name(), Self::version());
        event_tx.send(startup_event).await?;
        
        // Run the watcher
        let result = self.watch(event_tx.clone()).await;
        
        // Send shutdown event
        let shutdown_reason = match &result {
            Ok(_) => "normal".to_string(),
            Err(e) => format!("error: {}", e),
        };
        let shutdown_event = create_shutdown_event(Self::name(), &shutdown_reason);
        let _ = event_tx.send(shutdown_event).await;
        
        result
    }
}