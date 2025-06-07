use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::env;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixStream;
use tokio::time;
use tracing::{debug, error, info, warn};

use crate::config::{HyprlandConfig, WindowAugmentation, WorkspaceTracking};
use crate::error::{IngestorError, Result as IngestorResult};

use sinex_shared::{
    create_heartbeat_event, event_types::RawEventBuilder, sources,
    AgentMetrics, AgentStatus, DatabaseService, DlqManager,
    RetryConfig, retry_db_operation,
};
use sinex_db::models::RawEvent;

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

/// Hyprland event listener using socket2
pub struct HyprlandEventListener {
    config: HyprlandConfig,
    db: Arc<DatabaseService>,
    dlq: Arc<DlqManager>,
    metrics: Arc<Mutex<AgentMetrics>>,
    retry_config: RetryConfig,
    _hyprland_instance_sig: String,
    socket_path: PathBuf,
    hyprctl_cache: Arc<Mutex<HashMap<String, CacheEntry>>>,
    focus_history: Arc<Mutex<VecDeque<FocusHistoryEntry>>>,
    last_descriptions_emit: Arc<Mutex<DateTime<Utc>>>,
    last_config_reload: Arc<Mutex<DateTime<Utc>>>,
}

impl HyprlandEventListener {
    pub fn new(config: HyprlandConfig, db: Arc<DatabaseService>) -> IngestorResult<Self> {
        let dlq = Arc::new(DlqManager::new("hyprland-ingestor")?);
        let metrics = Arc::new(Mutex::new(AgentMetrics::new(
            "hyprland-ingestor",
            env!("CARGO_PKG_VERSION"),
        )));
        
        let retry_config = RetryConfig {
            max_retries: config.max_retries,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(config.retry_delay_secs),
            exponential_base: 2,
        };

        // Get Hyprland instance signature
        let hyprland_instance_sig = env::var("HYPRLAND_INSTANCE_SIGNATURE")
            .map_err(|_| IngestorError::application("HYPRLAND_INSTANCE_SIGNATURE not set. Is Hyprland running?"))?;
        
        // Build socket path
        let xdg_runtime = env::var("XDG_RUNTIME_DIR")
            .map_err(|_| IngestorError::application("XDG_RUNTIME_DIR not set"))?;
        let socket_path = PathBuf::from(xdg_runtime)
            .join("hypr")
            .join(&hyprland_instance_sig)
            .join(".socket2.sock");

        Ok(Self {
            config,
            db,
            dlq,
            metrics,
            retry_config,
            _hyprland_instance_sig: hyprland_instance_sig,
            socket_path,
            hyprctl_cache: Arc::new(Mutex::new(HashMap::new())),
            focus_history: Arc::new(Mutex::new(VecDeque::new())),
            last_descriptions_emit: Arc::new(Mutex::new(Utc::now() - chrono::Duration::hours(5))),
            last_config_reload: Arc::new(Mutex::new(Utc::now())),
        })
    }

    /// Start the event listener
    pub async fn start(self) -> IngestorResult<()> {
        info!(
            agent_name = "hyprland-ingestor",
            version = env!("CARGO_PKG_VERSION"),
            socket_path = %self.socket_path.display(),
            window_augmentation = ?self.config.window_augmentation,
            workspace_tracking = ?self.config.workspace_tracking,
            heartbeat_interval_secs = self.config.heartbeat_interval_secs,
            state_snapshot_interval_secs = self.config.state_snapshot_interval_secs,
            "Starting Hyprland event listener with socket2 capture"
        );

        // Emit startup event
        let startup_event = create_startup_event("hyprland-ingestor", env!("CARGO_PKG_VERSION"));
        self.insert_event(startup_event).await;

        // Spawn heartbeat task
        let heartbeat_handle = self.spawn_heartbeat_task();

        // Spawn state snapshot task
        let snapshot_handle = self.spawn_snapshot_task();

        // Spawn cache cleanup task
        let cache_cleanup_handle = self.spawn_cache_cleanup_task();

        // Start socket listener
        let socket_result = self.listen_socket_events().await;

        // Emit shutdown event
        let shutdown_reason = match &socket_result {
            Ok(_) => "normal".to_string(),
            Err(e) => format!("error: {}", e),
        };
        let shutdown_event = create_shutdown_event(
            "hyprland-ingestor",
            &shutdown_reason,
        );
        let _ = self.insert_event(shutdown_event).await;

        // Cancel background tasks
        heartbeat_handle.abort();
        snapshot_handle.abort();
        cache_cleanup_handle.abort();

        socket_result
    }

    /// Spawn heartbeat task
    fn spawn_heartbeat_task(&self) -> tokio::task::JoinHandle<()> {
        let db = Arc::clone(&self.db);
        let metrics = Arc::clone(&self.metrics);
        let heartbeat_interval = self.config.heartbeat_interval_secs;
        
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(heartbeat_interval));
            loop {
                interval.tick().await;
                let heartbeat = {
                    let metrics = metrics.lock().unwrap();
                    metrics.create_heartbeat(AgentStatus::Running)
                };
                let event = create_heartbeat_event(heartbeat);
                let _ = db.insert_event(&event).await;
            }
        })
    }

    /// Spawn state snapshot task
    fn spawn_snapshot_task(&self) -> tokio::task::JoinHandle<()> {
        let db = Arc::clone(&self.db);
        let snapshot_interval = self.config.state_snapshot_interval_secs;
        let descriptions_interval_hours = self.config.descriptions_interval_hours;
        let last_descriptions = Arc::clone(&self.last_descriptions_emit);
        
        tokio::spawn(async move {
            // Take initial snapshot with descriptions
            if let Err(e) = Self::capture_and_insert_snapshot(&db, &last_descriptions, true).await {
                error!("Failed to capture initial state snapshot: {}", e);
            }

            let mut interval = time::interval(Duration::from_secs(snapshot_interval));
            loop {
                interval.tick().await;
                
                // Check if we should include descriptions
                let should_include_descriptions = {
                    let last = last_descriptions.lock().unwrap();
                    Utc::now() - *last > chrono::Duration::hours(descriptions_interval_hours as i64)
                };
                
                if let Err(e) = Self::capture_and_insert_snapshot(&db, &last_descriptions, should_include_descriptions).await {
                    error!("Failed to capture state snapshot: {}", e);
                }
            }
        })
    }

    /// Spawn cache cleanup task
    fn spawn_cache_cleanup_task(&self) -> tokio::task::JoinHandle<()> {
        let cache = Arc::clone(&self.hyprctl_cache);
        let cache_ms = self.config.hyprctl_cache_ms;
        
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(cache_ms * 2));
            loop {
                interval.tick().await;
                let mut cache_lock = cache.lock().unwrap();
                let now = Instant::now();
                cache_lock.retain(|_, entry| {
                    now.duration_since(entry.timestamp).as_millis() < cache_ms as u128
                });
            }
        })
    }

    /// Listen to socket2 events
    async fn listen_socket_events(&self) -> IngestorResult<()> {
        loop {
            match UnixStream::connect(&self.socket_path).await {
                Ok(stream) => {
                    info!("Connected to Hyprland socket2");
                    if let Err(e) = self.process_socket_stream(stream).await {
                        error!("Socket stream error: {}", e);
                    }
                }
                Err(e) => {
                    error!("Failed to connect to socket2: {}", e);
                }
            }
            
            // Retry after a delay
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    /// Process events from the socket stream
    async fn process_socket_stream(&self, stream: UnixStream) -> Result<()> {
        let reader = BufReader::new(stream);
        let mut lines = reader.lines();

        while let Some(line) = lines.next_line().await? {
            if let Some((event_type, data)) = line.split_once(">>") {
                // Check if event should be ignored
                if self.config.ignore_events.contains(&event_type.to_string()) {
                    debug!("Ignoring event type: {}", event_type);
                    continue;
                }

                debug!(
                    event_type = %event_type,
                    data = %data,
                    "Received Hyprland event"
                );
                
                match self.process_hyprland_event(event_type, data).await {
                    Ok(Some(event)) => {
                        info!(
                            event_type = %event_type,
                            event_id = %event.id,
                            "Processed Hyprland event"
                        );
                        self.insert_event(event).await;
                    }
                    Ok(None) => {
                        debug!(
                            event_type = %event_type,
                            "Event filtered out"
                        );
                    }
                    Err(e) => {
                        warn!(
                            event_type = %event_type,
                            error = %e,
                            "Failed to process event"
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Process a single Hyprland event
    async fn process_hyprland_event(&self, event_type: &str, data: &str) -> Result<Option<RawEvent>> {
        let ts = Utc::now();
        
        // Parse event data based on event type
        let mut payload = self.parse_event_data(event_type, data)?;

        // Apply augmentation based on configuration
        match event_type {
            "activewindow" | "activewindowv2" => {
                if self.config.window_augmentation != WindowAugmentation::None {
                    self.augment_window_event(event_type, &mut payload).await?;
                }
                
                // Track focus history if enabled
                if self.config.track_focus_history {
                    self.update_focus_history(event_type, &payload, ts).await;
                }
            }
            "openwindow" | "closewindow" => {
                if self.config.window_augmentation >= WindowAugmentation::Detailed {
                    self.augment_window_lifecycle_event(event_type, &mut payload).await?;
                }
            }
            "workspace" | "workspacev2" => {
                if self.config.workspace_tracking != WorkspaceTracking::Events {
                    self.augment_workspace_event(event_type, &mut payload).await?;
                }
            }
            "configreloaded" => {
                *self.last_config_reload.lock().unwrap() = ts;
                
                // Capture rolling log if configured
                if self.config.rolling_log_on_reload {
                    if let Ok(log) = Self::get_rolling_log(0) {
                        payload["rolling_log"] = log;
                    }
                }
                
                // Also capture descriptions on reload
                *self.last_descriptions_emit.lock().unwrap() = ts - chrono::Duration::hours(5);
            }
            _ => {
                // Other events don't need augmentation
            }
        }

        Ok(Some(RawEventBuilder::new(
            sources::HYPRLAND,
            event_type,
            payload,
        )
        .with_orig_timestamp(ts)
        .build()))
    }

    /// Parse event data into JSON
    fn parse_event_data(&self, event_type: &str, data: &str) -> Result<Value> {
        Ok(match event_type {
            // Workspace events
            "workspace" => json!({ "workspace_name": data }),
            "workspacev2" => {
                let parts: Vec<&str> = data.splitn(2, ',').collect();
                json!({
                    "workspace_id": parts.get(0).unwrap_or(&""),
                    "workspace_name": parts.get(1).unwrap_or(&""),
                })
            }
            "createworkspace" | "destroyworkspace" => json!({ "workspace_name": data }),
            "createworkspacev2" | "destroyworkspacev2" => {
                let parts: Vec<&str> = data.splitn(2, ',').collect();
                json!({
                    "workspace_id": parts.get(0).unwrap_or(&""),
                    "workspace_name": parts.get(1).unwrap_or(&""),
                })
            }
            "moveworkspace" => {
                let parts: Vec<&str> = data.splitn(2, ',').collect();
                json!({
                    "workspace_name": parts.get(0).unwrap_or(&""),
                    "monitor_name": parts.get(1).unwrap_or(&""),
                })
            }
            "moveworkspacev2" => {
                let parts: Vec<&str> = data.splitn(3, ',').collect();
                json!({
                    "workspace_id": parts.get(0).unwrap_or(&""),
                    "workspace_name": parts.get(1).unwrap_or(&""),
                    "monitor_name": parts.get(2).unwrap_or(&""),
                })
            }
            "renameworkspace" => {
                let parts: Vec<&str> = data.splitn(2, ',').collect();
                json!({
                    "workspace_id": parts.get(0).unwrap_or(&""),
                    "new_name": parts.get(1).unwrap_or(&""),
                })
            }
            
            // Monitor events
            "focusedmon" => {
                let parts: Vec<&str> = data.splitn(2, ',').collect();
                json!({
                    "monitor_name": parts.get(0).unwrap_or(&""),
                    "workspace_name": parts.get(1).unwrap_or(&""),
                })
            }
            "focusedmonv2" => {
                let parts: Vec<&str> = data.splitn(2, ',').collect();
                json!({
                    "monitor_name": parts.get(0).unwrap_or(&""),
                    "workspace_id": parts.get(1).unwrap_or(&""),
                })
            }
            "monitoradded" | "monitorremoved" => json!({ "monitor_name": data }),
            "monitoraddedv2" | "monitorremovedv2" => {
                let parts: Vec<&str> = data.splitn(3, ',').collect();
                json!({
                    "monitor_id": parts.get(0).unwrap_or(&""),
                    "monitor_name": parts.get(1).unwrap_or(&""),
                    "monitor_description": parts.get(2).unwrap_or(&""),
                })
            }
            
            // Window events
            "activewindow" => {
                let parts: Vec<&str> = data.splitn(2, ',').collect();
                json!({
                    "window_class": parts.get(0).unwrap_or(&""),
                    "window_title": parts.get(1).unwrap_or(&""),
                })
            }
            "activewindowv2" => json!({ "window_address": data }),
            "openwindow" => {
                let parts: Vec<&str> = data.splitn(4, ',').collect();
                json!({
                    "window_address": parts.get(0).unwrap_or(&""),
                    "workspace_name": parts.get(1).unwrap_or(&""),
                    "window_class": parts.get(2).unwrap_or(&""),
                    "window_title": parts.get(3).unwrap_or(&""),
                })
            }
            "closewindow" => json!({ "window_address": data }),
            "movewindow" => {
                let parts: Vec<&str> = data.splitn(2, ',').collect();
                json!({
                    "window_address": parts.get(0).unwrap_or(&""),
                    "workspace_name": parts.get(1).unwrap_or(&""),
                })
            }
            "movewindowv2" => {
                let parts: Vec<&str> = data.splitn(3, ',').collect();
                json!({
                    "window_address": parts.get(0).unwrap_or(&""),
                    "workspace_id": parts.get(1).unwrap_or(&""),
                    "workspace_name": parts.get(2).unwrap_or(&""),
                })
            }
            "windowtitle" => json!({ "window_address": data }),
            "windowtitlev2" => {
                let parts: Vec<&str> = data.splitn(2, ',').collect();
                json!({
                    "window_address": parts.get(0).unwrap_or(&""),
                    "window_title": parts.get(1).unwrap_or(&""),
                })
            }
            "fullscreen" => json!({ "fullscreen": data == "1" }),
            "changefloatingmode" => {
                let parts: Vec<&str> = data.splitn(2, ',').collect();
                json!({
                    "window_address": parts.get(0).unwrap_or(&""),
                    "floating": parts.get(1).unwrap_or(&"") == &"1",
                })
            }
            "urgent" => json!({ "window_address": data }),
            "minimized" => {
                let parts: Vec<&str> = data.splitn(2, ',').collect();
                json!({
                    "window_address": parts.get(0).unwrap_or(&""),
                    "minimized": parts.get(1).unwrap_or(&"") == &"1",
                })
            }
            "pin" => {
                let parts: Vec<&str> = data.splitn(2, ',').collect();
                json!({
                    "window_address": parts.get(0).unwrap_or(&""),
                    "pinned": parts.get(1).unwrap_or(&"") == &"1",
                })
            }
            
            // Group events
            "togglegroup" => {
                let parts: Vec<&str> = data.splitn(2, ',').collect();
                let state = parts.get(0).unwrap_or(&"");
                let addresses: Vec<&str> = parts.get(1).unwrap_or(&"").split(',').collect();
                json!({
                    "group_created": state == &"1",
                    "window_addresses": addresses,
                })
            }
            "moveintogroup" | "moveoutofgroup" => json!({ "window_address": data }),
            "ignoregrouplock" | "lockgroups" => json!({ "enabled": data == "1" }),
            
            // Layer events
            "openlayer" | "closelayer" => json!({ "namespace": data }),
            
            // System events
            "activelayout" => {
                let parts: Vec<&str> = data.splitn(2, ',').collect();
                json!({
                    "keyboard_name": parts.get(0).unwrap_or(&""),
                    "layout_name": parts.get(1).unwrap_or(&""),
                })
            }
            "submap" => json!({ "submap_name": data }),
            "screencast" => {
                let parts: Vec<&str> = data.splitn(2, ',').collect();
                json!({
                    "state": parts.get(0).unwrap_or(&"") == &"1",
                    "owner": match *parts.get(1).unwrap_or(&"") {
                        "0" => "monitor",
                        "1" => "window",
                        _ => "unknown",
                    },
                })
            }
            "configreloaded" => json!({}),
            "bell" => json!({
                "window_address": if data.is_empty() { None } else { Some(data) },
            }),
            
            // Special workspace events
            "activespecial" => {
                let parts: Vec<&str> = data.splitn(2, ',').collect();
                json!({
                    "workspace_name": parts.get(0).unwrap_or(&""),
                    "monitor_name": parts.get(1).unwrap_or(&""),
                })
            }
            "activespecialv2" => {
                let parts: Vec<&str> = data.splitn(3, ',').collect();
                json!({
                    "workspace_id": parts.get(0).unwrap_or(&""),
                    "workspace_name": parts.get(1).unwrap_or(&""),
                    "monitor_name": parts.get(2).unwrap_or(&""),
                })
            }
            
            _ => json!({ "raw_data": data }),
        })
    }

    /// Augment window events with additional data
    async fn augment_window_event(&self, event_type: &str, payload: &mut Value) -> Result<()> {
        match event_type {
            "activewindow" => {
                if self.config.window_augmentation >= WindowAugmentation::Basic {
                    if let Ok(details) = self.get_cached_hyprctl("activewindow").await {
                        payload["window_details"] = details;
                    }
                }
            }
            "activewindowv2" => {
                let window_address = payload["window_address"].as_str().unwrap_or("");
                
                if self.config.window_augmentation >= WindowAugmentation::Basic && !window_address.is_empty() {
                    // Get window details from clients list
                    if let Ok(clients) = self.get_cached_hyprctl("clients").await {
                        if let Some(client) = clients.as_array()
                            .and_then(|arr| arr.iter().find(|c| c["address"] == window_address))
                        {
                            payload["window_info"] = client.clone();
                        }
                    }
                }
                
                // Include focus history if Full augmentation
                if self.config.window_augmentation >= WindowAugmentation::Full {
                    let history = self.focus_history.lock().unwrap();
                    let history_data: Vec<Value> = history.iter()
                        .take(self.config.focus_history_depth)
                        .map(|entry| json!({
                            "timestamp": entry.timestamp,
                            "window_address": entry.window_address,
                            "window_data": entry.window_data,
                        }))
                        .collect();
                    payload["focus_history"] = Value::Array(history_data);
                }
            }
            _ => {}
        }
        
        Ok(())
    }

    /// Augment window lifecycle events
    async fn augment_window_lifecycle_event(&self, event_type: &str, payload: &mut Value) -> Result<()> {
        match event_type {
            "openwindow" => {
                // Capture workspace state when window opens
                if let Some(workspace_name) = payload["workspace_name"].as_str() {
                    if let Ok(workspaces) = self.get_cached_hyprctl("workspaces").await {
                        if let Some(workspace) = workspaces.as_array()
                            .and_then(|arr| arr.iter().find(|w| w["name"] == workspace_name))
                        {
                            payload["workspace_state"] = workspace.clone();
                        }
                    }
                }
            }
            "closewindow" => {
                // Try to get final window state before it's gone
                let window_address = payload["window_address"].as_str().unwrap_or("");
                if !window_address.is_empty() {
                    if let Ok(clients) = self.get_cached_hyprctl("clients").await {
                        if let Some(client) = clients.as_array()
                            .and_then(|arr| arr.iter().find(|c| c["address"] == window_address))
                        {
                            payload["final_state"] = client.clone();
                        }
                    }
                }
            }
            _ => {}
        }
        
        Ok(())
    }

    /// Augment workspace events
    async fn augment_workspace_event(&self, _event_type: &str, payload: &mut Value) -> Result<()> {
        let workspace_id = payload["workspace_id"].as_str()
            .or_else(|| payload["workspace_name"].as_str())
            .map(|s| s.to_string());
        
        if let Some(ws_id) = workspace_id {
            match self.config.workspace_tracking {
                WorkspaceTracking::WithWindows => {
                    // Get window list for the workspace
                    if let Ok(clients) = self.get_cached_hyprctl("clients").await {
                        let windows: Vec<&Value> = clients.as_array()
                            .map(|arr| arr.iter()
                                .filter(|c| c["workspace"]["name"] == ws_id.as_str() || c["workspace"]["id"].to_string() == ws_id)
                                .collect())
                            .unwrap_or_default();
                        
                        let window_summary: Vec<Value> = windows.iter()
                            .map(|w| json!({
                                "address": w["address"],
                                "class": w["class"],
                                "title": w["title"],
                            }))
                            .collect();
                        
                        payload["windows"] = Value::Array(window_summary);
                    }
                }
                WorkspaceTracking::WithState => {
                    // Get full workspace state
                    if let Ok(workspaces) = self.get_cached_hyprctl("workspaces").await {
                        if let Some(workspace) = workspaces.as_array()
                            .and_then(|arr| arr.iter().find(|w| w["name"] == ws_id.as_str() || w["id"].to_string() == ws_id))
                        {
                            payload["workspace_state"] = workspace.clone();
                        }
                    }
                    
                    // Also get full window details
                    if let Ok(clients) = self.get_cached_hyprctl("clients").await {
                        let windows: Vec<Value> = clients.as_array()
                            .map(|arr| arr.iter()
                                .filter(|c| c["workspace"]["name"] == ws_id.as_str() || c["workspace"]["id"].to_string() == ws_id)
                                .cloned()
                                .collect())
                            .unwrap_or_default();
                        
                        payload["windows_full"] = Value::Array(windows);
                    }
                }
                _ => {}
            }
        }
        
        Ok(())
    }

    /// Update focus history
    async fn update_focus_history(&self, event_type: &str, payload: &Value, timestamp: DateTime<Utc>) {
        let window_address = match event_type {
            "activewindow" => {
                // Try to get address from window details if we augmented it
                payload["window_details"]["address"].as_str()
                    .map(|s| s.to_string())
            }
            "activewindowv2" => {
                payload["window_address"].as_str()
                    .map(|s| s.to_string())
            }
            _ => None,
        };

        if let Some(address) = window_address {
            let window_data = payload.get("window_info")
                .or_else(|| payload.get("window_details"))
                .cloned();
            
            let mut history = self.focus_history.lock().unwrap();
            history.push_front(FocusHistoryEntry {
                timestamp,
                window_address: address,
                window_data,
            });
            
            // Keep only configured depth
            while history.len() > self.config.focus_history_depth {
                history.pop_back();
            }
        }
    }

    /// Get cached hyprctl result or fetch fresh
    async fn get_cached_hyprctl(&self, command: &str) -> Result<Value> {
        // Check cache first
        {
            let cache = self.hyprctl_cache.lock().unwrap();
            if let Some(entry) = cache.get(command) {
                if entry.timestamp.elapsed().as_millis() < self.config.hyprctl_cache_ms as u128 {
                    return Ok(entry.data.clone());
                }
            }
        }

        // Fetch fresh data
        let output = Command::new("hyprctl")
            .args(&["-j", command])
            .output()
            .context("Failed to execute hyprctl")?;

        if !output.status.success() {
            anyhow::bail!("hyprctl failed: {}", String::from_utf8_lossy(&output.stderr));
        }

        let data: Value = serde_json::from_slice(&output.stdout)
            .context("Failed to parse hyprctl output")?;

        // Update cache
        {
            let mut cache = self.hyprctl_cache.lock().unwrap();
            cache.insert(command.to_string(), CacheEntry {
                data: data.clone(),
                timestamp: Instant::now(),
            });
        }

        Ok(data)
    }

    /// Insert event into database with retry logic
    async fn insert_event(&self, event: RawEvent) {
        let result = retry_db_operation(&self.retry_config, || async {
            self.db.insert_event(&event).await.map(|_| ())
        })
        .await;

        match result {
            Ok(_) => {
                self.metrics.lock().unwrap().increment_processed();
                debug!("Inserted event: {} {}", event.source, event.event_type);
            }
            Err(e) => {
                error!("Failed to insert event after retries: {}", e);
                
                // Write to DLQ
                match self.dlq.write_event(event.clone(), e.to_string(), self.retry_config.max_retries).await {
                    Ok(dlq_path) => {
                        self.metrics.lock().unwrap().increment_dlq();
                        
                        // Try to emit DLQ notification
                        let dlq_event = self.dlq.create_dlq_notification(&event, dlq_path, e.to_string());
                        
                        if let Err(e2) = self.db.insert_event(&dlq_event).await {
                            let _ = self.dlq.log_critical_failure(&format!(
                                "Failed to emit DLQ notification: {} (original error: {})",
                                e2, e
                            ));
                        }
                    }
                    Err(dlq_err) => {
                        let _ = self.dlq.log_critical_failure(&format!(
                            "Failed to write to DLQ: {} (original error: {})",
                            dlq_err, e
                        ));
                    }
                }
            }
        }
    }

    /// Capture and insert a state snapshot
    async fn capture_and_insert_snapshot(
        db: &Arc<DatabaseService>,
        last_descriptions: &Arc<Mutex<DateTime<Utc>>>,
        include_descriptions: bool,
    ) -> Result<()> {
        let snapshot_timestamp = Utc::now();
        
        // Get all instances first
        let instances = Self::get_hyprland_instances()?;
        
        // If multiple instances, capture state for each
        let mut all_snapshots = Vec::new();
        
        for (idx, instance) in instances.iter().enumerate() {
            let snapshot = Self::capture_instance_state(idx, include_descriptions)?;
            all_snapshots.push(json!({
                "instance_info": instance,
                "state": snapshot,
            }));
        }

        // Update last descriptions emit time if included
        if include_descriptions {
            *last_descriptions.lock().unwrap() = snapshot_timestamp;
        }

        let event = RawEventBuilder::new(
            sources::HYPRLAND,
            "state_snapshot",
            json!({
                "snapshots": all_snapshots,
                "snapshot_timestamp": snapshot_timestamp,
                "includes_descriptions": include_descriptions,
            }),
        )
        .with_orig_timestamp(snapshot_timestamp)
        .build();

        db.insert_event(&event).await?;
        Ok(())
    }

    /// Get Hyprland instances
    fn get_hyprland_instances() -> Result<Vec<HyprlandInstance>> {
        let output = Command::new("hyprctl")
            .args(&["-j", "instances"])
            .output()
            .context("Failed to execute hyprctl instances")?;

        if !output.status.success() {
            anyhow::bail!("hyprctl instances failed: {}", String::from_utf8_lossy(&output.stderr));
        }

        let instances: Vec<HyprlandInstance> = serde_json::from_slice(&output.stdout)
            .context("Failed to parse instances output")?;
        
        Ok(instances)
    }

    /// Capture state for a specific instance
    fn capture_instance_state(instance_idx: usize, include_descriptions: bool) -> Result<Value> {
        // Base batch command
        let mut batch_commands = vec![
            "version",
            "monitors",
            "workspaces",
            "clients",
            "devices",
            "layers",
            "cursorpos",
            "configerrors",
            "locked",
            "submap",
        ];

        // Add descriptions if needed
        if include_descriptions {
            batch_commands.push("descriptions");
        }

        let batch_cmd = batch_commands.join(";");
        
        // Execute batch command
        let output = Command::new("hyprctl")
            .args(&["-j", "-i", &instance_idx.to_string(), "--batch", &batch_cmd])
            .output()
            .context("Failed to execute hyprctl batch command")?;

        if !output.status.success() {
            anyhow::bail!("hyprctl batch failed: {}", String::from_utf8_lossy(&output.stderr));
        }

        // Parse batch output (it's a JSON array)
        let batch_results: Vec<Value> = serde_json::from_slice(&output.stdout)
            .context("Failed to parse batch output")?;

        // Build structured result
        let mut state = json!({
            "version": batch_results.get(0).cloned().unwrap_or(json!({})),
            "monitors": batch_results.get(1).cloned().unwrap_or(json!([])),
            "workspaces": batch_results.get(2).cloned().unwrap_or(json!([])),
            "clients": batch_results.get(3).cloned().unwrap_or(json!([])),
            "devices": batch_results.get(4).cloned().unwrap_or(json!({})),
            "layers": batch_results.get(5).cloned().unwrap_or(json!({})),
            "cursor_pos": batch_results.get(6).cloned().unwrap_or(json!({})),
            "config_errors": batch_results.get(7).cloned().unwrap_or(json!([])),
            "locked": batch_results.get(8).cloned().unwrap_or(json!({})),
            "submap": batch_results.get(9).cloned().unwrap_or(json!("")),
        });

        if include_descriptions {
            state["descriptions"] = batch_results.get(10).cloned().unwrap_or(json!({}));
        }

        Ok(state)
    }

    /// Get rolling log for an instance
    fn get_rolling_log(instance_idx: usize) -> Result<Value> {
        let output = Command::new("hyprctl")
            .args(&["-j", "-i", &instance_idx.to_string(), "rollinglog"])
            .output()
            .context("Failed to execute hyprctl rollinglog")?;

        if !output.status.success() {
            anyhow::bail!("hyprctl rollinglog failed: {}", String::from_utf8_lossy(&output.stderr));
        }

        // Rolling log might not be pure JSON, try to parse
        match serde_json::from_slice(&output.stdout) {
            Ok(json) => Ok(json),
            Err(_) => {
                // Fall back to string representation
                Ok(json!({
                    "raw_log": String::from_utf8_lossy(&output.stdout)
                }))
            }
        }
    }
}

/// Create a startup event
fn create_startup_event(agent_name: &str, version: &str) -> RawEvent {
    RawEventBuilder::new(
        sources::SINEX,
        "agent.startup",
        json!({
            "agent_name": agent_name,
            "version": version,
            "configuration": {
                "window_augmentation": "configured",
                "workspace_tracking": "configured",
            },
        }),
    )
    .build()
}

/// Create a shutdown event
fn create_shutdown_event(agent_name: &str, reason: &str) -> RawEvent {
    RawEventBuilder::new(
        sources::SINEX,
        "agent.shutdown",
        json!({
            "agent_name": agent_name,
            "reason": reason,
        }),
    )
    .build()
}