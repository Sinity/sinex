use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sinex_core::{EventSender, Timestamp};
use std::collections::{HashMap, VecDeque};
use std::env;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error, info};

use sinex_core::{EventType, EventSource, EventSourceContext, Result, event_type_constants, sources, RawEvent};

// ============================================================================
// Event Payloads
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WindowFocusedPayload {
    pub window_address: String,
    pub window_class: String,
    pub window_title: String,
    pub focused_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WindowOpenedPayload {
    pub window_address: String,
    pub workspace_id: String,
    pub window_class: String,
    pub window_title: String,
    pub opened_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WindowClosedPayload {
    pub window_address: String,
    pub closed_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WindowMovedPayload {
    pub window_address: String,
    pub from_workspace: Option<String>,
    pub to_workspace: String,
    pub moved_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WorkspaceChangedPayload {
    pub workspace_id: String,
    pub workspace_name: String,
    pub changed_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MonitorFocusedPayload {
    pub monitor_name: String,
    pub workspace_id: String,
    pub focused_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StateSnapshotPayload {
    pub timestamp: Timestamp,
    pub monitors: JsonValue,
    pub workspaces: JsonValue,
    pub clients: JsonValue,
}

// ============================================================================
// Event Types  
// ============================================================================

pub struct WindowFocused;
impl EventType for WindowFocused {
    type Payload = WindowFocusedPayload;
    type SourceImpl = HyprlandListener;
    const EVENT_NAME: &'static str = event_type_constants::window_manager::WINDOW_FOCUSED;
}

pub struct WindowOpened;
impl EventType for WindowOpened {
    type Payload = WindowOpenedPayload;
    type SourceImpl = HyprlandListener;
    const EVENT_NAME: &'static str = event_type_constants::window_manager::WINDOW_OPENED;
}

pub struct WindowClosed;
impl EventType for WindowClosed {
    type Payload = WindowClosedPayload;
    type SourceImpl = HyprlandListener;
    const EVENT_NAME: &'static str = event_type_constants::window_manager::WINDOW_CLOSED;
}

pub struct WindowMoved;
impl EventType for WindowMoved {
    type Payload = WindowMovedPayload;
    type SourceImpl = HyprlandListener;
    const EVENT_NAME: &'static str = event_type_constants::window_manager::WINDOW_MOVED;
}

pub struct WorkspaceChanged;
impl EventType for WorkspaceChanged {
    type Payload = WorkspaceChangedPayload;
    type SourceImpl = HyprlandListener;
    const EVENT_NAME: &'static str = event_type_constants::window_manager::WORKSPACE_CHANGED;
}

pub struct MonitorFocused;
impl EventType for MonitorFocused {
    type Payload = MonitorFocusedPayload;
    type SourceImpl = HyprlandListener;
    const EVENT_NAME: &'static str = event_type_constants::window_manager::MONITOR_FOCUSED;
}

pub struct StateSnapshot;
impl EventType for StateSnapshot {
    type Payload = StateSnapshotPayload;
    type SourceImpl = HyprlandStateSnapshotter;
    const EVENT_NAME: &'static str = event_type_constants::window_manager::STATE_SNAPSHOT;
}

// ============================================================================
// Event Source
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyprlandConfig {
    pub ignore_events: Vec<String>,
    pub state_snapshot_interval_secs: u64,
    pub window_augmentation: WindowAugmentation,
    pub workspace_tracking: WorkspaceTracking,
    pub track_focus_history: bool,
}

impl Default for HyprlandConfig {
    fn default() -> Self {
        Self {
            ignore_events: vec![],
            state_snapshot_interval_secs: 300, // 5 minutes
            window_augmentation: WindowAugmentation::Basic,
            workspace_tracking: WorkspaceTracking::Events,
            track_focus_history: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WindowAugmentation {
    None,
    Basic,
    Full,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WorkspaceTracking {
    Events,
    WithState,
}

/// Cache entry for hyprctl results
struct CacheEntry {
    data: Value,
    timestamp: Instant,
}

/// Focus history entry
#[derive(Debug, Clone)]
struct FocusHistoryEntry {
    #[allow(dead_code)]
    timestamp: Timestamp,
    #[allow(dead_code)]
    window_address: String,
    #[allow(dead_code)]
    window_data: Option<Value>,
}

// ============================================================================
// Event Sources
// ============================================================================

/// Real-time IPC monitor for Hyprland socket2 events
pub struct HyprlandIPCMonitor {
    config: HyprlandConfig,
    socket_path: PathBuf,
    hyprctl_cache: Arc<Mutex<HashMap<String, CacheEntry>>>,
    focus_history: Arc<Mutex<VecDeque<FocusHistoryEntry>>>,
}

/// Config for state snapshotter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotterConfig {
    pub interval_secs: u64,
}

impl Default for SnapshotterConfig {
    fn default() -> Self {
        Self {
            interval_secs: 300, // 5 minutes
        }
    }
}

/// Periodic state snapshotter using hyprctl
pub struct HyprlandStateSnapshotter {
    interval_secs: u64,
}

// Legacy alias for compatibility
pub type HyprlandListener = HyprlandIPCMonitor;

#[async_trait]
impl EventSource for HyprlandIPCMonitor {
    type Config = HyprlandConfig;
    
    const SOURCE_NAME: &'static str = sources::WINDOW_MANAGER_HYPRLAND;
    
    async fn initialize(ctx: EventSourceContext) -> Result<Self> {
        let config: Self::Config = serde_json::from_value(ctx.config)
            .map_err(|e| sinex_core::CoreError::Configuration(format!("Failed to parse config: {}", e)))?;
        
        // Get Hyprland instance signature
        let hyprland_instance_sig = env::var("HYPRLAND_INSTANCE_SIGNATURE")
            .map_err(|_| sinex_core::CoreError::Other("HYPRLAND_INSTANCE_SIGNATURE not set. Is Hyprland running?".to_string()))?;
        
        // Build socket path
        let xdg_runtime = env::var("XDG_RUNTIME_DIR")
            .map_err(|_| sinex_core::CoreError::Other("XDG_RUNTIME_DIR not set".to_string()))?;
        let socket_path = PathBuf::from(xdg_runtime)
            .join("hypr")
            .join(&hyprland_instance_sig)
            .join(".socket2.sock");

        info!(
            socket_path = ?socket_path,
            window_augmentation = ?config.window_augmentation,
            workspace_tracking = ?config.workspace_tracking,
            "Initializing Hyprland listener"
        );
        
        Ok(Self {
            config,
            socket_path,
            hyprctl_cache: Arc::new(Mutex::new(HashMap::new())),
            focus_history: Arc::new(Mutex::new(VecDeque::new())),
        })
    }
    
    async fn stream_events(&mut self, tx: EventSender) -> Result<()> {
        info!(
            socket_path = ?self.socket_path,
            window_augmentation = ?self.config.window_augmentation,
            workspace_tracking = ?self.config.workspace_tracking,
            "Starting Hyprland IPC monitor with socket2 capture"
        );

        // Spawn cache cleanup task
        let cache_cleanup_handle = self.spawn_cache_cleanup_task();

        // Start socket listener
        let socket_result = self.listen_socket_events(tx).await;

        // Cancel background task
        cache_cleanup_handle.abort();

        socket_result
    }
}

impl HyprlandIPCMonitor {
    /// Listen to socket2 events
    async fn listen_socket_events(&self, event_tx: EventSender) -> Result<()> {
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
    async fn process_event_stream(&self, stream: UnixStream, event_tx: &EventSender) -> Result<()> {
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

                // Map event types to our constants
                let event_type = match event_name {
                    "activewindow" | "activewindowv2" => event_type_constants::window_manager::WINDOW_FOCUSED,
                    "openwindow" => event_type_constants::window_manager::WINDOW_OPENED,
                    "closewindow" => event_type_constants::window_manager::WINDOW_CLOSED,
                    "movewindow" | "movewindowv2" => event_type_constants::window_manager::WINDOW_MOVED,
                    "workspace" | "workspacev2" => event_type_constants::window_manager::WORKSPACE_CHANGED,
                    "focusedmon" => event_type_constants::window_manager::MONITOR_FOCUSED,
                    _ => event_name, // Pass through unknown events
                };

                // Build raw event
                let raw_event = RawEvent {
                    id: sinex_ulid::Ulid::new(),
                    source: sources::WINDOW_MANAGER_HYPRLAND.to_string(),
                    event_type: event_type.to_string(),
                    ts_ingest: Utc::now(),
                    ts_orig: Some(Utc::now()),
                    host: gethostname::gethostname().to_string_lossy().to_string(),
                    ingestor_version: Some("0.1.0".to_string()),
                    payload_schema_id: None,
                    payload,
                };

                // Send event
                event_tx.send(raw_event).await.map_err(|_| sinex_core::CoreError::Other("Channel closed".to_string()))?;

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
                    "focused_at": Utc::now(),
                })
            }
            "activewindowv2" => {
                json!({
                    "window_address": event_data.to_string(),
                    "focused_at": Utc::now(),
                })
            }
            "openwindow" => {
                let parts: Vec<&str> = event_data.splitn(4, ',').collect();
                json!({
                    "window_address": parts.get(0).unwrap_or(&"").to_string(),
                    "workspace_id": parts.get(1).unwrap_or(&"").to_string(),
                    "window_class": parts.get(2).unwrap_or(&"").to_string(),
                    "window_title": parts.get(3).unwrap_or(&"").to_string(),
                    "opened_at": Utc::now(),
                })
            }
            "closewindow" => {
                json!({
                    "window_address": event_data.to_string(),
                    "closed_at": Utc::now(),
                })
            }
            // Workspace events
            "workspace" | "createworkspace" | "destroyworkspace" => {
                json!({
                    "workspace_name": event_data.to_string(),
                    "changed_at": Utc::now(),
                })
            }
            "workspacev2" | "createworkspacev2" | "destroyworkspacev2" => {
                let parts: Vec<&str> = event_data.splitn(2, ',').collect();
                json!({
                    "workspace_id": parts.get(0).unwrap_or(&"").to_string(),
                    "workspace_name": parts.get(1).unwrap_or(&"").to_string(),
                    "changed_at": Utc::now(),
                })
            }
            "focusedmon" => {
                let parts: Vec<&str> = event_data.splitn(2, ',').collect();
                json!({
                    "monitor_name": parts.get(0).unwrap_or(&"").to_string(),
                    "workspace_id": parts.get(1).unwrap_or(&"").to_string(),
                    "focused_at": Utc::now(),
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
            .map_err(|e| sinex_core::CoreError::Other(format!("Failed to execute hyprctl: {}", e)))?;

        if !output.status.success() {
            return Err(sinex_core::CoreError::Other(format!("hyprctl failed: {}", String::from_utf8_lossy(&output.stderr))));
        }

        let data: Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| sinex_core::CoreError::Other(format!("Failed to parse hyprctl output: {}", e)))?;

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

#[async_trait]
impl EventSource for HyprlandStateSnapshotter {
    type Config = SnapshotterConfig;
    
    const SOURCE_NAME: &'static str = "window_manager.hyprland_snapshotter";
    
    async fn initialize(ctx: EventSourceContext) -> Result<Self> {
        let config: Self::Config = serde_json::from_value(ctx.config)
            .map_err(|e| sinex_core::CoreError::Configuration(format!("Failed to parse config: {}", e)))?;
        
        info!(
            interval_secs = config.interval_secs,
            "Initializing Hyprland state snapshotter"
        );
        Ok(Self {
            interval_secs: config.interval_secs,
        })
    }
    
    async fn stream_events(&mut self, tx: EventSender) -> Result<()> {
        if self.interval_secs == 0 {
            info!("State snapshots disabled (interval_secs = 0)");
            // Keep running but don't send events
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        }
        
        info!(
            interval_secs = self.interval_secs,
            "Starting Hyprland state snapshotter"
        );
        
        let mut interval = time::interval(Duration::from_secs(self.interval_secs));
        
        loop {
            interval.tick().await;
            
            // Create state snapshot
            let snapshot = match Self::create_state_snapshot().await {
                Ok(data) => data,
                Err(e) => {
                    error!("Failed to create state snapshot: {}", e);
                    continue;
                }
            };

            let event = RawEvent {
                id: sinex_ulid::Ulid::new(),
                source: Self::SOURCE_NAME.to_string(),
                event_type: event_type_constants::window_manager::STATE_SNAPSHOT.to_string(),
                ts_ingest: Utc::now(),
                ts_orig: Some(Utc::now()),
                host: gethostname::gethostname().to_string_lossy().to_string(),
                ingestor_version: Some("0.1.0".to_string()),
                payload_schema_id: None,
                payload: snapshot,
            };

            tx.send(event).await.map_err(|_| sinex_core::CoreError::Other("Channel closed".to_string()))?;
        }
    }
}

impl HyprlandStateSnapshotter {
    /// Create a state snapshot
    async fn create_state_snapshot() -> Result<Value> {
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
}