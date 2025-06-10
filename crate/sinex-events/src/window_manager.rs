use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::info;

use sinex_core::{EventType, EventSource, Result, event_type_constants, sources};
use sinex_db::models::RawEvent;

// ============================================================================
// Event Payloads
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WindowFocusedPayload {
    pub window_address: String,
    pub window_class: String,
    pub window_title: String,
    pub focused_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WindowOpenedPayload {
    pub window_address: String,
    pub workspace_id: String,
    pub window_class: String,
    pub window_title: String,
    pub opened_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WindowClosedPayload {
    pub window_address: String,
    pub closed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WindowMovedPayload {
    pub window_address: String,
    pub from_workspace: Option<String>,
    pub to_workspace: String,
    pub moved_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WorkspaceChangedPayload {
    pub workspace_id: String,
    pub workspace_name: String,
    pub changed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MonitorFocusedPayload {
    pub monitor_name: String,
    pub workspace_id: String,
    pub focused_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StateSnapshotPayload {
    pub timestamp: DateTime<Utc>,
    pub monitors: serde_json::Value,
    pub workspaces: serde_json::Value,
    pub clients: serde_json::Value,
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
    type SourceImpl = HyprlandListener;
    const EVENT_NAME: &'static str = event_type_constants::window_manager::STATE_SNAPSHOT;
}

// ============================================================================
// Event Source
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyprlandConfig {
    pub socket_path: PathBuf,
    pub monitored_events: Vec<String>,
    /// Interval for periodic state snapshots (0 = disabled)
    pub state_snapshot_interval_secs: u64,
}

impl Default for HyprlandConfig {
    fn default() -> Self {
        Self {
            socket_path: PathBuf::from("/tmp/hypr/$HYPRLAND_INSTANCE_SIGNATURE/.socket2.sock"),
            monitored_events: vec![
                "activewindow".to_string(),
                "activewindowv2".to_string(),
                "openwindow".to_string(),
                "closewindow".to_string(),
                "movewindow".to_string(),
                "workspace".to_string(),
                "workspacev2".to_string(),
                "focusedmon".to_string(),
            ],
            state_snapshot_interval_secs: 300, // 5 minutes
        }
    }
}

pub struct HyprlandListener {
    config: HyprlandConfig,
}

#[async_trait]
impl EventSource for HyprlandListener {
    type Config = HyprlandConfig;
    
    const SOURCE_NAME: &'static str = sources::WINDOW_MANAGER_HYPRLAND;
    
    async fn initialize(config: Self::Config) -> Result<Self> {
        info!(
            socket_path = ?config.socket_path,
            events = ?config.monitored_events,
            "Initializing Hyprland listener"
        );
        Ok(Self { config })
    }
    
    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> Result<()> {
        info!(
            socket_path = ?self.config.socket_path,
            events = ?self.config.monitored_events,
            snapshot_interval = self.config.state_snapshot_interval_secs,
            "Starting Hyprland event source"
        );
        
        // Spawn socket monitoring task
        let _socket_tx = tx.clone();
        let socket_task = tokio::spawn(async move {
            // TODO: Integrate with existing Hyprland IPC logic
            // 1. Connect to socket
            // 2. Read events in format: EVENT>>DATA\n
            // 3. Parse and convert to RawEvents
            // 4. Send through socket_tx
            
            // This would process real-time events like:
            // - activewindow>>kitty,nvim
            // - openwindow>>0x1234,1,firefox,Mozilla Firefox
            // - workspace>>2
            
            // For now, just return Ok
            std::result::Result::<(), anyhow::Error>::Ok(())
        });
        
        // Spawn periodic state snapshot task if enabled
        let snapshot_task = if self.config.state_snapshot_interval_secs > 0 {
            let interval_secs = self.config.state_snapshot_interval_secs;
            let _snapshot_tx = tx.clone();
            
            Some(tokio::spawn(async move {
                let mut interval = tokio::time::interval(
                    std::time::Duration::from_secs(interval_secs)
                );
                
                loop {
                    interval.tick().await;
                    
                    // Create state snapshot
                    // TODO: Call hyprctl to get full state:
                    // - hyprctl monitors -j
                    // - hyprctl workspaces -j  
                    // - hyprctl clients -j
                    
                    // For now, placeholder
                    let _snapshot_event = sinex_db::models::RawEvent {
                        id: sinex_ulid::Ulid::new(),
                        source: sources::WINDOW_MANAGER_HYPRLAND.to_string(),
                        event_type: event_type_constants::window_manager::STATE_SNAPSHOT.to_string(),
                        ts_ingest: chrono::Utc::now(),
                        ts_orig: Some(chrono::Utc::now()),
                        host: gethostname::gethostname().to_string_lossy().to_string(),
                        ingestor_version: Some("0.1.0".to_string()),
                        payload_schema_id: None,
                        payload: serde_json::json!({
                            "timestamp": chrono::Utc::now(),
                            "monitors": [],
                            "workspaces": [],
                            "clients": []
                        }),
                    };
                    
                    // snapshot_tx.send(snapshot_event).await?;
                }
                
                // Never returns  
                #[allow(unreachable_code)]
                std::result::Result::<(), anyhow::Error>::Ok(())
            }))
        } else {
            None
        };
        
        // Wait for tasks
        match socket_task.await {
            Ok(Ok(())) => {},
            Ok(Err(e)) => return Err(sinex_core::CoreError::Other(format!("Socket task failed: {}", e))),
            Err(e) => return Err(sinex_core::CoreError::Other(format!("Socket task panicked: {}", e))),
        }
        
        if let Some(task) = snapshot_task {
            match task.await {
                Ok(Ok(())) => {},
                Ok(Err(e)) => return Err(sinex_core::CoreError::Other(e.to_string())),
                Err(e) => return Err(sinex_core::CoreError::Other(format!("Task panicked: {}", e))),
            }
        }
        
        Ok(())
    }
}