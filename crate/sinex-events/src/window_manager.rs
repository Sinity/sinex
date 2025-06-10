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
    pub title: String,
    pub class: String,
    pub pid: u32,
    pub workspace: String,
    pub focused_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WorkspaceChangedPayload {
    pub from_workspace: String,
    pub to_workspace: String,
    pub changed_at: DateTime<Utc>,
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

pub struct WorkspaceChanged;
impl EventType for WorkspaceChanged {
    type Payload = WorkspaceChangedPayload;
    type SourceImpl = HyprlandListener;
    const EVENT_NAME: &'static str = event_type_constants::window_manager::WORKSPACE_CHANGED;
}

// ============================================================================
// Event Source
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyprlandConfig {
    pub socket_path: PathBuf,
    pub monitored_events: Vec<String>,
}

impl Default for HyprlandConfig {
    fn default() -> Self {
        Self {
            socket_path: PathBuf::from("/tmp/hypr/hyprland.sock2"),
            monitored_events: vec![
                "activewindow".to_string(),
                "workspace".to_string(),
            ],
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
    
    async fn stream_events(&mut self, _tx: mpsc::Sender<RawEvent>) -> Result<()> {
        // TODO: Integrate with existing Hyprland IPC logic from
        // ingestor/hyprland/src/watcher.rs
        
        // Placeholder: In real implementation, this would:
        // 1. Connect to Hyprland IPC socket
        // 2. Subscribe to events
        // 3. Parse IPC messages
        // 4. Convert to RawEvents
        
        Ok(())
    }
}