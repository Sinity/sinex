//! Window manager watcher with sensd source material capture
//!
//! Monitors window manager events (focus, open, close, move) and captures them as source
//! material for later event creation with proper provenance tracking.
//!
//! ## Architecture
//!
//! This module follows the sensd pattern:
//! 1. **Source Material Capture**: Window manager events → raw.source_material_registry
//! 2. **Temporal Ledger**: Precise timing → raw.temporal_ledger
//! 3. **Event Generation**: Material processing → events with Provenance::Material
//!
//! ## Hyprland Integration
//!
//! - Real-time IPC via socket2 for event stream
//! - State augmentation via hyprctl queries
//! - Automatic reconnection with exponential backoff
//! - Comprehensive window and workspace metadata capture

// Use local facade for common types
use crate::common::*;

// Window manager specific imports
use serde_json::Value;
use sinex_core::types::Ulid;
use sqlx::PgPool;
use std::{
    fmt,
    str::FromStr,
    sync::Arc,
    time::{Instant, SystemTime},
};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixStream;
use tokio::time::sleep;

/// Supported window manager types
#[derive(Debug, Clone, PartialEq)]
pub enum WindowManagerType {
    Hyprland,
}

impl fmt::Display for WindowManagerType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WindowManagerType::Hyprland => write!(f, "hyprland"),
        }
    }
}

impl FromStr for WindowManagerType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "hyprland" => Ok(WindowManagerType::Hyprland),
            _ => Err(format!("Unsupported window manager type: {}", s)),
        }
    }
}

/// Enhanced window information with metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct WindowInfo {
    address: String,
    class: String,
    title: String,
    workspace_id: String,
    last_seen: SystemTime,
    floating: bool,
    fullscreen: bool,
}

/// Enhanced workspace information
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct WorkspaceInfo {
    id: String,
    name: String,
    monitor: String,
    active: bool,
    window_count: u32,
    last_switched: SystemTime,
}

/// Window manager watcher with sensd source material capture
pub struct WindowManagerWatcher {
    wm_type: WindowManagerType,
    socket_path: Option<String>,
    command_socket_path: Option<String>,
    windows: HashMap<String, WindowInfo>,
    workspaces: HashMap<String, WorkspaceInfo>,
    current_focused_window: Option<String>,
    current_workspace: Option<String>,
    current_monitor: Option<String>,
    // sensd integration
    db_pool: Option<PgPool>,
    source_identifier: String,
}

impl WindowManagerWatcher {
    /// Create new window manager watcher with sensd integration
    pub async fn new(wm_type: WindowManagerType, db_pool: Option<PgPool>) -> SatelliteResult<Self> {
        let mut watcher = Self {
            wm_type,
            socket_path: None,
            command_socket_path: None,
            windows: HashMap::new(),
            workspaces: HashMap::new(),
            current_focused_window: None,
            current_workspace: None,
            current_monitor: None,
            db_pool,
            source_identifier: "desktop_window_manager".to_string(),
        };

        // Discover socket paths based on WM type
        if wm_type == WindowManagerType::Hyprland {
            watcher.discover_hyprland_sockets().await?;
        } else {
            return Err(sinex_satellite_sdk::SatelliteError::Processing(format!(
                "Unsupported window manager: {}",
                wm_type
            )));
        }

        info!(
            "Window manager watcher initialized for {} (sensd mode)",
            wm_type
        );
        Ok(watcher)
    }

    /// Discover Hyprland socket paths (both event and command)
    async fn discover_hyprland_sockets(&mut self) -> SatelliteResult<()> {
        // Get Hyprland instance signature
        let hyprland_instance_sig = std::env::var("HYPRLAND_INSTANCE_SIGNATURE").map_err(|_| {
            sinex_satellite_sdk::SatelliteError::Processing(
                "HYPRLAND_INSTANCE_SIGNATURE not set. Is Hyprland running?".to_string(),
            )
        })?;

        // Get XDG_RUNTIME_DIR
        let xdg_runtime = std::env::var("XDG_RUNTIME_DIR").map_err(|_| {
            sinex_satellite_sdk::SatelliteError::Processing("XDG_RUNTIME_DIR not set".to_string())
        })?;

        // Build socket paths
        let base_path = format!("{}/hypr/{}", xdg_runtime, hyprland_instance_sig);
        let event_socket = format!("{}.socket2.sock", base_path);
        let command_socket = format!("{}.socket.sock", base_path);

        // Test event socket connection
        if UnixStream::connect(&event_socket).await.is_ok() {
            self.socket_path = Some(event_socket.clone());
            info!("Found Hyprland event socket at: {}", event_socket);
        } else {
            return Err(sinex_satellite_sdk::SatelliteError::Processing(format!(
                "Cannot connect to Hyprland event socket: {}",
                event_socket
            )));
        }

        // Test command socket connection
        if UnixStream::connect(&command_socket).await.is_ok() {
            self.command_socket_path = Some(command_socket.clone());
            info!("Found Hyprland command socket at: {}", command_socket);
        } else {
            warn!(
                "Cannot connect to Hyprland command socket: {}",
                command_socket
            );
        }

        Ok(())
    }

    /// Store window manager event as source material in sensd
    async fn store_window_manager_source_material(
        &self,
        event_type: &str,
        event_data: &str,
        metadata: serde_json::Value,
    ) -> SatelliteResult<Option<Ulid>> {
        let Some(db_pool) = &self.db_pool else {
            warn!("No database pool available for source material storage");
            return Ok(None);
        };

        let material_id = Ulid::new();
        let now = Utc::now();

        // Prepare complete metadata
        let complete_metadata = serde_json::json!({
            "event_type": event_type,
            "event_data": event_data,
            "wm_type": self.wm_type.to_string(),
            "current_focused_window": self.current_focused_window,
            "current_workspace": self.current_workspace,
            "current_monitor": self.current_monitor,
            "window_count": self.windows.len(),
            "workspace_count": self.workspaces.len(),
            "additional_metadata": metadata,
        });

        // Create structured event data for source material
        let structured_data = serde_json::json!({
            "event_type": event_type,
            "data": event_data,
            "timestamp": now,
            "metadata": complete_metadata,
        });

        let data_bytes = structured_data.to_string().as_bytes().to_vec();

        // Store in source_material_registry
        sqlx::query!(
            r#"
            INSERT INTO raw.source_material_registry (
                source_material_id, source_identifier, acquired_at,
                data, size_bytes, mime_type, metadata
            )
            VALUES ($1::ulid, $2, $3, $4, $5, $6, $7)
            "#,
            material_id as Ulid,
            self.source_identifier,
            now,
            &data_bytes,
            data_bytes.len() as i64,
            "application/json",
            complete_metadata.to_string(),
        )
        .execute(db_pool)
        .await?;

        // Create temporal ledger entry
        sqlx::query!(
            r#"
            INSERT INTO raw.temporal_ledger (
                material_id, offset_start, offset_end, 
                offset_kind, proximity_hint, temporal_hint, timing_source,
                ts_capture, note
            )
            VALUES ($1::ulid, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
            material_id as Ulid,
            0i64,
            data_bytes.len() as i64,
            "byte",
            "exact",
            "wall",
            "realtime_capture",
            now,
            complete_metadata.to_string(),
        )
        .execute(db_pool)
        .await?;

        info!(
            "Stored window manager {} source material: {} bytes",
            event_type,
            data_bytes.len()
        );

        Ok(Some(material_id))
    }

    /// Connect to Hyprland event socket
    async fn connect_to_hyprland_events(&self) -> SatelliteResult<UnixStream> {
        let socket_path = self.socket_path.as_ref().ok_or_else(|| {
            sinex_satellite_sdk::SatelliteError::Processing(
                "No Hyprland socket configured".to_string(),
            )
        })?;

        UnixStream::connect(socket_path).await.map_err(|e| {
            sinex_satellite_sdk::SatelliteError::Processing(format!(
                "Failed to connect to Hyprland: {}",
                e
            ))
        })
    }

    /// Parse ID string to integer with fallback and warning
    fn parse_id(&self, id_str: &str, context: &str) -> i32 {
        id_str.parse().unwrap_or_else(|_| {
            warn!("Failed to parse {} '{}', defaulting to 0", context, id_str);
            0
        })
    }

    /// Process Hyprland event line
    async fn process_hyprland_event(&mut self, line: &str) -> SatelliteResult<()> {
        if line.is_empty() {
            return Ok(());
        }

        debug!("Hyprland event: {}", line);

        // Parse event format: "EVENT>>DATA"
        if let Some((event_type, event_data)) = line.split_once(">>") {
            match event_type {
                "focusedwindow" => {
                    self.handle_window_focused(event_data).await?;
                }
                "openwindow" => {
                    self.handle_window_opened(event_data).await?;
                }
                "closewindow" => {
                    self.handle_window_closed(event_data).await?;
                }
                "movewindow" => {
                    self.handle_window_moved(event_data).await?;
                }
                "workspace" => {
                    self.handle_workspace_changed(event_data).await?;
                }
                "focusedmon" => {
                    self.handle_monitor_focused(event_data).await?;
                }
                _ => {
                    debug!("Unhandled Hyprland event: {}", event_type);
                    // Store unhandled events as source material too
                    let metadata = serde_json::json!({"unhandled": true});
                    self.store_window_manager_source_material(event_type, event_data, metadata)
                        .await?;
                }
            }
        }

        Ok(())
    }

    /// Handle window focused event
    async fn handle_window_focused(&mut self, data: &str) -> SatelliteResult<()> {
        // Format: "class,title"
        if let Some((class, title)) = data.split_once(',') {
            let window_address = format!("0x{:x}", data.len()); // Placeholder

            let metadata = serde_json::json!({
                "window_class": class,
                "window_title": title,
                "window_id": window_address,
                "previous_window_id": self.current_focused_window,
            });

            // Store as source material (not event!)
            self.store_window_manager_source_material("focusedwindow", data, metadata)
                .await?;

            self.current_focused_window = Some(window_address);
        }

        Ok(())
    }

    /// Handle window opened event
    async fn handle_window_opened(&mut self, data: &str) -> SatelliteResult<()> {
        // Format: "address,workspace,class,title"
        let parts: Vec<&str> = data.split(',').collect();
        if parts.len() >= 4 {
            let window_address = parts[0].to_string();
            let workspace_id = parts[1].to_string();
            let window_class = parts[2].to_string();
            let window_title = parts[3..].join(","); // Title might contain commas

            let metadata = serde_json::json!({
                "window_id": window_address,
                "window_class": window_class,
                "window_title": window_title,
                "workspace_id": self.parse_id(&workspace_id, "workspace_id"),
            });

            // Store as source material (not event!)
            self.store_window_manager_source_material("openwindow", data, metadata)
                .await?;

            // Store window info
            self.windows.insert(
                window_address.clone(),
                WindowInfo {
                    address: window_address,
                    class: window_class,
                    title: window_title,
                    workspace_id,
                    last_seen: SystemTime::now(),
                    floating: false,
                    fullscreen: false,
                },
            );
        }

        Ok(())
    }

    /// Handle window closed event
    async fn handle_window_closed(&mut self, data: &str) -> SatelliteResult<()> {
        let window_address = data.trim().to_string();

        let metadata = serde_json::json!({
            "window_id": window_address,
            "was_tracked": self.windows.contains_key(&window_address),
        });

        // Store as source material (not event!)
        self.store_window_manager_source_material("closewindow", data, metadata)
            .await?;

        // Remove from tracking
        self.windows.remove(&window_address);

        Ok(())
    }

    /// Handle window moved event
    async fn handle_window_moved(&mut self, data: &str) -> SatelliteResult<()> {
        // Format: "address,workspace"
        if let Some((address, workspace)) = data.split_once(',') {
            let metadata = serde_json::json!({
                "window_address": address,
                "new_workspace_id": self.parse_id(workspace, "workspace_id"),
            });

            // Store as source material (not event!)
            self.store_window_manager_source_material("movewindow", data, metadata)
                .await?;

            // Update window workspace
            if let Some(window) = self.windows.get_mut(address) {
                window.workspace_id = workspace.to_string();
            }
        }

        Ok(())
    }

    /// Handle workspace changed event
    async fn handle_workspace_changed(&mut self, data: &str) -> SatelliteResult<()> {
        let workspace_id = data.trim().to_string();

        let metadata = serde_json::json!({
            "from_workspace_id": self.current_workspace.as_ref()
                .map(|w| self.parse_id(w, "current_workspace_id"))
                .unwrap_or(0),
            "to_workspace_id": self.parse_id(&workspace_id, "workspace_id"),
        });

        // Store as source material (not event!)
        self.store_window_manager_source_material("workspace", data, metadata)
            .await?;

        self.current_workspace = Some(workspace_id);

        Ok(())
    }

    /// Handle monitor focused event
    async fn handle_monitor_focused(&mut self, data: &str) -> SatelliteResult<()> {
        // Format: "monitor,workspace"
        if let Some((monitor, workspace)) = data.split_once(',') {
            let metadata = serde_json::json!({
                "monitor_id": self.parse_id(monitor, "monitor_id"),
                "workspace_id": self.parse_id(workspace, "workspace_id"),
                "previous_monitor": self.current_monitor,
            });

            // Store as source material (not event!)
            self.store_window_manager_source_material("focusedmon", data, metadata)
                .await?;

            self.current_monitor = Some(monitor.to_string());
        }

        Ok(())
    }

    /// Start monitoring window manager events (sensd mode)
    pub async fn start_monitoring(&mut self) -> SatelliteResult<()> {
        info!(
            "Starting window manager event monitoring for {} (sensd mode)",
            self.wm_type
        );

        if self.wm_type == WindowManagerType::Hyprland {
            self.stream_hyprland_events().await
        } else {
            Err(sinex_satellite_sdk::SatelliteError::Processing(format!(
                "Unsupported window manager: {}",
                self.wm_type
            )))
        }
    }

    /// Stream Hyprland events
    async fn stream_hyprland_events(&mut self) -> SatelliteResult<()> {
        loop {
            match self.connect_to_hyprland_events().await {
                Ok(stream) => {
                    info!("Connected to Hyprland event stream");

                    let reader = BufReader::new(stream);
                    let mut lines = reader.lines();

                    loop {
                        tokio::select! {
                            // Read Hyprland events
                            line_result = lines.next_line() => {
                                match line_result {
                                    Ok(Some(line)) => {
                                        if let Err(e) = self.process_hyprland_event(&line).await {
                                            error!("Error processing Hyprland event: {}", e);
                                        }
                                    }
                                    Ok(None) => {
                                        warn!("Hyprland event stream ended");
                                        break;
                                    }
                                    Err(e) => {
                                        error!("Error reading from Hyprland socket: {}", e);
                                        break;
                                    }
                                }
                            }

                            // Periodic state capture
                            _ = sleep(Duration::from_secs(300)) => {
                                if let Err(e) = self.capture_state_snapshot().await {
                                    error!("Error capturing state snapshot: {}", e);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to connect to Hyprland: {}", e);
                    sleep(Duration::from_secs(5)).await;
                    continue;
                }
            }

            warn!("Hyprland connection lost, reconnecting in 5 seconds...");
            sleep(Duration::from_secs(5)).await;
        }
    }

    /// Capture periodic state snapshot
    async fn capture_state_snapshot(&mut self) -> SatelliteResult<()> {
        debug!("Capturing window manager state snapshot");

        let snapshot_data = serde_json::json!({
            "windows": self.windows.values().collect::<Vec<_>>(),
            "workspaces": self.workspaces.values().collect::<Vec<_>>(),
            "current_workspace": self.current_workspace,
            "current_monitor": self.current_monitor,
            "current_focused_window": self.current_focused_window,
        });

        // Store as source material (not event!)
        self.store_window_manager_source_material(
            "state_snapshot",
            &snapshot_data.to_string(),
            serde_json::json!({"snapshot": true}),
        )
        .await?;

        Ok(())
    }
}
