#![doc = include_str!("../docs/window_manager.md")]

// Use local facade for common types
use crate::common::*;

// Window manager specific imports
use sinex_core::payloads::{
    HyprlandMonitorFocusedPayload, HyprlandStateCapturedPayload, HyprlandWindowClosedPayload,
    HyprlandWindowFocusedPayload, HyprlandWindowMovedPayload, HyprlandWindowOpenedPayload,
    HyprlandWorkspaceSwitchedPayload, WindowGeometry,
};
use sinex_core::types::domain::{EventSource, EventType};
use sinex_core::types::events::EventPayload;
use sinex_core::{EventBuilder, Id, OffsetKind, Provenance, Ulid};
use sinex_node_sdk::stage_as_you_go::StageAsYouGoContext;
use std::{fmt, str::FromStr, time::SystemTime};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::watch;
use tokio::time::sleep;
#[cfg(not(test))]
use tokio_retry::strategy::jitter;
use tokio_retry::strategy::ExponentialBackoff;

/// Supported window manager types
///
/// TODO: Add support for additional window managers:
/// - Sway/i3 (i3 IPC protocol via i3ipc-rs)
/// - GNOME (D-Bus org.gnome.Shell interface)
/// - KDE Plasma (KWin D-Bus interface)
/// - X11 WMs (EWMH/X11 protocol via x11rb)
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum WindowManagerType {
    Hyprland,
}

/// Initial backoff delay for Hyprland reconnection attempts (milliseconds)
///
/// This is the starting point for the exponential backoff strategy. After a connection
/// failure, the watcher will wait this long before the first retry attempt.
const HYPRLAND_INITIAL_BACKOFF_MS: u64 = 500;

/// Maximum backoff delay for Hyprland reconnection attempts
///
/// The exponential backoff will cap at this value to prevent excessive delays.
/// With a 500ms initial backoff and factor of 2, this is reached after ~7 attempts.
const HYPRLAND_MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Time-to-live for window state entries in memory
///
/// Windows that haven't been seen in this duration will be removed from the internal
/// tracking map to prevent unbounded memory growth. This cleanup happens during the
/// periodic state snapshot.
const WINDOW_STATE_TTL: Duration = Duration::from_secs(48 * 60 * 60); // 48 hours

/// Socket read timeout for Hyprland event stream
///
/// If no events are received within this duration, the connection is considered stale
/// and will be re-established. This prevents hanging on a dead connection.
const HYPRLAND_SOCKET_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Interval between periodic state snapshots
///
/// A full snapshot of window and workspace state is captured at this interval to
/// provide a consistent baseline and to trigger stale window cleanup.
const STATE_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes

type BackoffStrategy = Box<dyn Iterator<Item = Duration> + Send>;

impl fmt::Display for WindowManagerType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WindowManagerType::Hyprland => write!(f, "hyprland"),
        }
    }
}

impl WindowManagerType {
    /// Returns the string representation of the window manager type
    pub fn as_str(&self) -> &str {
        match self {
            WindowManagerType::Hyprland => "hyprland",
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

/// Window manager watcher with Stage-as-You-Go capture
pub struct WindowManagerWatcher {
    wm_type: WindowManagerType,
    socket_path: Option<String>,
    command_socket_path: Option<String>,
    windows: HashMap<String, WindowInfo>,
    workspaces: HashMap<String, WorkspaceInfo>,
    current_focused_window: Option<String>,
    current_workspace: Option<String>,
    current_monitor: Option<String>,
    // Stage-as-you-go integration
    stage_context: Option<StageAsYouGoContext>,
    shutdown_rx: watch::Receiver<bool>,
    source_identifier: String,
}

impl WindowManagerWatcher {
    /// Create new window manager watcher with Stage-as-You-Go integration
    pub async fn new(
        wm_type: WindowManagerType,
        stage_context: StageAsYouGoContext,
        shutdown_rx: watch::Receiver<bool>,
    ) -> NodeResult<Self> {
        let mut watcher = Self {
            wm_type: wm_type.clone(),
            socket_path: None,
            command_socket_path: None,
            windows: HashMap::new(),
            workspaces: HashMap::new(),
            current_focused_window: None,
            current_workspace: None,
            current_monitor: None,
            stage_context: Some(stage_context),
            shutdown_rx,
            source_identifier: "desktop_window_manager".to_string(),
        };

        // Discover socket paths based on WM type
        if wm_type == WindowManagerType::Hyprland {
            watcher.discover_hyprland_sockets().await?;
        } else {
            return Err(sinex_node_sdk::NodeError::Processing(format!(
                "Unsupported window manager: {}",
                wm_type
            )));
        }

        info!(
            "Window manager watcher initialized for {} (stage-as-you-go mode)",
            wm_type
        );
        Ok(watcher)
    }

    /// Discover Hyprland socket paths (both event and command)
    async fn discover_hyprland_sockets(&mut self) -> NodeResult<()> {
        // Get Hyprland instance signature
        let hyprland_instance_sig = std::env::var("HYPRLAND_INSTANCE_SIGNATURE").map_err(|_| {
            sinex_node_sdk::NodeError::Processing(
                "HYPRLAND_INSTANCE_SIGNATURE not set. Is Hyprland running?".to_string(),
            )
        })?;

        // Get XDG_RUNTIME_DIR
        let xdg_runtime = std::env::var("XDG_RUNTIME_DIR").map_err(|_| {
            sinex_node_sdk::NodeError::Processing("XDG_RUNTIME_DIR not set".to_string())
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
            return Err(sinex_node_sdk::NodeError::Processing(format!(
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

    async fn register_material(
        &self,
        event_type: &str,
        metadata: serde_json::Value,
    ) -> NodeResult<Ulid> {
        let stage_context = self.stage_context.as_ref().ok_or_else(|| {
            sinex_node_sdk::NodeError::Processing(
                "Stage-as-you-go context not initialized".to_string(),
            )
        })?;

        stage_context
            .register_in_flight(&self.source_identifier, Some(event_type), metadata)
            .await
    }

    fn build_material_payload(
        &self,
        event_type: &str,
        event_data: &str,
        metadata: serde_json::Value,
    ) -> serde_json::Value {
        serde_json::json!({
            "event_type": event_type,
            "event_data": event_data,
            "wm_type": self.wm_type.to_string(),
            "timestamp": Utc::now(),
            "metadata": metadata,
        })
    }

    async fn emit_material_event(
        &self,
        material_id: Ulid,
        payload_bytes: Vec<u8>,
        mut event: Event<JsonValue>,
    ) -> NodeResult<()> {
        let stage_context = self.stage_context.as_ref().ok_or_else(|| {
            sinex_node_sdk::NodeError::Processing(
                "Stage-as-you-go context not initialized".to_string(),
            )
        })?;

        event.id = Some(Id::from_ulid(Ulid::new()));
        let offset_end = payload_bytes.len() as i64;

        stage_context
            .emit_event_with_provenance(event, material_id, Some(0), Some(offset_end))
            .await?;
        stage_context
            .finalize_source_material(
                material_id,
                &payload_bytes,
                Some("application/json"),
                Some("utf-8"),
            )
            .await?;
        Ok(())
    }

    /// Connect to Hyprland event socket
    async fn connect_to_hyprland_events(&self) -> NodeResult<UnixStream> {
        let socket_path = self.socket_path.as_ref().ok_or_else(|| {
            sinex_node_sdk::NodeError::Processing("No Hyprland socket configured".to_string())
        })?;

        UnixStream::connect(socket_path).await.map_err(|e| {
            sinex_node_sdk::NodeError::Processing(format!("Failed to connect to Hyprland: {}", e))
        })
    }

    /// Parse ID string to integer with fallback and warning
    fn parse_id(&self, id_str: &str, context: &str) -> i32 {
        id_str.parse().unwrap_or_else(|_| {
            warn!("Failed to parse {} '{}', defaulting to 0", context, id_str);
            0
        })
    }

    fn hyprland_backoff() -> BackoffStrategy {
        #[cfg(test)]
        {
            Box::new(Self::base_backoff_strategy())
        }
        #[cfg(not(test))]
        {
            Box::new(Self::base_backoff_strategy().map(jitter))
        }
    }

    fn base_backoff_strategy() -> ExponentialBackoff {
        ExponentialBackoff::from_millis(HYPRLAND_INITIAL_BACKOFF_MS)
            .factor(2)
            .max_delay(HYPRLAND_MAX_BACKOFF)
    }

    fn next_backoff(backoff: &mut BackoffStrategy) -> Duration {
        backoff.next().unwrap_or(HYPRLAND_MAX_BACKOFF)
    }

    /// Process Hyprland event line
    async fn process_hyprland_event(&mut self, line: &str) -> NodeResult<()> {
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
                    let metadata = serde_json::json!({
                        "unhandled": true,
                        "event_type": event_type,
                    });
                    let material_id = self.register_material(event_type, metadata.clone()).await?;
                    let material_payload =
                        self.build_material_payload(event_type, event_data, metadata);
                    let payload_bytes = serde_json::to_vec(&material_payload).map_err(|e| {
                        sinex_node_sdk::NodeError::Processing(format!(
                            "Failed to serialize window material payload: {e}"
                        ))
                    })?;
                    let payload = serde_json::json!({
                        "event_type": event_type,
                        "event_data": event_data,
                    });
                    let provenance = Provenance::Material {
                        id: Id::from_ulid(material_id),
                        anchor_byte: 0,
                        offset_start: Some(0),
                        offset_end: Some(payload_bytes.len() as i64),
                        offset_kind: OffsetKind::Byte,
                    };
                    let event = EventBuilder::dynamic(
                        EventSource::from_static("wm.hyprland"),
                        EventType::from_static("wm.unhandled"),
                        payload,
                    )
                    .with_provenance(provenance)
                    .build()
                    .map_err(|e| {
                        sinex_node_sdk::NodeError::Processing(format!("Failed to build event: {e}"))
                    })?;
                    self.emit_material_event(material_id, payload_bytes, event)
                        .await?;
                }
            }
        }

        Ok(())
    }

    /// Handle window focused event
    async fn handle_window_focused(&mut self, data: &str) -> NodeResult<()> {
        // Format: "class,title"
        if let Some((class, title)) = data.split_once(',') {
            // Try to find existing window by class and title, otherwise use deterministic hash
            let window_address = self
                .windows
                .iter()
                .find(|(_, info)| info.class == class && info.title == title)
                .map(|(addr, _)| addr.clone())
                .unwrap_or_else(|| {
                    use std::collections::hash_map::DefaultHasher;
                    use std::hash::{Hash, Hasher};
                    let mut hasher = DefaultHasher::new();
                    class.hash(&mut hasher);
                    title.hash(&mut hasher);
                    format!("0x{:x}", hasher.finish())
                });

            let workspace_id = self
                .current_workspace
                .as_deref()
                .map(|id| self.parse_id(id, "workspace_id"))
                .unwrap_or(0);
            let metadata = serde_json::json!({
                "window_class": class,
                "window_title": title,
                "window_id": window_address,
                "previous_window_id": self.current_focused_window,
                "workspace_id": workspace_id,
            });
            let material_id = self
                .register_material("focusedwindow", metadata.clone())
                .await?;
            let material_payload = self.build_material_payload("focusedwindow", data, metadata);
            let payload_bytes = serde_json::to_vec(&material_payload).map_err(|e| {
                sinex_node_sdk::NodeError::Processing(format!(
                    "Failed to serialize window material payload: {e}"
                ))
            })?;
            let payload = HyprlandWindowFocusedPayload {
                window_id: window_address.clone(),
                window_class: class.to_string(),
                window_title: title.to_string(),
                workspace_id,
                previous_window_id: self.current_focused_window.clone(),
            };
            let event = payload
                .from_material(material_id)
                .with_offset_start(0)
                .map_err(|e| {
                    sinex_node_sdk::NodeError::Processing(format!(
                        "Failed to set offset_start: {e}"
                    ))
                })?
                .with_offset_end(payload_bytes.len() as i64)
                .map_err(|e| {
                    sinex_node_sdk::NodeError::Processing(format!("Failed to set offset_end: {e}"))
                })?
                .build()
                .map_err(|e| {
                    sinex_node_sdk::NodeError::Processing(format!("Failed to build event: {e}"))
                })?
                .to_json_event()
                .map_err(|e| {
                    sinex_node_sdk::NodeError::Processing(format!(
                        "Failed to serialize window focused payload: {e}"
                    ))
                })?;
            self.emit_material_event(material_id, payload_bytes, event)
                .await?;

            // Update last_seen timestamp for focused window
            if let Some(window) = self.windows.get_mut(&window_address) {
                window.last_seen = SystemTime::now();
            }

            self.current_focused_window = Some(window_address);
        }

        Ok(())
    }

    /// Handle window opened event
    async fn handle_window_opened(&mut self, data: &str) -> NodeResult<()> {
        // Format: "address,workspace,class,title"
        let parts: Vec<&str> = data.split(',').collect();
        if parts.len() >= 4 {
            let window_address = parts[0].to_string();
            let workspace_id = parts[1].to_string();
            let window_class = parts[2].to_string();
            let window_title = parts[3..].join(","); // Title might contain commas

            let workspace_id_parsed = self.parse_id(&workspace_id, "workspace_id");
            let metadata = serde_json::json!({
                "window_id": window_address,
                "window_class": window_class,
                "window_title": window_title,
                "workspace_id": workspace_id_parsed,
            });
            let material_id = self
                .register_material("openwindow", metadata.clone())
                .await?;
            let material_payload = self.build_material_payload("openwindow", data, metadata);
            let payload_bytes = serde_json::to_vec(&material_payload).map_err(|e| {
                sinex_node_sdk::NodeError::Processing(format!(
                    "Failed to serialize window material payload: {e}"
                ))
            })?;
            let payload = HyprlandWindowOpenedPayload {
                window_id: window_address.clone(),
                window_class: window_class.clone(),
                window_title: window_title.clone(),
                workspace_id: workspace_id_parsed,
                monitor_id: 0,
                geometry: WindowGeometry {
                    x: 0,
                    y: 0,
                    width: 0,
                    height: 0,
                },
                floating: false,
            };
            let event = payload
                .from_material(material_id)
                .with_offset_start(0)
                .map_err(|e| {
                    sinex_node_sdk::NodeError::Processing(format!(
                        "Failed to set offset_start: {e}"
                    ))
                })?
                .with_offset_end(payload_bytes.len() as i64)
                .map_err(|e| {
                    sinex_node_sdk::NodeError::Processing(format!("Failed to set offset_end: {e}"))
                })?
                .build()
                .map_err(|e| {
                    sinex_node_sdk::NodeError::Processing(format!("Failed to build event: {e}"))
                })?
                .to_json_event()
                .map_err(|e| {
                    sinex_node_sdk::NodeError::Processing(format!(
                        "Failed to serialize window opened payload: {e}"
                    ))
                })?;
            self.emit_material_event(material_id, payload_bytes, event)
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
    async fn handle_window_closed(&mut self, data: &str) -> NodeResult<()> {
        let window_address = data.trim().to_string();

        let window_info = self.windows.get(&window_address).cloned();
        let metadata = serde_json::json!({
            "window_id": window_address,
            "was_tracked": window_info.is_some(),
        });
        let material_id = self
            .register_material("closewindow", metadata.clone())
            .await?;
        let material_payload = self.build_material_payload("closewindow", data, metadata);
        let payload_bytes = serde_json::to_vec(&material_payload).map_err(|e| {
            sinex_node_sdk::NodeError::Processing(format!(
                "Failed to serialize window material payload: {e}"
            ))
        })?;
        let payload = HyprlandWindowClosedPayload {
            window_id: window_address.clone(),
            window_class: window_info
                .as_ref()
                .map(|info| info.class.clone())
                .unwrap_or_default(),
            window_title: window_info
                .as_ref()
                .map(|info| info.title.clone())
                .unwrap_or_default(),
            workspace_id: window_info
                .as_ref()
                .map(|info| self.parse_id(&info.workspace_id, "workspace_id"))
                .unwrap_or(0),
            close_reason: None,
        };
        let event = payload
            .from_material(material_id)
            .with_offset_start(0)
            .map_err(|e| {
                sinex_node_sdk::NodeError::Processing(format!("Failed to set offset_start: {e}"))
            })?
            .with_offset_end(payload_bytes.len() as i64)
            .map_err(|e| {
                sinex_node_sdk::NodeError::Processing(format!("Failed to set offset_end: {e}"))
            })?
            .build()
            .map_err(|e| {
                sinex_node_sdk::NodeError::Processing(format!("Failed to build event: {e}"))
            })?
            .to_json_event()
            .map_err(|e| {
                sinex_node_sdk::NodeError::Processing(format!(
                    "Failed to serialize window closed payload: {e}"
                ))
            })?;
        self.emit_material_event(material_id, payload_bytes, event)
            .await?;

        // Remove from tracking
        self.windows.remove(&window_address);

        Ok(())
    }

    /// Handle window moved event
    async fn handle_window_moved(&mut self, data: &str) -> NodeResult<()> {
        // Format: "address,workspace"
        if let Some((address, workspace)) = data.split_once(',') {
            let metadata = serde_json::json!({
                "window_address": address,
                "new_workspace_id": self.parse_id(workspace, "workspace_id"),
            });
            let material_id = self
                .register_material("movewindow", metadata.clone())
                .await?;
            let material_payload = self.build_material_payload("movewindow", data, metadata);
            let payload_bytes = serde_json::to_vec(&material_payload).map_err(|e| {
                sinex_node_sdk::NodeError::Processing(format!(
                    "Failed to serialize window material payload: {e}"
                ))
            })?;
            let payload = HyprlandWindowMovedPayload {
                window_address: address.to_string(),
                new_workspace_id: self.parse_id(workspace, "workspace_id"),
                moved_at: Utc::now().to_rfc3339(),
            };
            let event = payload
                .from_material(material_id)
                .with_offset_start(0)
                .map_err(|e| {
                    sinex_node_sdk::NodeError::Processing(format!(
                        "Failed to set offset_start: {e}"
                    ))
                })?
                .with_offset_end(payload_bytes.len() as i64)
                .map_err(|e| {
                    sinex_node_sdk::NodeError::Processing(format!("Failed to set offset_end: {e}"))
                })?
                .build()
                .map_err(|e| {
                    sinex_node_sdk::NodeError::Processing(format!("Failed to build event: {e}"))
                })?
                .to_json_event()
                .map_err(|e| {
                    sinex_node_sdk::NodeError::Processing(format!(
                        "Failed to serialize window moved payload: {e}"
                    ))
                })?;
            self.emit_material_event(material_id, payload_bytes, event)
                .await?;

            // Update window workspace and last_seen timestamp
            if let Some(window) = self.windows.get_mut(address) {
                window.workspace_id = workspace.to_string();
                window.last_seen = SystemTime::now();
            }
        }

        Ok(())
    }

    /// Handle workspace changed event
    async fn handle_workspace_changed(&mut self, data: &str) -> NodeResult<()> {
        let workspace_id = data.trim().to_string();

        let metadata = serde_json::json!({
            "from_workspace_id": self.current_workspace.as_ref()
                .map(|w| self.parse_id(w, "current_workspace_id"))
                .unwrap_or(0),
            "to_workspace_id": self.parse_id(&workspace_id, "workspace_id"),
        });
        let material_id = self
            .register_material("workspace", metadata.clone())
            .await?;
        let material_payload = self.build_material_payload("workspace", data, metadata);
        let payload_bytes = serde_json::to_vec(&material_payload).map_err(|e| {
            sinex_node_sdk::NodeError::Processing(format!(
                "Failed to serialize window material payload: {e}"
            ))
        })?;
        let payload = HyprlandWorkspaceSwitchedPayload {
            from_workspace_id: self
                .current_workspace
                .as_ref()
                .map(|w| self.parse_id(w, "current_workspace_id"))
                .unwrap_or(0),
            to_workspace_id: self.parse_id(&workspace_id, "workspace_id"),
            monitor_id: self
                .current_monitor
                .as_ref()
                .map(|m| self.parse_id(m, "monitor_id"))
                .unwrap_or(0),
            active_window_id: self.current_focused_window.clone(),
        };
        let event = payload
            .from_material(material_id)
            .with_offset_start(0)
            .map_err(|e| {
                sinex_node_sdk::NodeError::Processing(format!("Failed to set offset_start: {e}"))
            })?
            .with_offset_end(payload_bytes.len() as i64)
            .map_err(|e| {
                sinex_node_sdk::NodeError::Processing(format!("Failed to set offset_end: {e}"))
            })?
            .build()
            .map_err(|e| {
                sinex_node_sdk::NodeError::Processing(format!("Failed to build event: {e}"))
            })?
            .to_json_event()
            .map_err(|e| {
                sinex_node_sdk::NodeError::Processing(format!(
                    "Failed to serialize workspace switched payload: {e}"
                ))
            })?;
        self.emit_material_event(material_id, payload_bytes, event)
            .await?;

        self.current_workspace = Some(workspace_id);

        Ok(())
    }

    /// Handle monitor focused event
    async fn handle_monitor_focused(&mut self, data: &str) -> NodeResult<()> {
        // Format: "monitor,workspace"
        if let Some((monitor, workspace)) = data.split_once(',') {
            let metadata = serde_json::json!({
                "monitor_id": self.parse_id(monitor, "monitor_id"),
                "workspace_id": self.parse_id(workspace, "workspace_id"),
                "previous_monitor": self.current_monitor,
            });
            let material_id = self
                .register_material("focusedmon", metadata.clone())
                .await?;
            let material_payload = self.build_material_payload("focusedmon", data, metadata);
            let payload_bytes = serde_json::to_vec(&material_payload).map_err(|e| {
                sinex_node_sdk::NodeError::Processing(format!(
                    "Failed to serialize window material payload: {e}"
                ))
            })?;
            let payload = HyprlandMonitorFocusedPayload {
                monitor_id: self.parse_id(monitor, "monitor_id"),
                workspace_id: self.parse_id(workspace, "workspace_id"),
                previous_monitor: self
                    .current_monitor
                    .as_ref()
                    .map(|m| self.parse_id(m, "monitor_id")),
                focused_at: Utc::now().to_rfc3339(),
            };
            let event = payload
                .from_material(material_id)
                .with_offset_start(0)
                .map_err(|e| {
                    sinex_node_sdk::NodeError::Processing(format!(
                        "Failed to set offset_start: {e}"
                    ))
                })?
                .with_offset_end(payload_bytes.len() as i64)
                .map_err(|e| {
                    sinex_node_sdk::NodeError::Processing(format!("Failed to set offset_end: {e}"))
                })?
                .build()
                .map_err(|e| {
                    sinex_node_sdk::NodeError::Processing(format!("Failed to build event: {e}"))
                })?
                .to_json_event()
                .map_err(|e| {
                    sinex_node_sdk::NodeError::Processing(format!(
                        "Failed to serialize monitor focused payload: {e}"
                    ))
                })?;
            self.emit_material_event(material_id, payload_bytes, event)
                .await?;

            self.current_monitor = Some(monitor.to_string());
        }

        Ok(())
    }

    /// Start monitoring window manager events (stage-as-you-go mode)
    pub async fn start_monitoring(&mut self) -> NodeResult<()> {
        info!(
            "Starting window manager event monitoring for {} (stage-as-you-go mode)",
            self.wm_type
        );

        if self.wm_type == WindowManagerType::Hyprland {
            self.stream_hyprland_events().await
        } else {
            Err(sinex_node_sdk::NodeError::Processing(format!(
                "Unsupported window manager: {}",
                self.wm_type
            )))
        }
    }

    /// Stream Hyprland events with exponential backoff reconnection
    async fn stream_hyprland_events(&mut self) -> NodeResult<()> {
        let mut consecutive_failures = 0;
        let mut reconnect_backoff = Self::hyprland_backoff();

        loop {
            if *self.shutdown_rx.borrow() {
                info!("Window manager watcher shutdown requested");
                break;
            }

            match self.connect_to_hyprland_events().await {
                Ok(stream) => {
                    info!("Connected to Hyprland event stream");
                    consecutive_failures = 0; // Reset on successful connection
                    reconnect_backoff = Self::hyprland_backoff();

                    let reader = BufReader::new(stream);
                    let mut lines = reader.lines();

                    loop {
                        tokio::select! {
                            shutdown_result = self.shutdown_rx.changed() => {
                                if shutdown_result.is_err() || *self.shutdown_rx.borrow() {
                                    info!("Window manager watcher shutdown requested");
                                    return Ok(());
                                }
                            }
                            // Read Hyprland events with timeout
                            line_result = tokio::time::timeout(
                                HYPRLAND_SOCKET_READ_TIMEOUT,
                                lines.next_line()
                            ) => {
                                match line_result {
                                    Ok(Ok(Some(line))) => {
                                        if let Err(e) = self.process_hyprland_event(&line).await {
                                            error!("Error processing Hyprland event: {}", e);
                                        }
                                    }
                                    Ok(Ok(None)) => {
                                        warn!("Hyprland event stream ended");
                                        break;
                                    }
                                    Ok(Err(e)) => {
                                        error!("Error reading from Hyprland socket: {}", e);
                                        break;
                                    }
                                    Err(_) => {
                                        warn!("Hyprland socket read timeout ({:?}), reconnecting", HYPRLAND_SOCKET_READ_TIMEOUT);
                                        break;
                                    }
                                }
                            }

                            // Periodic state capture
                            _ = sleep(STATE_SNAPSHOT_INTERVAL) => {
                                if let Err(e) = self.capture_state_snapshot().await {
                                    error!("Error capturing state snapshot: {}", e);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    consecutive_failures += 1;
                    error!(
                        "Failed to connect to Hyprland (attempt {}): {}",
                        consecutive_failures, e
                    );

                    let jittered_delay = Self::next_backoff(&mut reconnect_backoff);
                    warn!("Reconnecting to Hyprland in {:?}...", jittered_delay);
                    tokio::select! {
                        _ = sleep(jittered_delay) => {}
                        shutdown_result = self.shutdown_rx.changed() => {
                            if shutdown_result.is_err() || *self.shutdown_rx.borrow() {
                                info!("Window manager watcher shutdown requested");
                                return Ok(());
                            }
                        }
                    }
                    continue;
                }
            }

            consecutive_failures += 1;

            let jittered_delay = Self::next_backoff(&mut reconnect_backoff);
            warn!(
                "Hyprland connection lost, reconnecting in {:?}...",
                jittered_delay
            );
            tokio::select! {
                _ = sleep(jittered_delay) => {}
                shutdown_result = self.shutdown_rx.changed() => {
                    if shutdown_result.is_err() || *self.shutdown_rx.borrow() {
                        info!("Window manager watcher shutdown requested");
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }

    /// Clean up stale window entries older than TTL
    fn cleanup_stale_windows(&mut self) {
        let now = SystemTime::now();
        let initial_count = self.windows.len();

        self.windows.retain(|_addr, window| {
            if let Ok(elapsed) = now.duration_since(window.last_seen) {
                if elapsed > WINDOW_STATE_TTL {
                    debug!(
                        "Removing stale window entry: {} (class: {}, last seen: {:?} ago)",
                        window.address, window.class, elapsed
                    );
                    return false;
                }
            }
            true
        });

        let removed_count = initial_count - self.windows.len();
        if removed_count > 0 {
            info!(
                "Cleaned up {} stale window entries (TTL: {:?})",
                removed_count, WINDOW_STATE_TTL
            );
        }
    }

    /// Capture periodic state snapshot
    async fn capture_state_snapshot(&mut self) -> NodeResult<()> {
        debug!("Capturing window manager state snapshot");

        // Clean up stale windows before capturing state
        self.cleanup_stale_windows();

        let windows = self
            .windows
            .values()
            .map(|window| {
                serde_json::to_value(window).unwrap_or_else(|e| {
                    warn!("Failed to serialize window info: {}", e);
                    serde_json::json!({})
                })
            })
            .collect::<Vec<_>>();
        let workspaces = self
            .workspaces
            .values()
            .map(|workspace| {
                serde_json::to_value(workspace).unwrap_or_else(|e| {
                    warn!("Failed to serialize workspace info: {}", e);
                    serde_json::json!({})
                })
            })
            .collect::<Vec<_>>();
        let metadata = serde_json::json!({
            "snapshot": true,
            "window_count": self.windows.len(),
            "workspace_count": self.workspaces.len(),
        });
        let snapshot_payload = HyprlandStateCapturedPayload {
            windows,
            workspaces,
            monitors: Vec::new(),
            current_workspace: self
                .current_workspace
                .as_ref()
                .map(|w| self.parse_id(w, "current_workspace_id"))
                .unwrap_or(0),
            current_monitor: self
                .current_monitor
                .as_ref()
                .map(|m| self.parse_id(m, "monitor_id"))
                .unwrap_or(0),
            captured_at: Utc::now().to_rfc3339(),
        };
        let material_payload = self.build_material_payload(
            "state_snapshot",
            &serde_json::to_string(&snapshot_payload).unwrap_or_else(|_| "{}".to_string()),
            metadata.clone(),
        );
        let payload_bytes = serde_json::to_vec(&material_payload).map_err(|e| {
            sinex_node_sdk::NodeError::Processing(format!(
                "Failed to serialize window material payload: {e}"
            ))
        })?;
        let material_id = self.register_material("state_snapshot", metadata).await?;
        let event = snapshot_payload
            .from_material(material_id)
            .with_offset_start(0)
            .map_err(|e| {
                sinex_node_sdk::NodeError::Processing(format!("Failed to set offset_start: {e}"))
            })?
            .with_offset_end(payload_bytes.len() as i64)
            .map_err(|e| {
                sinex_node_sdk::NodeError::Processing(format!("Failed to set offset_end: {e}"))
            })?
            .build()
            .map_err(|e| {
                sinex_node_sdk::NodeError::Processing(format!("Failed to build event: {e}"))
            })?
            .to_json_event()
            .map_err(|e| {
                sinex_node_sdk::NodeError::Processing(format!(
                    "Failed to serialize window snapshot payload: {e}"
                ))
            })?;
        self.emit_material_event(material_id, payload_bytes, event)
            .await?;

        Ok(())
    }
}

#[cfg(test)]
impl WindowManagerWatcher {
    pub fn stub(wm_type: WindowManagerType) -> Self {
        Self {
            wm_type,
            socket_path: Some("/tmp/hyprland-stub.sock".to_string()),
            command_socket_path: None,
            windows: HashMap::new(),
            workspaces: HashMap::new(),
            current_focused_window: None,
            current_workspace: None,
            current_monitor: None,
            stage_context: None,
            shutdown_rx: watch::channel(false).1,
            source_identifier: "desktop_window_manager_stub".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn hyprland_backoff_grows_until_cap() {
        let mut backoff = WindowManagerWatcher::hyprland_backoff();
        let mut last_delay = Duration::from_millis(0);

        for _ in 0..10 {
            let next = WindowManagerWatcher::next_backoff(&mut backoff);
            assert!(
                next >= last_delay,
                "backoff sequence should be monotonically increasing"
            );
            last_delay = next;
        }

        assert_eq!(
            last_delay, HYPRLAND_MAX_BACKOFF,
            "backoff should saturate at the configured maximum"
        );
    }

    #[test]
    fn hyprland_backoff_resets_after_success() {
        let mut backoff = WindowManagerWatcher::hyprland_backoff();
        let first = WindowManagerWatcher::next_backoff(&mut backoff);
        assert!(
            first >= Duration::from_millis(HYPRLAND_INITIAL_BACKOFF_MS),
            "first delay should never be smaller than the configured initial backoff"
        );

        for _ in 0..5 {
            WindowManagerWatcher::next_backoff(&mut backoff);
        }

        let mut reset = WindowManagerWatcher::hyprland_backoff();
        let reset_first = WindowManagerWatcher::next_backoff(&mut reset);
        assert_eq!(
            reset_first, first,
            "resetting the backoff should restart the sequence"
        );
    }
}
