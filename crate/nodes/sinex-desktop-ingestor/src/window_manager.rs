#![doc = include_str!("../docs/window_manager.md")]

// Use local facade for common types
use crate::common::{
    Duration, Event, HashMap, JsonValue, NodeResult, Timestamp, debug, error, info, warn,
};
use sinex_primitives::env as shared_env;
use sinex_primitives::privacy::{self, ProcessingContext};

// Window manager specific imports
use sinex_node_sdk::stage_as_you_go::StageAsYouGoContext;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{
    HyprlandMonitorFocusedPayload, HyprlandStateCapturedPayload, HyprlandWindowClosedPayload,
    HyprlandWindowFocusedPayload, HyprlandWindowMovedPayload, HyprlandWindowOpenedPayload,
    HyprlandWorkspaceSwitchedPayload, WindowGeometry,
};
use sinex_primitives::{DynamicPayload, Id, OffsetKind, Provenance, Uuid};
use std::{
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
    time::SystemTime,
};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::watch;
use tokio::time::sleep;
use tokio_retry::strategy::ExponentialBackoff;
#[cfg(not(test))]
use tokio_retry::strategy::jitter;

/// Supported window manager types
/// Current runtime scope is Hyprland-only.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
const HYPRLAND_MAX_BACKOFF: Duration = Duration::from_mins(1);

/// Time-to-live for window state entries in memory
///
/// Windows that haven't been seen in this duration will be removed from the internal
/// tracking map to prevent unbounded memory growth. This cleanup happens during the
/// periodic state snapshot.
const WINDOW_STATE_TTL: Duration = Duration::from_hours(48);

/// Socket read timeout for Hyprland event stream
///
/// If no events are received within this duration, the connection is considered stale
/// and will be re-established. This prevents hanging on a dead connection.
const HYPRLAND_SOCKET_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Interval between periodic state snapshots
///
/// A full snapshot of window and workspace state is captured at this interval to
/// provide a consistent baseline and to trigger stale window cleanup.
const STATE_SNAPSHOT_INTERVAL: Duration = Duration::from_mins(5); // 5 minutes

type BackoffStrategy = Box<dyn Iterator<Item = Duration> + Send>;

const ERROR_CLASS_UNSUPPORTED_WINDOW_MANAGER: &str = "desktop_platform_unsupported_window_manager";
const ERROR_CLASS_HYPRLAND_SIGNATURE_MISSING: &str = "desktop_platform_hyprland_signature_missing";
const ERROR_CLASS_XDG_RUNTIME_MISSING: &str = "desktop_platform_xdg_runtime_missing";
const ERROR_CLASS_HYPRLAND_EVENT_SOCKET_UNAVAILABLE: &str =
    "desktop_platform_hyprland_event_socket_unavailable";

fn platform_error(message: impl Into<String>, class: &'static str) -> sinex_node_sdk::SinexError {
    sinex_node_sdk::SinexError::processing(message.into()).with_context("error_class", class)
}

fn env_string(name: &str) -> NodeResult<Option<String>> {
    shared_env::strict_var(name).map_err(|err| {
        platform_error(
            err.to_string(),
            ERROR_CLASS_HYPRLAND_EVENT_SOCKET_UNAVAILABLE,
        )
    })
}

fn collect_hyprland_candidates<I>(entries: I, hypr_dir: &Path) -> NodeResult<Vec<PathBuf>>
where
    I: IntoIterator<Item = std::io::Result<PathBuf>>,
{
    let mut candidates = Vec::new();

    for entry in entries {
        let path = entry.map_err(|error| {
            platform_error(
                format!(
                    "Cannot inspect Hyprland runtime entry under {}: {error}",
                    hypr_dir.display()
                ),
                ERROR_CLASS_HYPRLAND_EVENT_SOCKET_UNAVAILABLE,
            )
        })?;

        let socket_path = path.join(".socket2.sock");
        let has_socket = socket_path.try_exists().map_err(|error| {
            platform_error(
                format!(
                    "Cannot probe Hyprland event socket {}: {error}",
                    socket_path.display()
                ),
                ERROR_CLASS_HYPRLAND_EVENT_SOCKET_UNAVAILABLE,
            )
        })?;

        if has_socket {
            candidates.push(path);
        }
    }

    Ok(candidates)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HyprlandSocketPaths {
    event_socket: String,
    command_socket: String,
}

fn resolve_hyprland_runtime_dir() -> NodeResult<PathBuf> {
    if let Some(runtime_dir) = env_string("SINEX_HYPRLAND_RUNTIME_DIR")? {
        return Ok(PathBuf::from(runtime_dir));
    }

    if let Some(runtime_dir) = env_string("XDG_RUNTIME_DIR")? {
        return Ok(PathBuf::from(runtime_dir));
    }

    dirs::runtime_dir().ok_or_else(|| {
        platform_error(
            "No Hyprland runtime dir found. Set SINEX_HYPRLAND_RUNTIME_DIR or XDG_RUNTIME_DIR.",
            ERROR_CLASS_XDG_RUNTIME_MISSING,
        )
    })
}

fn derive_hyprland_command_socket(event_socket: &str) -> String {
    Path::new(event_socket)
        .parent()
        .map(|parent| parent.join(".socket.sock").to_string_lossy().into_owned())
        .unwrap_or_else(|| event_socket.replacen(".socket2.sock", ".socket.sock", 1))
}

fn parse_hyprland_numeric_id(raw: &str, context: &str) -> NodeResult<i32> {
    if let Ok(id) = raw.parse() {
        return Ok(id);
    }

    let mut suffix_start = raw.len();
    for (index, byte) in raw.bytes().enumerate().rev() {
        if byte.is_ascii_digit() {
            suffix_start = index;
        } else {
            break;
        }
    }

    if suffix_start < raw.len() {
        return raw[suffix_start..].parse().map_err(|error| {
            sinex_node_sdk::SinexError::processing(format!(
                "Failed to parse {context} '{raw}' as integer: {error}"
            ))
            .with_context("id_context", context)
            .with_context("id_value", raw.to_string())
        });
    }

    Err(sinex_node_sdk::SinexError::processing(format!(
        "Failed to parse {context} '{raw}' as integer"
    ))
    .with_context("id_context", context)
    .with_context("id_value", raw.to_string()))
}

fn select_hyprland_base_path(
    runtime_dir: &Path,
    explicit_signature: Option<String>,
) -> NodeResult<PathBuf> {
    let hypr_dir = runtime_dir.join("hypr");

    if let Some(signature) = explicit_signature {
        return Ok(hypr_dir.join(signature));
    }

    let entries = std::fs::read_dir(&hypr_dir).map_err(|error| {
        platform_error(
            format!(
                "Cannot read Hyprland runtime directory {}: {error}",
                hypr_dir.display()
            ),
            ERROR_CLASS_HYPRLAND_EVENT_SOCKET_UNAVAILABLE,
        )
    })?;

    let candidates = collect_hyprland_candidates(
        entries.map(|entry| entry.map(|value| value.path())),
        &hypr_dir,
    )?;

    match candidates.as_slice() {
        [candidate] => Ok(candidate.clone()),
        [] => Err(platform_error(
            format!(
                "No Hyprland event sockets found under {}",
                hypr_dir.display()
            ),
            ERROR_CLASS_HYPRLAND_EVENT_SOCKET_UNAVAILABLE,
        )),
        _ => Err(platform_error(
            format!(
                "Multiple Hyprland instances found under {}; set SINEX_HYPRLAND_INSTANCE_SIGNATURE or SINEX_HYPRLAND_EVENT_SOCKET",
                hypr_dir.display()
            ),
            ERROR_CLASS_HYPRLAND_SIGNATURE_MISSING,
        )),
    }
}

fn resolve_hyprland_socket_paths() -> NodeResult<HyprlandSocketPaths> {
    if let Some(event_socket) = env_string("SINEX_HYPRLAND_EVENT_SOCKET")? {
        let command_socket = env_string("SINEX_HYPRLAND_COMMAND_SOCKET")?
            .unwrap_or_else(|| derive_hyprland_command_socket(&event_socket));
        return Ok(HyprlandSocketPaths {
            event_socket,
            command_socket,
        });
    }

    let runtime_dir = resolve_hyprland_runtime_dir()?;
    let explicit_signature = env_string("SINEX_HYPRLAND_INSTANCE_SIGNATURE")?
        .or(env_string("HYPRLAND_INSTANCE_SIGNATURE")?);
    let base_path = select_hyprland_base_path(&runtime_dir, explicit_signature)?;

    Ok(HyprlandSocketPaths {
        event_socket: base_path
            .join(".socket2.sock")
            .to_string_lossy()
            .into_owned(),
        command_socket: base_path
            .join(".socket.sock")
            .to_string_lossy()
            .into_owned(),
    })
}

impl fmt::Display for WindowManagerType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WindowManagerType::Hyprland => write!(f, "hyprland"),
        }
    }
}

impl WindowManagerType {
    /// Returns the string representation of the window manager type
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            WindowManagerType::Hyprland => "hyprland",
        }
    }
}

impl FromStr for WindowManagerType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "hyprland" {
            Ok(WindowManagerType::Hyprland)
        } else {
            Err(format!("Unsupported window manager type: {s}"))
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
            return Err(platform_error(
                format!("Unsupported window manager: {wm_type}"),
                ERROR_CLASS_UNSUPPORTED_WINDOW_MANAGER,
            ));
        }

        info!(
            "Window manager watcher initialized for {} (stage-as-you-go mode)",
            wm_type
        );
        Ok(watcher)
    }

    /// Discover Hyprland socket paths (both event and command)
    async fn discover_hyprland_sockets(&mut self) -> NodeResult<()> {
        let sockets = resolve_hyprland_socket_paths()?;

        // Test event socket connection
        if UnixStream::connect(&sockets.event_socket).await.is_ok() {
            self.socket_path = Some(sockets.event_socket.clone());
            info!("Found Hyprland event socket at: {}", sockets.event_socket);
        } else {
            return Err(platform_error(
                format!(
                    "Cannot connect to Hyprland event socket: {}",
                    sockets.event_socket
                ),
                ERROR_CLASS_HYPRLAND_EVENT_SOCKET_UNAVAILABLE,
            ));
        }

        // Test command socket connection
        if UnixStream::connect(&sockets.command_socket).await.is_ok() {
            self.command_socket_path = Some(sockets.command_socket.clone());
            info!(
                "Found Hyprland command socket at: {}",
                sockets.command_socket
            );
        } else {
            warn!(
                "Cannot connect to Hyprland command socket: {}",
                sockets.command_socket
            );
        }

        Ok(())
    }

    async fn register_material(
        &self,
        event_type: &str,
        metadata: serde_json::Value,
    ) -> NodeResult<Uuid> {
        let stage_context = self.stage_context.as_ref().ok_or_else(|| {
            sinex_node_sdk::SinexError::processing(
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
            "timestamp": Timestamp::now(),
            "metadata": metadata,
        })
    }

    async fn emit_material_event(
        &self,
        material_id: Uuid,
        payload_bytes: Vec<u8>,
        event: Event<JsonValue>,
    ) -> NodeResult<()> {
        let stage_context = self.stage_context.as_ref().ok_or_else(|| {
            sinex_node_sdk::SinexError::processing(
                "Stage-as-you-go context not initialized".to_string(),
            )
        })?;

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
            sinex_node_sdk::SinexError::processing("No Hyprland socket configured".to_string())
        })?;

        UnixStream::connect(socket_path).await.map_err(|e| {
            sinex_node_sdk::SinexError::processing(format!("Failed to connect to Hyprland: {e}"))
        })
    }

    fn parse_optional_id_or_zero(&self, value: Option<&str>, context: &str) -> NodeResult<i32> {
        value.map_or(Ok(0), |id| parse_hyprland_numeric_id(id, context))
    }

    fn ensure_workspace_entry<'a>(
        &'a mut self,
        workspace_id: &str,
        workspace_name: Option<&str>,
        monitor: Option<&str>,
    ) -> &'a mut WorkspaceInfo {
        let now = SystemTime::now();
        let entry = self
            .workspaces
            .entry(workspace_id.to_string())
            .or_insert_with(|| WorkspaceInfo {
                id: workspace_id.to_string(),
                name: workspace_name
                    .filter(|name| !name.is_empty())
                    .unwrap_or(workspace_id)
                    .to_string(),
                monitor: monitor
                    .filter(|monitor| !monitor.is_empty())
                    .unwrap_or_default()
                    .to_string(),
                active: false,
                window_count: 0,
                last_switched: now,
            });

        if let Some(name) = workspace_name.filter(|name| !name.is_empty()) {
            entry.name = name.to_string();
        }
        if let Some(monitor) = monitor.filter(|monitor| !monitor.is_empty()) {
            entry.monitor = monitor.to_string();
        }
        entry.last_switched = now;
        entry
    }

    fn clear_active_workspaces(&mut self) {
        for workspace in self.workspaces.values_mut() {
            workspace.active = false;
        }
    }

    fn mark_workspace_active(
        &mut self,
        workspace_id: &str,
        workspace_name: Option<&str>,
        monitor: Option<&str>,
    ) {
        self.clear_active_workspaces();
        let entry = self.ensure_workspace_entry(workspace_id, workspace_name, monitor);
        entry.active = true;
    }

    fn adjust_workspace_window_count(
        &mut self,
        workspace_id: &str,
        workspace_name: Option<&str>,
        monitor: Option<&str>,
        delta: i32,
    ) {
        let entry = self.ensure_workspace_entry(workspace_id, workspace_name, monitor);
        if delta > 0 {
            entry.window_count = entry.window_count.saturating_add(delta as u32);
        } else if delta < 0 {
            entry.window_count = entry.window_count.saturating_sub(delta.unsigned_abs());
        }
    }

    fn serialize_snapshot_entry<T: serde::Serialize>(
        &self,
        value: &T,
        context: &str,
    ) -> NodeResult<JsonValue> {
        serde_json::to_value(value).map_err(|error| {
            sinex_node_sdk::SinexError::processing(format!(
                "Failed to serialize {context} for window manager snapshot: {error}"
            ))
            .with_context("snapshot_context", context.to_string())
        })
    }

    fn serialize_snapshot_payload(
        &self,
        payload: &HyprlandStateCapturedPayload,
    ) -> NodeResult<String> {
        serde_json::to_string(payload).map_err(|error| {
            sinex_node_sdk::SinexError::processing(format!(
                "Failed to serialize window manager snapshot payload: {error}"
            ))
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
                "activewindow" | "activewindowv2" => {
                    self.handle_window_focused(event_type, event_data).await?;
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
                "workspace" | "workspacev2" => {
                    self.handle_workspace_changed(event_type, event_data)
                        .await?;
                }
                "focusedmon" | "focusedmonv2" => {
                    self.handle_monitor_focused(event_type, event_data).await?;
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
                        sinex_node_sdk::SinexError::processing(format!(
                            "Failed to serialize window material payload: {e}"
                        ))
                    })?;
                    let payload = serde_json::json!({
                        "event_type": event_type,
                        "event_data": event_data,
                    });
                    let provenance = Provenance::Material {
                        id: Id::from_uuid(material_id),
                        anchor_byte: 0,
                        offset_start: Some(0),
                        offset_end: Some(payload_bytes.len() as i64),
                        offset_kind: OffsetKind::Byte,
                    };
                    let event = DynamicPayload::new(
                        EventSource::from_static("wm.hyprland"),
                        EventType::from_static("wm.unhandled"),
                        payload,
                    )
                    .with_provenance(provenance)
                    .build()
                    .map_err(|e| {
                        sinex_node_sdk::SinexError::processing(format!(
                            "Failed to build event: {e}"
                        ))
                    })?;
                    self.emit_material_event(material_id, payload_bytes, event)
                        .await?;
                }
            }
        }

        Ok(())
    }

    /// Handle window focused event
    async fn handle_window_focused(&mut self, event_type: &str, data: &str) -> NodeResult<()> {
        // Format:
        // - activewindow>>WINDOWCLASS,WINDOWTITLE
        // - activewindowv2>>WINDOWADDRESS
        if let Some((class, raw_title)) = data.split_once(',') {
            let privacy_engine = privacy::engine().map_err(|error| {
                sinex_node_sdk::SinexError::configuration(
                    "failed to initialize privacy engine".to_string(),
                )
                .with_context("component", "desktop_window_focus")
                .with_std_error(error)
            })?;
            let title = privacy_engine
                .process(raw_title, ProcessingContext::WindowTitle)
                .text;
            // Try to find existing window by class and title, otherwise use deterministic hash
            let window_address = self
                .windows
                .iter()
                .find(|(_, info)| info.class == class && info.title == title)
                .map_or_else(
                    || {
                        use std::collections::hash_map::DefaultHasher;
                        use std::hash::{Hash, Hasher};
                        let mut hasher = DefaultHasher::new();
                        class.hash(&mut hasher);
                        title.hash(&mut hasher);
                        format!("0x{:x}", hasher.finish())
                    },
                    |(addr, _)| addr.clone(),
                );

            let workspace_id = self
                .current_workspace
                .as_deref()
                .map_or(Ok(0), |id| parse_hyprland_numeric_id(id, "workspace_id"))?;
            let metadata = serde_json::json!({
                "window_class": class,
                "window_title": title,
                "window_id": window_address,
                "previous_window_id": self.current_focused_window,
                "workspace_id": workspace_id,
            });
            let material_id = self.register_material(event_type, metadata.clone()).await?;
            let material_payload = self.build_material_payload(event_type, data, metadata);
            let payload_bytes = serde_json::to_vec(&material_payload).map_err(|e| {
                sinex_node_sdk::SinexError::processing(format!(
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
                    sinex_node_sdk::SinexError::processing(format!(
                        "Failed to set offset_start: {e}"
                    ))
                })?
                .with_offset_end(payload_bytes.len() as i64)
                .map_err(|e| {
                    sinex_node_sdk::SinexError::processing(format!("Failed to set offset_end: {e}"))
                })?
                .build()
                .map_err(|e| {
                    sinex_node_sdk::SinexError::processing(format!("Failed to build event: {e}"))
                })?
                .to_json_event()
                .map_err(|e| {
                    sinex_node_sdk::SinexError::processing(format!(
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
            self.current_workspace = Some(workspace_id.to_string());
        } else {
            let window_address = data.trim();
            let window_info = self.windows.get(window_address).cloned().ok_or_else(|| {
                sinex_node_sdk::SinexError::processing(format!(
                    "activewindowv2 reported unknown window address: {window_address}"
                ))
                .with_context("window_address", window_address.to_string())
            })?;
            let window_class = window_info.class.clone();
            let window_title = window_info.title.clone();
            let workspace_id_raw = window_info.workspace_id.clone();
            let workspace_id = parse_hyprland_numeric_id(&workspace_id_raw, "workspace_id")?;
            let metadata = serde_json::json!({
                "window_class": window_class,
                "window_title": window_title,
                "window_id": window_address,
                "previous_window_id": self.current_focused_window,
                "workspace_id": workspace_id,
            });
            let material_id = self.register_material(event_type, metadata.clone()).await?;
            let material_payload = self.build_material_payload(event_type, data, metadata);
            let payload_bytes = serde_json::to_vec(&material_payload).map_err(|e| {
                sinex_node_sdk::SinexError::processing(format!(
                    "Failed to serialize window material payload: {e}"
                ))
            })?;
            let payload = HyprlandWindowFocusedPayload {
                window_id: window_address.to_string(),
                window_class,
                window_title,
                workspace_id,
                previous_window_id: self.current_focused_window.clone(),
            };
            let event = payload
                .from_material(material_id)
                .with_offset_start(0)
                .map_err(|e| {
                    sinex_node_sdk::SinexError::processing(format!(
                        "Failed to set offset_start: {e}"
                    ))
                })?
                .with_offset_end(payload_bytes.len() as i64)
                .map_err(|e| {
                    sinex_node_sdk::SinexError::processing(format!("Failed to set offset_end: {e}"))
                })?
                .build()
                .map_err(|e| {
                    sinex_node_sdk::SinexError::processing(format!("Failed to build event: {e}"))
                })?
                .to_json_event()
                .map_err(|e| {
                    sinex_node_sdk::SinexError::processing(format!(
                        "Failed to serialize window focused payload: {e}"
                    ))
                })?;
            self.emit_material_event(material_id, payload_bytes, event)
                .await?;

            if let Some(window) = self.windows.get_mut(window_address) {
                window.last_seen = SystemTime::now();
            }

            self.current_focused_window = Some(window_address.to_string());
            self.current_workspace = Some(workspace_id.to_string());
        }

        Ok(())
    }

    /// Handle window opened event
    async fn handle_window_opened(&mut self, data: &str) -> NodeResult<()> {
        // Format: "address,workspace,class,title"
        let parts: Vec<&str> = data.split(',').collect();
        if parts.len() >= 4 {
            let privacy_engine = privacy::engine().map_err(|error| {
                sinex_node_sdk::SinexError::configuration(
                    "failed to initialize privacy engine".to_string(),
                )
                .with_context("component", "desktop_window_open")
                .with_std_error(error)
            })?;
            let window_address = parts[0].to_string();
            let workspace_id = parts[1].to_string();
            let window_class = parts[2].to_string();
            let window_title = privacy_engine
                .process(&parts[3..].join(","), ProcessingContext::WindowTitle)
                .text
                .into_owned(); // Title might contain commas

            let workspace_id_parsed = parse_hyprland_numeric_id(&workspace_id, "workspace_id")?;
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
                sinex_node_sdk::SinexError::processing(format!(
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
                    sinex_node_sdk::SinexError::processing(format!(
                        "Failed to set offset_start: {e}"
                    ))
                })?
                .with_offset_end(payload_bytes.len() as i64)
                .map_err(|e| {
                    sinex_node_sdk::SinexError::processing(format!("Failed to set offset_end: {e}"))
                })?
                .build()
                .map_err(|e| {
                    sinex_node_sdk::SinexError::processing(format!("Failed to build event: {e}"))
                })?
                .to_json_event()
                .map_err(|e| {
                    sinex_node_sdk::SinexError::processing(format!(
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
                    workspace_id: workspace_id.clone(),
                    last_seen: SystemTime::now(),
                    floating: false,
                    fullscreen: false,
                },
            );
            let current_monitor = self.current_monitor.clone();
            self.adjust_workspace_window_count(&workspace_id, None, current_monitor.as_deref(), 1);
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
            sinex_node_sdk::SinexError::processing(format!(
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
            workspace_id: self.parse_optional_id_or_zero(
                window_info.as_ref().map(|info| info.workspace_id.as_str()),
                "workspace_id",
            )?,
            close_reason: None,
        };
        let event = payload
            .from_material(material_id)
            .with_offset_start(0)
            .map_err(|e| {
                sinex_node_sdk::SinexError::processing(format!("Failed to set offset_start: {e}"))
            })?
            .with_offset_end(payload_bytes.len() as i64)
            .map_err(|e| {
                sinex_node_sdk::SinexError::processing(format!("Failed to set offset_end: {e}"))
            })?
            .build()
            .map_err(|e| {
                sinex_node_sdk::SinexError::processing(format!("Failed to build event: {e}"))
            })?
            .to_json_event()
            .map_err(|e| {
                sinex_node_sdk::SinexError::processing(format!(
                    "Failed to serialize window closed payload: {e}"
                ))
            })?;
        self.emit_material_event(material_id, payload_bytes, event)
            .await?;

        // Remove from tracking
        if let Some(window_info) = window_info {
            let current_monitor = self.current_monitor.clone();
            self.adjust_workspace_window_count(
                &window_info.workspace_id,
                None,
                current_monitor.as_deref(),
                -1,
            );
        }
        self.windows.remove(&window_address);

        Ok(())
    }

    /// Handle window moved event
    async fn handle_window_moved(&mut self, data: &str) -> NodeResult<()> {
        // Format: "address,workspace"
        if let Some((address, workspace)) = data.split_once(',') {
            let new_workspace_id = parse_hyprland_numeric_id(workspace.trim(), "workspace_id")?;
            let metadata = serde_json::json!({
                "window_address": address,
                "new_workspace_id": new_workspace_id,
            });
            let material_id = self
                .register_material("movewindow", metadata.clone())
                .await?;
            let material_payload = self.build_material_payload("movewindow", data, metadata);
            let payload_bytes = serde_json::to_vec(&material_payload).map_err(|e| {
                sinex_node_sdk::SinexError::processing(format!(
                    "Failed to serialize window material payload: {e}"
                ))
            })?;
            let payload = HyprlandWindowMovedPayload {
                window_address: address.to_string(),
                new_workspace_id,
                moved_at: Timestamp::now().to_string(),
            };
            let event = payload
                .from_material(material_id)
                .with_offset_start(0)
                .map_err(|e| {
                    sinex_node_sdk::SinexError::processing(format!(
                        "Failed to set offset_start: {e}"
                    ))
                })?
                .with_offset_end(payload_bytes.len() as i64)
                .map_err(|e| {
                    sinex_node_sdk::SinexError::processing(format!("Failed to set offset_end: {e}"))
                })?
                .build()
                .map_err(|e| {
                    sinex_node_sdk::SinexError::processing(format!("Failed to build event: {e}"))
                })?
                .to_json_event()
                .map_err(|e| {
                    sinex_node_sdk::SinexError::processing(format!(
                        "Failed to serialize window moved payload: {e}"
                    ))
                })?;
            self.emit_material_event(material_id, payload_bytes, event)
                .await?;

            let mut previous_workspace_id = None;
            let mut new_workspace_id = None;
            if let Some(window) = self.windows.get_mut(address) {
                previous_workspace_id = Some(window.workspace_id.clone());
                window.workspace_id = workspace.trim().to_string();
                window.last_seen = SystemTime::now();
                new_workspace_id = Some(window.workspace_id.clone());
            }

            if let (Some(previous_workspace_id), Some(new_workspace_id)) =
                (previous_workspace_id, new_workspace_id)
                && previous_workspace_id != new_workspace_id
            {
                let current_monitor = self.current_monitor.clone();
                self.adjust_workspace_window_count(
                    &previous_workspace_id,
                    None,
                    current_monitor.as_deref(),
                    -1,
                );
                self.adjust_workspace_window_count(
                    &new_workspace_id,
                    None,
                    current_monitor.as_deref(),
                    1,
                );
            }
        }

        Ok(())
    }

    /// Handle workspace changed event
    async fn handle_workspace_changed(&mut self, event_type: &str, data: &str) -> NodeResult<()> {
        let (workspace_id_raw, workspace_name) = data
            .split_once(',')
            .map(|(workspace_id, workspace_name)| {
                (workspace_id.trim(), Some(workspace_name.trim()))
            })
            .unwrap_or_else(|| (data.trim(), None));
        if workspace_id_raw.is_empty() {
            return Ok(());
        }

        let from_workspace_id = self
            .parse_optional_id_or_zero(self.current_workspace.as_deref(), "current_workspace_id")?;
        let to_workspace_id = parse_hyprland_numeric_id(workspace_id_raw, "workspace_id")?;
        let monitor_id =
            self.parse_optional_id_or_zero(self.current_monitor.as_deref(), "monitor_id")?;

        let metadata = serde_json::json!({
            "from_workspace_id": from_workspace_id,
            "to_workspace_id": to_workspace_id,
            "workspace_name": workspace_name,
        });
        let material_id = self.register_material(event_type, metadata.clone()).await?;
        let material_payload = self.build_material_payload(event_type, data, metadata);
        let payload_bytes = serde_json::to_vec(&material_payload).map_err(|e| {
            sinex_node_sdk::SinexError::processing(format!(
                "Failed to serialize window material payload: {e}"
            ))
        })?;
        let payload = HyprlandWorkspaceSwitchedPayload {
            from_workspace_id,
            to_workspace_id,
            monitor_id,
            active_window_id: self.current_focused_window.clone(),
        };
        let event = payload
            .from_material(material_id)
            .with_offset_start(0)
            .map_err(|e| {
                sinex_node_sdk::SinexError::processing(format!("Failed to set offset_start: {e}"))
            })?
            .with_offset_end(payload_bytes.len() as i64)
            .map_err(|e| {
                sinex_node_sdk::SinexError::processing(format!("Failed to set offset_end: {e}"))
            })?
            .build()
            .map_err(|e| {
                sinex_node_sdk::SinexError::processing(format!("Failed to build event: {e}"))
            })?
            .to_json_event()
            .map_err(|e| {
                sinex_node_sdk::SinexError::processing(format!(
                    "Failed to serialize workspace switched payload: {e}"
                ))
            })?;
        self.emit_material_event(material_id, payload_bytes, event)
            .await?;

        let current_monitor = self.current_monitor.clone();
        self.mark_workspace_active(workspace_id_raw, workspace_name, current_monitor.as_deref());
        self.current_workspace = Some(workspace_id_raw.to_string());

        Ok(())
    }

    /// Handle monitor focused event
    async fn handle_monitor_focused(&mut self, event_type: &str, data: &str) -> NodeResult<()> {
        // Format:
        // - focusedmon>>MONNAME,WORKSPACENAME
        // - focusedmonv2>>MONNAME,WORKSPACEID
        if let Some((monitor, workspace)) = data.split_once(',') {
            let monitor_id = parse_hyprland_numeric_id(monitor.trim(), "monitor_id")?;
            let workspace_id = parse_hyprland_numeric_id(workspace.trim(), "workspace_id")?;
            let previous_monitor = self
                .current_monitor
                .as_ref()
                .map(|value| parse_hyprland_numeric_id(value, "monitor_id"))
                .transpose()?;
            let metadata = serde_json::json!({
                "monitor_id": monitor_id,
                "workspace_id": workspace_id,
                "previous_monitor": self.current_monitor,
            });
            let material_id = self.register_material(event_type, metadata.clone()).await?;
            let material_payload = self.build_material_payload(event_type, data, metadata);
            let payload_bytes = serde_json::to_vec(&material_payload).map_err(|e| {
                sinex_node_sdk::SinexError::processing(format!(
                    "Failed to serialize window material payload: {e}"
                ))
            })?;
            let payload = HyprlandMonitorFocusedPayload {
                monitor_id,
                workspace_id,
                previous_monitor,
                focused_at: Timestamp::now().to_string(),
            };
            let event = payload
                .from_material(material_id)
                .with_offset_start(0)
                .map_err(|e| {
                    sinex_node_sdk::SinexError::processing(format!(
                        "Failed to set offset_start: {e}"
                    ))
                })?
                .with_offset_end(payload_bytes.len() as i64)
                .map_err(|e| {
                    sinex_node_sdk::SinexError::processing(format!("Failed to set offset_end: {e}"))
                })?
                .build()
                .map_err(|e| {
                    sinex_node_sdk::SinexError::processing(format!("Failed to build event: {e}"))
                })?
                .to_json_event()
                .map_err(|e| {
                    sinex_node_sdk::SinexError::processing(format!(
                        "Failed to serialize monitor focused payload: {e}"
                    ))
                })?;
            self.emit_material_event(material_id, payload_bytes, event)
                .await?;

            let workspace_id_str = workspace_id.to_string();
            self.mark_workspace_active(&workspace_id_str, None, Some(monitor.trim()));
            self.current_monitor = Some(monitor_id.to_string());
            self.current_workspace = Some(workspace_id_str);
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
            Err(sinex_node_sdk::SinexError::processing(format!(
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
                            () = sleep(STATE_SNAPSHOT_INTERVAL) => {
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
                        () = sleep(jittered_delay) => {}
                        shutdown_result = self.shutdown_rx.changed() => {
                            if shutdown_result.is_err() || *self.shutdown_rx.borrow() {
                                info!("Window manager watcher shutdown requested");
                                return Ok(());
                            }
                        }
                    }
                }
            }

            consecutive_failures += 1;

            let jittered_delay = Self::next_backoff(&mut reconnect_backoff);
            warn!(
                "Hyprland connection lost, reconnecting in {:?}...",
                jittered_delay
            );
            tokio::select! {
                () = sleep(jittered_delay) => {}
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
            if let Ok(elapsed) = now.duration_since(window.last_seen)
                && elapsed > WINDOW_STATE_TTL
            {
                debug!(
                    "Removing stale window entry: {} (class: {}, last seen: {:?} ago)",
                    window.address, window.class, elapsed
                );
                return false;
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
            .map(|window| self.serialize_snapshot_entry(window, "window"))
            .collect::<NodeResult<Vec<_>>>()?;
        let workspaces = self
            .workspaces
            .values()
            .map(|workspace| self.serialize_snapshot_entry(workspace, "workspace"))
            .collect::<NodeResult<Vec<_>>>()?;
        let metadata = serde_json::json!({
            "snapshot": true,
            "window_count": self.windows.len(),
            "workspace_count": self.workspaces.len(),
        });
        let snapshot_payload = HyprlandStateCapturedPayload {
            windows,
            workspaces,
            monitors: Vec::new(),
            current_workspace: self.parse_optional_id_or_zero(
                self.current_workspace.as_deref(),
                "current_workspace_id",
            )?,
            current_monitor: self
                .parse_optional_id_or_zero(self.current_monitor.as_deref(), "monitor_id")?,
            captured_at: Timestamp::now().to_string(),
        };
        let material_payload = self.build_material_payload(
            "state_snapshot",
            &self.serialize_snapshot_payload(&snapshot_payload)?,
            metadata.clone(),
        );
        let payload_bytes = serde_json::to_vec(&material_payload).map_err(|e| {
            sinex_node_sdk::SinexError::processing(format!(
                "Failed to serialize window material payload: {e}"
            ))
        })?;
        let material_id = self.register_material("state_snapshot", metadata).await?;
        let event = snapshot_payload
            .from_material(material_id)
            .with_offset_start(0)
            .map_err(|e| {
                sinex_node_sdk::SinexError::processing(format!("Failed to set offset_start: {e}"))
            })?
            .with_offset_end(payload_bytes.len() as i64)
            .map_err(|e| {
                sinex_node_sdk::SinexError::processing(format!("Failed to set offset_end: {e}"))
            })?
            .build()
            .map_err(|e| {
                sinex_node_sdk::SinexError::processing(format!("Failed to build event: {e}"))
            })?
            .to_json_event()
            .map_err(|e| {
                sinex_node_sdk::SinexError::processing(format!(
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
    #[must_use]
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
    use sinex_primitives::Uuid;
    #[cfg(unix)]
    use std::ffi::OsString;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn hyprland_backoff_grows_until_cap() -> TestResult<()> {
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
        Ok(())
    }

    #[sinex_test]
    async fn hyprland_backoff_resets_after_success() -> TestResult<()> {
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
        Ok(())
    }

    #[sinex_test]
    async fn derive_command_socket_uses_same_instance_dir() -> TestResult<()> {
        let command_socket =
            derive_hyprland_command_socket("/run/user/1000/hypr/test/.socket2.sock");
        assert_eq!(command_socket, "/run/user/1000/hypr/test/.socket.sock");
        Ok(())
    }

    #[sinex_test]
    async fn select_hyprland_base_path_discovers_single_socket_dir() -> TestResult<()> {
        let runtime_dir = std::env::temp_dir().join(format!("sinex-wm-{}", Uuid::now_v7()));
        let candidate = runtime_dir.join("hypr").join("instance-a");
        std::fs::create_dir_all(&candidate)?;
        std::fs::write(candidate.join(".socket2.sock"), b"stub")?;

        let selected = select_hyprland_base_path(&runtime_dir, None)?;
        assert_eq!(selected, candidate);

        let _ = std::fs::remove_dir_all(&runtime_dir);
        Ok(())
    }

    #[sinex_test]
    async fn select_hyprland_base_path_requires_override_for_multiple_instances() -> TestResult<()>
    {
        let runtime_dir = std::env::temp_dir().join(format!("sinex-wm-{}", Uuid::now_v7()));
        let first = runtime_dir.join("hypr").join("instance-a");
        let second = runtime_dir.join("hypr").join("instance-b");
        std::fs::create_dir_all(&first)?;
        std::fs::create_dir_all(&second)?;
        std::fs::write(first.join(".socket2.sock"), b"stub")?;
        std::fs::write(second.join(".socket2.sock"), b"stub")?;

        let error = select_hyprland_base_path(&runtime_dir, None).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("SINEX_HYPRLAND_INSTANCE_SIGNATURE")
        );

        let _ = std::fs::remove_dir_all(&runtime_dir);
        Ok(())
    }

    #[sinex_test]
    async fn collect_hyprland_candidates_rejects_entry_iteration_failures() -> TestResult<()> {
        let runtime_dir = std::env::temp_dir().join(format!("sinex-wm-{}", Uuid::now_v7()));
        let hypr_dir = runtime_dir.join("hypr");
        std::fs::create_dir_all(&hypr_dir)?;

        let error = collect_hyprland_candidates(
            vec![Err(std::io::Error::other("broken directory entry"))],
            &hypr_dir,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("Cannot inspect Hyprland runtime entry")
        );

        let _ = std::fs::remove_dir_all(&runtime_dir);
        Ok(())
    }

    #[sinex_test]
    async fn parse_hyprland_numeric_id_accepts_numeric_suffixes() -> TestResult<()> {
        assert_eq!(parse_hyprland_numeric_id("DP-1", "monitor_id")?, 1);
        assert_eq!(parse_hyprland_numeric_id("3", "workspace_id")?, 3);
        Ok(())
    }

    #[sinex_test]
    async fn workspace_helpers_track_counts_and_active_workspace() -> TestResult<()> {
        let mut watcher = WindowManagerWatcher::stub(WindowManagerType::Hyprland);

        watcher.adjust_workspace_window_count("1", Some("main"), Some("monitor-a"), 2);
        watcher.adjust_workspace_window_count("2", Some("code"), None, 1);
        watcher.mark_workspace_active("2", Some("code"), Some("monitor-b"));

        let main = watcher
            .workspaces
            .get("1")
            .expect("workspace 1 should be tracked");
        assert_eq!(main.id, "1");
        assert_eq!(main.name, "main");
        assert_eq!(main.monitor, "monitor-a");
        assert_eq!(main.window_count, 2);
        assert!(!main.active);

        let code = watcher
            .workspaces
            .get("2")
            .expect("workspace 2 should be tracked");
        assert_eq!(code.id, "2");
        assert_eq!(code.name, "code");
        assert_eq!(code.monitor, "monitor-b");
        assert_eq!(code.window_count, 1);
        assert!(code.active);
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_serial_test]
    async fn resolve_hyprland_socket_paths_rejects_non_utf8_event_socket_override() -> TestResult<()>
    {
        let mut env = EnvGuard::new();
        env.set(
            "SINEX_HYPRLAND_EVENT_SOCKET",
            OsString::from_vec(vec![0x66, 0x6f, 0x80, 0x6f]),
        );
        env.clear("SINEX_HYPRLAND_COMMAND_SOCKET");

        let error = resolve_hyprland_socket_paths()
            .expect_err("non-UTF8 event socket overrides must fail honestly");
        let message = error.to_string();

        assert!(message.contains("SINEX_HYPRLAND_EVENT_SOCKET"));
        assert!(message.contains("not valid UTF-8"));
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_serial_test]
    async fn resolve_hyprland_runtime_dir_rejects_non_utf8_override() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set(
            "SINEX_HYPRLAND_RUNTIME_DIR",
            OsString::from_vec(vec![0x66, 0x6f, 0x80, 0x6f]),
        );
        env.clear("XDG_RUNTIME_DIR");

        let error = resolve_hyprland_runtime_dir()
            .expect_err("non-UTF8 runtime dir overrides must fail honestly");
        let message = error.to_string();

        assert!(message.contains("SINEX_HYPRLAND_RUNTIME_DIR"));
        assert!(message.contains("not valid UTF-8"));
        Ok(())
    }

    #[sinex_test]
    async fn handle_workspace_changed_rejects_invalid_workspace_id() -> TestResult<()> {
        let mut watcher = WindowManagerWatcher::stub(WindowManagerType::Hyprland);

        let error = watcher
            .handle_workspace_changed("workspace", "not-a-number")
            .await
            .expect_err("invalid workspace ids must not be coerced to zero");

        assert!(error.to_string().contains("workspace_id"));
        Ok(())
    }

    #[sinex_test]
    async fn handle_monitor_focused_rejects_invalid_monitor_id() -> TestResult<()> {
        let mut watcher = WindowManagerWatcher::stub(WindowManagerType::Hyprland);

        let error = watcher
            .handle_monitor_focused("focusedmon", "left,2")
            .await
            .expect_err("invalid monitor ids must not be coerced to zero");

        assert!(error.to_string().contains("monitor_id"));
        Ok(())
    }

    #[sinex_test]
    async fn capture_state_snapshot_rejects_invalid_current_workspace_id() -> TestResult<()> {
        let mut watcher = WindowManagerWatcher::stub(WindowManagerType::Hyprland);
        watcher.current_workspace = Some("oops".to_string());

        let error = watcher
            .capture_state_snapshot()
            .await
            .expect_err("invalid snapshot ids must fail before emitting");

        assert!(error.to_string().contains("current_workspace_id"));
        Ok(())
    }
}
