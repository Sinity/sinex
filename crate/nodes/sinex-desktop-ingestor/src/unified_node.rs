//! Unified desktop node implementing Node
//!
//! This module implements the desktop node node supporting snapshot, historical, and
//! continuous scanning modes for desktop events.

// Use unified SDK prelude for common types
use serde::{Deserialize, Serialize};
use sinex_node_sdk::error_helpers::{ConfigAccessor, parse_config_value, parse_typed_config};
use sinex_node_sdk::prelude::*;
use std::collections::HashMap;
use tracing::{error, info, instrument, warn};

use crate::{
    ClipboardWatcher, WindowManagerWatcher,
    activitywatch_history::{
        ActivityWatchEntryKind, ActivityWatchHistoryEntry, ensure_activitywatch_sqlite,
        read_activitywatch_history,
    },
    window_manager::WindowManagerType,
};
use camino::Utf8PathBuf;
use serde_json::json;
use sinex_node_sdk::{
    BufferedRecordMaterializer, BufferedRecordSourceHarness, EventTransport,
    RecordProcessingOutcome, RecordReadHorizon, RecordSources, RecordWarningDisposition,
    SourceRecordAnchor, SqliteRowCheckpoint, SqliteSnapshotLinker, SqliteSnapshotPolicy,
    SqliteSnapshotState,
    acquisition_manager::{AcquisitionManager, RotationPolicy},
    ingestor_node::IngestorNode,
    nats_publisher::NatsPublisher,
    stage_as_you_go::StageAsYouGoContext,
    supervised_watcher::spawn_watcher_with_panic_catch,
    wait_for_shutdown_signal,
    watcher_handle::WatcherHandle,
};
use sinex_primitives::{
    HostName, Seconds, Timestamp, Uuid, env as shared_env,
    events::{
        payload::PayloadExt,
        payloads::{
            ActivityWatchAfkChangedPayload, ActivityWatchBrowserTabActivePayload,
            ActivityWatchWindowActivePayload,
        },
    },
    privacy::{self, ProcessingContext},
};
use std::sync::Arc;
use tokio::sync::watch;

/// Desktop monitoring configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DesktopConfig {
    /// Enable clipboard monitoring
    pub clipboard_enabled: bool,
    /// Enable window manager monitoring
    pub window_manager_enabled: bool,
    /// Window manager type (currently only "hyprland")
    pub window_manager_type: WindowManagerType,
    /// Clipboard monitoring interval (seconds)
    pub clipboard_poll_interval_secs: Seconds,
    /// Require Hyprland to be present (if false, runs in degraded mode)
    pub require_hyprland: bool,
    /// Optional `ActivityWatch` `SQLite` database path used for truthful historical imports.
    pub activitywatch_db_path: Option<Utf8PathBuf>,
}

fn default_activitywatch_db_path_from(data_dir: Option<std::path::PathBuf>) -> Option<Utf8PathBuf> {
    let data_dir = data_dir?;
    match Utf8PathBuf::from_path_buf(data_dir.clone()) {
        Ok(data_dir) => Some(data_dir.join("activitywatch/aw-server-rust/sqlite.db")),
        Err(path) => {
            warn!(
                path = %path.display(),
                "Data directory path is not valid UTF-8; default ActivityWatch history path is unavailable"
            );
            None
        }
    }
}

impl Default for DesktopConfig {
    fn default() -> Self {
        Self {
            clipboard_enabled: true,
            window_manager_enabled: true,
            window_manager_type: WindowManagerType::Hyprland,
            // Native clipboard API is fast, poll at 100ms (but Seconds type is u64, so minimum is 1 second)
            // We'll handle the actual poll interval in the watcher code
            clipboard_poll_interval_secs: Seconds::from_secs(1),
            // Allow running in headless/degraded mode by default
            require_hyprland: false,
            activitywatch_db_path: default_activitywatch_db_path_from(dirs::data_dir()),
        }
    }
}

/// Desktop state snapshot for exploration and diagnostics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DesktopState {
    /// When the snapshot was taken
    pub captured_at: Timestamp,

    /// Enabled source types
    pub enabled_sources: Vec<String>,

    /// Clipboard status
    pub clipboard_status: Option<ClipboardStatus>,

    /// Window manager status
    pub window_manager_status: Option<WindowManagerStatus>,

    /// Recent activity summary
    pub recent_activity: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClipboardStatus {
    pub monitoring_active: bool,
    pub last_clipboard_change: Option<Timestamp>,
    pub clipboard_content_hash: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowManagerStatus {
    pub wm_type: String,
    pub connection_active: bool,
    pub current_workspace: Option<String>,
    pub active_window: Option<String>,
    pub total_windows: u32,
    pub last_error: Option<String>,
}

/// Health tracking for desktop monitors
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DesktopMonitorHealth {
    /// Clipboard monitor active and working
    pub clipboard_active: bool,
    /// Window manager monitor active and working
    pub window_manager_active: bool,
    /// Last error from clipboard monitor
    pub clipboard_last_error: Option<String>,
    /// Last error from window manager monitor
    pub window_manager_last_error: Option<String>,
    /// Last successful clipboard event
    pub clipboard_last_success: Option<Timestamp>,
    /// Last successful window manager event
    pub window_manager_last_success: Option<Timestamp>,
}

/// Persistent state for `IngestorNode`
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DesktopPersistentState {
    pub health: DesktopMonitorHealth,
    pub last_state: Option<DesktopState>,
    #[serde(default)]
    pub activitywatch_last_row_id: i64,
    #[serde(default)]
    pub activitywatch_snapshot: SqliteSnapshotState,
}

/// Unified desktop node implementing Node with Stage-as-You-Go
///
/// This node captures desktop activity as source material first, then generates
/// events with proper provenance tracking via `JetStream` capture.
pub struct DesktopNode {
    /// Runtime state captured during initialization
    runtime: Option<NodeRuntimeState>,
    /// Configuration
    config: DesktopConfig,
    /// Stage-as-you-go context for event emission
    stage_context: Option<StageAsYouGoContext>,
    /// Acquisition manager for material capture
    acquisition: Option<Arc<AcquisitionManager>>,

    /// Watcher handles
    // We store the Watcher instance inside the handle's material context until started
    clipboard_watcher: Option<WatcherHandle<ClipboardWatcher>>,
    window_manager_watcher: Option<WatcherHandle<WindowManagerWatcher>>,

    /// Shutdown signal sender for watchers created during the continuous loop.
    /// Populated in `run_continuous` so `ensure_watchers_running` can subscribe
    /// fresh receivers when restarting dead watchers.
    watcher_shutdown_tx: Option<watch::Sender<bool>>,
}

impl DesktopNode {
    const _MS_PER_EVENT: u64 = 10;
    const _BYTES_PER_EVENT: u64 = 256;

    /// Create a new unified desktop node
    #[must_use]
    pub fn new() -> Self {
        Self {
            runtime: None,
            config: DesktopConfig::default(),
            stage_context: None,
            acquisition: None,
            clipboard_watcher: None,
            window_manager_watcher: None,
            watcher_shutdown_tx: None,
        }
    }

    fn collapse_shutdown_errors(mut errors: Vec<(&'static str, SinexError)>) -> NodeResult<()> {
        if errors.is_empty() {
            return Ok(());
        }

        let (step, error) = errors.remove(0);
        let mut combined = error.with_context("shutdown_step", step);
        for (index, (step, extra)) in errors.into_iter().enumerate() {
            combined = combined
                .with_context(format!("additional_shutdown_step_{}", index + 1), step)
                .with_context(
                    format!("additional_shutdown_error_{}", index + 1),
                    extra.to_string(),
                );
        }
        Err(combined)
    }

    fn is_platform_missing_error(err: &SinexError) -> bool {
        err.context_map()
            .get("error_class")
            .is_some_and(|class| class.starts_with("desktop_platform_"))
    }

    /// Take a snapshot of current desktop state
    #[instrument(skip(self), fields(node = "desktop"))]
    async fn take_snapshot(&self, health: &DesktopMonitorHealth) -> NodeResult<DesktopState> {
        let mut enabled_sources = Vec::new();
        let mut clipboard_status = None;
        let mut window_manager_status = None;

        // Check enabled sources
        if self.config.clipboard_enabled {
            enabled_sources.push("clipboard".to_string());

            // Try to get clipboard status
            clipboard_status = Some(ClipboardStatus {
                monitoring_active: self
                    .clipboard_watcher
                    .as_ref()
                    .is_some_and(sinex_node_sdk::WatcherHandle::is_active),
                last_clipboard_change: health.clipboard_last_success,
                clipboard_content_hash: None, // Would need to hash current clipboard
                last_error: health.clipboard_last_error.clone(),
            });
        }

        if self.config.window_manager_enabled {
            enabled_sources.push("window_manager".to_string());

            // Try to get window manager status
            window_manager_status = Some(WindowManagerStatus {
                wm_type: self.config.window_manager_type.to_string(),
                connection_active: self
                    .window_manager_watcher
                    .as_ref()
                    .is_some_and(sinex_node_sdk::WatcherHandle::is_active),
                current_workspace: None, // Would need to query WM
                active_window: None,     // Would need to query WM
                total_windows: 0,        // Would need to query WM
                last_error: health.window_manager_last_error.clone(),
            });
        }

        let state = DesktopState {
            captured_at: Timestamp::now(),
            enabled_sources,
            clipboard_status,
            window_manager_status,
            recent_activity: vec!["Desktop node snapshot taken".to_string()],
        };

        Ok(state)
    }

    fn initialize_watcher_handles(&mut self) {
        if self.config.clipboard_enabled && self.clipboard_watcher.is_none() {
            // Create initialized handle
            let handle = WatcherHandle::initialized("clipboard");
            self.clipboard_watcher = Some(handle);
        }

        if self.config.window_manager_enabled && self.window_manager_watcher.is_none() {
            let handle = WatcherHandle::initialized("window_manager");
            self.window_manager_watcher = Some(handle);
        }
    }

    fn configured_activitywatch_db_path(&self) -> Option<&Utf8PathBuf> {
        self.config.activitywatch_db_path.as_ref()
    }

    fn checkpoint_activitywatch_row_id(checkpoint: &Checkpoint) -> NodeResult<Option<i64>> {
        match checkpoint {
            Checkpoint::None => Ok(None),
            Checkpoint::External { position, .. } => match position.get("activitywatch_row_id") {
                Some(serde_json::Value::Number(value)) => {
                    let row_id = value.as_i64().ok_or_else(|| {
                        SinexError::validation(
                            "desktop ActivityWatch checkpoint row id must fit in i64",
                        )
                        .with_context("activitywatch_row_id", value.to_string())
                    })?;
                    if row_id < 0 {
                        return Err(
                            SinexError::validation(
                                "desktop ActivityWatch checkpoint has invalid negative activitywatch_row_id",
                            )
                            .with_context("activitywatch_row_id", row_id.to_string()),
                        );
                    }
                    Ok(Some(row_id))
                }
                Some(other) => Err(SinexError::validation(
                    "desktop ActivityWatch checkpoint row id must be an integer",
                )
                .with_context("activitywatch_row_id", other.to_string())),
                None => Ok(None),
            },
            _ => Err(SinexError::checkpoint(
                "desktop ActivityWatch history requires an external checkpoint",
            )
            .with_context("checkpoint", checkpoint.description())),
        }
    }

    fn historical_activitywatch_start_row(
        state: &DesktopPersistentState,
        from: &Checkpoint,
    ) -> NodeResult<i64> {
        Ok(Self::checkpoint_activitywatch_row_id(from)?.unwrap_or(state.activitywatch_last_row_id))
    }

    fn historical_activitywatch_start_row_for_scan(
        state: &DesktopPersistentState,
        from: &Checkpoint,
        replaying: bool,
    ) -> NodeResult<i64> {
        match Self::historical_activitywatch_start_row(state, from) {
            Ok(row_id) => Ok(row_id),
            Err(error) if !replaying && !matches!(from, Checkpoint::External { .. }) => {
                warn!(
                    checkpoint = ?from,
                    fallback_row_id = state.activitywatch_last_row_id,
                    error = %error,
                    "Desktop historical scan received a non-ActivityWatch checkpoint during normal startup; falling back to persisted ActivityWatch row state"
                );
                Ok(state.activitywatch_last_row_id)
            }
            Err(error) => Err(error),
        }
    }

    fn activitywatch_runtime_handles(
        &self,
    ) -> NodeResult<(&AcquisitionManager, &StageAsYouGoContext)> {
        let acquisition = self
            .acquisition
            .as_ref()
            .ok_or_else(|| SinexError::lifecycle("Desktop acquisition manager not initialized"))?;
        let stage_context = self
            .stage_context
            .as_ref()
            .ok_or_else(|| SinexError::lifecycle("Desktop stage context not initialized"))?;
        Ok((acquisition.as_ref(), stage_context))
    }

    fn redact_window_title(value: &str) -> NodeResult<String> {
        Ok(privacy::process(value, ProcessingContext::WindowTitle)
            .map_err(|error| {
                SinexError::configuration("failed to initialize privacy engine".to_string())
                    .with_context("component", "desktop_window_title_redaction")
                    .with_std_error(error)
            })?
            .text
            .into_owned())
    }

    fn redact_document(value: &str) -> NodeResult<String> {
        Ok(privacy::process(value, ProcessingContext::Document)
            .map_err(|error| {
                SinexError::configuration("failed to initialize privacy engine".to_string())
                    .with_context("component", "desktop_document_redaction")
                    .with_std_error(error)
            })?
            .text
            .into_owned())
    }

    fn require_activitywatch_string_field(
        entry: &ActivityWatchHistoryEntry,
        field: &str,
    ) -> NodeResult<String> {
        match entry.data.get(field) {
            Some(serde_json::Value::String(value)) => Ok(value.clone()),
            Some(other) => Err(SinexError::validation(format!(
                "ActivityWatch row {} field '{field}' must be a string, got {other}",
                entry.row_id
            ))),
            None => Err(SinexError::validation(format!(
                "ActivityWatch row {} is missing required field '{field}'",
                entry.row_id
            ))),
        }
    }

    fn require_activitywatch_nonempty_string_field(
        entry: &ActivityWatchHistoryEntry,
        field: &str,
    ) -> NodeResult<String> {
        let value = Self::require_activitywatch_string_field(entry, field)?;
        if value.trim().is_empty() {
            return Err(SinexError::validation(format!(
                "ActivityWatch row {} field '{field}' must not be empty",
                entry.row_id
            )));
        }
        Ok(value)
    }

    async fn stage_activitywatch_material(
        materializer: &BufferedRecordMaterializer,
        entry: &ActivityWatchHistoryEntry,
    ) -> NodeResult<SourceRecordAnchor> {
        let mut material_bytes =
            serde_json::to_vec(&entry.raw_material_payload()).map_err(|error| {
                SinexError::serialization("failed to serialize ActivityWatch source material")
                    .with_std_error(&error)
            })?;
        material_bytes.push(b'\n');

        materializer
            .append_stable_bytes(material_bytes)
            .await
            .map_err(|error| {
                SinexError::service("failed to append ActivityWatch material").with_source(error)
            })
    }

    fn build_activitywatch_event(
        entry: &ActivityWatchHistoryEntry,
        material_id: Uuid,
        offset_start: i64,
        offset_end: i64,
    ) -> NodeResult<sinex_primitives::events::Event<serde_json::Value>> {
        let host = HostName::new(entry.host.clone()).map_err(|error| {
            SinexError::validation("invalid ActivityWatch hostname").with_source(error)
        })?;

        match entry.kind {
            ActivityWatchEntryKind::Window => {
                let app = Self::require_activitywatch_nonempty_string_field(entry, "app")?;
                let title = Self::require_activitywatch_string_field(entry, "title")?;

                ActivityWatchWindowActivePayload {
                    app,
                    title: Self::redact_window_title(&title)?,
                    duration_ms: entry.duration_ms,
                    bucket_id: entry.bucket_id.clone(),
                }
                .into_builder()
                .hostname(host)
                .from_material(material_id, offset_start)
                .at_time(entry.started_at)
                .with_offset_start(offset_start)
                .map_err(|error| {
                    SinexError::service("failed to set ActivityWatch offset").with_source(error)
                })?
                .with_offset_end(offset_end)
                .map_err(|error| {
                    SinexError::service("failed to set ActivityWatch offset").with_source(error)
                })?
                .build()
                .map_err(|error| {
                    SinexError::service("failed to build ActivityWatch window event")
                        .with_source(error)
                })?
                .to_json_event()
                .map_err(|error| {
                    SinexError::serialization("failed to encode ActivityWatch window event")
                        .with_source(error)
                })
            }
            ActivityWatchEntryKind::Web => {
                let browser = Self::require_activitywatch_nonempty_string_field(entry, "app")?;
                let title = Self::require_activitywatch_string_field(entry, "title")?;
                let url = Self::require_activitywatch_string_field(entry, "url")?;

                ActivityWatchBrowserTabActivePayload {
                    browser,
                    title: Self::redact_window_title(&title)?,
                    url: Self::redact_document(&url)?,
                    duration_ms: entry.duration_ms,
                    bucket_id: entry.bucket_id.clone(),
                }
                .into_builder()
                .hostname(host)
                .from_material(material_id, offset_start)
                .at_time(entry.started_at)
                .with_offset_start(offset_start)
                .map_err(|error| {
                    SinexError::service("failed to set ActivityWatch offset").with_source(error)
                })?
                .with_offset_end(offset_end)
                .map_err(|error| {
                    SinexError::service("failed to set ActivityWatch offset").with_source(error)
                })?
                .build()
                .map_err(|error| {
                    SinexError::service("failed to build ActivityWatch web event")
                        .with_source(error)
                })?
                .to_json_event()
                .map_err(|error| {
                    SinexError::serialization("failed to encode ActivityWatch web event")
                        .with_source(error)
                })
            }
            ActivityWatchEntryKind::Afk => {
                let status = Self::require_activitywatch_nonempty_string_field(entry, "status")?;

                ActivityWatchAfkChangedPayload {
                    status,
                    duration_ms: entry.duration_ms,
                    bucket_id: entry.bucket_id.clone(),
                }
                .into_builder()
                .hostname(host)
                .from_material(material_id, offset_start)
                .at_time(entry.started_at)
                .with_offset_start(offset_start)
                .map_err(|error| {
                    SinexError::service("failed to set ActivityWatch offset").with_source(error)
                })?
                .with_offset_end(offset_end)
                .map_err(|error| {
                    SinexError::service("failed to set ActivityWatch offset").with_source(error)
                })?
                .build()
                .map_err(|error| {
                    SinexError::service("failed to build ActivityWatch afk event")
                        .with_source(error)
                })?
                .to_json_event()
                .map_err(|error| {
                    SinexError::serialization("failed to encode ActivityWatch afk event")
                        .with_source(error)
                })
            }
        }
    }

    async fn emit_activitywatch_entry(
        &self,
        materializer: &BufferedRecordMaterializer,
        entry: &ActivityWatchHistoryEntry,
    ) -> NodeResult<()> {
        let (_, stage_context) = self.activitywatch_runtime_handles()?;
        let anchor = Self::stage_activitywatch_material(materializer, entry).await?;
        let event = Self::build_activitywatch_event(
            entry,
            anchor.material_id,
            anchor.offset_start,
            anchor.offset_end,
        )?;

        stage_context
            .emit_event_with_provenance(
                event,
                anchor.material_id,
                Some(anchor.offset_start),
                Some(anchor.offset_end),
            )
            .await
            .map(|_| ())
            .map_err(|error| {
                SinexError::messaging("failed to emit ActivityWatch event").with_source(error)
            })
    }
}

impl Default for DesktopNode {
    fn default() -> Self {
        Self::new()
    }
}

impl DesktopNode {
    fn live_watcher_error<M>(handle: Option<&WatcherHandle<M>>) -> Option<String> {
        handle.and_then(|watcher| watcher.health().last_error)
    }

    fn env_string_override(name: &str) -> NodeResult<Option<String>> {
        shared_env::strict_var(name)
    }

    fn parse_window_manager_type_override(raw: &str) -> NodeResult<WindowManagerType> {
        raw.parse::<WindowManagerType>().map_err(|error| {
            SinexError::processing(format!("Invalid window manager type `{raw}`: {error}"))
        })
    }

    fn parse_bool_env_override(var_name: &str, raw: &str) -> NodeResult<bool> {
        raw.parse::<bool>().map_err(|error| {
            SinexError::processing(format!("Invalid {var_name} value `{raw}`: {error}"))
        })
    }

    fn apply_config_overrides<S: ConfigAccessor>(
        config: &mut DesktopConfig,
        source: &S,
    ) -> NodeResult<()> {
        if let Some(context_config) = parse_typed_config::<DesktopConfig, _>("desktop", source)? {
            *config = context_config;
        }

        if let Some(enabled) = parse_config_value::<bool, _>("clipboard_enabled", source)? {
            config.clipboard_enabled = enabled;
        }
        if let Some(enabled) = parse_config_value::<bool, _>("window_manager_enabled", source)? {
            config.window_manager_enabled = enabled;
        }
        if let Some(wm_type_str) = parse_config_value::<String, _>("window_manager_type", source)? {
            config.window_manager_type = Self::parse_window_manager_type_override(&wm_type_str)?;
        }
        if let Some(interval) =
            parse_config_value::<Seconds, _>("clipboard_poll_interval_secs", source)?
        {
            config.clipboard_poll_interval_secs = interval;
        }
        if let Some(require_hyprland) = parse_config_value::<bool, _>("require_hyprland", source)? {
            config.require_hyprland = require_hyprland;
        }
        if let Some(path) = parse_config_value::<String, _>("activitywatch_db_path", source)? {
            config.activitywatch_db_path = Some(Utf8PathBuf::from(path));
        }

        Ok(())
    }

    fn apply_env_overrides(config: &mut DesktopConfig) -> NodeResult<()> {
        if let Some(val) = Self::env_string_override("SINEX_DESKTOP_REQUIRE_HYPRLAND")? {
            config.require_hyprland =
                Self::parse_bool_env_override("SINEX_DESKTOP_REQUIRE_HYPRLAND", &val)?;
        }
        if let Some(path) = Self::env_string_override("SINEX_ACTIVITYWATCH_DB_PATH")? {
            config.activitywatch_db_path = Some(Utf8PathBuf::from(path));
        }

        Ok(())
    }

    fn clipboard_connected(&self) -> bool {
        self.config.clipboard_enabled
            && self
                .clipboard_watcher
                .as_ref()
                .is_some_and(WatcherHandle::is_active)
    }

    fn window_manager_connected(&self) -> bool {
        self.config.window_manager_enabled
            && self
                .window_manager_watcher
                .as_ref()
                .is_some_and(WatcherHandle::is_active)
    }

    /// Ensure all configured watchers are running, restarting any that have died.
    ///
    /// Called periodically from the continuous loop (every 30 seconds).
    /// Follows the same pattern as the system ingestor's `ensure_watchers_running`.
    async fn ensure_watchers_running(
        &mut self,
        state: &mut DesktopPersistentState,
        mut watcher_shutdown_rx: watch::Receiver<bool>,
    ) -> NodeResult<()> {
        let stage_context = self
            .stage_context
            .as_ref()
            .ok_or_else(|| SinexError::lifecycle("Stage context not initialized"))?;

        // Clipboard Watcher
        if self.config.clipboard_enabled
            && let Some(handle) = &mut self.clipboard_watcher
            && !handle.is_active()
        {
            match ClipboardWatcher::new(
                self.config.clipboard_poll_interval_secs,
                stage_context.clone(),
                watcher_shutdown_rx.clone(),
            ) {
                Ok(mut watcher) => {
                    *handle = WatcherHandle::initialized("clipboard");
                    let health = handle.health_tracker();
                    let task = spawn_watcher_with_panic_catch(
                        "clipboard",
                        Some(Arc::clone(&health)),
                        async move { watcher.start_monitoring().await },
                    );
                    handle.start(task, None)?;
                    state.health.clipboard_active = true;
                    state.health.clipboard_last_error = None;
                }
                Err(e) => {
                    if !Self::is_platform_missing_error(&e) || self.config.require_hyprland {
                        error!("Failed to initialize clipboard watcher: {}", e);
                        state.health.clipboard_active = false;
                        state.health.clipboard_last_error = Some(e.to_string());
                    } else {
                        warn!("Clipboard watcher skipped: {}", e);
                    }
                }
            }
        }

        // Window Manager Watcher
        if self.config.window_manager_enabled
            && let Some(handle) = &mut self.window_manager_watcher
            && !handle.is_active()
        {
            match WindowManagerWatcher::new(
                self.config.window_manager_type.clone(),
                stage_context.clone(),
                watcher_shutdown_rx,
            )
            .await
            {
                Ok(mut watcher) => {
                    *handle = WatcherHandle::initialized("window_manager");
                    let health = handle.health_tracker();
                    let task = spawn_watcher_with_panic_catch(
                        "window_manager",
                        Some(Arc::clone(&health)),
                        async move { watcher.start_monitoring().await },
                    );
                    handle.start(task, None)?;
                    state.health.window_manager_active = true;
                    state.health.window_manager_last_error = None;
                }
                Err(e) => {
                    if !Self::is_platform_missing_error(&e) || self.config.require_hyprland {
                        error!("Failed to initialize window manager watcher: {}", e);
                        state.health.window_manager_active = false;
                        state.health.window_manager_last_error = Some(e.to_string());
                    } else {
                        warn!("Window manager watcher skipped: {}", e);
                    }
                }
            }
        }

        Ok(())
    }
}

impl IngestorNode for DesktopNode {
    type Config = DesktopConfig;
    type State = DesktopPersistentState;

    fn name(&self) -> &'static str {
        "desktop-watcher"
    }

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            supports_continuous: true,
            supports_snapshot: true,
            supports_interactive: false,
            max_scan_size: Some(1000),
            supports_concurrent: false,
            manages_own_continuous_loop: true,
            ..NodeCapabilities::default()
        }
    }

    #[instrument(skip(self, runtime, _state), fields(node = "desktop"))]
    async fn initialize(
        &mut self,
        mut config: Self::Config,
        runtime: &NodeRuntimeState,
        _state: &mut Self::State,
    ) -> NodeResult<()> {
        let service_name = runtime.service_info().service_name().to_string();

        info!(
            node = self.name(),
            service = %service_name,
            "Initializing desktop node"
        );

        Self::apply_config_overrides(&mut config, runtime)?;
        Self::apply_env_overrides(&mut config)?;

        info!(
            clipboard_enabled = config.clipboard_enabled,
            window_manager_enabled = config.window_manager_enabled,
            window_manager_type = %config.window_manager_type,
            clipboard_poll_interval_secs = config.clipboard_poll_interval_secs.as_secs(),
            require_hyprland = config.require_hyprland,
            activitywatch_db_path = ?config.activitywatch_db_path,
            "Desktop node configuration"
        );

        let publisher: Arc<NatsPublisher> = match runtime.transport() {
            EventTransport::Nats(publisher) => Arc::clone(publisher),
        };
        AcquisitionManager::bootstrap_streams(publisher.nats_client()).await?;

        let acquisition =
            Arc::new(runtime.acquisition_manager(RotationPolicy::default(), "desktop")?);
        let stage_context = StageAsYouGoContext::from_runtime(runtime)
            .with_acquisition_manager(Arc::clone(&acquisition))
            .with_default_reconciliation();

        self.runtime = Some(runtime.clone());
        self.config = config;
        self.stage_context = Some(stage_context);
        self.acquisition = Some(acquisition);

        self.initialize_watcher_handles();

        Ok(())
    }

    #[instrument(skip(self, state), fields(node = "desktop"))]
    async fn scan_snapshot(
        &mut self,
        state: &mut Self::State,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        let started_at = Timestamp::now();
        let start_time = std::time::Instant::now();

        let snapshot = self.take_snapshot(&state.health).await?;
        state.last_state = Some(snapshot.clone());
        let finished_at = Timestamp::now();

        let report = ScanReport {
            events_processed: snapshot.enabled_sources.len() as u64,
            duration: start_time.elapsed(),
            final_checkpoint: Checkpoint::timestamp(finished_at, None),
            time_range: Some((started_at, finished_at)),
            node_stats: HashMap::new(),
            successful_targets: vec!["desktop_snapshot".to_string()],
            failed_targets: vec![],
            warnings: vec![],
        };
        Ok(report)
    }

    #[instrument(skip(self, state), fields(node = "desktop"))]
    async fn scan_historical(
        &mut self,
        state: &mut Self::State,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        info!(
            checkpoint = ?from,
            replay = args.replay.is_some(),
            "Starting desktop historical scan"
        );
        let start_time = std::time::Instant::now();

        let Some(db_path) = self.configured_activitywatch_db_path().cloned() else {
            return Ok(ScanReport {
                events_processed: 0,
                duration: start_time.elapsed(),
                final_checkpoint: from,
                time_range: None,
                node_stats: HashMap::new(),
                successful_targets: Vec::new(),
                failed_targets: vec![(
                    "desktop_activitywatch_historical".to_string(),
                    "ActivityWatch historical import is not configured".to_string(),
                )],
                warnings: vec![
                    "ActivityWatch historical import is not configured; no desktop history was scanned".to_string(),
                ],
            });
        };

        ensure_activitywatch_sqlite(&db_path).map_err(|error| {
            SinexError::configuration(format!(
                "ActivityWatch database at {db_path} is unusable: {error}"
            ))
        })?;

        let start_row_id =
            Self::historical_activitywatch_start_row_for_scan(state, &from, args.replay.is_some())?;
        let mut first_ts = None;
        let mut last_ts = None;
        let acquisition =
            Arc::clone(self.acquisition.as_ref().ok_or_else(|| {
                SinexError::lifecycle("Desktop acquisition manager not initialized")
            })?);
        let source = RecordSources::sqlite(
            db_path.clone(),
            db_path.as_str(),
            read_activitywatch_history,
            |entry: &ActivityWatchHistoryEntry| entry.row_id,
        )
        .with_snapshot_policy(SqliteSnapshotPolicy::audit_default());
        let harness = BufferedRecordSourceHarness::buffered_default(source, acquisition);
        let horizon = until
            .end_time()
            .map_or(RecordReadHorizon::Unbounded, RecordReadHorizon::Until);
        let mut checkpoint = SqliteRowCheckpoint::new(start_row_id);
        let node = &*self;
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            SinexError::lifecycle("Desktop runtime not initialized for ActivityWatch import")
        })?;
        let mut import_report = harness
            .read_process_lenient_with_snapshot(
                &mut checkpoint,
                horizon,
                &mut state.activitywatch_snapshot,
                self.acquisition.as_ref().ok_or_else(|| {
                    SinexError::lifecycle("Desktop acquisition manager not initialized")
                })?,
                |entry, ctx| {
                    let started_at = entry.started_at;
                    let ended_at = entry.ended_at;
                    if first_ts.is_none() {
                        first_ts = Some(started_at);
                    }
                    last_ts = Some(ended_at);
                    async move {
                        node.emit_activitywatch_entry(ctx.materializer(), &entry)
                            .await
                            .map(|()| RecordProcessingOutcome::Processed)
                    }
                },
                |_| RecordWarningDisposition::Retry,
            )
            .await
            .map_err(|error| {
                SinexError::io(format!(
                    "Failed to read ActivityWatch history from {db_path}: {error}"
                ))
            })?;

        harness
            .finalize_with_snapshot_evidence(
                "desktop-activitywatch-historical",
                &mut import_report,
                Some(SqliteSnapshotLinker::new(runtime.db_pool())),
            )
            .await?;

        if let Some(error) = import_report.warnings.into_iter().next() {
            return Err(error);
        }

        let row_id_cursor = checkpoint.row_id;
        if row_id_cursor > state.activitywatch_last_row_id {
            state.activitywatch_last_row_id = row_id_cursor;
        }

        Ok(ScanReport {
            events_processed: import_report.processed_records as u64,
            duration: start_time.elapsed(),
            final_checkpoint: Checkpoint::external(
                json!({ "activitywatch_row_id": row_id_cursor }),
                format!("ActivityWatch row {row_id_cursor}"),
            ),
            time_range: first_ts.zip(last_ts),
            node_stats: HashMap::new(),
            successful_targets: vec!["desktop_activitywatch_historical".to_string()],
            failed_targets: Vec::new(),
            warnings: if import_report.processed_records == 0 {
                vec![format!(
                    "No new ActivityWatch rows found in {} beyond row {}",
                    db_path, start_row_id
                )]
            } else {
                Vec::new()
            },
        })
    }

    #[instrument(skip(self, state, shutdown_rx), fields(node = "desktop"))]
    async fn run_continuous(
        &mut self,
        state: &mut Self::State,
        _start: ContinuousStart,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> NodeResult<ScanReport> {
        info!("Starting continuous desktop monitoring");
        let started_at = Timestamp::now();
        let start_time = std::time::Instant::now();
        let mut warnings = Vec::new();

        // Ensure handles are initialized
        self.initialize_watcher_handles();

        // Create a local watch channel so ensure_watchers_running can subscribe
        // fresh receivers when restarting dead watchers.
        let (watcher_tx, watcher_rx) = watch::channel(false);
        self.watcher_shutdown_tx = Some(watcher_tx);

        // Bootstrap watchers on first entry
        self.ensure_watchers_running(state, watcher_rx).await;

        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        // Skip the first tick — watchers were just started above.
        interval.tick().await;

        loop {
            tokio::select! {
                shutdown_result = shutdown_rx.changed() => {
                    if shutdown_result.is_err() {
                        let warning =
                            "desktop continuous monitoring shutdown channel dropped before explicit shutdown";
                        warn!("{warning}");
                        warnings.push(warning.to_string());
                    }
                    break;
                }
                _ = interval.tick() => {
                    // Check and restart watchers if needed
                    let fresh_rx = self.watcher_shutdown_tx
                        .as_ref()
                        .expect("watcher_shutdown_tx should be set")
                        .subscribe();
                    if let Err(e) = self.ensure_watchers_running(state, fresh_rx).await {
                        warn!(error = %e, "Failed to ensure desktop watchers are running");
                    }
                }
            }
        }

        self.watcher_shutdown_tx = None;
        let finished_at = Timestamp::now();

        Ok(ScanReport {
            events_processed: 0,
            duration: start_time.elapsed(),
            final_checkpoint: Checkpoint::timestamp(finished_at, None),
            time_range: Some((started_at, finished_at)),
            node_stats: HashMap::new(),
            successful_targets: vec!["desktop_continuous".to_string()],
            failed_targets: vec![],
            warnings,
        })
    }

    async fn shutdown(&mut self, _state: &Self::State) -> NodeResult<()> {
        let mut shutdown_errors = Vec::new();
        if let Some(handle) = self.clipboard_watcher.take()
            && let Err(error) = handle.shutdown().await
        {
            shutdown_errors.push(("clipboard watcher", error));
        }
        if let Some(handle) = self.window_manager_watcher.take()
            && let Err(error) = handle.shutdown().await
        {
            shutdown_errors.push(("window manager watcher", error));
        }
        Self::collapse_shutdown_errors(shutdown_errors)
    }

    // Impl ExplorationProvider via IngestorNode interface override
    fn get_source_state(&self, state: &Self::State) -> NodeResult<SourceState> {
        let recent_activity = if let Some(ref s) = state.last_state {
            s.recent_activity
                .iter()
                .enumerate()
                .map(|(i, desc)| ActivityEntry {
                    timestamp: s.captured_at - time::Duration::minutes(i as i64),
                    description: desc.clone(),
                    data: None,
                })
                .collect()
        } else {
            vec![]
        };

        let active_sources = [
            self.config.clipboard_enabled,
            self.config.window_manager_enabled,
        ]
        .iter()
        .filter(|&&enabled| enabled)
        .count() as u64;
        let connected_sources = [self.clipboard_connected(), self.window_manager_connected()]
            .iter()
            .filter(|&&active| active)
            .count() as u64;
        let healthy = active_sources > 0 && connected_sources == active_sources;
        let is_connected = active_sources > 0 && connected_sources > 0;
        let clipboard_error = Self::live_watcher_error(self.clipboard_watcher.as_ref())
            .or_else(|| state.health.clipboard_last_error.clone());
        let window_manager_error = Self::live_watcher_error(self.window_manager_watcher.as_ref())
            .or_else(|| state.health.window_manager_last_error.clone());
        let mut metadata = HashMap::new();
        metadata.insert("enabled_sources".to_string(), json!(active_sources));
        metadata.insert("connected_sources".to_string(), json!(connected_sources));
        metadata.insert(
            "watcher_health".to_string(),
            json!({
                "clipboard_active": self.clipboard_connected(),
                "clipboard_error": clipboard_error,
                "window_manager_active": self.window_manager_connected(),
                "window_manager_error": window_manager_error,
            }),
        );
        let description = if active_sources == 0 {
            "Desktop Source (all watchers disabled)".to_string()
        } else if connected_sources == 0 {
            format!("Desktop Source ({active_sources} enabled watcher(s), none connected)")
        } else if connected_sources < active_sources {
            format!(
                "Desktop Source ({connected_sources}/{active_sources} watcher(s) connected, degraded)"
            )
        } else {
            format!("Desktop Source ({connected_sources}/{active_sources} watcher(s) connected)")
        };

        Ok(SourceState {
            description,
            last_updated: state.last_state.as_ref().map(|s| s.captured_at),
            total_items: None,
            healthy,
            recent_activity,
            metadata,
            is_connected,
            lag_seconds: None,
        })
    }

    fn get_ingestion_history(
        &self,
        _state: &Self::State,
        _limit: u64,
    ) -> NodeResult<Vec<IngestionHistoryEntry>> {
        Err(SinexError::invalid_state(
            "ingestion history is not implemented for desktop watcher sources",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sinex_db::{DbPool, DbPoolExt};
    use sinex_node_sdk::{IngestorNodeAdapter, NodeRunner, ShutdownConfig};
    use sinex_primitives::{
        Pagination,
        domain::{EventSource, EventType},
    };
    #[cfg(unix)]
    use std::ffi::OsString;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;
    use std::time::Duration;
    use xtask::sandbox::{
        EnvGuard, TestContext, TestIngestdConfig, TestResult, node_runtime::TestRuntimeBuilder,
        sinex_serial_test, sinex_test, start_test_ingestd_with_config, timing::Timeouts,
    };

    fn sample_activitywatch_entry(
        kind: ActivityWatchEntryKind,
        data: serde_json::Value,
    ) -> xtask::sandbox::TestResult<ActivityWatchHistoryEntry> {
        Ok(ActivityWatchHistoryEntry {
            row_id: 7,
            bucket_id: "aw-watcher-window_sinnix-prime".to_string(),
            kind,
            host: "sinnix-prime".to_string(),
            started_at: Timestamp::from_unix_timestamp(10)
                .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?,
            ended_at: Timestamp::from_unix_timestamp(11)
                .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?,
            duration_ms: 1000,
            data,
        })
    }

    fn raw_node_config<T: Serialize>(config: &T) -> TestResult<HashMap<String, serde_json::Value>> {
        let value = serde_json::to_value(config)?;
        let serde_json::Value::Object(object) = value else {
            return Err(color_eyre::eyre::eyre!(
                "node config must serialize to a JSON object"
            ));
        };
        Ok(object.into_iter().collect())
    }

    fn tune_batcher_for_runtime_proof(
        config: &mut HashMap<String, serde_json::Value>,
        service_prefix: &str,
    ) -> String {
        let suffix = Uuid::now_v7();
        let service_name = format!("{service_prefix}-{suffix}");
        config.insert("batch_size".to_string(), json!(1));
        config.insert("batch_timeout_ms".to_string(), json!(20));
        config.insert(
            "consumer_group".to_string(),
            json!(format!("proof-{suffix}")),
        );
        service_name
    }

    async fn wait_for_source_material_consumer(ctx: &TestContext) -> TestResult<()> {
        let env = sinex_primitives::environment::environment();
        let nats = ctx.nats_handle()?;
        let js = nats.jetstream_with_client(ctx.nats_client());
        let stream = env.nats_stream_name("SOURCE_MATERIAL");
        nats.wait_for_consumer_on_stream(&js, &stream, Duration::from_secs(Timeouts::STANDARD))
            .await?;
        Ok(())
    }

    async fn wait_for_event_count(
        pool: DbPool,
        source: &'static str,
        event_type: &'static str,
        expected_count: i64,
    ) -> TestResult<()> {
        let source = EventSource::new(source)?;
        let event_type = EventType::new(event_type)?;
        xtask::sandbox::timing::WaitHelpers::wait_for_condition(
            move || {
                let pool = pool.clone();
                let source = source.clone();
                let event_type = event_type.clone();
                async move {
                    let count = pool
                        .events()
                        .count_by_source_and_event_type(&source, &event_type)
                        .await
                        .map_err(|error| color_eyre::eyre::eyre!("database error: {error}"))?;
                    Ok::<bool, color_eyre::eyre::Report>(count == expected_count)
                }
            },
            Timeouts::STANDARD,
        )
        .await
    }

    async fn persisted_events(
        pool: &DbPool,
        source: &str,
        event_type: &str,
    ) -> TestResult<Vec<sinex_primitives::events::Event<serde_json::Value>>> {
        let source = EventSource::new(source)?;
        let event_type = EventType::new(event_type)?;
        let mut events = pool
            .events()
            .get_by_source(&source, Pagination::new(Some(100), None))
            .await?;
        events.retain(|event| event.event_type == event_type);
        events.sort_by_key(|event| (event.ts_orig, event.id));
        Ok(events)
    }

    fn assert_material_provenance_rows(
        rows: &[sinex_primitives::events::Event<serde_json::Value>],
        label: &str,
    ) -> TestResult<()> {
        for (index, event) in rows.iter().enumerate() {
            match event.provenance() {
                sinex_primitives::events::Provenance::Material { anchor_byte, .. }
                    if *anchor_byte >= 0 => {}
                other => {
                    return Err(color_eyre::eyre::eyre!(
                        "{label} row {index} has invalid provenance: {other:?}"
                    ));
                }
            }
        }
        Ok(())
    }

    fn write_activitywatch_fixture(path: &Utf8PathBuf) -> TestResult<()> {
        let conn = rusqlite::Connection::open(path.as_str())?;
        conn.execute_batch(
            "
            CREATE TABLE buckets (
              id INTEGER PRIMARY KEY,
              name TEXT NOT NULL
            );
            CREATE TABLE events (
              bucketrow INTEGER NOT NULL,
              starttime INTEGER NOT NULL,
              endtime INTEGER NOT NULL,
              data TEXT,
              FOREIGN KEY(bucketrow) REFERENCES buckets(id)
            );
            INSERT INTO buckets (id, name) VALUES
              (1, 'aw-watcher-window_sinnix-prime'),
              (2, 'aw-watcher-web_sinnix-prime'),
              (3, 'aw-watcher-afk_sinnix-prime');
            INSERT INTO events (bucketrow, starttime, endtime, data) VALUES
              (1, 1000000000, 4000000000, '{\"app\":\"kitty\",\"title\":\"main.rs\"}'),
              (2, 5000000000, 9000000000, '{\"app\":\"Firefox\",\"title\":\"Docs\",\"url\":\"https://example.com\"}'),
              (3, 10000000000, 16000000000, '{\"status\":\"afk\"}');
            ",
        )?;
        Ok(())
    }

    #[sinex_test]
    async fn desktop_default_activitywatch_db_path_uses_utf8_data_dir()
    -> xtask::sandbox::TestResult<()> {
        let path = default_activitywatch_db_path_from(Some(std::path::PathBuf::from("/tmp/data")))
            .ok_or_else(|| color_eyre::eyre::eyre!("default ActivityWatch path should exist"))?;

        assert_eq!(
            path,
            Utf8PathBuf::from("/tmp/data/activitywatch/aw-server-rust/sqlite.db")
        );
        Ok(())
    }

    #[sinex_serial_test]
    async fn desktop_default_activitywatch_db_path_prefers_xdg_data_home()
    -> xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("XDG_DATA_HOME", "/tmp/xdg-data");

        let config = DesktopConfig::default();

        assert_eq!(
            config.activitywatch_db_path,
            Some(Utf8PathBuf::from(
                "/tmp/xdg-data/activitywatch/aw-server-rust/sqlite.db"
            ))
        );
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_test]
    async fn desktop_default_activitywatch_db_path_rejects_non_utf8_data_dir()
    -> xtask::sandbox::TestResult<()> {
        let invalid_dir =
            std::path::PathBuf::from(OsString::from_vec(vec![b'/', b't', b'm', b'p', b'/', 0xff]));

        assert_eq!(default_activitywatch_db_path_from(Some(invalid_dir)), None);
        Ok(())
    }

    #[sinex_test]
    async fn scan_historical_persists_activitywatch_through_node_runtime(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let temp_dir = tempfile::tempdir()?;
        let db_path = Utf8PathBuf::from_path_buf(temp_dir.path().join("activitywatch.db"))
            .map_err(|path| {
                color_eyre::eyre::eyre!("ActivityWatch temp path is not UTF-8: {}", path.display())
            })?;
        write_activitywatch_fixture(&db_path)?;

        let nats = ctx.nats_handle()?;
        let ingest_config = TestIngestdConfig {
            nats: nats.connection_config(),
            database_url: ctx.database_url().to_string(),
            work_dir: Some(temp_dir.path().join("ingestd")),
            ..Default::default()
        };
        let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;
        wait_for_source_material_consumer(&ctx).await?;

        let mut config = DesktopConfig::default();
        config.clipboard_enabled = false;
        config.window_manager_enabled = false;
        config.require_hyprland = false;
        config.activitywatch_db_path = Some(db_path);
        let mut raw_config = raw_node_config(&config)?;
        let service_name =
            tune_batcher_for_runtime_proof(&mut raw_config, "desktop-activitywatch-runtime-proof");

        let publisher = Arc::new(NatsPublisher::new(ctx.nats_client()));
        let checkpoint_path = temp_dir
            .path()
            .join("desktop-runtime-proof.checkpoint.json");
        let adapter =
            IngestorNodeAdapter::new(DesktopNode::new()).with_shutdown_config(ShutdownConfig {
                checkpoint_path: Some(checkpoint_path),
                ..ShutdownConfig::default()
            });
        let mut runner = NodeRunner::new(adapter);
        runner
            .initialize_with_transport(
                service_name,
                raw_config,
                Some(ctx.pool.clone()),
                EventTransport::Nats(publisher),
                temp_dir.path().join("runner"),
                false,
            )
            .await?;

        let report = runner
            .run_scan(
                Checkpoint::None,
                TimeHorizon::Historical {
                    end_time: Timestamp::now(),
                },
                ScanArgs::default(),
            )
            .await?;
        assert_eq!(report.events_processed, 3);

        wait_for_event_count(ctx.pool.clone(), "activitywatch", "window.active", 1).await?;
        wait_for_event_count(ctx.pool.clone(), "activitywatch", "browser.tab.active", 1).await?;
        wait_for_event_count(ctx.pool.clone(), "activitywatch", "afk.changed", 1).await?;

        let window_rows = persisted_events(&ctx.pool, "activitywatch", "window.active").await?;
        let browser_rows =
            persisted_events(&ctx.pool, "activitywatch", "browser.tab.active").await?;
        let afk_rows = persisted_events(&ctx.pool, "activitywatch", "afk.changed").await?;
        assert_material_provenance_rows(&window_rows, "ActivityWatch window")?;
        assert_material_provenance_rows(&browser_rows, "ActivityWatch browser")?;
        assert_material_provenance_rows(&afk_rows, "ActivityWatch afk")?;
        assert_eq!(
            window_rows
                .first()
                .and_then(|event| event.payload.get("app"))
                .and_then(serde_json::Value::as_str),
            Some("kitty")
        );
        assert_eq!(
            browser_rows
                .first()
                .and_then(|event| event.payload.get("browser"))
                .and_then(serde_json::Value::as_str),
            Some("Firefox")
        );
        assert_eq!(
            afk_rows
                .first()
                .and_then(|event| event.payload.get("status"))
                .and_then(serde_json::Value::as_str),
            Some("afk")
        );

        let rerun_report = runner
            .run_scan(
                report.final_checkpoint.clone(),
                TimeHorizon::Historical {
                    end_time: Timestamp::now(),
                },
                ScanArgs::default(),
            )
            .await?;
        assert_eq!(rerun_report.events_processed, 0);
        assert_eq!(
            persisted_events(&ctx.pool, "activitywatch", "window.active")
                .await?
                .len(),
            1
        );
        assert_eq!(
            persisted_events(&ctx.pool, "activitywatch", "browser.tab.active")
                .await?
                .len(),
            1
        );
        assert_eq!(
            persisted_events(&ctx.pool, "activitywatch", "afk.changed")
                .await?
                .len(),
            1
        );

        runner.shutdown().await?;
        ingest_handle.stop().await?;
        Ok(())
    }

    #[sinex_test]
    async fn activitywatch_historical_start_row_prefers_checkpoint_when_present()
    -> xtask::sandbox::TestResult<()> {
        let state = DesktopPersistentState {
            activitywatch_last_row_id: 42,
            ..DesktopPersistentState::default()
        };
        let checkpoint = Checkpoint::external(
            json!({ "activitywatch_row_id": 12 }),
            "ActivityWatch row 12",
        );

        assert_eq!(
            DesktopNode::historical_activitywatch_start_row(&state, &checkpoint)?,
            12
        );
        Ok(())
    }

    #[sinex_test]
    async fn activitywatch_historical_start_row_falls_back_to_state()
    -> xtask::sandbox::TestResult<()> {
        let state = DesktopPersistentState {
            activitywatch_last_row_id: 42,
            ..DesktopPersistentState::default()
        };

        assert_eq!(
            DesktopNode::historical_activitywatch_start_row(&state, &Checkpoint::None)?,
            42
        );
        Ok(())
    }

    #[sinex_test]
    async fn activitywatch_historical_start_row_rejects_negative_checkpoint_row_id()
    -> xtask::sandbox::TestResult<()> {
        let state = DesktopPersistentState {
            activitywatch_last_row_id: 42,
            ..DesktopPersistentState::default()
        };
        let checkpoint = Checkpoint::external(
            json!({ "activitywatch_row_id": -1 }),
            "ActivityWatch row -1",
        );

        let error = DesktopNode::historical_activitywatch_start_row(&state, &checkpoint)
            .expect_err("negative ActivityWatch checkpoint row ids must fail honestly");
        assert!(
            error
                .to_string()
                .contains("invalid negative activitywatch_row_id")
        );
        Ok(())
    }

    #[sinex_test]
    async fn activitywatch_historical_start_row_rejects_non_integer_checkpoint_row_id()
    -> xtask::sandbox::TestResult<()> {
        let state = DesktopPersistentState {
            activitywatch_last_row_id: 42,
            ..DesktopPersistentState::default()
        };
        let checkpoint = Checkpoint::external(
            json!({ "activitywatch_row_id": "twelve" }),
            "ActivityWatch row twelve",
        );

        let error = DesktopNode::historical_activitywatch_start_row(&state, &checkpoint)
            .expect_err("non-integer ActivityWatch checkpoint row ids must fail honestly");
        assert!(error.to_string().contains("row id must be an integer"));
        Ok(())
    }

    #[sinex_test]
    async fn activitywatch_historical_start_row_rejects_incompatible_checkpoint_kind()
    -> xtask::sandbox::TestResult<()> {
        let state = DesktopPersistentState {
            activitywatch_last_row_id: 42,
            ..DesktopPersistentState::default()
        };
        let checkpoint = Checkpoint::timestamp(Timestamp::now(), None);

        let error = DesktopNode::historical_activitywatch_start_row(&state, &checkpoint)
            .expect_err("non-external ActivityWatch checkpoints must fail honestly");
        let message = format!("{error:#}");
        assert!(message.contains("requires an external checkpoint"));
        assert!(message.contains("timestamp"));
        Ok(())
    }

    #[sinex_test]
    async fn activitywatch_historical_start_row_for_normal_scan_falls_back_to_state()
    -> xtask::sandbox::TestResult<()> {
        let state = DesktopPersistentState {
            activitywatch_last_row_id: 42,
            ..DesktopPersistentState::default()
        };
        let checkpoint = Checkpoint::timestamp(Timestamp::now(), None);

        assert_eq!(
            DesktopNode::historical_activitywatch_start_row_for_scan(&state, &checkpoint, false)?,
            42
        );
        Ok(())
    }

    #[sinex_test]
    async fn activitywatch_historical_start_row_for_replay_keeps_external_checkpoint_requirement()
    -> xtask::sandbox::TestResult<()> {
        let state = DesktopPersistentState {
            activitywatch_last_row_id: 42,
            ..DesktopPersistentState::default()
        };
        let checkpoint = Checkpoint::timestamp(Timestamp::now(), None);

        let error =
            DesktopNode::historical_activitywatch_start_row_for_scan(&state, &checkpoint, true)
                .expect_err("replay scans must still require an external ActivityWatch checkpoint");
        assert!(
            error
                .to_string()
                .contains("requires an external checkpoint")
        );
        Ok(())
    }

    #[sinex_test]
    async fn desktop_config_overrides_reject_invalid_window_manager_type()
    -> xtask::sandbox::TestResult<()> {
        let mut config = DesktopConfig::default();
        let overrides = HashMap::from([("window_manager_type".to_string(), json!("sway"))]);

        let error = DesktopNode::apply_config_overrides(&mut config, &overrides)
            .expect_err("invalid window manager overrides should fail honestly");
        let message = error.to_string();

        assert!(message.contains("window manager type"));
        assert!(message.contains("sway"));
        Ok(())
    }

    #[sinex_test]
    async fn desktop_env_override_rejects_invalid_require_hyprland_value()
    -> xtask::sandbox::TestResult<()> {
        let error = DesktopNode::parse_bool_env_override("SINEX_DESKTOP_REQUIRE_HYPRLAND", "maybe")
            .expect_err("invalid env bool should fail honestly");
        let message = error.to_string();

        assert!(message.contains("SINEX_DESKTOP_REQUIRE_HYPRLAND"));
        assert!(message.contains("maybe"));
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_serial_test]
    async fn desktop_env_override_rejects_non_unicode_activitywatch_db_path()
    -> xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set(
            "SINEX_ACTIVITYWATCH_DB_PATH",
            OsString::from_vec(vec![0x66, 0x6f, 0x80, 0x6f]),
        );
        env.clear("SINEX_DESKTOP_REQUIRE_HYPRLAND");

        let mut config = DesktopConfig::default();
        let error = DesktopNode::apply_env_overrides(&mut config)
            .expect_err("non-UTF8 ActivityWatch overrides must fail honestly");
        let message = error.to_string();

        assert!(message.contains("SINEX_ACTIVITYWATCH_DB_PATH"));
        assert!(message.contains("not valid UTF-8"));
        Ok(())
    }

    #[sinex_test]
    async fn desktop_source_state_is_disconnected_when_enabled_watchers_are_inactive()
    -> xtask::sandbox::TestResult<()> {
        let node = DesktopNode::new();
        let source = IngestorNode::get_source_state(&node, &DesktopPersistentState::default())?;

        assert!(!source.is_connected);
        assert!(!source.healthy);
        assert!(
            source.description.contains("none connected"),
            "unexpected description: {}",
            source.description
        );
        assert_eq!(
            source
                .metadata
                .get("enabled_sources")
                .and_then(serde_json::Value::as_u64),
            Some(2)
        );
        assert_eq!(
            source
                .metadata
                .get("connected_sources")
                .and_then(serde_json::Value::as_u64),
            Some(0)
        );
        assert_eq!(source.last_updated, None);
        Ok(())
    }

    #[sinex_test]
    async fn desktop_source_state_ignores_stale_persisted_watcher_flags()
    -> xtask::sandbox::TestResult<()> {
        let node = DesktopNode::new();
        let state = DesktopPersistentState {
            health: DesktopMonitorHealth {
                clipboard_active: true,
                window_manager_active: true,
                ..DesktopMonitorHealth::default()
            },
            ..DesktopPersistentState::default()
        };

        let source = IngestorNode::get_source_state(&node, &state)?;

        assert!(!source.is_connected);
        assert!(!source.healthy);
        assert_eq!(
            source
                .metadata
                .get("connected_sources")
                .and_then(serde_json::Value::as_u64),
            Some(0)
        );
        assert_eq!(
            source.metadata.get("watcher_health"),
            Some(&json!({
                "clipboard_active": false,
                "clipboard_error": serde_json::Value::Null,
                "window_manager_active": false,
                "window_manager_error": serde_json::Value::Null,
            }))
        );
        Ok(())
    }

    #[sinex_test]
    async fn desktop_source_state_reports_live_watcher_handle_activity()
    -> xtask::sandbox::TestResult<()> {
        let mut node = DesktopNode::new();
        let task = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        node.clipboard_watcher = Some(WatcherHandle::running("clipboard", task, None, None));

        let source = IngestorNode::get_source_state(&node, &DesktopPersistentState::default())?;

        assert!(source.is_connected);
        assert!(!source.healthy);
        assert!(source.description.contains("degraded"));
        assert_eq!(
            source
                .metadata
                .get("connected_sources")
                .and_then(serde_json::Value::as_u64),
            Some(1)
        );
        assert_eq!(
            source.metadata.get("watcher_health"),
            Some(&json!({
                "clipboard_active": true,
                "clipboard_error": serde_json::Value::Null,
                "window_manager_active": false,
                "window_manager_error": serde_json::Value::Null,
            }))
        );
        Ok(())
    }

    #[sinex_test]
    async fn desktop_source_state_surfaces_live_watcher_errors() -> xtask::sandbox::TestResult<()> {
        let mut node = DesktopNode::new();
        let handle = WatcherHandle::initialized("clipboard");
        handle.record_error("clipboard watcher crashed".to_string());
        node.clipboard_watcher = Some(handle);

        let source = IngestorNode::get_source_state(&node, &DesktopPersistentState::default())?;

        assert_eq!(
            source.metadata.get("watcher_health"),
            Some(&json!({
                "clipboard_active": false,
                "clipboard_error": "clipboard watcher crashed",
                "window_manager_active": false,
                "window_manager_error": serde_json::Value::Null,
            }))
        );
        Ok(())
    }

    #[sinex_test]
    async fn desktop_source_state_marks_disabled_configuration_unhealthy()
    -> xtask::sandbox::TestResult<()> {
        let mut node = DesktopNode::new();
        node.config.clipboard_enabled = false;
        node.config.window_manager_enabled = false;

        let source = IngestorNode::get_source_state(&node, &DesktopPersistentState::default())?;

        assert!(!source.is_connected);
        assert!(!source.healthy);
        assert!(source.description.contains("all watchers disabled"));
        assert_eq!(
            source
                .metadata
                .get("enabled_sources")
                .and_then(serde_json::Value::as_u64),
            Some(0)
        );
        assert_eq!(
            source
                .metadata
                .get("connected_sources")
                .and_then(serde_json::Value::as_u64),
            Some(0)
        );
        Ok(())
    }

    #[sinex_test]
    async fn desktop_run_continuous_warns_when_shutdown_sender_drops(
        ctx: xtask::sandbox::TestContext,
    ) -> xtask::sandbox::TestResult<()> {
        let runtime = TestRuntimeBuilder::new(&ctx, "desktop-shutdown-drop")
            .with_dry_run(true)
            .build()
            .await?;

        let mut node = DesktopNode::new();
        let mut config = DesktopConfig::default();
        config.clipboard_enabled = false;
        config.window_manager_enabled = false;
        config.activitywatch_db_path = None;
        let mut state = DesktopPersistentState::default();
        node.initialize(config, &runtime.runtime, &mut state)
            .await?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(async move {
            let mut node = node;
            let mut state = state;
            node.run_continuous(
                &mut state,
                ContinuousStart::from_checkpoint(Checkpoint::None),
                shutdown_rx,
            )
            .await
        });

        tokio::task::yield_now().await;
        drop(shutdown_tx);

        let report = task.await??;
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("shutdown channel dropped")),
            "expected shutdown channel drop warning, got: {:?}",
            report.warnings
        );
        Ok(())
    }

    #[sinex_test]
    async fn desktop_run_continuous_returns_immediately_when_shutdown_already_requested(
        ctx: xtask::sandbox::TestContext,
    ) -> xtask::sandbox::TestResult<()> {
        let runtime = TestRuntimeBuilder::new(&ctx, "desktop-pre-signaled-shutdown")
            .with_dry_run(true)
            .build()
            .await?;

        let mut node = DesktopNode::new();
        let mut config = DesktopConfig::default();
        config.clipboard_enabled = false;
        config.window_manager_enabled = false;
        config.activitywatch_db_path = None;
        let mut state = DesktopPersistentState::default();
        node.initialize(config, &runtime.runtime, &mut state)
            .await?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let _ = shutdown_tx.send(true);

        let report = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            node.run_continuous(
                &mut state,
                ContinuousStart::from_checkpoint(Checkpoint::None),
                shutdown_rx,
            ),
        )
        .await??;
        assert!(
            report.warnings.is_empty(),
            "pre-signaled shutdown should not be reported as a dropped shutdown channel: {:?}",
            report.warnings
        );

        node.shutdown(&state).await?;
        Ok(())
    }

    #[sinex_test]
    async fn desktop_run_continuous_reports_elapsed_time_window(
        ctx: xtask::sandbox::TestContext,
    ) -> xtask::sandbox::TestResult<()> {
        let runtime = TestRuntimeBuilder::new(&ctx, "desktop-time-window")
            .with_dry_run(true)
            .build()
            .await?;

        let mut node = DesktopNode::new();
        let mut config = DesktopConfig::default();
        config.clipboard_enabled = false;
        config.window_manager_enabled = false;
        config.activitywatch_db_path = None;
        let mut state = DesktopPersistentState::default();
        node.initialize(config, &runtime.runtime, &mut state)
            .await?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(async move {
            let mut node = node;
            let mut state = state;
            node.run_continuous(
                &mut state,
                ContinuousStart::from_checkpoint(Checkpoint::None),
                shutdown_rx,
            )
            .await
        });

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        drop(shutdown_tx);

        let report = task.await??;
        let (started_at, finished_at) = report
            .time_range
            .expect("desktop continuous report should include an elapsed time window");
        assert!(finished_at > started_at);
        assert_eq!(
            report.final_checkpoint,
            Checkpoint::timestamp(finished_at, None)
        );
        Ok(())
    }

    #[sinex_test]
    async fn activitywatch_window_event_rejects_missing_app_field() -> xtask::sandbox::TestResult<()>
    {
        let entry = sample_activitywatch_entry(
            ActivityWatchEntryKind::Window,
            json!({ "title": "main.rs" }),
        )?;

        let error = DesktopNode::build_activitywatch_event(&entry, Uuid::now_v7(), 0, 32)
            .expect_err("missing ActivityWatch app should fail honestly");

        assert!(error.to_string().contains("missing required field 'app'"));
        Ok(())
    }

    #[sinex_test]
    async fn activitywatch_window_event_keeps_nonempty_redacted_title()
    -> xtask::sandbox::TestResult<()> {
        let entry = sample_activitywatch_entry(
            ActivityWatchEntryKind::Window,
            json!({ "app": "Alacritty", "title": "main.rs" }),
        )?;

        let event = DesktopNode::build_activitywatch_event(&entry, Uuid::now_v7(), 0, 32)?;

        assert_eq!(event.payload["title"], json!("main.rs"));
        Ok(())
    }

    #[sinex_test]
    async fn activitywatch_web_event_rejects_non_string_url_field() -> xtask::sandbox::TestResult<()>
    {
        let entry = sample_activitywatch_entry(
            ActivityWatchEntryKind::Web,
            json!({ "app": "Firefox", "title": "Docs", "url": 42 }),
        )?;

        let error = DesktopNode::build_activitywatch_event(&entry, Uuid::now_v7(), 0, 32)
            .expect_err("non-string ActivityWatch url should fail honestly");

        assert!(error.to_string().contains("field 'url' must be a string"));
        Ok(())
    }

    #[sinex_test]
    async fn activitywatch_web_event_keeps_nonempty_redacted_url() -> xtask::sandbox::TestResult<()>
    {
        let entry = sample_activitywatch_entry(
            ActivityWatchEntryKind::Web,
            json!({ "app": "Firefox", "title": "Docs", "url": "https://example.com/docs" }),
        )?;

        let event = DesktopNode::build_activitywatch_event(&entry, Uuid::now_v7(), 0, 32)?;

        assert_eq!(event.payload["url"], json!("https://example.com/docs"));
        Ok(())
    }

    #[sinex_test]
    async fn activitywatch_afk_event_rejects_empty_status_field() -> xtask::sandbox::TestResult<()>
    {
        let entry =
            sample_activitywatch_entry(ActivityWatchEntryKind::Afk, json!({ "status": "   " }))?;

        let error = DesktopNode::build_activitywatch_event(&entry, Uuid::now_v7(), 0, 32)
            .expect_err("empty ActivityWatch status should fail honestly");

        assert!(
            error
                .to_string()
                .contains("field 'status' must not be empty")
        );
        Ok(())
    }
}
