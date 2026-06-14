//! `desktop.window-manager` source.
//!
//! Reads line-delimited IPC events from the Hyprland Unix socket
//! (`socket2.sock`). Each line has the form `TYPE>>DATA`. The parser
//! dispatches by `TYPE` to the appropriate `HyprlandWindow*` payload.
//!
//! Adapter: `UnixSocketStreamAdapter`
//! Anchor: `StreamFrame` (live stream, no durable cursor)
//! Privacy tier: `Sensitive` — window-title fields are policy-scoped by payload path.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, ParsedEventIntent, ParserContext, ParserId, ParserManifest, SourceId,
    TimingEvidence,
};
use sinex_primitives::privacy::{ProcessingContext, SensitivityHint};
use sinex_macros::SourceMeta;
use sinex_primitives::source_contracts::{AccessScope, ResourceProfile, RunnerPack, PrivacyTier, CheckpointFamily, RuntimeShape, RetentionPolicy, OccurrenceIdentity, Horizon};
use sinex_primitives::temporal::Timestamp;

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult};

// ---------------------------------------------------------------------------
// Parser config
// ---------------------------------------------------------------------------

/// Configuration for [`HyprlandParser`].
///
/// At runtime the source host deserialises the source JSON config into this
/// struct via `UnixSocketStreamAdapter::Config` (the outer socket config), and
/// passes parser-specific fields here.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HyprlandParserConfig {
    /// Whether to degrade gracefully when Hyprland is absent.
    #[serde(default)]
    pub require_hyprland: bool,
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Parses line-delimited Hyprland IPC events from a `UnixSocketStreamAdapter`.
///
/// Each record is one socket line: `TYPE>>DATA`.  The parser splits on `>>`
/// and dispatches by `TYPE` to the appropriate payload.  Unknown event types
/// produce a `wm.unhandled` intent rather than being dropped, so the full IPC
/// stream is captured.
///
/// # activewindow / activewindowv2 merging
///
/// Hyprland fires a pair for every focus change:
/// - `activewindow` (v1): `class,title` — no address
/// - `activewindowv2` (v2): `address` — no class/title
///
/// We buffer the v1 fields and merge them into the v2 event so the emitted
/// `window.focused` event has both `window_id` (from v2) and `window_class` /
/// `window_title` (from v1).  If anything other than `activewindowv2` arrives
/// while `pending_activewindow` is set, the buffered partial is flushed first.
#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "desktop.window-manager",
    namespace = "desktop",
    event_source = "wm.hyprland",
    event_type = "window.opened",
    event_types = "window.closed, window.focused, window.moved, window.title_changed, workspace.switched, monitor.focused, state.captured, wm.unhandled",
    adapter = "UnixSocketStreamAdapter",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Continuous),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Anchor,
    access_scope = AccessScope::RuntimeBridge { surface: "window_manager" },
    implementation = "sinexd",
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::LiveWatcher,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::LiveObservation,
    runtime_shape = RuntimeShape::Continuous,
)]
pub struct HyprlandParser {
    /// Buffered (window_class, window_title) from the most recent `activewindow`
    /// v1 event waiting to be merged with the following `activewindowv2`.
    pending_activewindow: Option<(String, String)>,
}

#[async_trait]
impl MaterialParser for HyprlandParser {
    type Config = HyprlandParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("hyprland-ipc"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::UnixSocket],
            source_id: SourceId::from_static("desktop.window-manager"),
            declared_event_types: vec![
                (
                    EventSource::from_static("wm.hyprland"),
                    EventType::from_static("window.opened"),
                ),
                (
                    EventSource::from_static("wm.hyprland"),
                    EventType::from_static("window.closed"),
                ),
                (
                    EventSource::from_static("wm.hyprland"),
                    EventType::from_static("window.focused"),
                ),
                (
                    EventSource::from_static("wm.hyprland"),
                    EventType::from_static("window.moved"),
                ),
                (
                    EventSource::from_static("wm.hyprland"),
                    EventType::from_static("window.title_changed"),
                ),
                (
                    EventSource::from_static("wm.hyprland"),
                    EventType::from_static("workspace.switched"),
                ),
                (
                    EventSource::from_static("wm.hyprland"),
                    EventType::from_static("monitor.focused"),
                ),
                (
                    EventSource::from_static("wm.hyprland"),
                    EventType::from_static("state.captured"),
                ),
                (
                    EventSource::from_static("wm.hyprland"),
                    EventType::from_static("wm.unhandled"),
                ),
            ],
            privacy_contexts: vec![ProcessingContext::Document],
            // Window titles are free-form user text that may embed anything;
            // exported for policy tooling, never auto-acted (#1611).
            sensitivity_hints: vec![
                SensitivityHint::FreeText,
                SensitivityHint::PotentiallySensitive,
            ],
            description: "Parses Hyprland IPC socket events into typed window-manager events."
                .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: sinex_primitives::parser::SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let line = std::str::from_utf8(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("invalid UTF-8 in Hyprland IPC: {e}")))?;

        let line = line.trim();
        if line.is_empty() {
            return Ok(vec![]);
        }

        let (typ, data) = if let Some(pair) = line.split_once(">>") {
            pair
        } else {
            // Malformed line — flush any pending activewindow buffer, then capture as unhandled.
            let mut intents = self.flush_pending_activewindow(&record, ctx);
            intents.push(self.build_intent(
                "wm.unhandled",
                serde_json::json!({ "event_type": "unknown", "event_data": line }),
                &record,
                ctx,
            )?);
            return Ok(intents);
        };

        // activewindow (v1) — buffer class+title, do not emit yet; wait for v2.
        if typ == "activewindow" {
            let (class, title) = data.split_once(',').unwrap_or((data, ""));
            // Flush any stale pending (shouldn't happen, but be safe).
            let intents = self.flush_pending_activewindow(&record, ctx);
            self.pending_activewindow = Some((class.to_string(), title.to_string()));
            return Ok(intents);
        }

        // activewindowv2 — merge with buffered v1 class+title and emit one complete event.
        if typ == "activewindowv2" {
            let window_id = data.trim();
            let (window_class, window_title) = self.pending_activewindow.take().unzip();
            let payload = serde_json::json!({
                "window_id": window_id,
                "window_class": window_class,
                "window_title": window_title,
            });
            return Ok(vec![self.build_intent("window.focused", payload, &record, ctx)?]);
        }

        // Any other event type — flush stale pending activewindow first.
        let mut intents = self.flush_pending_activewindow(&record, ctx);

        match dispatch_hyprland_event(typ, data)? {
            Some((event_type_str, payload)) => {
                intents.push(self.build_intent(event_type_str, payload, &record, ctx)?);
            }
            None => {} // Suppressed (e.g. windowtitle v1)
        }

        Ok(intents)
    }

    fn baseline_adapter_config() -> serde_json::Value {
        let socket_path = std::env::var("SINEX_HYPRLAND_EVENT_SOCKET")
            .ok()
            .filter(|value| !value.is_empty())
            .or_else(|| {
                let runtime_dir = std::env::var("SINEX_HYPRLAND_RUNTIME_DIR")
                    .or_else(|_| std::env::var("XDG_RUNTIME_DIR"))
                    .ok()
                    .filter(|value| !value.is_empty())?;
                let signature = std::env::var("SINEX_HYPRLAND_INSTANCE_SIGNATURE")
                    .or_else(|_| std::env::var("HYPRLAND_INSTANCE_SIGNATURE"))
                    .ok()
                    .filter(|value| !value.is_empty())?;
                Some(
                    PathBuf::from(runtime_dir)
                        .join("hypr")
                        .join(signature)
                        .join(".socket2.sock")
                        .to_string_lossy()
                        .into_owned(),
                )
            });

        match socket_path {
            Some(socket_path) => serde_json::json!({
                "socket_path": socket_path,
                "reconnect_on_eof": true,
            }),
            None => serde_json::json!({}),
        }
    }
}

// ---------------------------------------------------------------------------
// Hyprland event dispatch
// ---------------------------------------------------------------------------

/// Returns `(sinex_event_type, payload_json)` for a parsed Hyprland IPC line.
///
/// Unknown event types map to `"wm.unhandled"` with the raw data preserved.
fn dispatch_hyprland_event(
    typ: &str,
    data: &str,
) -> ParserResult<Option<(&'static str, serde_json::Value)>> {
    let (event_type, payload) = match typ {
        "openwindow" => {
            // openwindow>>address,workspaceid,workspacename,class,title
            let parts: Vec<&str> = data.splitn(5, ',').collect();
            let workspace_id = parts.get(1).and_then(|s| s.parse::<i32>().ok());
            (
                "window.opened",
                serde_json::json!({
                    "window_id": parts.first().unwrap_or(&""),
                    "workspace_id": workspace_id,
                    "workspace_name": parts.get(2).unwrap_or(&""),
                    "window_class": parts.get(3).unwrap_or(&""),
                    "window_title": parts.get(4).unwrap_or(&""),
                }),
            )
        }
        "closewindow" => (
            "window.closed",
            serde_json::json!({ "window_id": data.trim() }),
        ),
        // activewindow / activewindowv2 are handled before dispatch in parse_record
        // (stateful v1+v2 merge). These arms are intentionally absent here.
        "movewindow" => {
            // movewindow>>address,workspaceid,workspacename
            let parts: Vec<&str> = data.splitn(3, ',').collect();
            let workspace_id = parts.get(1).and_then(|s| s.parse::<i32>().ok());
            (
                "window.moved",
                serde_json::json!({
                    "window_id": parts.first().unwrap_or(&""),
                    "workspace_id": workspace_id,
                    "workspace_name": parts.get(2).unwrap_or(&""),
                }),
            )
        }
        "windowtitlev2" => {
            // windowtitlev2>>address,title
            let (addr, title) = data.split_once(',').unwrap_or((data, ""));
            (
                "window.title_changed",
                serde_json::json!({
                    "window_id": addr,
                    "window_title": title,
                }),
            )
        }
        "windowtitle" => {
            // Hyprland emits both `windowtitle` (v1, address-only hint) and
            // `windowtitlev2` (v2, address + title). The v1 hint cannot
            // satisfy the `window.title_changed` schema (which requires
            // `window_title`), and v2 fires for the same change with the
            // actual title. Suppress the v1 hint to keep DLQ clean.
            return Ok(None);
        }
        "workspace" | "workspacev2" => {
            // workspace>>workspaceid (v1) or workspacev2>>workspaceid,workspacename (v2)
            let (id_str, name) = data.split_once(',').unwrap_or((data, ""));
            let to_workspace_id = id_str.trim().parse::<i32>().map_err(|_| {
                ParserError::Parse(format!(
                    "workspace id is not an integer: '{id_str}' (raw: '{data}')"
                ))
            })?;
            let workspace_name = if name.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::Value::String(name.to_string())
            };
            (
                "workspace.switched",
                serde_json::json!({
                    "to_workspace_id": to_workspace_id,
                    "workspace_name": workspace_name,
                }),
            )
        }
        "focusedmon" | "focusedmonv2" => {
            // focusedmon>>monitorname,workspacename
            let (monitor, workspace) = data.split_once(',').unwrap_or((data, ""));
            (
                "monitor.focused",
                serde_json::json!({
                    "monitor_name": monitor.trim(),
                    "workspace_name": workspace,
                }),
            )
        }
        _ => {
            // Everything else — unhandled but captured.
            (
                "wm.unhandled",
                serde_json::json!({
                    "event_type": typ,
                    "event_data": data,
                }),
            )
        }
    };

    Ok(Some((event_type, payload)))
}

// ---------------------------------------------------------------------------
// Source factory registration
// ---------------------------------------------------------------------------

impl HyprlandParser {
    /// Build a `ParsedEventIntent` for a single Hyprland IPC event.
    fn build_intent(
        &self,
        event_type_str: &'static str,
        payload: serde_json::Value,
        record: &sinex_primitives::parser::SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<ParsedEventIntent> {
        Ok(ParsedEventIntent::builder()
            .source_id(ctx.source_id.clone())
            .parser_id(ParserId::from_static("hyprland-ipc"))
            .parser_version("1.0.0")
            .event_type(EventType::from_static(event_type_str))
            .event_source(EventSource::from_static("wm.hyprland"))
            .payload(payload)
            .ts_orig(Timestamp::now())
            .timing(TimingEvidence::Atemporal)
            .anchor(record.anchor.clone())
            .privacy_context(ProcessingContext::Document)
            .build())
    }

    /// Flush a stale `pending_activewindow` as a partial `window.focused` intent.
    ///
    /// Called when any event other than `activewindowv2` arrives while the v1
    /// class/title buffer is set. Emits what we have (class + title, no
    /// `window_id`) rather than silently dropping the observation.
    fn flush_pending_activewindow(
        &mut self,
        record: &sinex_primitives::parser::SourceRecord,
        ctx: &ParserContext,
    ) -> Vec<ParsedEventIntent> {
        let Some((window_class, window_title)) = self.pending_activewindow.take() else {
            return vec![];
        };
        let payload = serde_json::json!({
            "window_class": window_class,
            "window_title": window_title,
        });
        match self.build_intent("window.focused", payload, record, ctx) {
            Ok(intent) => vec![intent],
            Err(_) => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{LazyLock, Mutex};
    use xtask::sandbox::prelude::*;

    static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn clear_hyprland_env() {
        unsafe {
            std::env::remove_var("SINEX_HYPRLAND_EVENT_SOCKET");
            std::env::remove_var("SINEX_HYPRLAND_RUNTIME_DIR");
            std::env::remove_var("XDG_RUNTIME_DIR");
            std::env::remove_var("SINEX_HYPRLAND_INSTANCE_SIGNATURE");
            std::env::remove_var("HYPRLAND_INSTANCE_SIGNATURE");
        }
    }

    #[sinex_test]
    async fn baseline_adapter_config_prefers_explicit_event_socket() -> TestResult<()> {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        clear_hyprland_env();
        unsafe {
            std::env::set_var(
                "SINEX_HYPRLAND_EVENT_SOCKET",
                "/run/user/1000/hypr/explicit/.socket2.sock",
            );
            std::env::set_var("SINEX_HYPRLAND_RUNTIME_DIR", "/run/user/1000");
            std::env::set_var("SINEX_HYPRLAND_INSTANCE_SIGNATURE", "derived");
        }

        let config = <HyprlandParser as MaterialParser>::baseline_adapter_config();

        assert_eq!(
            config["socket_path"],
            "/run/user/1000/hypr/explicit/.socket2.sock"
        );
        assert_eq!(config["reconnect_on_eof"], true);
        clear_hyprland_env();
        Ok(())
    }

    #[sinex_test]
    async fn baseline_adapter_config_derives_socket_from_bridge_env() -> TestResult<()> {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        clear_hyprland_env();
        unsafe {
            std::env::set_var("SINEX_HYPRLAND_RUNTIME_DIR", "/run/user/1000");
            std::env::set_var("SINEX_HYPRLAND_INSTANCE_SIGNATURE", "abc123");
        }

        let config = <HyprlandParser as MaterialParser>::baseline_adapter_config();

        assert_eq!(
            config["socket_path"],
            "/run/user/1000/hypr/abc123/.socket2.sock"
        );
        assert_eq!(config["reconnect_on_eof"], true);
        clear_hyprland_env();
        Ok(())
    }
}
