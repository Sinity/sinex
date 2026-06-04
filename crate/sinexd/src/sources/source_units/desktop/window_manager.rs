//! `desktop.window-manager` source unit.
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
    InputShapeKind, ParsedEventIntent, ParserContext, ParserId, ParserManifest, SourceUnitId,
    TimingEvidence,
};
use sinex_primitives::privacy::{ProcessingContext, SensitivityHint};
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitBinding, SourceUnitBuildImpact, SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{register_source_unit, register_source_unit_binding};

use crate::node_sdk::parser::{MaterialParser, ParserError, ParserResult, UnixSocketStreamAdapter};

use crate::register_adapter_ingestor;

// ---------------------------------------------------------------------------
// Source unit descriptor
// ---------------------------------------------------------------------------

register_source_unit! {
    SourceUnitDescriptor {
        id: "desktop.window-manager",
        namespace: "desktop",
        event_types: &[
            ("wm.hyprland", "window.opened"),
            ("wm.hyprland", "window.closed"),
            ("wm.hyprland", "window.focused"),
            ("wm.hyprland", "window.moved"),
            ("wm.hyprland", "window.title_changed"),
            ("wm.hyprland", "workspace.switched"),
            ("wm.hyprland", "monitor.focused"),
            ("wm.hyprland", "state.captured"),
            ("wm.hyprland", "wm.unhandled"),
        ],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "anchor_stream_frame",
            "window_title_policy_scoped",
        ],
        occurrence_identity: OccurrenceIdentity::Anchor,
        access_policy: "target_runtime_bridge:window_manager",
    }
}

// ---------------------------------------------------------------------------
// Binding
// ---------------------------------------------------------------------------

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:desktop.window-manager"),
        "desktop.window-manager",
        "desktop",
    )
    .implementation("sinexd")
    .adapter("UnixSocketStreamAdapter")
    .output_event_type("window.opened")
    .privacy_context("document")
    .material_policy("wm_socket_stream")
    .checkpoint_policy("live_stream")
    .resource_shape("unix_socket_watcher")
    .source_unit_id("desktop.window-manager")
    .runner_pack("sinexd-source-unit")
    .checkpoint_family(CheckpointFamily::LiveObservation)
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("desktop_window_manager")
    .implementation_mode("sinexd:source-unit")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

// ---------------------------------------------------------------------------
// Parser config
// ---------------------------------------------------------------------------

/// Configuration for [`HyprlandParser`].
///
/// At runtime the source-unit host deserialises the node JSON config into this
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
#[derive(Debug, Clone, Default)]
pub struct HyprlandParser;

#[async_trait]
impl MaterialParser for HyprlandParser {
    type Config = HyprlandParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("hyprland-ipc"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::UnixSocket],
            source_unit_id: SourceUnitId::from_static("desktop.window-manager"),
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
            proof_obligations: vec![
                "anchor_stream_frame".into(),
                "window_title_policy_scoped".into(),
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

        let (event_type_str, payload) = if let Some((typ, data)) = line.split_once(">>") {
            match dispatch_hyprland_event(typ, data)? {
                Some(pair) => pair,
                None => return Ok(vec![]),
            }
        } else {
            // Malformed line — capture as unhandled.
            (
                "wm.unhandled",
                serde_json::json!({
                    "event_type": "unknown",
                    "event_data": line,
                }),
            )
        };

        let ts_now = Timestamp::now();

        let intent = ParsedEventIntent::builder()
            .source_unit_id(ctx.source_unit_id.clone())
            .parser_id(ParserId::from_static("hyprland-ipc"))
            .parser_version("1.0.0")
            .event_type(EventType::new(event_type_str).map_err(|e| {
                ParserError::Parse(format!("invalid event type '{event_type_str}': {e}"))
            })?)
            .event_source(EventSource::from_static("wm.hyprland"))
            .payload(payload)
            .ts_orig(ts_now)
            .timing(TimingEvidence::StagedAtFallback)
            .anchor(record.anchor.clone())
            .privacy_context(ProcessingContext::Document)
            .build();

        Ok(vec![intent])
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
            (
                "window.opened",
                serde_json::json!({
                    "window_id": parts.first().unwrap_or(&""),
                    "workspace_id": parts.get(1).unwrap_or(&""),
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
        "activewindow" => {
            // activewindow>>class,title
            let (class, title) = data.split_once(',').unwrap_or((data, ""));
            (
                "window.focused",
                serde_json::json!({
                    "window_class": class,
                    "window_title": title,
                }),
            )
        }
        "activewindowv2" => (
            "window.focused",
            serde_json::json!({ "window_id": data.trim() }),
        ),
        "movewindow" => {
            // movewindow>>address,workspaceid,workspacename
            let parts: Vec<&str> = data.splitn(3, ',').collect();
            (
                "window.moved",
                serde_json::json!({
                    "window_id": parts.first().unwrap_or(&""),
                    "workspace_id": parts.get(1).unwrap_or(&""),
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
            let (id, name) = data.split_once(',').unwrap_or((data, ""));
            (
                "workspace.switched",
                serde_json::json!({
                    "workspace_id": id,
                    "workspace_name": name,
                }),
            )
        }
        "focusedmon" | "focusedmonv2" => {
            let (monitor, workspace) = data.split_once(',').unwrap_or((data, ""));
            (
                "monitor.focused",
                serde_json::json!({
                    "monitor": monitor,
                    "workspace": workspace,
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
// Node factory registration
// ---------------------------------------------------------------------------

register_adapter_ingestor!(
    source_unit_id: "desktop.window-manager",
    adapter: UnixSocketStreamAdapter,
    parser: HyprlandParser,
);

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
