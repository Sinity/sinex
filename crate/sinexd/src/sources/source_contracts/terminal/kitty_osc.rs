//! `terminal.kitty-osc-live` — Kitty OSC live terminal observations.
//!
//! This source contract and parser cover line-framed JSON material produced by
//! the Kitty shell-integration bridge. The live receiver is deliberately
//! separate: this parser is the admission-facing contract for framed command
//! observations once material reaches Sinex.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sinex_macros::SourceMeta;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
    ParserManifest, SourceId, SourceRecord, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape,
};
use sinex_primitives::temporal::Timestamp;
use std::path::PathBuf;

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KittyOscParserConfig;

#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "terminal.kitty-osc-live",
    namespace = "terminal",
    event_source = "shell.kitty",
    event_type = "command.executed",
    adapter = "UnixSocketStreamAdapter",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Continuous),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(terminal_session, sequence, command, cwd, ts)"),
    access_scope = AccessScope::RuntimeBridge { surface: "kitty_osc" },
    implementation = "live-capture",
    capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:terminal.activity.check, operation:terminal.activity.reconnect, operation:terminal.activity.pause, operation:terminal.activity.resume, operation:terminal.activity.drain, operation:terminal.activity.inspect",
    privacy_context = ProcessingContext::Command,
    resource_profile = ResourceProfile::LiveWatcher,
    runner_pack = RunnerPack::Live,
    checkpoint_family = CheckpointFamily::LiveObservation,
    runtime_shape = RuntimeShape::Continuous,
)]
pub struct KittyOscParser;

#[async_trait]
impl MaterialParser for KittyOscParser {
    type Config = KittyOscParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("kitty-osc-live"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::UnixSocket],
            source_id: SourceId::from_static("terminal.kitty-osc-live"),
            declared_event_types: vec![(
                EventSource::from_static("shell.kitty"),
                EventType::from_static("command.executed"),
            )],
            privacy_contexts: vec![ProcessingContext::Command],
            sensitivity_hints: Vec::new(),
            description: "Parses Kitty OSC command frames into command.executed events.".into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let frame: serde_json::Value = serde_json::from_slice(&record.bytes)
            .map_err(|error| ParserError::Parse(format!("kitty OSC JSON frame: {error}")))?;

        let command = required_string(&frame, "command")?.to_owned();
        let kitty_window_id = required_string(&frame, "kitty_window_id")?.to_owned();
        let kitty_tab_id = required_string(&frame, "kitty_tab_id")?.to_owned();
        let sequence = optional_u64(&frame, "sequence");
        let (ts_orig, timing) = timestamp_from_frame(&frame).map_or_else(
            || (ctx.acquisition_time, TimingEvidence::StagedAtFallback),
            |timestamp| {
                (
                    timestamp,
                    TimingEvidence::Intrinsic {
                        field: "timestamp".into(),
                        confidence: TimingConfidence::Intrinsic,
                    },
                )
            },
        );

        let payload = serde_json::json!({
            "command": command,
            "working_directory": optional_string(&frame, "working_directory")
                .or_else(|| optional_string(&frame, "cwd")),
            "exit_status": optional_i64(&frame, "exit_status"),
            "execution_time_ms": optional_u64(&frame, "execution_time_ms"),
            "shell_type": optional_string(&frame, "shell_type"),
            "kitty_window_id": kitty_window_id,
            "kitty_tab_id": kitty_tab_id,
        });

        let mut occurrence_fields = vec![
            ("command".to_string(), command),
            ("kitty_window_id".to_string(), kitty_window_id),
            ("kitty_tab_id".to_string(), kitty_tab_id),
        ];
        occurrence_fields.push((
            "sequence_or_frame".to_string(),
            sequence
                .map(|sequence| format!("sequence:{sequence}"))
                .unwrap_or_else(|| format!("frame:{}", stream_frame_index(&record.anchor))),
        ));
        occurrence_fields.push(("ts_orig".to_string(), ts_orig.format_rfc3339()));
        if let Some(cwd) =
            optional_string(&frame, "working_directory").or_else(|| optional_string(&frame, "cwd"))
        {
            occurrence_fields.push(("cwd".to_string(), cwd.to_owned()));
        }

        let anchor = match record.anchor {
            MaterialAnchor::StreamFrame { .. } => record.anchor,
            _ => MaterialAnchor::StreamFrame {
                material_offset: 0,
                frame_index: sequence.unwrap_or(0),
            },
        };

        Ok(vec![
            ParsedEventIntent::builder()
                .source_id(ctx.source_id.clone())
                .parser_id(ParserId::from_static("kitty-osc-live"))
                .parser_version("1.0.0")
                .event_source(EventSource::from_static("shell.kitty"))
                .event_type(EventType::from_static("command.executed"))
                .payload(payload)
                .ts_orig(ts_orig)
                .timing(timing)
                .anchor(anchor)
                .occurrence_key(OccurrenceKey {
                    source_id: SourceId::from_static("terminal.kitty-osc-live"),
                    fields: occurrence_fields,
                })
                .privacy_context(ProcessingContext::Command)
                .build(),
        ])
    }

    fn baseline_adapter_config() -> serde_json::Value {
        match kitty_osc_socket_path() {
            Some(socket_path) => serde_json::json!({
                "socket_path": socket_path,
                "mode": "listen",
                "reconnect_on_eof": true,
            }),
            None => serde_json::json!({}),
        }
    }
}

fn kitty_osc_socket_path() -> Option<String> {
    std::env::var("SINEX_KITTY_OSC_SOCKET")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            let runtime_dir = std::env::var("SINEX_KITTY_OSC_RUNTIME_DIR")
                .or_else(|_| std::env::var("XDG_RUNTIME_DIR"))
                .ok()
                .filter(|value| !value.is_empty())?;
            Some(
                PathBuf::from(runtime_dir)
                    .join("sinex")
                    .join("kitty-osc.sock")
                    .to_string_lossy()
                    .into_owned(),
            )
        })
}

fn required_string<'a>(value: &'a serde_json::Value, field: &str) -> ParserResult<&'a str> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ParserError::Parse(format!("kitty OSC frame missing `{field}`")))
}

fn optional_string<'a>(value: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    value.get(field).and_then(serde_json::Value::as_str)
}

fn optional_i64(value: &serde_json::Value, field: &str) -> Option<i64> {
    value.get(field).and_then(serde_json::Value::as_i64)
}

fn optional_u64(value: &serde_json::Value, field: &str) -> Option<u64> {
    value.get(field).and_then(serde_json::Value::as_u64)
}

fn timestamp_from_frame(value: &serde_json::Value) -> Option<Timestamp> {
    value
        .get("timestamp_ns")
        .and_then(serde_json::Value::as_i64)
        .and_then(|value| Timestamp::from_unix_timestamp_nanos(i128::from(value)))
}

fn stream_frame_index(anchor: &MaterialAnchor) -> u64 {
    match anchor {
        MaterialAnchor::StreamFrame { frame_index, .. } => *frame_index,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::Id;
    use sinex_primitives::events::SourceMaterial;
    use sinex_primitives::parser::ParserContext;
    use std::sync::{Mutex, OnceLock};
    use xtask::sandbox::prelude::*;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn env_lock() -> &'static Mutex<()> {
        ENV_LOCK.get_or_init(|| Mutex::new(()))
    }

    struct EnvGuard {
        values: [(&'static str, Option<std::ffi::OsString>); 3],
    }

    impl EnvGuard {
        fn clear() -> Self {
            let guard = Self {
                values: [
                    (
                        "SINEX_KITTY_OSC_SOCKET",
                        std::env::var_os("SINEX_KITTY_OSC_SOCKET"),
                    ),
                    (
                        "SINEX_KITTY_OSC_RUNTIME_DIR",
                        std::env::var_os("SINEX_KITTY_OSC_RUNTIME_DIR"),
                    ),
                    ("XDG_RUNTIME_DIR", std::env::var_os("XDG_RUNTIME_DIR")),
                ],
            };
            unsafe {
                std::env::remove_var("SINEX_KITTY_OSC_SOCKET");
                std::env::remove_var("SINEX_KITTY_OSC_RUNTIME_DIR");
                std::env::remove_var("XDG_RUNTIME_DIR");
            }
            guard
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, value) in &self.values {
                unsafe {
                    match value {
                        Some(value) => std::env::set_var(key, value),
                        None => std::env::remove_var(key),
                    }
                }
            }
        }
    }

    fn ctx() -> ParserContext {
        ParserContext {
            source_id: SourceId::from_static("terminal.kitty-osc-live"),
            source_material_id: Id::<SourceMaterial>::from_uuid(uuid::Uuid::nil()),
            record_anchor: MaterialAnchor::StreamFrame {
                material_offset: 0,
                frame_index: 7,
            },
            operation_id: uuid::Uuid::nil(),
            job_id: uuid::Uuid::nil(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn record(json: &str) -> SourceRecord {
        SourceRecord {
            material_id: Id::<SourceMaterial>::from_uuid(uuid::Uuid::nil()),
            anchor: MaterialAnchor::StreamFrame {
                material_offset: 0,
                frame_index: 7,
            },
            bytes: json.as_bytes().to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        }
    }

    #[sinex_test]
    async fn kitty_osc_parser_emits_command_executed_intent() -> TestResult<()> {
        let mut parser = KittyOscParser;
        let intents = parser
            .parse_record(
                record(
                    r#"{
                        "sequence": 7,
                        "command": "git status",
                        "cwd": "/realm/project/sinex",
                        "exit_status": 0,
                        "execution_time_ms": 12,
                        "shell_type": "zsh",
                        "kitty_window_id": "window-1",
                        "kitty_tab_id": "tab-1",
                        "timestamp_ns": 1700000000000000000
                    }"#,
                ),
                &ctx(),
            )
            .await?;

        assert_eq!(intents.len(), 1);
        let intent = &intents[0];
        assert_eq!(intent.source_id.as_str(), "terminal.kitty-osc-live");
        assert_eq!(intent.event_source.as_str(), "shell.kitty");
        assert_eq!(intent.event_type.as_str(), "command.executed");
        assert_eq!(intent.payload["command"], "git status");
        assert_eq!(intent.payload["working_directory"], "/realm/project/sinex");
        assert_eq!(intent.payload["kitty_window_id"], "window-1");
        assert_eq!(intent.payload["kitty_tab_id"], "tab-1");
        assert_eq!(
            intent
                .occurrence_key
                .as_ref()
                .map(|key| key.source_id.as_str()),
            Some("terminal.kitty-osc-live")
        );
        Ok(())
    }

    #[sinex_test]
    async fn kitty_osc_parser_rejects_missing_command() -> TestResult<()> {
        let mut parser = KittyOscParser;
        let result = parser
            .parse_record(
                record(
                    r#"{
                        "kitty_window_id": "window-1",
                        "kitty_tab_id": "tab-1"
                    }"#,
                ),
                &ctx(),
            )
            .await;

        assert!(matches!(result, Err(ParserError::Parse(message)) if message.contains("command")));
        Ok(())
    }

    #[sinex_test]
    async fn kitty_osc_parser_falls_back_to_frame_identity_and_staged_timing() -> TestResult<()> {
        let mut parser = KittyOscParser;
        let intents = parser
            .parse_record(
                record(
                    r#"{
                        "command": "git status",
                        "cwd": "/realm/project/sinex",
                        "kitty_window_id": "window-1",
                        "kitty_tab_id": "tab-1"
                    }"#,
                ),
                &ctx(),
            )
            .await?;

        let intent = &intents[0];
        assert_eq!(intent.timing, TimingEvidence::StagedAtFallback);
        let occurrence = intent
            .occurrence_key
            .as_ref()
            .expect("Kitty events should carry occurrence identity");
        assert!(
            occurrence
                .fields
                .iter()
                .any(|(field, value)| field == "sequence_or_frame" && value == "frame:7"),
            "frame index should distinguish repeated commands when sequence is absent"
        );
        assert!(
            occurrence
                .fields
                .iter()
                .any(|(field, _)| field == "ts_orig"),
            "fallback timing should still participate in occurrence identity"
        );
        Ok(())
    }

    #[sinex_test]
    async fn kitty_osc_manifest_declares_package_event_pair() -> TestResult<()> {
        let parser = KittyOscParser;
        let manifest = parser.manifest();
        assert_eq!(manifest.source_id.as_str(), "terminal.kitty-osc-live");
        assert!(
            manifest
                .accepted_input_shapes
                .contains(&InputShapeKind::UnixSocket)
        );
        assert_eq!(
            manifest.declared_event_types,
            vec![(
                EventSource::from_static("shell.kitty"),
                EventType::from_static("command.executed")
            )]
        );
        Ok(())
    }

    #[sinex_test]
    async fn kitty_osc_baseline_adapter_config_prefers_explicit_socket() -> TestResult<()> {
        let _guard = env_lock().lock().expect("env lock poisoned");
        let _env = EnvGuard::clear();
        unsafe {
            std::env::set_var("SINEX_KITTY_OSC_SOCKET", "/run/user/1000/sinex/custom.sock");
            std::env::set_var("SINEX_KITTY_OSC_RUNTIME_DIR", "/run/user/1000/ignored");
        }

        let config = <KittyOscParser as MaterialParser>::baseline_adapter_config();

        assert_eq!(config["socket_path"], "/run/user/1000/sinex/custom.sock");
        assert_eq!(config["mode"], "listen");
        assert_eq!(config["reconnect_on_eof"], true);
        Ok(())
    }

    #[sinex_test]
    async fn kitty_osc_baseline_adapter_config_uses_runtime_dir() -> TestResult<()> {
        let _guard = env_lock().lock().expect("env lock poisoned");
        let _env = EnvGuard::clear();
        unsafe {
            std::env::set_var("SINEX_KITTY_OSC_RUNTIME_DIR", "/run/user/1000");
        }

        let config = <KittyOscParser as MaterialParser>::baseline_adapter_config();

        assert_eq!(config["socket_path"], "/run/user/1000/sinex/kitty-osc.sock");
        assert_eq!(config["mode"], "listen");
        assert_eq!(config["reconnect_on_eof"], true);
        Ok(())
    }

    #[sinex_test]
    async fn kitty_osc_baseline_config_deserializes_as_listener_mode() -> TestResult<()> {
        use crate::runtime::parser::{UnixSocketStreamConfig, UnixSocketStreamMode};

        let _guard = env_lock().lock().expect("env lock poisoned");
        let _env = EnvGuard::clear();
        unsafe {
            std::env::set_var("SINEX_KITTY_OSC_SOCKET", "/run/user/1000/sinex/kitty.sock");
        }

        let config = <KittyOscParser as MaterialParser>::baseline_adapter_config();
        let config: UnixSocketStreamConfig = serde_json::from_value(config)?;

        assert_eq!(config.mode, UnixSocketStreamMode::Listen);
        assert_eq!(
            config.socket_path.as_str(),
            "/run/user/1000/sinex/kitty.sock"
        );
        assert!(config.reconnect_on_eof);
        Ok(())
    }

    #[sinex_test]
    async fn kitty_osc_baseline_adapter_config_is_empty_without_runtime_dir() -> TestResult<()> {
        let _guard = env_lock().lock().expect("env lock poisoned");
        let _env = EnvGuard::clear();

        let config = <KittyOscParser as MaterialParser>::baseline_adapter_config();

        assert_eq!(config, serde_json::json!({}));
        Ok(())
    }
}
