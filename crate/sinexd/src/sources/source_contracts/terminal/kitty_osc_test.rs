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
