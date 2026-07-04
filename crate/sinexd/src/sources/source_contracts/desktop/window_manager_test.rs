use super::*;
use std::sync::{LazyLock, Mutex};
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{MaterialAnchor, SourceRecord};
use sinex_primitives::primitives::Uuid;
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

fn parser_context(anchor: MaterialAnchor) -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("desktop.window-manager"),
        source_material_id: Id::<SourceMaterial>::from_uuid(Uuid::now_v7()),
        record_anchor: anchor,
        operation_id: Uuid::now_v7(),
        job_id: Uuid::now_v7(),
        host: "test-host".to_string(),
        acquisition_time: Timestamp::now(),
    }
}

fn source_record(line: &str, ts: Timestamp) -> SourceRecord {
    SourceRecord {
        material_id: Id::<SourceMaterial>::from_uuid(Uuid::now_v7()),
        anchor: MaterialAnchor::StreamFrame {
            material_offset: 0,
            frame_index: 0,
        },
        bytes: line.as_bytes().to_vec(),
        logical_path: None,
        source_ts_hint: Some(TimingEvidence::RealtimeCapture {
            value: ts,
            capture_source: "unix_socket.listen".to_string(),
        }),
        metadata: serde_json::Value::Null,
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

#[sinex_test]
async fn pending_activewindow_flush_keeps_original_realtime_hint() -> TestResult<()> {
    let first_ts = Timestamp::from_unix_timestamp(1_700_000_001)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let later_ts = Timestamp::from_unix_timestamp(1_700_000_099)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let mut parser = HyprlandParser::default();

    let first_record = source_record("activewindow>>kitty,compile output\n", first_ts);
    let ctx = parser_context(first_record.anchor.clone());
    let first = parser.parse_record(first_record, &ctx).await?;
    assert!(first.is_empty());

    let later_record = source_record(
        "openwindow>>0xabc,1,dev,firefox,docs\n",
        later_ts,
    );
    let ctx = parser_context(later_record.anchor.clone());
    let intents = parser.parse_record(later_record, &ctx).await?;

    assert_eq!(intents.len(), 2);
    let flushed = &intents[0];
    assert_eq!(flushed.event_type, EventType::from_static("window.focused"));
    assert_eq!(flushed.ts_orig, first_ts);
    assert_eq!(
        flushed.timing,
        TimingEvidence::RealtimeCapture {
            value: first_ts,
            capture_source: "unix_socket.listen".to_string(),
        }
    );

    let opened = &intents[1];
    assert_eq!(opened.event_type, EventType::from_static("window.opened"));
    assert_eq!(opened.ts_orig, later_ts);
    Ok(())
}

#[sinex_test]
async fn activewindowv2_merge_keeps_activewindow_realtime_hint() -> TestResult<()> {
    let activewindow_ts = Timestamp::from_unix_timestamp(1_700_000_010)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let activewindowv2_ts = Timestamp::from_unix_timestamp(1_700_000_011)
        .ok_or_else(|| color_eyre::eyre::eyre!("valid timestamp"))?;
    let mut parser = HyprlandParser::default();

    let v1_record = source_record("activewindow>>kitty,shell\n", activewindow_ts);
    let ctx = parser_context(v1_record.anchor.clone());
    let v1 = parser.parse_record(v1_record, &ctx).await?;
    assert!(v1.is_empty());

    let v2_record = source_record("activewindowv2>>0x123\n", activewindowv2_ts);
    let ctx = parser_context(v2_record.anchor.clone());
    let intents = parser.parse_record(v2_record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    let focused = &intents[0];
    assert_eq!(focused.event_type, EventType::from_static("window.focused"));
    assert_eq!(focused.ts_orig, activewindow_ts);
    assert_eq!(
        focused.timing,
        TimingEvidence::RealtimeCapture {
            value: activewindow_ts,
            capture_source: "unix_socket.listen".to_string(),
        }
    );
    Ok(())
}
