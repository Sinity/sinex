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

// ---------------------------------------------------------------------------
// Per-event-type parser coverage
//
// Real-shaped lines confirmed against a live Hyprland instance on this host
// (`.socket2.sock`, read-only capture) during the sinex-60r verification
// session: activewindow/activewindowv2/windowtitle/windowtitlev2 formats
// match exactly. openwindow/closewindow/movewindow/workspace/focusedmon
// formats are per Hyprland's IPC documentation (not observed live this
// session, since no window opened/closed/moved during capture).
// ---------------------------------------------------------------------------

fn now_ts() -> Timestamp {
    Timestamp::now()
}

#[sinex_test]
async fn openwindow_emits_window_opened_with_parsed_workspace_id() -> TestResult<()> {
    let mut parser = HyprlandParser::default();
    let record = source_record(
        "openwindow>>0xabc123,3,dev,firefox,Sinex — docs",
        now_ts(),
    );
    let ctx = parser_context(record.anchor.clone());
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    let intent = &intents[0];
    assert_eq!(intent.event_type, EventType::from_static("window.opened"));
    assert_eq!(intent.payload["window_id"], "0xabc123");
    assert_eq!(intent.payload["workspace_id"], 3);
    assert_eq!(intent.payload["workspace_name"], "dev");
    assert_eq!(intent.payload["window_class"], "firefox");
    assert_eq!(intent.payload["window_title"], "Sinex — docs");
    Ok(())
}

#[sinex_test]
async fn closewindow_emits_window_closed() -> TestResult<()> {
    let mut parser = HyprlandParser::default();
    let record = source_record("closewindow>>0xabc123", now_ts());
    let ctx = parser_context(record.anchor.clone());
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    assert_eq!(
        intents[0].event_type,
        EventType::from_static("window.closed")
    );
    assert_eq!(intents[0].payload["window_id"], "0xabc123");
    Ok(())
}

#[sinex_test]
async fn movewindow_emits_window_moved_with_parsed_workspace_id() -> TestResult<()> {
    let mut parser = HyprlandParser::default();
    let record = source_record("movewindow>>0xabc123,2,browse", now_ts());
    let ctx = parser_context(record.anchor.clone());
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    let intent = &intents[0];
    assert_eq!(intent.event_type, EventType::from_static("window.moved"));
    assert_eq!(intent.payload["window_id"], "0xabc123");
    assert_eq!(intent.payload["workspace_id"], 2);
    assert_eq!(intent.payload["workspace_name"], "browse");
    Ok(())
}

#[sinex_test]
async fn windowtitlev2_emits_title_changed() -> TestResult<()> {
    let mut parser = HyprlandParser::default();
    let record = source_record("windowtitlev2>>0xabc123,new title here", now_ts());
    let ctx = parser_context(record.anchor.clone());
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    let intent = &intents[0];
    assert_eq!(
        intent.event_type,
        EventType::from_static("window.title_changed")
    );
    assert_eq!(intent.payload["window_id"], "0xabc123");
    assert_eq!(intent.payload["window_title"], "new title here");
    Ok(())
}

#[sinex_test]
async fn windowtitle_v1_is_suppressed() -> TestResult<()> {
    // v1 `windowtitle` carries only an address, no title — it cannot satisfy
    // the window.title_changed schema and is superseded by windowtitlev2
    // firing for the same change, so it must be dropped, not emitted.
    let mut parser = HyprlandParser::default();
    let record = source_record("windowtitle>>0xabc123", now_ts());
    let ctx = parser_context(record.anchor.clone());
    let intents = parser.parse_record(record, &ctx).await?;

    assert!(intents.is_empty());
    Ok(())
}

#[sinex_test]
async fn workspace_v1_emits_workspace_switched() -> TestResult<()> {
    let mut parser = HyprlandParser::default();
    let record = source_record("workspace>>4", now_ts());
    let ctx = parser_context(record.anchor.clone());
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    let intent = &intents[0];
    assert_eq!(
        intent.event_type,
        EventType::from_static("workspace.switched")
    );
    assert_eq!(intent.payload["to_workspace_id"], 4);
    assert_eq!(intent.payload["workspace_name"], serde_json::Value::Null);
    Ok(())
}

#[sinex_test]
async fn workspacev2_emits_workspace_switched_with_name() -> TestResult<()> {
    let mut parser = HyprlandParser::default();
    let record = source_record("workspacev2>>4,dev", now_ts());
    let ctx = parser_context(record.anchor.clone());
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    let intent = &intents[0];
    assert_eq!(
        intent.event_type,
        EventType::from_static("workspace.switched")
    );
    assert_eq!(intent.payload["to_workspace_id"], 4);
    assert_eq!(intent.payload["workspace_name"], "dev");
    Ok(())
}

#[sinex_test]
async fn workspace_with_non_integer_id_is_a_parse_error() -> TestResult<()> {
    let mut parser = HyprlandParser::default();
    let record = source_record("workspace>>special", now_ts());
    let ctx = parser_context(record.anchor.clone());
    let result = parser.parse_record(record, &ctx).await;

    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
async fn focusedmon_emits_monitor_focused() -> TestResult<()> {
    let mut parser = HyprlandParser::default();
    let record = source_record("focusedmon>>DP-1,dev", now_ts());
    let ctx = parser_context(record.anchor.clone());
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    let intent = &intents[0];
    assert_eq!(
        intent.event_type,
        EventType::from_static("monitor.focused")
    );
    assert_eq!(intent.payload["monitor_name"], "DP-1");
    assert_eq!(intent.payload["workspace_name"], "dev");
    Ok(())
}

#[sinex_test]
async fn focusedmonv2_emits_monitor_focused() -> TestResult<()> {
    let mut parser = HyprlandParser::default();
    let record = source_record("focusedmonv2>>DP-1,4", now_ts());
    let ctx = parser_context(record.anchor.clone());
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    assert_eq!(
        intents[0].event_type,
        EventType::from_static("monitor.focused")
    );
    assert_eq!(intents[0].payload["monitor_name"], "DP-1");
    Ok(())
}

#[sinex_test]
async fn unknown_event_type_emits_wm_unhandled_not_dropped() -> TestResult<()> {
    // Hyprland adds new IPC event types across versions; unrecognized types
    // must be captured, never silently dropped.
    let mut parser = HyprlandParser::default();
    let record = source_record("configreloaded>>", now_ts());
    let ctx = parser_context(record.anchor.clone());
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].event_type, EventType::from_static("wm.unhandled"));
    assert_eq!(intents[0].payload["event_type"], "configreloaded");
    Ok(())
}

#[sinex_test]
async fn malformed_line_without_separator_emits_wm_unhandled() -> TestResult<()> {
    let mut parser = HyprlandParser::default();
    let record = source_record("this line has no separator", now_ts());
    let ctx = parser_context(record.anchor.clone());
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].event_type, EventType::from_static("wm.unhandled"));
    assert_eq!(intents[0].payload["event_type"], "unknown");
    Ok(())
}

#[sinex_test]
async fn empty_line_yields_no_intents() -> TestResult<()> {
    let mut parser = HyprlandParser::default();
    let record = source_record("", now_ts());
    let ctx = parser_context(record.anchor.clone());
    let intents = parser.parse_record(record, &ctx).await?;

    assert!(intents.is_empty());
    Ok(())
}

#[sinex_test]
async fn malformed_line_flushes_stale_pending_activewindow() -> TestResult<()> {
    // A malformed line arriving while a v1 activewindow is buffered must
    // flush the buffered partial rather than silently discarding it.
    let mut parser = HyprlandParser::default();

    let v1_record = source_record("activewindow>>kitty,shell", now_ts());
    let ctx = parser_context(v1_record.anchor.clone());
    let v1 = parser.parse_record(v1_record, &ctx).await?;
    assert!(v1.is_empty());

    let malformed_record = source_record("garbage no separator here", now_ts());
    let ctx = parser_context(malformed_record.anchor.clone());
    let intents = parser.parse_record(malformed_record, &ctx).await?;

    assert_eq!(intents.len(), 2);
    assert_eq!(
        intents[0].event_type,
        EventType::from_static("window.focused")
    );
    assert_eq!(intents[0].payload["window_class"], "kitty");
    assert_eq!(intents[1].event_type, EventType::from_static("wm.unhandled"));
    Ok(())
}
