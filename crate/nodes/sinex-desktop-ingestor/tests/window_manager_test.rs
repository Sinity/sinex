//! Integration tests for window manager logic in sinex-desktop-ingestor.
//!
//! The window manager module's core logic (event parsing, state tracking,
//! Hyprland socket I/O) is private to the crate. These tests exercise the
//! publicly accessible surface: `WindowManagerType` enum behavior, payload
//! types, serde roundtrips, `EventPayload` trait implementations, config
//! defaults, and health state tracking.
//!
//! For internal logic tests (backoff strategy, event processing, stale window
//! cleanup), see the `#[cfg(test)] mod tests` block in `src/window_manager.rs`.

use sinex_desktop_ingestor::unified_node::DesktopConfig;
use sinex_desktop_ingestor::{DesktopMonitorHealth, DesktopNode, DesktopState, WindowManagerType};
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{
    HyprlandMonitorFocusedPayload, HyprlandStateCapturedPayload, HyprlandWindowClosedPayload,
    HyprlandWindowFocusedPayload, HyprlandWindowMovedPayload, HyprlandWindowOpenedPayload,
    HyprlandWorkspaceSwitchedPayload, WindowGeometry,
};
use xtask::sandbox::prelude::*;

// ---------------------------------------------------------------------------
// WindowManagerType: Display, FromStr, as_str, PartialEq, serde
// ---------------------------------------------------------------------------

#[sinex_test]
async fn window_manager_type_display() -> TestResult<()> {
    let wm = WindowManagerType::Hyprland;
    assert_eq!(format!("{wm}"), "hyprland");
    Ok(())
}

#[sinex_test]
async fn window_manager_type_as_str() -> TestResult<()> {
    let wm = WindowManagerType::Hyprland;
    assert_eq!(wm.as_str(), "hyprland");
    Ok(())
}

#[sinex_test]
async fn window_manager_type_from_str_valid() -> TestResult<()> {
    let parsed: WindowManagerType = "hyprland".parse().map_err(|e: String| eyre!(e))?;
    assert_eq!(parsed, WindowManagerType::Hyprland);
    Ok(())
}

#[sinex_test]
async fn window_manager_type_from_str_invalid() -> TestResult<()> {
    let result: Result<WindowManagerType, _> = "sway".parse();
    assert!(result.is_err(), "unsupported WM types should be rejected");

    let result: Result<WindowManagerType, _> = "".parse();
    assert!(result.is_err(), "empty string should be rejected");

    let result: Result<WindowManagerType, _> = "HYPRLAND".parse();
    assert!(
        result.is_err(),
        "case-sensitive: uppercase should be rejected"
    );

    Ok(())
}

#[sinex_test]
async fn window_manager_type_serde_roundtrip() -> TestResult<()> {
    let original = WindowManagerType::Hyprland;
    let json = serde_json::to_string(&original)?;
    let deserialized: WindowManagerType = serde_json::from_str(&json)?;
    assert_eq!(deserialized, original);
    Ok(())
}

#[sinex_test]
async fn window_manager_type_equality() -> TestResult<()> {
    let a = WindowManagerType::Hyprland;
    let b = WindowManagerType::Hyprland;
    assert_eq!(a, b);

    // Clone should produce equal value
    let c = a.clone();
    assert_eq!(a, c);

    Ok(())
}

// ---------------------------------------------------------------------------
// DesktopConfig: defaults and serde
// ---------------------------------------------------------------------------

#[sinex_test]
async fn desktop_config_defaults_are_sane() -> TestResult<()> {
    let config = DesktopConfig::default();

    assert!(
        config.clipboard_enabled,
        "clipboard should be enabled by default"
    );
    assert!(
        config.window_manager_enabled,
        "window manager should be enabled by default"
    );
    assert_eq!(
        config.window_manager_type,
        WindowManagerType::Hyprland,
        "default WM type should be Hyprland"
    );
    assert!(
        config.clipboard_poll_interval_secs.as_secs() >= 1,
        "poll interval should be at least 1 second"
    );
    assert!(
        !config.require_hyprland,
        "Hyprland should not be required by default (degraded mode allowed)"
    );

    Ok(())
}

#[sinex_test]
async fn desktop_config_serde_roundtrip() -> TestResult<()> {
    let original = DesktopConfig::default();
    let json = serde_json::to_string(&original)?;
    let deserialized: DesktopConfig = serde_json::from_str(&json)?;

    assert_eq!(deserialized.clipboard_enabled, original.clipboard_enabled);
    assert_eq!(
        deserialized.window_manager_enabled,
        original.window_manager_enabled
    );
    assert_eq!(
        deserialized.window_manager_type,
        original.window_manager_type
    );
    assert_eq!(deserialized.require_hyprland, original.require_hyprland);

    Ok(())
}

// ---------------------------------------------------------------------------
// DesktopMonitorHealth: defaults
// ---------------------------------------------------------------------------

#[sinex_test]
async fn desktop_monitor_health_defaults() -> TestResult<()> {
    let health = DesktopMonitorHealth::default();

    assert!(
        !health.clipboard_active,
        "clipboard should not be active by default"
    );
    assert!(
        !health.window_manager_active,
        "window manager should not be active by default"
    );
    assert!(health.clipboard_last_error.is_none());
    assert!(health.window_manager_last_error.is_none());
    assert!(health.clipboard_last_success.is_none());
    assert!(health.window_manager_last_success.is_none());

    Ok(())
}

#[sinex_test]
async fn desktop_monitor_health_serde_roundtrip() -> TestResult<()> {
    let health = DesktopMonitorHealth {
        clipboard_active: true,
        window_manager_active: false,
        clipboard_last_error: None,
        window_manager_last_error: Some("connection refused".to_string()),
        clipboard_last_success: None,
        window_manager_last_success: None,
    };

    let json = serde_json::to_string(&health)?;
    let roundtripped: DesktopMonitorHealth = serde_json::from_str(&json)?;

    assert!(roundtripped.clipboard_active);
    assert!(!roundtripped.window_manager_active);
    assert_eq!(
        roundtripped.window_manager_last_error.as_deref(),
        Some("connection refused")
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// DesktopNode: creation
// ---------------------------------------------------------------------------

#[sinex_test]
async fn desktop_node_creation() -> TestResult<()> {
    // DesktopNode::new() should succeed without any OS resources
    let _node = DesktopNode::new();
    Ok(())
}

// ---------------------------------------------------------------------------
// HyprlandWindowFocusedPayload: serde and trait
// ---------------------------------------------------------------------------

#[sinex_test]
async fn window_focused_payload_serde_roundtrip() -> TestResult<()> {
    let original = HyprlandWindowFocusedPayload {
        window_id: "0x5a3b2c1d".to_string(),
        window_class: "Alacritty".to_string(),
        window_title: "~/projects".to_string(),
        workspace_id: 3,
        previous_window_id: Some("0x1234abcd".to_string()),
    };

    let json = serde_json::to_string(&original)?;
    let deserialized: HyprlandWindowFocusedPayload = serde_json::from_str(&json)?;

    assert_eq!(deserialized.window_id, "0x5a3b2c1d");
    assert_eq!(deserialized.window_class, "Alacritty");
    assert_eq!(deserialized.window_title, "~/projects");
    assert_eq!(deserialized.workspace_id, 3);
    assert_eq!(
        deserialized.previous_window_id.as_deref(),
        Some("0x1234abcd")
    );

    Ok(())
}

#[sinex_test]
async fn window_focused_payload_event_source_and_type() -> TestResult<()> {
    let payload = HyprlandWindowFocusedPayload::test_default();

    assert_eq!(payload.event_source().as_ref(), "wm.hyprland");
    assert_eq!(payload.event_type().as_ref(), "window.focused");

    Ok(())
}

#[sinex_test]
async fn window_focused_no_previous_window() -> TestResult<()> {
    let payload = HyprlandWindowFocusedPayload {
        window_id: "0xabc".to_string(),
        window_class: "Firefox".to_string(),
        window_title: "New Tab".to_string(),
        workspace_id: 1,
        previous_window_id: None,
    };

    let json = serde_json::to_string(&payload)?;
    let roundtripped: HyprlandWindowFocusedPayload = serde_json::from_str(&json)?;

    assert!(
        roundtripped.previous_window_id.is_none(),
        "first focused window has no predecessor"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// HyprlandWindowOpenedPayload: serde and trait
// ---------------------------------------------------------------------------

#[sinex_test]
async fn window_opened_payload_serde_roundtrip() -> TestResult<()> {
    let original = HyprlandWindowOpenedPayload {
        window_id: "0xdeadbeef".to_string(),
        window_class: "code".to_string(),
        window_title: "main.rs - Visual Studio Code".to_string(),
        workspace_id: 2,
        monitor_id: 0,
        geometry: WindowGeometry {
            x: 100,
            y: 50,
            width: 1920,
            height: 1080,
        },
        floating: false,
    };

    let json = serde_json::to_string(&original)?;
    let deserialized: HyprlandWindowOpenedPayload = serde_json::from_str(&json)?;

    assert_eq!(deserialized.window_id, "0xdeadbeef");
    assert_eq!(deserialized.window_class, "code");
    assert_eq!(deserialized.workspace_id, 2);
    assert_eq!(deserialized.geometry.width, 1920);
    assert_eq!(deserialized.geometry.height, 1080);
    assert!(!deserialized.floating);

    Ok(())
}

#[sinex_test]
async fn window_opened_payload_event_source_and_type() -> TestResult<()> {
    let payload = HyprlandWindowOpenedPayload::test_default();

    assert_eq!(payload.event_source().as_ref(), "wm.hyprland");
    assert_eq!(payload.event_type().as_ref(), "window.opened");

    Ok(())
}

#[sinex_test]
async fn window_opened_floating_window() -> TestResult<()> {
    let payload = HyprlandWindowOpenedPayload {
        floating: true,
        ..HyprlandWindowOpenedPayload::test_default()
    };

    let json = serde_json::to_string(&payload)?;
    let roundtripped: HyprlandWindowOpenedPayload = serde_json::from_str(&json)?;

    assert!(roundtripped.floating);

    Ok(())
}

// ---------------------------------------------------------------------------
// HyprlandWindowClosedPayload: serde and trait
// ---------------------------------------------------------------------------

#[sinex_test]
async fn window_closed_payload_serde_roundtrip() -> TestResult<()> {
    let original = HyprlandWindowClosedPayload {
        window_id: "0xabc123".to_string(),
        window_class: "firefox".to_string(),
        window_title: "Closing tab".to_string(),
        workspace_id: 1,
        close_reason: Some("user_closed".to_string()),
    };

    let json = serde_json::to_string(&original)?;
    let deserialized: HyprlandWindowClosedPayload = serde_json::from_str(&json)?;

    assert_eq!(deserialized.window_id, "0xabc123");
    assert_eq!(deserialized.window_class, "firefox");
    assert_eq!(deserialized.close_reason.as_deref(), Some("user_closed"));

    Ok(())
}

#[sinex_test]
async fn window_closed_payload_event_source_and_type() -> TestResult<()> {
    let payload = HyprlandWindowClosedPayload {
        window_id: "0x1".to_string(),
        window_class: "test".to_string(),
        window_title: "Test".to_string(),
        workspace_id: 0,
        close_reason: None,
    };

    assert_eq!(payload.event_source().as_ref(), "wm.hyprland");
    assert_eq!(payload.event_type().as_ref(), "window.closed");

    Ok(())
}

#[sinex_test]
async fn window_closed_without_tracked_info() -> TestResult<()> {
    // When a window closes that wasn't tracked, class/title are empty strings
    let payload = HyprlandWindowClosedPayload {
        window_id: "0xunknown".to_string(),
        window_class: String::new(),
        window_title: String::new(),
        workspace_id: 0,
        close_reason: None,
    };

    let json = serde_json::to_string(&payload)?;
    let roundtripped: HyprlandWindowClosedPayload = serde_json::from_str(&json)?;

    assert!(roundtripped.window_class.is_empty());
    assert!(roundtripped.window_title.is_empty());
    assert!(roundtripped.close_reason.is_none());

    Ok(())
}

// ---------------------------------------------------------------------------
// HyprlandWindowMovedPayload: serde and trait
// ---------------------------------------------------------------------------

#[sinex_test]
async fn window_moved_payload_serde_roundtrip() -> TestResult<()> {
    let original = HyprlandWindowMovedPayload {
        window_address: "0xfeed".to_string(),
        new_workspace_id: 5,
        moved_at: "2025-01-01T12:00:00Z".to_string(),
    };

    let json = serde_json::to_string(&original)?;
    let deserialized: HyprlandWindowMovedPayload = serde_json::from_str(&json)?;

    assert_eq!(deserialized.window_address, "0xfeed");
    assert_eq!(deserialized.new_workspace_id, 5);

    Ok(())
}

#[sinex_test]
async fn window_moved_payload_event_source_and_type() -> TestResult<()> {
    let payload = HyprlandWindowMovedPayload {
        window_address: "0x1".to_string(),
        new_workspace_id: 1,
        moved_at: String::new(),
    };

    assert_eq!(payload.event_source().as_ref(), "wm.hyprland");
    assert_eq!(payload.event_type().as_ref(), "window.moved");

    Ok(())
}

// ---------------------------------------------------------------------------
// HyprlandWorkspaceSwitchedPayload: serde and trait
// ---------------------------------------------------------------------------

#[sinex_test]
async fn workspace_switched_payload_serde_roundtrip() -> TestResult<()> {
    let original = HyprlandWorkspaceSwitchedPayload {
        from_workspace_id: 1,
        to_workspace_id: 3,
        monitor_id: 0,
        active_window_id: Some("0xabc".to_string()),
    };

    let json = serde_json::to_string(&original)?;
    let deserialized: HyprlandWorkspaceSwitchedPayload = serde_json::from_str(&json)?;

    assert_eq!(deserialized.from_workspace_id, 1);
    assert_eq!(deserialized.to_workspace_id, 3);
    assert_eq!(deserialized.monitor_id, 0);
    assert_eq!(deserialized.active_window_id.as_deref(), Some("0xabc"));

    Ok(())
}

#[sinex_test]
async fn workspace_switched_payload_event_source_and_type() -> TestResult<()> {
    let payload = HyprlandWorkspaceSwitchedPayload {
        from_workspace_id: 0,
        to_workspace_id: 1,
        monitor_id: 0,
        active_window_id: None,
    };

    assert_eq!(payload.event_source().as_ref(), "wm.hyprland");
    assert_eq!(payload.event_type().as_ref(), "workspace.switched");

    Ok(())
}

// ---------------------------------------------------------------------------
// HyprlandMonitorFocusedPayload: serde and trait
// ---------------------------------------------------------------------------

#[sinex_test]
async fn monitor_focused_payload_serde_roundtrip() -> TestResult<()> {
    let original = HyprlandMonitorFocusedPayload {
        monitor_id: 1,
        workspace_id: 4,
        previous_monitor: Some(0),
        focused_at: "2025-06-15T09:30:00Z".to_string(),
    };

    let json = serde_json::to_string(&original)?;
    let deserialized: HyprlandMonitorFocusedPayload = serde_json::from_str(&json)?;

    assert_eq!(deserialized.monitor_id, 1);
    assert_eq!(deserialized.workspace_id, 4);
    assert_eq!(deserialized.previous_monitor, Some(0));

    Ok(())
}

#[sinex_test]
async fn monitor_focused_payload_event_source_and_type() -> TestResult<()> {
    let payload = HyprlandMonitorFocusedPayload {
        monitor_id: 0,
        workspace_id: 1,
        previous_monitor: None,
        focused_at: String::new(),
    };

    assert_eq!(payload.event_source().as_ref(), "wm.hyprland");
    assert_eq!(payload.event_type().as_ref(), "monitor.focused");

    Ok(())
}

// ---------------------------------------------------------------------------
// HyprlandStateCapturedPayload: serde and trait
// ---------------------------------------------------------------------------

#[sinex_test]
async fn state_captured_payload_serde_roundtrip() -> TestResult<()> {
    let original = HyprlandStateCapturedPayload {
        windows: vec![json!({ "class": "firefox", "title": "Home" })],
        workspaces: vec![json!({ "id": 1, "name": "main" })],
        monitors: vec![],
        current_workspace: 1,
        current_monitor: 0,
        captured_at: "2025-06-15T10:00:00Z".to_string(),
    };

    let json = serde_json::to_string(&original)?;
    let deserialized: HyprlandStateCapturedPayload = serde_json::from_str(&json)?;

    assert_eq!(deserialized.windows.len(), 1);
    assert_eq!(deserialized.workspaces.len(), 1);
    assert!(deserialized.monitors.is_empty());
    assert_eq!(deserialized.current_workspace, 1);
    assert_eq!(deserialized.current_monitor, 0);

    Ok(())
}

#[sinex_test]
async fn state_captured_payload_event_source_and_type() -> TestResult<()> {
    let payload = HyprlandStateCapturedPayload {
        windows: vec![],
        workspaces: vec![],
        monitors: vec![],
        current_workspace: 0,
        current_monitor: 0,
        captured_at: String::new(),
    };

    assert_eq!(payload.event_source().as_ref(), "wm.hyprland");
    assert_eq!(payload.event_type().as_ref(), "state.captured");

    Ok(())
}

#[sinex_test]
async fn state_captured_empty_snapshot() -> TestResult<()> {
    // An empty state snapshot (no windows, no workspaces) is valid
    let payload = HyprlandStateCapturedPayload {
        windows: vec![],
        workspaces: vec![],
        monitors: vec![],
        current_workspace: 0,
        current_monitor: 0,
        captured_at: "2025-01-01T00:00:00Z".to_string(),
    };

    let json = serde_json::to_string(&payload)?;
    let roundtripped: HyprlandStateCapturedPayload = serde_json::from_str(&json)?;

    assert!(roundtripped.windows.is_empty());
    assert!(roundtripped.workspaces.is_empty());
    assert!(roundtripped.monitors.is_empty());

    Ok(())
}

// ---------------------------------------------------------------------------
// WindowGeometry: serde
// ---------------------------------------------------------------------------

#[sinex_test]
async fn window_geometry_serde_roundtrip() -> TestResult<()> {
    let original = WindowGeometry {
        x: -100,
        y: 50,
        width: 2560,
        height: 1440,
    };

    let json = serde_json::to_string(&original)?;
    let deserialized: WindowGeometry = serde_json::from_str(&json)?;

    assert_eq!(deserialized.x, -100);
    assert_eq!(deserialized.y, 50);
    assert_eq!(deserialized.width, 2560);
    assert_eq!(deserialized.height, 1440);

    Ok(())
}

#[sinex_test]
async fn window_geometry_zero_dimensions() -> TestResult<()> {
    // Zero-sized geometry is valid (e.g., minimized or unmapped windows)
    let geo = WindowGeometry {
        x: 0,
        y: 0,
        width: 0,
        height: 0,
    };

    let json = serde_json::to_string(&geo)?;
    let roundtripped: WindowGeometry = serde_json::from_str(&json)?;

    assert_eq!(roundtripped.width, 0);
    assert_eq!(roundtripped.height, 0);

    Ok(())
}

// ---------------------------------------------------------------------------
// DesktopState: serde
// ---------------------------------------------------------------------------

#[sinex_test]
async fn desktop_state_serde_roundtrip() -> TestResult<()> {
    use sinex_desktop_ingestor::{ClipboardStatus, WindowManagerStatus};
    use sinex_primitives::temporal::Timestamp;

    let state = DesktopState {
        captured_at: Timestamp::now(),
        enabled_sources: vec!["clipboard".to_string(), "window_manager".to_string()],
        clipboard_status: Some(ClipboardStatus {
            monitoring_active: true,
            last_clipboard_change: None,
            clipboard_content_hash: Some("abc123".to_string()),
            last_error: None,
        }),
        window_manager_status: Some(WindowManagerStatus {
            wm_type: "hyprland".to_string(),
            connection_active: true,
            current_workspace: Some("3".to_string()),
            active_window: Some("0xabc".to_string()),
            total_windows: 5,
            last_error: None,
        }),
        recent_activity: vec!["Desktop node snapshot taken".to_string()],
    };

    let json = serde_json::to_string(&state)?;
    let roundtripped: DesktopState = serde_json::from_str(&json)?;

    assert_eq!(roundtripped.enabled_sources.len(), 2);
    assert!(roundtripped.clipboard_status.is_some());
    let clip = roundtripped.clipboard_status.unwrap();
    assert!(clip.monitoring_active);
    assert_eq!(clip.clipboard_content_hash.as_deref(), Some("abc123"));

    assert!(roundtripped.window_manager_status.is_some());
    let wm = roundtripped.window_manager_status.unwrap();
    assert_eq!(wm.wm_type, "hyprland");
    assert!(wm.connection_active);
    assert_eq!(wm.total_windows, 5);

    Ok(())
}
