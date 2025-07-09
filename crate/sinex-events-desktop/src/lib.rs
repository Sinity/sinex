pub mod clipboard;
pub mod window_manager;

// Re-export desktop event types and configs
pub use clipboard::{ClipboardChanged, ClipboardSelection, ClipboardChangedPayload, ClipboardSelectionPayload, ClipboardConfig};
pub use window_manager::{
    MonitorFocused, StateSnapshot, WindowClosed, WindowFocused, WindowMoved, WindowOpened,
    WorkspaceChanged, WindowFocusedPayload, WindowOpenedPayload, WindowClosedPayload, 
    WindowMovedPayload, WorkspaceChangedPayload, MonitorFocusedPayload, StateSnapshotPayload,
};

use sinex_core::register_events;

// Register all desktop event types using the macro
register_events! {
    "copied" => (clipboard, ClipboardChangedPayload),
    "selected" => (clipboard, ClipboardSelectionPayload),
    "window.focused" => (wm.hyprland, WindowFocusedPayload),
    "window.opened" => (wm.hyprland, WindowOpenedPayload),
    "window.closed" => (wm.hyprland, WindowClosedPayload),
    "window.moved" => (wm.hyprland, WindowMovedPayload),
    "workspace.switched" => (wm.hyprland, WorkspaceChangedPayload),
    "monitor.focused" => (wm.hyprland, MonitorFocusedPayload),
    "state.captured" => (wm.hyprland, StateSnapshotPayload),
}
