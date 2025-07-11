pub mod clipboard;
pub mod typed_clipboard;
pub mod typed_clipboard_adapter;
pub mod window_manager;

// Re-export desktop event types and configs
pub use clipboard::{
    ClipboardChanged, ClipboardChangedPayload, ClipboardConfig, ClipboardSelection,
    ClipboardSelectionPayload,
};
pub use typed_clipboard_adapter::TypedClipboardAdapter;
pub use window_manager::{
    MonitorFocused, MonitorFocusedPayload, StateSnapshot, StateSnapshotPayload, WindowClosed,
    WindowClosedPayload, WindowFocused, WindowFocusedPayload, WindowMoved, WindowMovedPayload,
    WindowOpened, WindowOpenedPayload, WorkspaceChanged, WorkspaceChangedPayload,
};

// Re-export CoreError so the #[with_context] macro can find it
pub use sinex_core::CoreError;

use sinex_core::register_events;

// Register all desktop event types using the macro  
register_events! {
    "clipboard.copied" => (clipboard, ClipboardChangedPayload),
    "clipboard.selected" => (clipboard, ClipboardSelectionPayload),
    "window.focused" => (wm.hyprland, WindowFocusedPayload),
    "window.opened" => (wm.hyprland, WindowOpenedPayload),
    "window.closed" => (wm.hyprland, WindowClosedPayload),
    "window.moved" => (wm.hyprland, WindowMovedPayload),
    "workspace.switched" => (wm.hyprland, WorkspaceChangedPayload),
    "monitor.focused" => (wm.hyprland, MonitorFocusedPayload),
    "state.captured" => (wm.hyprland, StateSnapshotPayload),
}
