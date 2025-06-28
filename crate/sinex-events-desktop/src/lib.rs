pub mod clipboard;
pub mod window_manager;

// Re-export desktop event types
pub use clipboard::{ClipboardChanged, ClipboardSelection};
pub use window_manager::{
    MonitorFocused, StateSnapshot, WindowClosed, WindowFocused, WindowMoved, WindowOpened,
    WorkspaceChanged,
};
