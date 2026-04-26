#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]

//! Desktop ingestor integrating clipboard and window-sensing feeds.

mod activitywatch_history;
mod clipboard;
mod window_manager;

pub mod unified_node;


pub use clipboard::ClipboardWatcher;
pub use window_manager::{WindowManagerType, WindowManagerWatcher};

// Re-export the new unified node as the primary interface
pub use unified_node::{
    ClipboardStatus, DesktopMonitorHealth, DesktopNode, DesktopState, WindowManagerStatus,
};
