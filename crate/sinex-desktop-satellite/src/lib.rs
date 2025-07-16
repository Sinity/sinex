//! Unified Desktop Satellite
//!
//! Coordinates multiple desktop event sources:
//! - Clipboard events (copy/cut/paste)  
//! - Window manager events (Hyprland focus, movement, workspaces)
//!
//! This module provides the unified StatefulStreamProcessor architecture from Part 16.

mod clipboard;
mod window_manager;

// New unified processor module
pub mod unified_processor;

pub use clipboard::ClipboardWatcher;
pub use window_manager::WindowManagerWatcher;

// Re-export for convenience
pub use sinex_core::RawEvent;

// Re-export the new unified processor as the primary interface
pub use unified_processor::{DesktopProcessor, DesktopState, ClipboardStatus, WindowManagerStatus};