//! Unified Terminal Satellite for Sinex
//!
//! This satellite handles all terminal-related event sources:
//! - shell.atuin: Rich shell history from Atuin
//! - shell.history: Shell history file parsing  
//! - shell.kitty: Real-time Kitty terminal events
//! - shell.recording: Terminal session recording (asciinema)
//! - shell.scrollback: Terminal content capture
//!
//! This module provides the unified StatefulStreamProcessor architecture from Part 16.

mod atuin;
mod history;
mod kitty;
mod recording;
mod scanner;
mod scrollback;

// New unified processor module
pub mod unified_processor;

// Re-export the new unified processor as the primary interface
pub use unified_processor::{TerminalProcessor, TerminalState, HistoryFileStatus, AtuinStatus};

// Re-export individual watchers for compatibility
pub use atuin::AtuinWatcher;
pub use history::HistoryWatcher;
pub use kitty::KittyWatcher;
pub use recording::RecordingWatcher;
pub use scrollback::ScrollbackWatcher;

// Re-export for convenience
pub use sinex_core::RawEvent;