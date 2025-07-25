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
//!
//! ## Terminal Activity Capture Strategy (TIM-GenericTerminalLogging)
//!
//! ### Asciinema Integration
//! - **Recording**: Full PTY session capture with timing information
//! - **Format**: Newline-delimited JSON (header + events)
//! - **Auto-start**: Can be configured in shell profile
//! - **Storage**: Recordings stored as .cast files with blob management
//!
//! ### Atuin Integration  
//! - **Rich History**: Structured command history across all shells
//! - **Metadata**: Exit codes, duration, CWD, hostname, session ID
//! - **Real-time**: File watching on SQLite database
//! - **Privacy**: Respects Atuin's sync encryption settings
//!
//! ### Shell History Files
//! - **Fallback**: Direct parsing of .bash_history, .zsh_history
//! - **Format-aware**: Handles timestamps, multi-line commands
//! - **Incremental**: Tracks position to avoid duplicates
//!
//! ### Implementation Patterns
//! - **Unified Processor**: Single entry point for all terminal sources
//! - **State Management**: Tracks last seen positions/timestamps
//! - **Batch Processing**: Efficient handling of bulk history imports
//! - **Event Correlation**: Links commands to sessions and recordings

mod atuin;
mod history;
mod kitty;
mod recording;
mod scrollback;

// New unified processor module
pub mod unified_processor;

// Re-export the new unified processor as the primary interface
pub use unified_processor::{AtuinStatus, HistoryFileStatus, TerminalProcessor, TerminalState};

// Re-export individual watchers for compatibility
pub use atuin::AtuinWatcher;
pub use history::HistoryWatcher;
pub use kitty::KittyWatcher;
pub use recording::RecordingWatcher;
pub use scrollback::ScrollbackWatcher;

// Re-export for convenience
pub use sinex_events::RawEvent;
