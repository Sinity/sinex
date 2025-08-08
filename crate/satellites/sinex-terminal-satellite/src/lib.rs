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
//!
//! ## Architectural Decision: Layered Capture Strategy (ADR-008)
//!
//! We use a multi-layered approach combining:
//! 1. **Atuin** (primary): Structured command history with metadata
//! 2. **Asciinema** (primary): Full session I/O capture for replayability
//! 3. **Shell history** (fallback): Direct parsing when tools unavailable
//! 4. **Kitty RC** (supplemental): Terminal-specific semantic events
//!
//! This layered approach was chosen over single-tool solutions because:
//! - No single tool captures everything (commands + output + context)
//! - Atuin provides queryable structured data but misses output
//! - Asciinema captures everything but requires parsing
//! - Combination gives both structure and completeness
//!
//! Data correlation happens via timestamps, CWD, and session IDs.
//!
//! ## Future Enhancements (Not Yet Implemented)
//!
//! ### Session Correlation
//! - Environment variable tracking (`SINEX_TERMINAL_SESSION_ULID`)
//! - Cross-process command correlation
//! - Unified session IDs between Atuin and Asciinema
//!
//! ### Privacy and Filtering
//! - Regex-based sensitive command filtering
//! - Password/secret redaction in recordings
//! - User-configurable privacy rules
//! - Audit mode vs full capture mode
//! - Directory-based recording exclusions
//!
//! ### Command Analysis
//! - Automatic command type categorization (git, docker, npm, etc.)
//! - Frequency analysis and productivity metrics
//! - Error rate tracking by command type
//! - Command sequence pattern detection
//! - Common workflow identification
//!
//! ### Performance Optimizations
//! - Adaptive batch sizes based on system load
//! - Parallel processing of multiple terminal sources
//! - Cross-source deduplication
//! - Incremental checkpointing during large imports

mod atuin;
mod history;
mod kitty;
mod recording;
mod scrollback;
mod shell_detection;

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
pub use sinex_core::db::models::RawEvent;
