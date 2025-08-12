//! Unified Terminal Satellite for Sinex
//!
//! This satellite captures terminal-related data through sensd Source Material system:
//! - Atuin database monitoring via sensd AppendStream sensor
//! - Shell history file monitoring via sensd AppendStream sensor
//! - Terminal recordings via sensd TreeWatch sensor  
//! - Kitty terminal integration via sensd AppendStream sensor
//!
//! ## Architecture: sensd-First Data Capture
//!
//! All terminal data flows through sensd Source Material system:
//! 1. **Source Material Capture**: Raw terminal data captured by sensd sensors
//! 2. **Temporal Ledger**: Ordered material slices with provenance tracking
//! 3. **Event Generation**: Events created from Source Material with proper provenance
//! 4. **Provenance Chain**: Every event references its Source Material origin
//!
//! ### sensd Sensor Integration
//!
//! - **AppendStream sensors** for:
//!   - Atuin SQLite database monitoring
//!   - Shell history files (.bash_history, .zsh_history, fish_history)
//!   - Kitty remote control socket monitoring
//!
//! - **TreeWatch sensors** for:
//!   - Terminal recording directories (asciinema .cast files)
//!
//! ### Event Types Generated
//!
//! All events have `Provenance::Material` with references to Source Material:
//! - `terminal.atuin_command_executed` - Commands from Atuin database
//! - `terminal.bash_historical_command` - Commands from bash history
//! - `terminal.zsh_historical_command` - Commands from zsh history  
//! - `terminal.fish_historical_command` - Commands from fish history
//! - `terminal.recording_started` - Asciinema recording begins
//! - `terminal.recording_ended` - Asciinema recording completes
//! - `terminal.kitty_window_state` - Kitty window/tab state
//! - `terminal.kitty_content_captured` - Kitty scrollback content
//!
//! ## Eliminated Direct Event Creation
//!
//! Previous modules (atuin.rs, kitty.rs, recording.rs, scrollback.rs, history.rs)
//! that created events directly have been removed. All terminal data capture now
//! goes through sensd Source Material system first.

pub mod sensd_integration;
mod shell_detection;

// New unified processor module
pub mod unified_processor;

// Re-export the new unified processor as the primary interface
pub use unified_processor::{TerminalProcessor, TerminalState};

// Re-export sensd integration
pub use sensd_integration::{
    run_terminal_with_sensd, SensdIntegrationConfig, SensdTerminalProcessor,
};
