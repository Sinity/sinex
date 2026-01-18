#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]
#![doc = include_str!("../../../../docs/current/architecture/UserInteraction_And_Query_Architecture.md")]

//! Terminal ingestor that streams command history via the shared processor pattern.

pub mod shell_detection;

// Fish shell history SQLite parser
pub mod fish_history;

// New unified processor module
pub mod unified_processor;

// Re-export the new unified processor as the primary interface
pub use unified_processor::{
    HistorySourceConfig, TerminalConfig, TerminalProcessor, TerminalState,
};
