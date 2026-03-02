#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]
#![doc = include_str!("../../../../docs/current/architecture/UserInteraction_And_Query_Architecture.md")]

//! Terminal ingestor that streams command history via the shared node pattern.

pub mod shell_detection;

// Fish shell history SQLite parser
pub mod fish_history;

pub mod unified_node;

pub use unified_node::{HistorySourceConfig, TerminalConfig, TerminalNode, TerminalState};
