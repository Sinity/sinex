#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../../../lib/sinex-satellite-sdk/docs/overview.md")]
#![doc = include_str!("../../../../docs/current/architecture/UserInteraction_And_Query_Architecture.md")]

//! Terminal satellite that streams command history via the shared processor pattern.

// Sensd integration - REMOVED (migrating to AcquisitionManager)
// pub mod sensd_integration;
pub mod shell_detection;

// New unified processor module
pub mod unified_processor;

// Re-export the new unified processor as the primary interface
pub use unified_processor::{
    HistorySourceConfig, TerminalConfig, TerminalProcessor, TerminalState,
};

// Re-export sensd integration - REMOVED
// pub use sensd_integration::{
//     run_terminal_with_sensd, SensdIntegrationConfig, SensdTerminalProcessor,
// };
