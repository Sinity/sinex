#![doc = include_str!("../doc/README.md")]
#![doc = include_str!("../doc/overview.md")]
#![doc = include_str!("../../../lib/sinex-satellite-sdk/doc/overview.md")]
#![doc = include_str!("../../../../docs/architecture/UserInteraction_And_Query_Architecture.md")]

//! Terminal satellite that streams command history via the shared processor pattern.

// Sensd integration - REMOVED (migrating to AcquisitionManager)
// pub mod sensd_integration;
pub mod shell_detection;

// New unified processor module
pub mod unified_processor;

// Re-export the new unified processor as the primary interface
pub use unified_processor::{TerminalProcessor, TerminalState};

// Re-export sensd integration
pub use sensd_integration::{
    run_terminal_with_sensd, SensdIntegrationConfig, SensdTerminalProcessor,
};
