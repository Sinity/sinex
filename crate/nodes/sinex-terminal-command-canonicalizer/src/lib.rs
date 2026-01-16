#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../../../../docs/current/architecture/UserInteraction_And_Query_Architecture.md")]
#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]

//! Terminal command canonicalizer.

pub mod unified_processor;

pub use unified_processor::TerminalCommandCanonicalizer;
