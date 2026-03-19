#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]

//! Terminal command canonicalizer.

pub mod unified_node;

pub use unified_node::{TerminalCommandCanonicalizer, TerminalCommandCanonicalizerNode};
