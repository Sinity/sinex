#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../../../../docs/architecture.md")]
#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]

//! Filesystem ingestor facade.

pub mod unified_node;

// Re-export the unified node as the primary interface
pub use unified_node::{FilesystemConfig, FilesystemNode, FilesystemState};
