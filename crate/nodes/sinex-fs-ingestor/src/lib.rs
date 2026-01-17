#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../../../../docs/current/architecture/Core_Architecture.md")]
#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]

//! Filesystem ingestor facade.

pub mod unified_processor;

// Re-export the unified processor as the primary interface
pub use unified_processor::{FilesystemConfig, FilesystemProcessor, FilesystemState};

// Main type alias for convenience
pub use unified_processor::FilesystemProcessor as FilesystemWatcher;
