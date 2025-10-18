#![doc = include_str!("../doc/README.md")]
#![doc = include_str!("../../../../docs/architecture/Core_Architecture.md")]
#![doc = include_str!("../../../lib/sinex-satellite-sdk/doc/overview.md")]

//! Filesystem watcher satellite facade.

pub mod unified_processor;

// Re-export the unified processor as the primary interface
pub use unified_processor::{FilesystemConfig, FilesystemProcessor, FilesystemState};

// Main type alias for convenience
pub use unified_processor::FilesystemProcessor as FilesystemWatcher;
