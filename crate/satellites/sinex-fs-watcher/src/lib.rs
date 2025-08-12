//! Filesystem watcher satellite for Sinex using sensd integration
//!
//! Independent satellite service that monitors filesystem changes through sensd's
//! MaterialSliceStream and generates events with proper provenance.
//!
//! This module provides the unified StatefulStreamProcessor architecture that uses
//! ONLY sensd for source material capture - no direct filesystem monitoring.

pub mod unified_processor;

// Re-export the unified processor as the primary interface
pub use unified_processor::{
    FilesystemConfig, FilesystemProcessor, FilesystemState, MaterialSlice,
};

// Main type alias for convenience
pub use unified_processor::FilesystemProcessor as FilesystemWatcher;
