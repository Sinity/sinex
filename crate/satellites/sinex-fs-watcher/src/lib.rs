//! Filesystem watcher satellite for Sinex
//!
//! Independent satellite service that monitors filesystem changes
//! and sends events to sinex-ingestd.
//!
//! This module provides the unified StatefulStreamProcessor architecture from Part 16.

pub mod sensd_integration;
pub mod unified_processor;

// Re-export the new unified processor as the primary interface
pub use unified_processor::{
    FilesystemConfig, FilesystemProcessor, FilesystemState, RenameOperation,
};

// Re-export sensd integration types
pub use sensd_integration::{
    run_with_sensd, MaterialSlice, SensdFilesystemProcessor, SensdIntegrationConfig,
};

// Main type alias for convenience
pub use unified_processor::FilesystemProcessor as FilesystemWatcher;
