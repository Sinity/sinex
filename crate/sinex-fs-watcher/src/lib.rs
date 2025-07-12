//! Filesystem watcher satellite for Sinex
//!
//! Independent satellite service that monitors filesystem changes
//! and sends events to sinex-ingestd.
//!
//! This module provides the new unified StatefulStreamProcessor architecture from Part 16
//! and maintains backward compatibility with the old EventSource interface.

pub mod unified_processor;

// Re-export the new unified processor as the primary interface
pub use unified_processor::{FilesystemProcessor, FilesystemConfig, FilesystemState, RenameOperation};

// Legacy module for backward compatibility with old EventSource interface
#[allow(dead_code)]
pub mod legacy {
    use async_trait::async_trait;
    use sinex_satellite_sdk::{
        event_source::{EventSource, EventSourceContext},
        SatelliteResult,
    };
    
    /// Legacy filesystem watcher using EventSource trait
    pub struct LegacyFilesystemWatcher {
        // Legacy implementation fields here
    }
    
    impl LegacyFilesystemWatcher {
        pub fn new() -> Self {
            Self {}
        }
    }
    
    #[async_trait]
    impl EventSource for LegacyFilesystemWatcher {
        async fn initialize(&mut self, _ctx: EventSourceContext) -> SatelliteResult<()> {
            // Legacy implementation
            Ok(())
        }
        
        async fn start_streaming(&mut self) -> SatelliteResult<()> {
            // Legacy implementation
            Ok(())
        }
        
        fn source_name(&self) -> &str {
            "fs"
        }
        
        fn event_types(&self) -> Vec<&str> {
            vec!["file.created", "file.modified", "file.deleted"]
        }
    }
}

// Legacy type alias for backward compatibility
pub use unified_processor::FilesystemProcessor as FilesystemWatcher;