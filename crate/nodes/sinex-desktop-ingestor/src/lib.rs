#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]
#![doc = include_str!("../../../../docs/current/architecture/UserInteraction_And_Query_Architecture.md")]

//! Desktop ingestor integrating clipboard and window-sensing feeds.

mod clipboard;
mod window_manager;

// New unified processor module
pub mod unified_processor;

// Local facade module to reduce import verbosity
mod common {
    // Core types facade
    pub use sinex_core::{db::models::Event, types::Timestamp, JsonValue};

    pub use sinex_processor_runtime::{
        ActivityEntry, CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry,
        MissingItem, SourceState,
    };
    // SDK facade for common processor types
    pub use sinex_node_sdk::{
        error_helpers::{parse_config_value, parse_typed_config, path_utils},
        stream_processor::{
            Checkpoint, Node, NodeCapabilities, NodeInitContext, NodeRuntimeState, NodeType,
            ScanArgs, ScanEstimate, ScanReport, TimeHorizon,
        },
        NodeError, NodeResult,
    };

    // External dependencies
    pub use {
        async_trait::async_trait,
        chrono::{DateTime, Utc},
        serde::{Deserialize, Serialize},
        std::{
            collections::{HashMap, VecDeque},
            time::Duration,
        },
        tokio::{process::Command, time::interval},
        tracing::{debug, error, info, instrument, warn},
    };
}

pub use clipboard::ClipboardWatcher;
pub use window_manager::{WindowManagerType, WindowManagerWatcher};

// Re-export the new unified processor as the primary interface
pub use unified_processor::{
    ClipboardStatus, DesktopMonitorHealth, DesktopProcessor, DesktopState, WindowManagerStatus,
};
