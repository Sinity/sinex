#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]

//! Desktop ingestor integrating clipboard and window-sensing feeds.

mod activitywatch_history;
mod clipboard;
mod window_manager;

pub mod unified_node;

// Local facade module to reduce import verbosity
mod common {
    // Core types facade
    pub use sinex_db::models::Event;
    pub use sinex_primitives::{JsonValue, temporal::Timestamp};

    pub use sinex_node_sdk::{ActivityEntry, CoverageAnalysis, IngestionHistoryEntry, SourceState};
    // SDK facade for common node types
    pub use sinex_node_sdk::{
        NodeResult, SinexError,
        error_helpers::{ConfigAccessor, parse_config_value, parse_typed_config, path_utils},
        runtime::stream::{
            Checkpoint, NodeCapabilities, NodeRuntimeState, ScanArgs, ScanReport, TimeHorizon,
        },
    };

    // External dependencies
    pub use {
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

// Re-export the new unified node as the primary interface
pub use unified_node::{
    ClipboardStatus, DesktopMonitorHealth, DesktopNode, DesktopState, WindowManagerStatus,
};
