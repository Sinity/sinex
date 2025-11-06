#![doc = include_str!("../doc/README.md")]
#![doc = include_str!("../doc/overview.md")]
#![doc = include_str!("../../../lib/sinex-satellite-sdk/doc/overview.md")]
#![doc = include_str!("../../../../docs/architecture/UserInteraction_And_Query_Architecture.md")]

//! Desktop satellite integrating clipboard and window-sensing feeds.

mod clipboard;
mod window_manager;

// New unified processor module
pub mod unified_processor;

// Sensd integration modules - REMOVED (migrating to AcquisitionManager)
// pub mod desktop_sensd_integration;
// pub mod sensd_job_submitter;

// Local facade module to reduce import verbosity
#[allow(unused_imports)]
mod common {
    // Core types facade
    pub use sinex_core::{
        db::models::Event,
        types::{domain::SanitizedPath, Timestamp},
        JsonValue,
    };

    pub use sinex_processor_runtime::{
        ActivityEntry, CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry,
        MissingItem, SourceState,
    };
    // SDK facade for common processor types
    pub use sinex_satellite_sdk::{
        annex::{AnnexConfig, BlobManager},
        error_helpers::{parse_config_value, parse_typed_config, path_utils, processing_error},
        stream_processor::{
            Checkpoint, ProcessorCapabilities, ProcessorInitContext, ProcessorRuntimeState,
            ProcessorType, ScanArgs, ScanEstimate, ScanReport, StatefulStreamProcessor,
            TimeHorizon,
        },
        SatelliteError, SatelliteResult,
    };

    // External dependencies
    pub use {
        async_trait::async_trait,
        camino::Utf8PathBuf,
        chrono::{DateTime, Utc},
        color_eyre::eyre,
        serde::{Deserialize, Serialize},
        std::{
            collections::{HashMap, VecDeque},
            time::Duration,
        },
        tokio::{process::Command, sync::mpsc, time::interval},
        tracing::{debug, error, info, instrument, warn, Span},
    };
}

pub use clipboard::ClipboardWatcher;
pub use window_manager::{WindowManagerType, WindowManagerWatcher};

// Re-export the new unified processor as the primary interface
pub use unified_processor::{ClipboardStatus, DesktopProcessor, DesktopState, WindowManagerStatus};
