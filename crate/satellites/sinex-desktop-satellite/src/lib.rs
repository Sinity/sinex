//! Desktop Satellite with sensd Source Material Capture
//!
//! Coordinates multiple desktop event sources using the sensd pattern:
//! - Clipboard events (copy/cut/paste) → source material first
//! - Window manager events (Hyprland focus, movement, workspaces) → source material first
//!
//! ## Architecture
//!
//! This satellite uses the sensd pattern for ALL desktop data:
//! 1. **Source Material Capture**: Desktop activity → raw.source_material_registry
//! 2. **Temporal Ledger**: Precise timing → raw.temporal_ledger
//! 3. **Event Generation**: Material processing → events with Provenance::Material

mod clipboard;
mod window_manager;

// New unified processor module
pub mod unified_processor;

// Sensd integration module
pub mod desktop_sensd_integration;

// Local facade module to reduce import verbosity
mod common {
    // Core types facade
    pub use sinex_core::{
        db::models::RawEvent,
        types::{domain::SanitizedPath, events::Event, Timestamp},
    };

    // SDK facade for common processor types
    pub use sinex_satellite_sdk::{
        annex::{AnnexConfig, BlobManager},
        checkpoint::CheckpointManager,
        cli::{
            ActivityEntry, CoverageAnalysis, ExplorationProvider, ExportFormat,
            IngestionHistoryEntry, MissingItem, SourceState,
        },
        error_helpers::{parse_config_value, parse_typed_config, path_utils, processing_error},
        stream_processor::{
            Checkpoint, ProcessorCapabilities, ProcessorType, ScanArgs, ScanEstimate, ScanReport,
            StatefulStreamProcessor, StreamProcessorContext, TimeHorizon,
        },
        SatelliteResult,
    };

    // External dependencies
    pub use {
        async_trait::async_trait,
        camino::Utf8PathBuf,
        chrono::{DateTime, Utc},
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
