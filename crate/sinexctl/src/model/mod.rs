pub mod format_registry;

pub use format_registry::{
    CommandCatalogEntry, CommandEffect, CommandFamily, CommandOutputContract, FormatCapability,
};

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

/// Output format for CLI commands
#[derive(Debug, Clone, Copy, PartialEq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum OutputFormat {
    /// Human-readable table (default)
    #[default]
    Table,
    /// JSON output — the entire response serialized as one finite pretty-printed document
    Json,
    /// NDJSON (newline-delimited JSON) — one JSON object per line, for streaming/bulk use
    Ndjson,
    /// YAML output
    Yaml,
    /// Graphviz DOT language (for provenance graphs)
    Dot,
}

/// RuntimeModule role enum (matches backend)
#[derive(Debug, Clone, Copy, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeModuleRole {
    /// Capture modules (filesystem, terminal, system, etc.)
    Capture,
    /// Derived modules (analytics, search, etc.)
    Derived,
    /// Core services (event_engine)
    Core,
    /// Gateway
    Gateway,
}

impl std::fmt::Display for RuntimeModuleRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Capture => write!(f, "capture"),
            Self::Derived => write!(f, "derived"),
            Self::Core => write!(f, "core"),
            Self::Gateway => write!(f, "gateway"),
        }
    }
}
