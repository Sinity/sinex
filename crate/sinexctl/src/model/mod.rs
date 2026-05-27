pub mod format_registry;

pub use format_registry::{CommandCatalogEntry, CommandEffect, CommandFamily, FormatCapability};

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
    /// JSON output (one object per line)
    Json,
    /// YAML output
    Yaml,
    /// Graphviz DOT language (for provenance graphs)
    Dot,
}

/// Node role enum (matches backend)
#[derive(Debug, Clone, Copy, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    /// Capture nodes (filesystem, terminal, system, etc.)
    Capture,
    /// Derived nodes (analytics, search, etc.)
    Derived,
    /// Core services (ingestd)
    Core,
    /// Gateway
    Gateway,
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Capture => write!(f, "capture"),
            Self::Derived => write!(f, "derived"),
            Self::Core => write!(f, "core"),
            Self::Gateway => write!(f, "gateway"),
        }
    }
}
