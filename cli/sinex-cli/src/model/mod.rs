pub mod nodes;
pub mod replay;
pub mod search;

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

/// Output format for CLI commands
#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    /// Human-readable table (default)
    Table,
    /// JSON output (one object per line)
    Json,
    /// YAML output
    Yaml,
}

impl Default for OutputFormat {
    fn default() -> Self {
        Self::Table
    }
}

/// Node role enum (matches backend)
#[derive(Debug, Clone, Copy, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    /// Capture nodes (filesystem, terminal, system, etc.)
    Capture,
    /// Synthesis nodes (analytics, search, etc.)
    Synthesis,
    /// Core services (ingestd)
    Core,
    /// Gateway
    Gateway,
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Capture => write!(f, "capture"),
            Self::Synthesis => write!(f, "synthesis"),
            Self::Core => write!(f, "core"),
            Self::Gateway => write!(f, "gateway"),
        }
    }
}
