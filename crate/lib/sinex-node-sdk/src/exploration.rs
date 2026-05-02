use crate::NodeResult;
use serde::{Deserialize, Serialize};
use sinex_primitives::SanitizedPath;
use sinex_primitives::temporal::Timestamp;
use std::collections::HashMap;

pub use crate::automaton_base::IngestionHistoryEntry;

use crate::automaton_base::ActivityEntry;

/// Current state of the source
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceState {
    /// Whether connected to source
    pub is_connected: bool,

    /// Whether the source is healthy
    pub healthy: bool,

    /// Source description or status message
    pub description: String,

    /// Last observed source update timestamp, if known
    pub last_updated: Option<Timestamp>,

    /// Replication lag in seconds
    pub lag_seconds: Option<f64>,

    /// Recent activity log
    pub recent_activity: Vec<ActivityEntry>,

    /// Total items found
    pub total_items: Option<u64>,

    /// Source-specific metadata/metrics
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Export format options
#[derive(Debug, Clone, Copy)]
pub enum ExportFormat {
    Json,
    Csv,
    Raw,
}

/// Trait for node-specific exploration capabilities
pub trait ExplorationProvider {
    /// Get current source state
    fn get_source_state(&self) -> NodeResult<SourceState>;

    /// Get ingestion history
    fn get_ingestion_history(&self, limit: u64) -> NodeResult<Vec<IngestionHistoryEntry>>;

    /// Export data for debugging
    fn export_data(&self, path: &SanitizedPath, format: ExportFormat) -> NodeResult<()>;
}
