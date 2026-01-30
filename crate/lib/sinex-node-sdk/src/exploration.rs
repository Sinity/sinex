use crate::NodeResult;
use serde::{Deserialize, Serialize};
use sinex_primitives::temporal::OffsetDateTime;
use sinex_primitives::SanitizedPath;
use std::collections::HashMap;

/// Missing item information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MissingItem {
    /// Item identifier in source system
    pub source_id: String,

    /// Item timestamp
    pub timestamp: OffsetDateTime,

    /// Brief description
    pub description: String,

    /// Reason for being missing
    pub missing_reason: Option<String>,
}

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

    /// Last update timestamp
    pub last_updated: OffsetDateTime,

    /// Replication lag in seconds
    pub lag_seconds: Option<f64>,

    /// Recent activity log
    pub recent_activity: Vec<ActivityEntry>,

    /// Total items found
    pub total_items: Option<u64>,

    /// Source-specific metadata/metrics
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Coverage analysis results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoverageAnalysis {
    /// Analysis time range
    pub time_range: (OffsetDateTime, OffsetDateTime),

    /// Total items in source
    pub source_total: u64,

    /// Total items in Sinex
    pub sinex_total: u64,

    /// Coverage percentage (0.0 - 100.0)
    pub coverage_percentage: f64,

    /// Number of missing items
    pub missing_count: u64,

    /// Number of duplicate items
    pub duplicate_count: u64,

    /// Sample of missing items
    pub missing_samples: Vec<MissingItem>,

    /// Recommendations for improvement
    pub recommendations: Vec<String>,
}

/// Export format options
#[derive(Debug, Clone, Copy)]
pub enum ExportFormat {
    Json,
    Csv,
    Raw,
}

/// Trait for processor-specific exploration capabilities
pub trait ExplorationProvider {
    /// Get current source state
    fn get_source_state(&self) -> NodeResult<SourceState>;

    /// Get ingestion history
    fn get_ingestion_history(&self, limit: u64) -> NodeResult<Vec<IngestionHistoryEntry>>;

    /// Perform coverage analysis
    fn get_coverage_analysis(
        &self,
        time_range: Option<(OffsetDateTime, OffsetDateTime)>,
    ) -> NodeResult<CoverageAnalysis>;

    /// Export data for debugging
    fn export_data(&self, path: &SanitizedPath, format: ExportFormat) -> NodeResult<()>;
}
