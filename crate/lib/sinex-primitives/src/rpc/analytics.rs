//! Analytics types

use serde::{Deserialize, Serialize};

// ─────────────────────────────────────────────────────────────
// analytics.event_count_by_source
// ─────────────────────────────────────────────────────────────

/// Request: `analytics.event_count_by_source`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventCountBySourceRequest {
    /// Number of days to look back (default: 7)
    #[serde(default)]
    pub days_back: Option<i64>,
}

/// Source count entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceCount {
    pub source: String,
    pub count: i64,
}

/// Response: `analytics.event_count_by_source`
pub type EventCountBySourceResponse = Vec<SourceCount>;

// ─────────────────────────────────────────────────────────────
// analytics.activity_heatmap
// ─────────────────────────────────────────────────────────────

/// Request: `analytics.activity_heatmap`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ActivityHeatmapRequest {
    /// Bucket size in minutes (default: 60, max: 1440)
    #[serde(default)]
    pub bucket_size_minutes: Option<i64>,
    /// Number of buckets to return (default: 100)
    #[serde(default)]
    pub limit: Option<i64>,
}

/// Heatmap bucket entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeatmapBucket {
    pub bucket: String,
    pub count: i64,
}

/// Response: `analytics.activity_heatmap`
pub type ActivityHeatmapResponse = Vec<HeatmapBucket>;

// ─────────────────────────────────────────────────────────────
// analytics.sources_statistics
// ─────────────────────────────────────────────────────────────

/// Request: `analytics.sources_statistics`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourcesStatisticsRequest {
    /// Limit number of sources (default: 100)
    #[serde(default)]
    pub limit: Option<i64>,
}

/// Source statistics entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceStatistics {
    pub source: String,
    pub event_count: i64,
    pub first_seen: Option<String>,
    pub last_seen: Option<String>,
}

/// Response: `analytics.sources_statistics`
pub type SourcesStatisticsResponse = Vec<SourceStatistics>;
