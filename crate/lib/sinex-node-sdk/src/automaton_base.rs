//! Shared exploration-history and provenance helpers for node implementations.

use crate::runtime::stream::ScanReport;
use serde::{Deserialize, Serialize};
use sinex_primitives::temporal::Timestamp;

// ============================================================================
// Activity tracking types shared with node CLI exploration flows.
// ============================================================================

/// Entry representing recent activity for exploration display
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityEntry {
    /// Timestamp of activity
    pub timestamp: Timestamp,
    /// Activity description
    pub description: String,
    /// Optional associated data
    pub data: Option<serde_json::Value>,
}

/// Entry in ingestion history for tracking processing runs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionHistoryEntry {
    /// Scan/ingestion ID
    pub id: String,
    /// Start time
    pub started_at: Timestamp,
    /// End time (if completed)
    pub completed_at: Option<Timestamp>,
    /// Number of events generated
    pub events_generated: u64,
    /// Scan report summary
    pub scan_report: Option<ScanReport>,
    /// Error message if the run failed
    pub error: Option<String>,
}

// ============================================================================
// Provenance utilities for derived events
// ============================================================================

use serde_json::Value as JsonValue;
use sinex_primitives::events::{Event, EventId};

/// Maximum number of parent IDs to include in provenance.
/// Keeps provenance data bounded while maintaining meaningful lineage.
pub const MAX_PROVENANCE_IDS: usize = 10;

/// Extract event IDs from event references, limiting to max count.
///
/// Filters out events without IDs (new events not yet persisted).
///
/// # Example
/// ```rust,ignore
/// let refs: Vec<&Event<JsonValue>> = events.iter().collect();
/// let ids = event_ids_from_events(refs, MAX_PROVENANCE_IDS);
/// ```
#[must_use]
pub fn event_ids_from_events(events: Vec<&Event<JsonValue>>, max: usize) -> Vec<EventId> {
    events.into_iter().filter_map(|e| e.id).take(max).collect()
}

/// Extract event IDs from owned events, limiting to max count.
///
/// Filters out events without IDs (new events not yet persisted).
#[must_use]
pub fn event_ids_from_owned_events(events: &[Event<JsonValue>], max: usize) -> Vec<EventId> {
    events.iter().filter_map(|e| e.id).take(max).collect()
}
