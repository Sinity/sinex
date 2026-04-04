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
use sinex_primitives::events::{Event, EventId, Provenance};
use uuid::Uuid;

/// Maximum number of parent IDs to include in provenance.
/// Keeps provenance data bounded while maintaining meaningful lineage.
pub const MAX_PROVENANCE_IDS: usize = 10;

/// Create synthesis provenance from a slice of event IDs.
///
/// Returns a Provenance with the first ID as primary parent and remaining IDs
/// as additional parents. If the slice is empty, returns a bootstrap provenance.
///
/// # Example
/// ```rust,ignore
/// use sinex_node_sdk::automaton_base::provenance_from_ids;
///
/// let ids = vec![event1.id.clone(), event2.id.clone()];
/// let provenance = provenance_from_ids(&ids);
/// ```
#[must_use]
pub fn provenance_from_ids(ids: &[EventId]) -> Provenance {
    if let Some(first) = ids.first().copied() {
        Provenance::from_synthesis_safe(first, ids.iter().skip(1).copied().collect())
    } else {
        bootstrap_provenance()
    }
}

/// Create a bootstrap provenance for derived events with no specific parent.
///
/// This is used when an automaton needs to emit an event but has no specific
/// parent events to link to (e.g., periodic aggregation with no recent events).
/// The bootstrap ID is a well-known sentinel that indicates "derived without
/// specific lineage".
#[must_use]
pub fn bootstrap_provenance() -> Provenance {
    let bootstrap = EventId::from_uuid(Uuid::from_bytes([
        0x01, 0x90, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00,
    ]));
    Provenance::from_synthesis_safe(bootstrap, vec![])
}

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
