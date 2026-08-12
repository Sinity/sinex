//! Shared exploration-history and provenance helpers for runtime modules.

use crate::runtime::stream::ScanReport;
use serde::{Deserialize, Serialize};
use sinex_primitives::temporal::Timestamp;

// ============================================================================
// Activity tracking types shared with runtime CLI exploration flows.
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

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::events::SourceMaterial;
    use sinex_primitives::events::builder::Provenance;
    use sinex_primitives::{EventSource, EventType, HostName, Id};
    use xtask::sandbox::prelude::sinex_test;

    fn fixture_events(n: usize) -> Vec<Event<JsonValue>> {
        (0..n)
            .map(|_| Event {
                id: Some(Id::<Event<JsonValue>>::new()),
                source: EventSource::new("test.source").unwrap(),
                event_type: EventType::new("test.type").unwrap(),
                payload: JsonValue::Null,
                ts_orig: None,
                ts_quality: None,
                host: HostName::new("test-host").unwrap(),
                module_run_id: None,
                payload_schema_id: None,
                provenance: Provenance::from_material(Id::<SourceMaterial>::new(), 0, None, None),
                anchor_payload_hash: None,
                associated_blob_ids: None,
                temporal_policy: None,
                semantics_version: None,
                scope_key: None,
                equivalence_key: None,
                created_by_operation_id: None,
                automaton_model: None,
                product_class: None,
                claim_support: None,
                derivation_declaration_id: None,
                derivation_epoch_id: None,
                derivation_lane_id: None,
                adjudication_event_id: None,
            })
            .collect()
    }

    /// sinex-amcf: truncating parent-ID lists at `MAX_PROVENANCE_IDS` with a
    /// plain `.take(max)` silently drops the excess ids -- no warning, no
    /// error, no signal to the caller that provenance lineage was cut. A
    /// derived event with more than 10 real parents ends up with an
    /// incomplete, unflagged lineage record.
    #[sinex_test]
    #[ignore = "sinex-amcf open: event_ids_from_owned_events silently truncates parent-id lists past MAX_PROVENANCE_IDS with no warning"]
    async fn truncation_past_max_provenance_ids_is_not_silent() -> xtask::sandbox::TestResult<()> {
        let events = fixture_events(MAX_PROVENANCE_IDS + 5);
        let ids = event_ids_from_owned_events(&events, MAX_PROVENANCE_IDS);

        // The bug: this currently just silently returns exactly `max` ids
        // with zero signal that 5 real parents were dropped. A fixed
        // version must surface the truncation somehow (return a count,
        // log, or a Result variant) rather than doing this silently.
        assert!(
            ids.len() < events.len(),
            "sanity: fixture must have more events than the cap"
        );
        panic!(
            "sinex-amcf: event_ids_from_owned_events truncated {} parent ids down to {} with no \
             error, warning, or other signal that lineage was cut -- this must not be silent",
            events.len(),
            ids.len()
        );
    }
}
