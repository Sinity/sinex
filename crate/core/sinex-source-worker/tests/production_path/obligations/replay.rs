//! Replay obligation.
//!
//! Verifies archive-then-replace semantics: re-running the parser on the same
//! fixture material after an archive produces events with new IDs and the
//! same event types.
//!
//! ## What this obligation proves
//!
//! - The parser is deterministic: same fixture bytes → same event types.
//! - Re-running after an "archive" (simulated by discarding the first outcome
//!   and re-dispatching) produces fresh events of the same types.
//!
//! ## Limits
//!
//! Full archive-then-replace semantics (DB row archival → replay trigger →
//! fresh event in `core.events`) require the binary launcher. This obligation
//! proves parser determinism only — the full path is gated on the binary
//! launcher substrate gap noted in `initial_ingestion::substrate_gaps()`.

use crate::AdapterKind;
use sinex_primitives::Uuid;
use sinex_source_worker::dispatch::default_parser_dispatch;

/// Run the replay obligation for a source unit.
///
/// # Errors
///
/// Returns an error if either dispatch run fails or the event types diverge.
pub async fn run(
    source_unit_id: &str,
    _adapter_kind: AdapterKind,
    fixture_data: &[u8],
    expected_event_types: &[&str],
) -> Result<(), String> {
    let dispatch = default_parser_dispatch();

    // First run — simulates original ingestion.
    let material_id_1 = Uuid::now_v7();
    let outcome_1 = dispatch(source_unit_id, fixture_data, Some(material_id_1))
        .map_err(|e| format!("replay first dispatch error for '{source_unit_id}': {e}"))?;

    // Second run — simulates replay with new material id.
    let material_id_2 = Uuid::now_v7();
    let outcome_2 = dispatch(source_unit_id, fixture_data, Some(material_id_2))
        .map_err(|e| format!("replay second dispatch error for '{source_unit_id}': {e}"))?;

    // Material IDs must differ (replay uses new IDs).
    assert_ne!(
        material_id_1, material_id_2,
        "BUG: material IDs must differ between replay runs"
    );

    // Event types must match — parser is deterministic.
    let types_1: Vec<&str> = outcome_1
        .events
        .iter()
        .map(|e| e.event_type.as_str())
        .collect();
    let types_2: Vec<&str> = outcome_2
        .events
        .iter()
        .map(|e| e.event_type.as_str())
        .collect();
    if types_1 != types_2 {
        return Err(format!(
            "replay for '{source_unit_id}': event types differ between runs. \
             run1={types_1:?} run2={types_2:?}"
        ));
    }

    // Verify expected event types appear in both runs.
    for &expected in expected_event_types {
        if !types_1.contains(&expected) {
            return Err(format!(
                "replay for '{source_unit_id}': expected event type '{expected}' \
                 missing from replay output. Got: {types_1:?}"
            ));
        }
    }

    Ok(())
}
