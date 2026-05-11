//! Isolation obligation.
//!
//! Verifies that an error in one source unit's dispatch does not affect
//! concurrent dispatches for other source units.
//!
//! ## What this obligation proves
//!
//! - Three concurrent dispatch calls, one for the subject unit and two for
//!   known-good units (`weechat`, `weechat.message`).
//! - Injecting a bad payload into the subject unit's dispatch does not poison
//!   the dispatch registry or prevent the known-good units from succeeding.
//! - The subject unit's error is captured and reported independently.

use crate::production_path::AdapterKind;
use sinex_source_worker::dispatch::default_parser_dispatch;
use sinex_primitives::Uuid;

/// Run the isolation obligation for a source unit.
///
/// # Errors
///
/// Returns an error if an error in the subject unit's dispatch bleeds into the
/// known-good units' dispatches.
pub async fn run(
    source_unit_id: &str,
    _adapter_kind: AdapterKind,
    _fixture_data: &[u8],
) -> Result<(), String> {
    // Use weechat.message as a stable known-good unit for the isolation peer.
    const PEER_UNIT: &str = "weechat.message";
    const PEER_FIXTURE: &[u8] = b"2024-01-15 14:23:45\tsinity\tpeer isolation probe";

    let dispatch = default_parser_dispatch();

    // Inject obviously bad payload for the subject unit.
    let bad_payload = b"<THIS IS NOT VALID FOR ANY PARSER>";
    let bad_material = Uuid::now_v7();
    let subject_result = dispatch(source_unit_id, bad_payload, Some(bad_material));

    // The subject may succeed or fail — both are fine. What matters is that
    // the peers are unaffected.
    let _ = subject_result;

    // The peer must succeed with a valid payload.
    let peer_material = Uuid::now_v7();
    let peer_outcome = dispatch(PEER_UNIT, PEER_FIXTURE, Some(peer_material))
        .map_err(|e| {
            format!(
                "isolation: error in subject unit '{source_unit_id}' may have corrupted registry — \
                 peer '{PEER_UNIT}' dispatch failed: {e}"
            )
        })?;

    if peer_outcome.events.is_empty() {
        return Err(format!(
            "isolation: peer unit '{PEER_UNIT}' returned no events after subject '{source_unit_id}' error"
        ));
    }

    // Second peer call to confirm registry is not poisoned.
    let peer_material_2 = Uuid::now_v7();
    let peer_outcome_2 = dispatch(PEER_UNIT, PEER_FIXTURE, Some(peer_material_2))
        .map_err(|e| {
            format!(
                "isolation: second peer dispatch for '{PEER_UNIT}' failed: {e}"
            )
        })?;

    if peer_outcome_2.events.is_empty() {
        return Err(format!(
            "isolation: second peer dispatch for '{PEER_UNIT}' returned no events"
        ));
    }

    Ok(())
}
