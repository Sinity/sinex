//! Drain obligation.
//!
//! Verifies that the drain controller reaches `Drained` state cleanly when
//! signalled mid-flight, with no in-flight work outstanding.
//!
//! ## What this obligation proves
//!
//! - `SourceWorkerDrainController::request_drain()` advances state from `Idle`
//!   to `StoppingAccept`.
//! - `finish_active_work()` + subsequent phases reach `Drained` without error
//!   and within the configured timeout.
//!
//! ## Limits
//!
//! Full drain-under-load (active NATS inflow + drain signal) requires the
//! binary launcher. This obligation exercises the drain state machine in
//! isolation.

use crate::AdapterKind;
use sinex_source_worker::drain::{DrainPhase, SourceWorkerDrainController};
use std::sync::Arc;
use std::time::Duration;

/// Run the drain obligation for a source unit.
///
/// # Errors
///
/// Returns an error if the drain state machine does not reach `Drained`.
pub async fn run(
    source_unit_id: &str,
    _adapter_kind: AdapterKind,
    _fixture_data: &[u8],
) -> Result<(), String> {
    let controller = Arc::new(SourceWorkerDrainController::new());

    // Simulate one unit of in-flight work.
    controller.enter_work();

    // Signal drain; should transition to StoppingAccept.
    let initiated = controller.request_drain(source_unit_id).await;
    if !initiated {
        return Err(format!(
            "drain for '{source_unit_id}': request_drain() returned false on first call"
        ));
    }

    let phase = controller.current_phase().await;
    if phase != DrainPhase::StoppingAccept {
        return Err(format!(
            "drain for '{source_unit_id}': expected StoppingAccept after request, got {phase:?}"
        ));
    }

    // Complete the in-flight unit — finish_active_work should unblock immediately.
    controller.exit_work();

    // Drive through the remaining phases within a short timeout.
    tokio::time::timeout(Duration::from_secs(5), async {
        controller.finish_active_work(source_unit_id).await;
        controller.flush_intents(source_unit_id).await;
        controller
            .wait_confirmations(source_unit_id, Duration::from_millis(100))
            .await;
        controller.finalize_materials(source_unit_id).await;
        controller.save_checkpoint(source_unit_id).await;
        controller.mark_drained(source_unit_id).await;
    })
    .await
    .map_err(|_| {
        format!("drain for '{source_unit_id}': timed out advancing drain phases after 5s")
    })?;

    let final_phase = controller.current_phase().await;
    if final_phase != DrainPhase::Drained {
        return Err(format!(
            "drain for '{source_unit_id}': expected Drained after full phase sequence, got {final_phase:?}"
        ));
    }

    Ok(())
}
