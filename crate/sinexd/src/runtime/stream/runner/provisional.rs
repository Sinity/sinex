//! Bridge checkpoint loading for `RuntimeRunner`.
//!
//! With the confirmed-delivery redesign (#2187 / #2202, "Option C") automata
//! receive the full post-redaction `Event<JsonValue>` directly from the
//! confirmed-events stream, so the old provisional-payload resolution path
//! (`resolve_provisionals_to_events`, `fetch_persisted_event`,
//! `build_event_from_provisional`) was deleted. What remains here is the
//! bridge's checkpoint-state loader.

use super::{CheckpointManager, RuntimeResult, RuntimeRunner, SinexError};

impl RuntimeRunner {
    #[cfg(feature = "messaging")]
    pub(super) async fn load_bridge_checkpoint_state(
        checkpoint_manager: &CheckpointManager,
    ) -> RuntimeResult<crate::runtime::checkpoint::CheckpointState> {
        checkpoint_manager.load_checkpoint().await.map_err(|error| {
            SinexError::checkpoint("Failed to load checkpoint state for automaton bridge")
                .with_source(error)
        })
    }
}
