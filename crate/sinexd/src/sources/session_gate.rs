//! Live-capture session gate.
//!
//! Live-session source drivers (screen/audio capture) consult the general
//! `core.source_session_state` control plane before each capture cycle so an
//! operator `pause`/`disable-session`/private-mode block genuinely suspends
//! capture rather than only recording intent. This is the runtime half of the
//! `media.*.{enable,disable,pause,resume}-session` operations.

use sinex_db::{DbPool, DbPoolExt};

/// Whether operator session-control has suspended live capture for this
/// `(source, mode, scope)`.
///
/// Returns `true` when the current control row is `disabled`/`paused` or has
/// private mode engaged. Fails **open** (returns `false`, so capture proceeds)
/// when no control row exists yet or the state cannot be read: a deployment
/// that enabled the binding captures by default, and a transient DB error must
/// never silently stop capture without an operator decision.
pub(crate) async fn live_capture_suspended(
    pool: &DbPool,
    source_id: &str,
    mode_id: &str,
    session_scope: &str,
) -> bool {
    match pool
        .source_session_states()
        .current_for_scope(source_id, mode_id, session_scope)
        .await
    {
        Ok(Some(state)) => {
            state.private_mode_blocked
                || matches!(state.lifecycle_state.as_str(), "disabled" | "paused")
        }
        Ok(None) | Err(_) => false,
    }
}
