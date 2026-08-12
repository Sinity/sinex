//! Live-capture session gate.
//!
//! Live-session source drivers (screen/audio capture) consult this gate before
//! each capture cycle so capture is suspended whenever an operator has paused or
//! disabled the mode, set a per-session private flag, or engaged global private
//! mode. This is the runtime half of the
//! `media.*.{enable,disable,pause,resume}-session` operations, composed with the
//! operator-wide private-mode control.
//!
//! Two failure stances, by design:
//! - **Operator lifecycle** (pause/disable) fails **open** when the database
//!   query itself fails. A missing control row lets a deployment-enabled
//!   binding keep capturing because the operator never touched the controls.
//! - **Private mode** fails **closed**: if the private-mode state cannot be read
//!   we suppress capture, because the privacy-safe default for the most
//!   sensitive sources (screen/audio) is to not capture when we cannot prove
//!   private mode is off. A simply-absent state file reads as `disabled` (not an
//!   error), so a fresh host still captures.
//! - **Missing database pool** fails **closed**: the session-control state is
//!   unknown in Edge Mode, so media capture is suppressed.

use std::path::Path;

use serde_json::Value;
use sinex_db::repositories::{SourceSessionStateRecord, SourceSessionStateUpsert};
use sinex_db::{DbPool, DbPoolExt};
use sinex_primitives::privacy::{RuntimePrivateModeState, load_private_mode_state};
use sinex_primitives::temporal::Timestamp;
use uuid::Uuid;

const GATE_DETAIL_KEY: &str = "private_mode_gate_blocked";

/// Why a capture cycle was suspended. `None` (via [`CaptureGateDecision`]) means
/// capture proceeds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CaptureSuspendReason {
    /// Operator-wide private mode is active and scoped to this source.
    PrivateMode,
    /// Private-mode state could not be read; suppressed fail-closed.
    PrivateModeUnavailable,
    /// The per-session `private_mode_blocked` flag is set on the control row.
    PrivateModeSessionFlag,
    /// The session-control database is unavailable, so the current posture is
    /// unknown and capture is suppressed.
    SessionStateUnavailable,
    /// Operator lifecycle control: the mode is `paused` or `disabled`.
    Lifecycle(String),
}

impl CaptureSuspendReason {
    /// Stable, low-cardinality label for logs/telemetry.
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::PrivateMode => "private_mode",
            Self::PrivateModeUnavailable => "private_mode_unavailable",
            Self::PrivateModeSessionFlag => "private_mode_session_flag",
            Self::SessionStateUnavailable => "session_state_unavailable",
            Self::Lifecycle(_) => "operator_lifecycle",
        }
    }
}

/// The gate's verdict for one capture cycle.
#[derive(Debug, Clone)]
pub(crate) struct CaptureGateDecision {
    suspended: Option<CaptureSuspendReason>,
}

impl CaptureGateDecision {
    /// Capture proceeds.
    pub(crate) fn active() -> Self {
        Self { suspended: None }
    }

    fn suspended(reason: CaptureSuspendReason) -> Self {
        Self {
            suspended: Some(reason),
        }
    }

    /// Whether this cycle's capture must be skipped.
    pub(crate) fn is_suspended(&self) -> bool {
        self.suspended.is_some()
    }

    /// Stable label naming why capture is suspended, or `"active"`.
    pub(crate) fn reason_label(&self) -> &'static str {
        self.suspended
            .as_ref()
            .map_or("active", CaptureSuspendReason::label)
    }
}

/// Whether operator-wide private mode suppresses this source right now.
///
/// Mirrors the source-class scoping used by the adapter-source private-mode
/// path: an empty `affected_source_classes` means "all sources"; otherwise the
/// source's class prefix (`media` from `media.screen-ocr`) or its full id must
/// match.
fn private_mode_blocks(state: &RuntimePrivateModeState, source_id: &str) -> bool {
    if !state.is_active_at(Timestamp::now()) {
        return false;
    }
    let source_class = source_id
        .split_once('.')
        .map_or(source_id, |(class, _)| class);
    state.affected_source_classes.is_empty()
        || state
            .affected_source_classes
            .iter()
            .any(|class| class == source_class || class == source_id)
}

fn gate_owned_flag(detail: &Value) -> bool {
    detail
        .get(GATE_DETAIL_KEY)
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn gate_detail(detail: &Value, blocked: bool) -> Value {
    let mut detail = match detail {
        Value::Object(map) => Value::Object(map.clone()),
        other => serde_json::json!({"source_detail": other}),
    };
    if let Some(map) = detail.as_object_mut() {
        if blocked {
            map.insert(GATE_DETAIL_KEY.to_string(), Value::Bool(true));
        } else {
            map.remove(GATE_DETAIL_KEY);
        }
    }
    detail
}

async fn persist_private_mode_flag(
    pool: &DbPool,
    state: &SourceSessionStateRecord,
    blocked: bool,
) -> bool {
    pool.source_session_states()
        .upsert(SourceSessionStateUpsert {
            source_id: state.source_id.clone(),
            mode_id: state.mode_id.clone(),
            session_scope: state.session_scope.clone(),
            operation_id: Uuid::now_v7(),
            result_status: state.result_status.clone(),
            lifecycle_state: state.lifecycle_state.clone(),
            visibility_state: state.visibility_state.clone(),
            private_mode_blocked: blocked,
            runtime_state_ref: state.runtime_state_ref.clone(),
            coverage_ref: state.coverage_ref.clone(),
            debt_ref: state.debt_ref.clone(),
            requested_by: state.requested_by.clone(),
            reason: state.reason.clone(),
            detail: gate_detail(&state.detail, blocked),
        })
        .await
        .is_ok()
}

/// Evaluate the capture gate for one `(source, mode, scope)` cycle, composing
/// global private mode (fail-closed) with the operator lifecycle control
/// (fail-open for database query errors). Private-mode reasons take precedence
/// over lifecycle so an operator who paused *and* engaged private mode sees the
/// privacy reason.
pub(crate) async fn evaluate_capture_gate(
    pool: Option<&DbPool>,
    private_mode_state_dir: &Path,
    source_id: &str,
    mode_id: &str,
    session_scope: &str,
) -> CaptureGateDecision {
    // Privacy first, fail-closed.
    match load_private_mode_state(private_mode_state_dir) {
        Ok(state) if private_mode_blocks(&state, source_id) => {
            if let Some(pool) = pool
                && let Ok(Some(session_state)) = pool
                    .source_session_states()
                    .current_for_scope(source_id, mode_id, session_scope)
                    .await
                && !session_state.private_mode_blocked
            {
                // Preserve the gate's actual privacy block in the shared
                // session state so operator surfaces do not report an active
                // session while capture is suppressed.
                let _ = persist_private_mode_flag(pool, &session_state, true).await;
            }
            return CaptureGateDecision::suspended(CaptureSuspendReason::PrivateMode);
        }
        Ok(_) => {}
        Err(_) => {
            return CaptureGateDecision::suspended(CaptureSuspendReason::PrivateModeUnavailable);
        }
    }

    let Some(pool) = pool else {
        return CaptureGateDecision::suspended(CaptureSuspendReason::SessionStateUnavailable);
    };

    // Operator lifecycle + per-session private flag. A database query failure
    // remains fail-open for lifecycle state, matching the pre-existing
    // operator-control contract.
    match pool
        .source_session_states()
        .current_for_scope(source_id, mode_id, session_scope)
        .await
    {
        Ok(Some(state)) => {
            if state.private_mode_blocked {
                if gate_owned_flag(&state.detail) {
                    // This flag is a transient reflection of global private
                    // mode. Clear it once private mode is inactive, while
                    // preserving manually asserted session flags.
                    if !persist_private_mode_flag(pool, &state, false).await {
                        return CaptureGateDecision::suspended(
                            CaptureSuspendReason::PrivateModeSessionFlag,
                        );
                    }
                } else {
                    return CaptureGateDecision::suspended(
                        CaptureSuspendReason::PrivateModeSessionFlag,
                    );
                }
            }
            if matches!(state.lifecycle_state.as_str(), "disabled" | "paused") {
                CaptureGateDecision::suspended(CaptureSuspendReason::Lifecycle(
                    state.lifecycle_state,
                ))
            } else {
                CaptureGateDecision::active()
            }
        }
        Ok(None) | Err(_) => CaptureGateDecision::active(),
    }
}

#[cfg(test)]
#[path = "session_gate_test.rs"]
mod tests;
