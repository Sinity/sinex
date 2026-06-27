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
//! - **Operator lifecycle** (pause/disable) fails **open**: a missing control
//!   row or a transient DB error lets a deployment-enabled binding keep
//!   capturing — an operator who never touched the controls still gets capture,
//!   and a DB blip never silently stops it.
//! - **Private mode** fails **closed**: if the private-mode state cannot be read
//!   we suppress capture, because the privacy-safe default for the most
//!   sensitive sources (screen/audio) is to not capture when we cannot prove
//!   private mode is off. A simply-absent state file reads as `disabled` (not an
//!   error), so a fresh host still captures.

use std::path::Path;

use sinex_db::{DbPool, DbPoolExt};
use sinex_primitives::privacy::{RuntimePrivateModeState, load_private_mode_state};
use sinex_primitives::temporal::Timestamp;

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
        self.suspended.as_ref().map_or("active", CaptureSuspendReason::label)
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

/// Evaluate the capture gate for one `(source, mode, scope)` cycle, composing
/// global private mode (fail-closed) with the operator lifecycle control
/// (fail-open). Private-mode reasons take precedence over lifecycle so an
/// operator who paused *and* engaged private mode sees the privacy reason.
pub(crate) async fn evaluate_capture_gate(
    pool: &DbPool,
    private_mode_state_dir: &Path,
    source_id: &str,
    mode_id: &str,
    session_scope: &str,
) -> CaptureGateDecision {
    // Privacy first, fail-closed.
    match load_private_mode_state(private_mode_state_dir) {
        Ok(state) if private_mode_blocks(&state, source_id) => {
            return CaptureGateDecision::suspended(CaptureSuspendReason::PrivateMode);
        }
        Ok(_) => {}
        Err(_) => {
            return CaptureGateDecision::suspended(CaptureSuspendReason::PrivateModeUnavailable);
        }
    }

    // Operator lifecycle + per-session private flag, fail-open.
    match pool
        .source_session_states()
        .current_for_scope(source_id, mode_id, session_scope)
        .await
    {
        Ok(Some(state)) => {
            if state.private_mode_blocked {
                CaptureGateDecision::suspended(CaptureSuspendReason::PrivateModeSessionFlag)
            } else if matches!(state.lifecycle_state.as_str(), "disabled" | "paused") {
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
mod tests {
    use super::*;
    use sinex_db::repositories::SourceSessionStateUpsert;
    use sinex_primitives::domain::OperationStatus;
    use sinex_primitives::privacy::{save_private_mode_state, RuntimePrivateModeState};
    use uuid::Uuid;
    use xtask::sandbox::prelude::*;

    const SOURCE: &str = "media.audio-transcript";
    const MODE: &str = "source:media.audio-transcript.live-session";

    fn upsert(lifecycle: &str, private_mode_blocked: bool) -> SourceSessionStateUpsert {
        SourceSessionStateUpsert {
            source_id: SOURCE.to_string(),
            mode_id: MODE.to_string(),
            session_scope: "default".to_string(),
            operation_id: Uuid::now_v7(),
            result_status: OperationStatus::Success,
            lifecycle_state: lifecycle.to_string(),
            visibility_state: "idle".to_string(),
            private_mode_blocked,
            runtime_state_ref: "media.session_runtime.observed:test".to_string(),
            coverage_ref: "coverage:media.audio-transcript.live_session".to_string(),
            debt_ref: "debt:media.audio-transcript.live_session".to_string(),
            requested_by: Some("operator".to_string()),
            reason: None,
            detail: serde_json::json!({}),
        }
    }

    /// A state dir with no private-mode file → private mode reads as disabled.
    fn empty_state_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    #[sinex_test]
    async fn no_control_row_fails_open(ctx: TestContext) -> TestResult<()> {
        let dir = empty_state_dir();
        let decision = evaluate_capture_gate(ctx.pool(), dir.path(), SOURCE, MODE, "default").await;
        assert!(!decision.is_suspended(), "deployment-enabled capture proceeds");
        Ok(())
    }

    #[sinex_test]
    async fn lifecycle_paused_and_disabled_suspend(ctx: TestContext) -> TestResult<()> {
        let dir = empty_state_dir();
        let repo = ctx.pool().source_session_states();

        repo.upsert(upsert("enabled", false)).await?;
        assert!(
            !evaluate_capture_gate(ctx.pool(), dir.path(), SOURCE, MODE, "default")
                .await
                .is_suspended()
        );

        repo.upsert(upsert("paused", false)).await?;
        let paused = evaluate_capture_gate(ctx.pool(), dir.path(), SOURCE, MODE, "default").await;
        assert_eq!(paused.reason_label(), "operator_lifecycle");

        repo.upsert(upsert("disabled", false)).await?;
        assert!(
            evaluate_capture_gate(ctx.pool(), dir.path(), SOURCE, MODE, "default")
                .await
                .is_suspended()
        );
        Ok(())
    }

    #[sinex_test]
    async fn per_session_private_flag_suspends(ctx: TestContext) -> TestResult<()> {
        let dir = empty_state_dir();
        ctx.pool()
            .source_session_states()
            .upsert(upsert("enabled", true))
            .await?;
        let decision = evaluate_capture_gate(ctx.pool(), dir.path(), SOURCE, MODE, "default").await;
        assert_eq!(decision.reason_label(), "private_mode_session_flag");
        Ok(())
    }

    #[sinex_test]
    async fn global_private_mode_suspends_even_when_enabled(ctx: TestContext) -> TestResult<()> {
        let dir = empty_state_dir();
        // Mode is operator-enabled...
        ctx.pool()
            .source_session_states()
            .upsert(upsert("enabled", false))
            .await?;
        // ...but global private mode is engaged for all source classes.
        save_private_mode_state(
            dir.path(),
            &RuntimePrivateModeState::enabled_by("operator", Vec::new(), Timestamp::now()),
        )?;
        let decision = evaluate_capture_gate(ctx.pool(), dir.path(), SOURCE, MODE, "default").await;
        assert_eq!(decision.reason_label(), "private_mode");
        Ok(())
    }

    #[sinex_test]
    async fn private_mode_scoped_to_other_class_does_not_suspend(
        ctx: TestContext,
    ) -> TestResult<()> {
        let dir = empty_state_dir();
        ctx.pool()
            .source_session_states()
            .upsert(upsert("enabled", false))
            .await?;
        // Private mode affects only `terminal`, not `media`.
        save_private_mode_state(
            dir.path(),
            &RuntimePrivateModeState::enabled_by(
                "operator",
                vec!["terminal".to_string()],
                Timestamp::now(),
            ),
        )?;
        assert!(
            !evaluate_capture_gate(ctx.pool(), dir.path(), SOURCE, MODE, "default")
                .await
                .is_suspended(),
            "private mode scoped to another class leaves media capture running"
        );
        Ok(())
    }

    #[sinex_test]
    async fn unreadable_private_mode_state_fails_closed(ctx: TestContext) -> TestResult<()> {
        let dir = empty_state_dir();
        // Write a corrupt private-mode state file so the read errors.
        let path = sinex_primitives::privacy::private_mode_state_path(dir.path());
        std::fs::create_dir_all(path.parent().expect("state path has a parent"))
            .expect("create state dir");
        std::fs::write(&path, b"{ not valid json").expect("write corrupt state");
        let decision = evaluate_capture_gate(ctx.pool(), dir.path(), SOURCE, MODE, "default").await;
        assert_eq!(decision.reason_label(), "private_mode_unavailable");
        Ok(())
    }
}
