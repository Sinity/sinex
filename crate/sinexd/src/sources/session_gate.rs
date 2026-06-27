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

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_db::repositories::SourceSessionStateUpsert;
    use sinex_primitives::domain::OperationStatus;
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

    #[sinex_test]
    async fn no_control_row_fails_open(ctx: TestContext) -> TestResult<()> {
        // A deployment-enabled binding with no operator control row captures.
        assert!(!live_capture_suspended(ctx.pool(), SOURCE, MODE, "default").await);
        Ok(())
    }

    #[sinex_test]
    async fn paused_and_disabled_and_private_suspend(ctx: TestContext) -> TestResult<()> {
        let repo = ctx.pool().source_session_states();

        repo.upsert(upsert("enabled", false)).await?;
        assert!(!live_capture_suspended(ctx.pool(), SOURCE, MODE, "default").await);

        repo.upsert(upsert("paused", false)).await?;
        assert!(live_capture_suspended(ctx.pool(), SOURCE, MODE, "default").await);

        repo.upsert(upsert("disabled", false)).await?;
        assert!(live_capture_suspended(ctx.pool(), SOURCE, MODE, "default").await);

        // Private mode suspends even while lifecycle is enabled.
        repo.upsert(upsert("enabled", true)).await?;
        assert!(live_capture_suspended(ctx.pool(), SOURCE, MODE, "default").await);
        Ok(())
    }
}
