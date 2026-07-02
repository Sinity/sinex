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
