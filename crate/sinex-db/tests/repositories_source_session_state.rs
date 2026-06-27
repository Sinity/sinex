use serde_json::json;
use sinex_db::repositories::{DbPoolExt, SourceSessionStateUpsert};
use sinex_primitives::domain::OperationStatus;
use uuid::Uuid;
use xtask::sandbox::prelude::*;

fn session_state(
    operation_id: Uuid,
    lifecycle_state: &str,
    visibility_state: &str,
) -> SourceSessionStateUpsert {
    SourceSessionStateUpsert {
        source_id: "media.screen-ocr".to_string(),
        mode_id: "source:media.screen-ocr.live-session".to_string(),
        session_scope: "default".to_string(),
        operation_id,
        result_status: OperationStatus::Success,
        lifecycle_state: lifecycle_state.to_string(),
        visibility_state: visibility_state.to_string(),
        private_mode_blocked: false,
        runtime_state_ref:
            "media.session_runtime.observed:media.screen-ocr:source:media.screen-ocr.live-session"
                .to_string(),
        coverage_ref: "coverage:media.screen-ocr.live_session".to_string(),
        debt_ref: "debt:media.screen-ocr.live_session".to_string(),
        requested_by: Some("operator".to_string()),
        reason: None,
        detail: json!({ "action": "pause", "capability_issue": 1043 }),
    }
}

#[sinex_test]
async fn source_session_state_upsert_keeps_current_scope_row(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool().source_session_states();
    let first_operation_id = Uuid::now_v7();
    let second_operation_id = Uuid::now_v7();

    let first = repo
        .upsert(session_state(first_operation_id, "enabled", "idle"))
        .await?;
    let second = repo
        .upsert(session_state(second_operation_id, "paused", "suspended"))
        .await?;

    // Same (source, mode, scope) → one current row, latest control wins.
    assert_eq!(first.id, second.id);
    assert_eq!(second.operation_id, second_operation_id);
    assert_eq!(second.lifecycle_state, "paused");
    assert_eq!(second.visibility_state, "suspended");

    let rows = repo.list_current_by_source("media.screen-ocr").await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].operation_id, second_operation_id);

    let current = repo
        .current_for_scope(
            "media.screen-ocr",
            "source:media.screen-ocr.live-session",
            "default",
        )
        .await?
        .expect("current scope row exists");
    assert_eq!(current.lifecycle_state, "paused");
    assert_eq!(current.requested_by.as_deref(), Some("operator"));
    Ok(())
}

#[sinex_test]
async fn source_session_state_distinct_scopes_coexist(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool().source_session_states();

    repo.upsert(session_state(Uuid::now_v7(), "enabled", "idle"))
        .await?;
    let mut other = session_state(Uuid::now_v7(), "disabled", "suspended");
    other.session_scope = "display:HDMI-A-1".to_string();
    repo.upsert(other).await?;

    let rows = repo.list_current_by_source("media.screen-ocr").await?;
    assert_eq!(rows.len(), 2, "distinct session scopes are independent rows");
    Ok(())
}
