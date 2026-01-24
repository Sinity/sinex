use chrono::{Duration as ChronoDuration, Utc};
use serde_json::json;
use sinex_core::db::replay::state_machine::{ReplayScope, ReplayState, ReplayStateMachine};
use sinex_services::AnalyticsService;
use sinex_test_utils::prelude::*;
use std::collections::HashMap;

#[sinex_test]
async fn replay_outcomes_surface_in_analytics(ctx: TestContext) -> TestResult<()> {
    // Use insert_test_event which creates proper source material for FK constraints
    ctx.pool
        .events()
        .insert_test_event("fs-test", "file.created", json!({"path": "/tmp"}))
        .await?;

    let replay = ReplayStateMachine::new(ctx.pool.clone());
    let end = Utc::now();
    let scope = ReplayScope {
        processor_id: "fs-test".to_string(),
        time_window: Some((
            end - ChronoDuration::hours(1),
            end + ChronoDuration::minutes(1),
        )),
        material_filter: None,
        filters: HashMap::new(),
    };

    let planned = replay
        .create_operation(scope.clone(), "tester".into())
        .await?;
    let preview = replay.generate_preview_summary(&scope).await?;
    replay.update_preview(planned.operation_id, preview).await?;
    replay
        .approve(planned.operation_id, "approver".into())
        .await?;
    replay
        .transition(planned.operation_id, ReplayState::Executing)
        .await?;
    replay
        .transition(planned.operation_id, ReplayState::Committing)
        .await?;
    replay
        .transition(planned.operation_id, ReplayState::Completed)
        .await?;

    let analytics = AnalyticsService::new(ctx.pool.clone());
    let operations = analytics
        .list_replay_operations(Some(ReplayState::Completed))
        .await?;

    let operation = operations
        .into_iter()
        .find(|op| op.operation_id == planned.operation_id)
        .expect("completed replay should be listed");
    assert_eq!(operation.outcome.as_deref(), Some("success"));

    Ok(())
}
