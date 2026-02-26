use serde_json::json;
use sinex_db::replay::state_machine::{ReplayScope, ReplayState, ReplayStateMachine};
use sinex_primitives::temporal;
use sinex_primitives::DynamicPayload;
use sinex_services::AnalyticsService;
use std::collections::HashMap;
use time::Duration;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn replay_outcomes_surface_in_analytics(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    // Publish test event via NATS pipeline
    ctx.publish(DynamicPayload::new(
        "fs-test",
        "file.created",
        json!({"path": "/tmp"}),
    ))
    .await?;

    let replay = ReplayStateMachine::new(ctx.pool.clone());
    let end = temporal::now();
    let scope = ReplayScope {
        processor_id: "fs-test".to_string(),
        time_window: Some(((end - Duration::hours(1)), (end + Duration::minutes(1)))),
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
    assert_eq!(operation.outcome, Some(sinex_primitives::domain::ReplayOutcome::Success));

    Ok(())
}
