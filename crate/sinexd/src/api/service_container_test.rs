use super::recover_stale_replay_operations;
use sinex_db::DbPoolExt;
use sinex_primitives::events::{EventPayload, payloads::FileCreatedPayload};
use crate::api::{ReplayScope, ReplayState};
use std::collections::HashMap;
use sqlx::postgres::PgPoolOptions;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn stale_replay_recovery_accepts_clean_state(ctx: TestContext) -> TestResult<()> {
    let replay = sinex_db::replay::state_machine::ReplayStateMachine::new(ctx.pool.clone());
    recover_stale_replay_operations(&replay).await?;
    Ok(())
}

#[sinex_test]
async fn stale_replay_recovery_surfaces_startup_failures() -> TestResult<()> {
    let pool = PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_millis(10))
        .connect_lazy("postgresql://127.0.0.1:1/sinex_test")?;
    let replay = sinex_db::replay::state_machine::ReplayStateMachine::new(pool);

    let error = recover_stale_replay_operations(&replay)
        .await
        .expect_err("startup recovery should fail honestly when the pool is unusable");

    let message = error.to_string();
    assert!(message.contains("Failed to recover stale replay operations on startup"));
    assert!(message.contains("gateway.recover_stale_replay_operations"));
    Ok(())
}

#[sinex_test]
async fn startup_recovery_restores_a_cascade_stranded_by_an_immediate_restart(
    ctx: TestContext,
) -> TestResult<()> {
    let replay = sinex_db::replay::state_machine::ReplayStateMachine::new(ctx.pool.clone());
    let operation = replay
        .create_operation(
            ReplayScope {
                source_name: "startup-recovery-test".to_string(),
                filters: HashMap::new(),
                ..Default::default()
            },
            "test:planner".to_string(),
        )
        .await?;
    replay
        .update_preview(
            operation.operation_id,
            serde_json::json!({ "total_events": 1 }),
        )
        .await?;
    replay
        .approve(operation.operation_id, "admin:approver".to_string())
        .await?;
    replay
        .transition(operation.operation_id, ReplayState::Executing)
        .await?;

    let material_id = ctx
        .create_source_material(Some("startup-recovery-material"))
        .await?;
    let event = ctx
        .pool
        .events()
        .insert(
            FileCreatedPayload {
                path: "/tmp/startup-recovery.txt".into(),
                size: 1,
                created_at: sinex_primitives::temporal::Timestamp::now(),
                permissions: None,
            }
            .from_material(material_id)
            .build()?,
        )
        .await?;
    let event_id = event.id.expect("startup recovery event must have an id");

    ctx.pool
        .events()
        .execute_cascade_archive(
            &[*event_id.as_uuid()],
            "superseded by replay re-execution",
            &operation.operation_id.to_string(),
            "test",
        )
        .await?;
    let mut tx = ctx.pool.begin().await?;
    replay
        .record_scope_invalidations_pending_with_tx(
            &mut tx,
            operation.operation_id,
            1,
            0,
            0,
            1,
            &[*event_id.as_uuid()],
        )
        .await?;
    tx.commit().await?;

    let just_before_restart = sinex_primitives::temporal::now() - time::Duration::seconds(30);
    sqlx::query!(
        r#"
        UPDATE core.operations_log
        SET preview_summary = jsonb_set(
                preview_summary,
                '{started_at}',
                to_jsonb($2::timestamptz),
                true
            )
        WHERE id = $1::uuid
        "#,
        operation.operation_id,
        *just_before_restart,
    )
    .execute(&ctx.pool)
    .await?;

    recover_stale_replay_operations(&replay).await?;

    let recovered = replay.load_operation(operation.operation_id).await?;
    assert_eq!(recovered.state, ReplayState::Failed);
    assert!(
        recovered
            .error_details
            .as_deref()
            .is_some_and(|details| details.contains("restored")),
        "startup recovery should report the cascade restore: {:?}",
        recovered.error_details
    );
    assert!(
        ctx.pool.events().get_by_id(event_id).await?.is_some(),
        "immediate startup recovery must restore the archived event"
    );

    Ok(())
}
