use chrono::Utc;
use sinex_core::db::replay::state_machine::{ReplayCheckpoint, ReplayScope, ReplayStateMachine};
use sinex_test_utils::prelude::*;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::time::Duration;

#[sinex_test]
async fn replay_checkpoint_updates_block_on_row_lock(ctx: TestContext) -> TestResult<()> {
    let replay = ReplayStateMachine::new(ctx.pool.clone());
    let scope = ReplayScope {
        processor_id: "checkpoint-lock".to_string(),
        time_window: None,
        material_filter: None,
        filters: HashMap::new(),
    };
    let operation = replay.create_operation(scope, "tester".to_string()).await?;

    let mut tx = ctx.pool.begin().await?;
    sqlx::query!(
        r#"
        SELECT preview_summary
        FROM core.operations_log
        WHERE id::uuid = $1::uuid
        FOR UPDATE
        "#,
        operation.operation_id.to_uuid()
    )
    .fetch_one(&mut *tx)
    .await?;

    let done = Arc::new(AtomicBool::new(false));
    let done_flag = done.clone();
    let replay = ReplayStateMachine::new(ctx.pool.clone());
    let checkpoint = ReplayCheckpoint {
        processed_events: 1,
        total_events: 2,
        last_event_id: None,
        batch_number: 1,
        savepoint_id: None,
        updated_at: Utc::now(),
    };

    let handle = tokio::spawn(async move {
        let result = replay
            .update_checkpoint(operation.operation_id, &checkpoint)
            .await;
        done_flag.store(true, Ordering::SeqCst);
        result
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !done.load(Ordering::SeqCst),
        "checkpoint update should wait on row lock"
    );

    tx.rollback().await?;

    handle.await??;
    assert!(done.load(Ordering::SeqCst));
    Ok(())
}
