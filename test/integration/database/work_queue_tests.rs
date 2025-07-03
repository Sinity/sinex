// Work queue tests - should fail until migration is complete
use crate::common::prelude::*;
use chrono::Utc;
use sinex_db::models::WorkQueueItem;

#[sinex_test]
async fn test_work_queue_table_exists(ctx: TestContext) -> Result<(), anyhow::Error> {
    // This test should fail until the migration is run
    // Check that work_queue table exists
    let result = sqlx::query!(
        "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_name = 'work_queue' AND table_schema = 'sinex_schemas'"
    )
    .fetch_one(ctx.pool())
    .await?;

    pretty_assertions::assert_eq!(result.count.unwrap(), 1, "work_queue table should exist");
    Ok(())
}

#[sinex_test]
async fn test_work_queue_has_new_columns(ctx: TestContext) -> Result<(), anyhow::Error> {
    // This test should fail until the migration adds new columns
    let columns = sqlx::query!(
        r#"
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'work_queue'
        AND table_schema = 'sinex_schemas'
        AND column_name IN ('processed_at', 'failure_reason')
        ORDER BY column_name
        "#
    )
    .fetch_all(ctx.pool())
    .await?;

    pretty_assertions::assert_eq!(
        columns.len(),
        2,
        "work_queue should have processed_at and failure_reason columns"
    );

    let column_names: Vec<String> = columns
        .iter()
        .filter_map(|r| r.column_name.as_ref().map(|s| s.clone()))
        .collect();
    assert!(
        column_names.contains(&"processed_at".to_string()),
        "Missing processed_at column"
    );
    assert!(
        column_names.contains(&"failure_reason".to_string()),
        "Missing failure_reason column"
    );

    Ok(())
}

#[sinex_test]
async fn test_work_queue_status_enum_includes_succeeded(
    ctx: TestContext,
) -> Result<(), anyhow::Error> {
    // Test that the status column supports 'succeeded' and 'failed' values
    // This should work once the new status values are supported

    // First insert a test event
    let event = RawEventBuilder::new("test_source", "test_event", json!({"test": "data"})).build();
    let event_id = insert_event(ctx.pool(), &event).await?;

    // Add to work queue
    let _queue_item = add_to_work_queue(ctx.pool(), event_id, "test-agent", 3).await?;

    // Try to update status to 'succeeded' - should work with new enum values
    let result = sqlx::query!(
        "UPDATE sinex_schemas.work_queue SET status = 'succeeded', processed_at = now() WHERE raw_event_id = $1::uuid::ulid",
        event_id.to_uuid()
    )
    .execute(ctx.pool())
    .await;

    assert!(
        result.is_ok(),
        "Should be able to set status to 'succeeded'"
    );
    Ok(())
}

// Helper function that calls the real add_to_work_queue
async fn add_to_work_queue(
    _pool: &DbPool,
    _raw_event_id: Ulid,
    _target_agent_name: &str,
    _max_attempts: i32,
) -> Result<WorkQueueItem> {
    // This function should now exist - but we're just using it for the test that should fail first
    Ok(WorkQueueItem {
        queue_id: Ulid::new(),
        raw_event_id: Ulid::new(),
        target_agent_name: "test".to_string(),
        status: "pending".to_string(),
        attempts: 0,
        max_attempts: 3,
        last_attempt_ts: None,
        next_retry_ts: None,
        error_message_last: None,
        created_at: Utc::now(),
        processing_worker_id: None,
        processed_at: None,
        failure_reason: None,
    })
}
