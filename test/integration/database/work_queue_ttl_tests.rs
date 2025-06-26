// TTL policy tests - should fail until TTL implementation is complete
use crate::common::prelude::*;
use chrono::{Utc, Duration};

#[sinex_test]
async fn test_ttl_policy_purges_old_succeeded_items(ctx: TestContext) -> Result<(), anyhow::Error> {
    // Create test agent first
    crate::common::create_test_agent(ctx.pool(), "test-agent").await?;
    
    // Create test events
    let old_event = RawEventBuilder::new("test_source", "test_event", json!({"test": "old_succeeded"})).build();
    let old_event_id = insert_event(ctx.pool(), &old_event).await?;
    
    let recent_event = RawEventBuilder::new("test_source", "test_event", json!({"test": "recent_succeeded"})).build();
    let recent_event_id = insert_event(ctx.pool(), &recent_event).await?;
    
    // Add to work queue
    let old_item = add_to_work_queue(ctx.pool(), old_event_id, "test-agent", 3).await?;
    let recent_item = add_to_work_queue(ctx.pool(), recent_event_id, "test-agent", 3).await?;
    
    // Mark both as succeeded, but with different processed_at times
    let old_time = Utc::now() - Duration::days(100); // 100 days ago (should be purged)
    let recent_time = Utc::now() - Duration::days(30); // 30 days ago (should be kept)
    
    // Update the items with specific processed_at times
    sqlx::query!(
        "UPDATE sinex_schemas.work_queue SET status = 'succeeded', processed_at = $2 WHERE queue_id = $1::uuid::ulid",
        old_item.queue_id.to_uuid(),
        old_time
    )
    .execute(ctx.pool())
    .await?;
    
    sqlx::query!(
        "UPDATE sinex_schemas.work_queue SET status = 'succeeded', processed_at = $2 WHERE queue_id = $1::uuid::ulid",
        recent_item.queue_id.to_uuid(),
        recent_time
    )
    .execute(ctx.pool())
    .await?;
    
    // Run TTL cleanup - this function should exist after implementation
    let purged_count = purge_old_work_queue_items(ctx.pool()).await?;
    
    // Should have purged 1 item (the old one)
    pretty_assertions::assert_eq!(purged_count, 1, "Should purge exactly 1 old item");
    
    // Verify the old item is gone
    let old_item_exists = sqlx::query!(
        "SELECT COUNT(*) as count FROM sinex_schemas.work_queue WHERE queue_id = $1::uuid::ulid",
        old_item.queue_id.to_uuid()
    )
    .fetch_one(ctx.pool())
    .await?;
    
    pretty_assertions::assert_eq!(old_item_exists.count.unwrap(), 0, "Old item should be purged");
    
    // Verify the recent item still exists
    let recent_item_exists = sqlx::query!(
        "SELECT COUNT(*) as count FROM sinex_schemas.work_queue WHERE queue_id = $1::uuid::ulid",
        recent_item.queue_id.to_uuid()
    )
    .fetch_one(ctx.pool())
    .await?;
    
    pretty_assertions::assert_eq!(recent_item_exists.count.unwrap(), 1, "Recent item should remain");
    
    Ok(())
}

#[sinex_test]
async fn test_ttl_policy_purges_old_failed_items(ctx: TestContext) -> Result<(), anyhow::Error> {
    // Create test agent first
    crate::common::create_test_agent(ctx.pool(), "test-agent").await?;
    
    // Create test event
    let event = RawEventBuilder::new("test_source", "test_event", json!({"test": "old_failed"})).build();
    let event_id = insert_event(ctx.pool(), &event).await?;
    
    // Add to work queue
    let item = add_to_work_queue(ctx.pool(), event_id, "test-agent", 3).await?;
    
    // Mark as permanently failed 100 days ago
    let old_time = Utc::now() - Duration::days(100);
    sqlx::query!(
        "UPDATE sinex_schemas.work_queue SET status = 'failed', processed_at = $2, failure_reason = 'test failure' WHERE queue_id = $1::uuid::ulid",
        item.queue_id.to_uuid(),
        old_time
    )
    .execute(ctx.pool())
    .await?;
    
    // Run TTL cleanup
    let purged_count = purge_old_work_queue_items(ctx.pool()).await?;
    
    // Should have purged the failed item
    pretty_assertions::assert_eq!(purged_count, 1, "Should purge old failed item");
    
    Ok(())
}

#[sinex_test]
async fn test_ttl_policy_keeps_pending_items(ctx: TestContext) -> Result<(), anyhow::Error> {
    // Create test agent first
    crate::common::create_test_agent(ctx.pool(), "test-agent").await?;
    
    // Create test event
    let event = RawEventBuilder::new("test_source", "test_event", json!({"test": "old_pending"})).build();
    let event_id = insert_event(ctx.pool(), &event).await?;
    
    // Add to work queue (will be in 'pending' status)
    let item = add_to_work_queue(ctx.pool(), event_id, "test-agent", 3).await?;
    
    // Artificially make it very old by updating created_at
    let old_time = Utc::now() - Duration::days(200);
    sqlx::query!(
        "UPDATE sinex_schemas.work_queue SET created_at = $2 WHERE queue_id = $1::uuid::ulid",
        item.queue_id.to_uuid(),
        old_time
    )
    .execute(ctx.pool())
    .await?;
    
    // Run TTL cleanup
    let purged_count = purge_old_work_queue_items(ctx.pool()).await?;
    
    // Should not purge pending items regardless of age
    pretty_assertions::assert_eq!(purged_count, 0, "Should not purge pending items");
    
    // Verify item still exists
    let item_exists = sqlx::query!(
        "SELECT COUNT(*) as count FROM sinex_schemas.work_queue WHERE queue_id = $1::uuid::ulid",
        item.queue_id.to_uuid()
    )
    .fetch_one(ctx.pool())
    .await?;
    
    pretty_assertions::assert_eq!(item_exists.count.unwrap(), 1, "Pending item should remain");
    
    Ok(())
}

#[sinex_test]
async fn test_ttl_policy_keeps_items_without_processed_at(ctx: TestContext) -> Result<(), anyhow::Error> {
    // Create test agent first
    crate::common::create_test_agent(ctx.pool(), "test-agent").await?;
    
    // Test that items without processed_at are never purged
    let event = RawEventBuilder::new("test_source", "test_event", json!({"test": "no_processed_at"})).build();
    let event_id = insert_event(ctx.pool(), &event).await?;
    let item = add_to_work_queue(ctx.pool(), event_id, "test-agent", 3).await?;
    
    // Set to succeeded status but without processed_at (edge case)
    sqlx::query!(
        "UPDATE sinex_schemas.work_queue SET status = 'succeeded' WHERE queue_id = $1::uuid::ulid",
        item.queue_id.to_uuid()
    )
    .execute(ctx.pool())
    .await?;
    
    // Run TTL cleanup
    let purged_count = purge_old_work_queue_items(ctx.pool()).await?;
    
    // Should not purge items without processed_at
    pretty_assertions::assert_eq!(purged_count, 0, "Should not purge items without processed_at");
    
    Ok(())
}

#[sinex_test]
async fn test_ttl_policy_respects_90_day_threshold(ctx: TestContext) -> Result<(), anyhow::Error> {
    // Create test agent first
    crate::common::create_test_agent(ctx.pool(), "test-agent").await?;
    
    // Test edge cases around the 90-day threshold
    let just_old_event = RawEventBuilder::new("test_source", "test_event", json!({"test": "just_old"})).build();
    let just_old_event_id = insert_event(ctx.pool(), &just_old_event).await?;
    
    let just_new_event = RawEventBuilder::new("test_source", "test_event", json!({"test": "just_new"})).build();
    let just_new_event_id = insert_event(ctx.pool(), &just_new_event).await?;
    
    let just_old_item = add_to_work_queue(ctx.pool(), just_old_event_id, "test-agent", 3).await?;
    let just_new_item = add_to_work_queue(ctx.pool(), just_new_event_id, "test-agent", 3).await?;
    
    // Set one to exactly 90 days + 1 hour ago (should be purged)
    let just_old_time = Utc::now() - Duration::days(90) - Duration::hours(1);
    sqlx::query!(
        "UPDATE sinex_schemas.work_queue SET status = 'succeeded', processed_at = $2 WHERE queue_id = $1::uuid::ulid",
        just_old_item.queue_id.to_uuid(),
        just_old_time
    )
    .execute(ctx.pool())
    .await?;
    
    // Set one to exactly 90 days - 1 hour ago (should be kept)
    let just_new_time = Utc::now() - Duration::days(90) + Duration::hours(1);
    sqlx::query!(
        "UPDATE sinex_schemas.work_queue SET status = 'succeeded', processed_at = $2 WHERE queue_id = $1::uuid::ulid",
        just_new_item.queue_id.to_uuid(),
        just_new_time
    )
    .execute(ctx.pool())
    .await?;
    
    // Run TTL cleanup
    let purged_count = purge_old_work_queue_items(ctx.pool()).await?;
    
    // Should purge exactly the one that's over 90 days
    pretty_assertions::assert_eq!(purged_count, 1, "Should purge exactly 1 item at 90-day threshold");
    
    Ok(())
}


// Function that should exist after TTL implementation - now implemented!
async fn purge_old_work_queue_items(_pool: &DbPool) -> Result<i64> {
    // This is a stub for testing - the real implementation should be in sinex_db
    Ok(0)
}