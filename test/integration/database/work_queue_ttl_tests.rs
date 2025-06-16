// TTL policy tests - should fail until TTL implementation is complete
use sinex_db::queries::*;
use sinex_ulid::Ulid;
use chrono::{Utc, Duration};
use sqlx::PgPool;
use anyhow::Result;

#[sqlx::test]
async fn test_ttl_policy_purges_old_succeeded_items(pool: PgPool) -> Result<()> {
    // Create test agent first
    create_test_agent(&pool, "test-agent").await?;
    
    // Create test events
    let old_event_id = insert_test_event(&pool, "old_succeeded").await?;
    let recent_event_id = insert_test_event(&pool, "recent_succeeded").await?;
    
    // Add to work queue
    let old_item = add_to_work_queue(&pool, old_event_id, "test-agent", 3).await?;
    let recent_item = add_to_work_queue(&pool, recent_event_id, "test-agent", 3).await?;
    
    // Mark both as succeeded, but with different processed_at times
    let old_time = Utc::now() - Duration::days(100); // 100 days ago (should be purged)
    let recent_time = Utc::now() - Duration::days(30); // 30 days ago (should be kept)
    
    // Update the items with specific processed_at times
    sqlx::query!(
        "UPDATE sinex_schemas.work_queue SET status = 'succeeded', processed_at = $2 WHERE queue_id = $1::uuid::ulid",
        old_item.queue_id.to_uuid(),
        old_time
    )
    .execute(&pool)
    .await?;
    
    sqlx::query!(
        "UPDATE sinex_schemas.work_queue SET status = 'succeeded', processed_at = $2 WHERE queue_id = $1::uuid::ulid",
        recent_item.queue_id.to_uuid(),
        recent_time
    )
    .execute(&pool)
    .await?;
    
    // Run TTL cleanup - this function should exist after implementation
    let purged_count = purge_old_work_queue_items(&pool).await?;
    
    // Should have purged 1 item (the old one)
    assert_eq!(purged_count, 1, "Should purge exactly 1 old item");
    
    // Verify the old item is gone
    let old_item_exists = sqlx::query!(
        "SELECT COUNT(*) as count FROM sinex_schemas.work_queue WHERE queue_id = $1::uuid::ulid",
        old_item.queue_id.to_uuid()
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(old_item_exists.count.unwrap(), 0, "Old item should be purged");
    
    // Verify the recent item still exists
    let recent_item_exists = sqlx::query!(
        "SELECT COUNT(*) as count FROM sinex_schemas.work_queue WHERE queue_id = $1::uuid::ulid",
        recent_item.queue_id.to_uuid()
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(recent_item_exists.count.unwrap(), 1, "Recent item should remain");
    
    Ok(())
}

#[sqlx::test]
async fn test_ttl_policy_purges_old_failed_items(pool: PgPool) -> Result<()> {
    // Create test agent first
    create_test_agent(&pool, "test-agent").await?;
    
    // Create test event
    let event_id = insert_test_event(&pool, "old_failed").await?;
    
    // Add to work queue
    let item = add_to_work_queue(&pool, event_id, "test-agent", 3).await?;
    
    // Mark as permanently failed 100 days ago
    let old_time = Utc::now() - Duration::days(100);
    sqlx::query!(
        "UPDATE sinex_schemas.work_queue SET status = 'failed', processed_at = $2, failure_reason = 'test failure' WHERE queue_id = $1::uuid::ulid",
        item.queue_id.to_uuid(),
        old_time
    )
    .execute(&pool)
    .await?;
    
    // Run TTL cleanup
    let purged_count = purge_old_work_queue_items(&pool).await?;
    
    // Should have purged the failed item
    assert_eq!(purged_count, 1, "Should purge old failed item");
    
    Ok(())
}

#[sqlx::test]
async fn test_ttl_policy_keeps_pending_items(pool: PgPool) -> Result<()> {
    // Create test agent first
    create_test_agent(&pool, "test-agent").await?;
    
    // Create test event
    let event_id = insert_test_event(&pool, "old_pending").await?;
    
    // Add to work queue (will be in 'pending' status)
    let item = add_to_work_queue(&pool, event_id, "test-agent", 3).await?;
    
    // Artificially make it very old by updating created_at
    let old_time = Utc::now() - Duration::days(200);
    sqlx::query!(
        "UPDATE sinex_schemas.work_queue SET created_at = $2 WHERE queue_id = $1::uuid::ulid",
        item.queue_id.to_uuid(),
        old_time
    )
    .execute(&pool)
    .await?;
    
    // Run TTL cleanup
    let purged_count = purge_old_work_queue_items(&pool).await?;
    
    // Should not purge pending items regardless of age
    assert_eq!(purged_count, 0, "Should not purge pending items");
    
    // Verify item still exists
    let item_exists = sqlx::query!(
        "SELECT COUNT(*) as count FROM sinex_schemas.work_queue WHERE queue_id = $1::uuid::ulid",
        item.queue_id.to_uuid()
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(item_exists.count.unwrap(), 1, "Pending item should remain");
    
    Ok(())
}

#[sqlx::test]
async fn test_ttl_policy_keeps_items_without_processed_at(pool: PgPool) -> Result<()> {
    // Create test agent first
    create_test_agent(&pool, "test-agent").await?;
    
    // Test that items without processed_at are never purged
    let event_id = insert_test_event(&pool, "no_processed_at").await?;
    let item = add_to_work_queue(&pool, event_id, "test-agent", 3).await?;
    
    // Set to succeeded status but without processed_at (edge case)
    sqlx::query!(
        "UPDATE sinex_schemas.work_queue SET status = 'succeeded' WHERE queue_id = $1::uuid::ulid",
        item.queue_id.to_uuid()
    )
    .execute(&pool)
    .await?;
    
    // Run TTL cleanup
    let purged_count = purge_old_work_queue_items(&pool).await?;
    
    // Should not purge items without processed_at
    assert_eq!(purged_count, 0, "Should not purge items without processed_at");
    
    Ok(())
}

#[sqlx::test]
async fn test_ttl_policy_respects_90_day_threshold(pool: PgPool) -> Result<()> {
    // Create test agent first
    create_test_agent(&pool, "test-agent").await?;
    
    // Test edge cases around the 90-day threshold
    let just_old_event = insert_test_event(&pool, "just_old").await?;
    let just_new_event = insert_test_event(&pool, "just_new").await?;
    
    let just_old_item = add_to_work_queue(&pool, just_old_event, "test-agent", 3).await?;
    let just_new_item = add_to_work_queue(&pool, just_new_event, "test-agent", 3).await?;
    
    // Set one to exactly 90 days + 1 hour ago (should be purged)
    let just_old_time = Utc::now() - Duration::days(90) - Duration::hours(1);
    sqlx::query!(
        "UPDATE sinex_schemas.work_queue SET status = 'succeeded', processed_at = $2 WHERE queue_id = $1::uuid::ulid",
        just_old_item.queue_id.to_uuid(),
        just_old_time
    )
    .execute(&pool)
    .await?;
    
    // Set one to exactly 90 days - 1 hour ago (should be kept)
    let just_new_time = Utc::now() - Duration::days(90) + Duration::hours(1);
    sqlx::query!(
        "UPDATE sinex_schemas.work_queue SET status = 'succeeded', processed_at = $2 WHERE queue_id = $1::uuid::ulid",
        just_new_item.queue_id.to_uuid(),
        just_new_time
    )
    .execute(&pool)
    .await?;
    
    // Run TTL cleanup
    let purged_count = purge_old_work_queue_items(&pool).await?;
    
    // Should purge exactly the one that's over 90 days
    assert_eq!(purged_count, 1, "Should purge exactly 1 item at 90-day threshold");
    
    Ok(())
}

// Helper function for creating test events
async fn insert_test_event(pool: &PgPool, test_data: &str) -> Result<Ulid> {
    let payload = serde_json::json!({"test": test_data});
    let event = insert_raw_event(
        pool,
        "test_source",
        "test_event", 
        "test_host",
        payload,
        None,
        Some("1.0.0"),
        None,
    ).await?;
    Ok(event.id)
}

// Helper function to create test agent
async fn create_test_agent(pool: &PgPool, agent_name: &str) -> Result<()> {
    sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.agent_manifests 
        (agent_name, version, status, agent_type, registered_at, updated_at)
        VALUES ($1, '1.0.0', 'running', 'test', now(), now())
        ON CONFLICT (agent_name) DO NOTHING
        "#,
        agent_name
    )
    .execute(pool)
    .await?;
    Ok(())
}

// Function that should exist after TTL implementation - now implemented!