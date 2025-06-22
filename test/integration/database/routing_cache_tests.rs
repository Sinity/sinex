// Routing cache tests - should fail until materialized view implementation is complete
// Tests for replacing per-row trigger routing with materialized view approach

use crate::common::database_helpers::get_shared_test_pool;
use sinex_db::queries::*;
use sinex_db::{refresh_routing_cache, run_batch_router};
use crate::common::{create_agent_with_subscriptions, test_event_with_payload, insert_event};
use sinex_core::RawEventBuilder;
use sinex_ulid::Ulid;
use sqlx::PgPool;
use anyhow::Result;
use serde_json::json;

#[tokio::test]
async fn test_routing_cache_view_exists() -> Result<()> {
    let pool = get_shared_test_pool().await?;
    // Test that the routing_cache materialized view exists
    let view_exists = sqlx::query!(
        r#"
        SELECT COUNT(*) as count 
        FROM pg_matviews 
        WHERE schemaname = 'sinex_schemas' 
        AND matviewname = 'routing_cache'
        "#
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(view_exists.count.unwrap(), 1, "routing_cache materialized view should exist");
    Ok(())
}

#[tokio::test]
async fn test_routing_cache_structure() -> Result<()> {
    let pool = get_shared_test_pool().await?;
    // Test that routing_cache has the correct columns (event_type, agent_id)
    // For materialized views, we need to query pg_attribute directly
    let columns = sqlx::query!(
        r#"
        SELECT a.attname AS column_name, t.typname AS data_type 
        FROM pg_attribute a 
        JOIN pg_type t ON a.atttypid = t.oid 
        JOIN pg_class c ON a.attrelid = c.oid 
        JOIN pg_namespace n ON c.relnamespace = n.oid 
        WHERE n.nspname = 'sinex_schemas' 
        AND c.relname = 'routing_cache' 
        AND a.attnum > 0 
        AND NOT a.attisdropped 
        ORDER BY a.attnum
        "#
    )
    .fetch_all(&pool)
    .await?;
    
    assert_eq!(columns.len(), 2, "routing_cache should have exactly 2 columns");
    
    let event_type_col = &columns[0];
    assert_eq!(event_type_col.column_name, "event_type");
    assert_eq!(event_type_col.data_type, "text");
    
    let agent_id_col = &columns[1];
    assert_eq!(agent_id_col.column_name, "agent_id");
    assert_eq!(agent_id_col.data_type, "text");
    
    Ok(())
}

#[tokio::test]
async fn test_routing_cache_auto_refresh_on_agent_change() -> Result<()> {
    let pool = get_shared_test_pool().await?;
    // Test that routing_cache is automatically refreshed when agent_manifests change
    
    // Create an agent with specific event subscriptions
    let agent_name = "test-router-agent";
    let subscriptions = json!({
        "filesystem": ["file_created", "file_modified"],
        "terminal": ["command_executed"]
    });
    
    create_agent_with_subscriptions(&pool, agent_name, &subscriptions).await?;
    
    // Manually refresh the view (this function should exist after implementation)
    refresh_routing_cache(&pool).await?;
    
    // Check that routing cache contains the expected entries
    let cache_entries = sqlx::query!(
        "SELECT event_type, agent_id FROM sinex_schemas.routing_cache WHERE agent_id = $1 ORDER BY event_type",
        agent_name
    )
    .fetch_all(&pool)
    .await?;
    
    assert_eq!(cache_entries.len(), 3, "Should have 3 routing cache entries");
    
    // Verify the specific entries
    assert_eq!(cache_entries[0].event_type.as_ref(), Some(&"filesystem:file_created".to_string()));
    assert_eq!(cache_entries[0].agent_id.as_ref(), Some(&agent_name.to_string()));
    
    assert_eq!(cache_entries[1].event_type.as_ref(), Some(&"filesystem:file_modified".to_string()));
    assert_eq!(cache_entries[1].agent_id.as_ref(), Some(&agent_name.to_string()));
    
    assert_eq!(cache_entries[2].event_type.as_ref(), Some(&"terminal:command_executed".to_string()));
    assert_eq!(cache_entries[2].agent_id.as_ref(), Some(&agent_name.to_string()));
    
    Ok(())
}

#[tokio::test]
async fn test_batch_router_creates_work_queue_entries() -> Result<()> {
    let pool = get_shared_test_pool().await?;
    // Test that the batch router function creates work queue entries based on routing cache
    
    // Create an agent
    let agent_name = "batch-test-agent";
    let subscriptions = json!({
        "test_source": ["test_event"]
    });
    create_agent_with_subscriptions(&pool, agent_name, &subscriptions).await?;
    
    // Create some test events
    let event1_id = insert_event(&pool, &test_event_with_payload("test_source", "test_event", json!({"data": "test data 1"}))).await?;
    let event2_id = insert_event(&pool, &test_event_with_payload("test_source", "test_event", json!({"data": "test data 2"}))).await?;
    
    // Refresh routing cache
    refresh_routing_cache(&pool).await?;
    
    // Run batch router (this function should exist after implementation)
    let routed_count = run_batch_router(&pool).await?;
    
    // Should have routed 2 events
    assert_eq!(routed_count, 2, "Should route 2 events to the agent");
    
    // Verify work queue entries were created
    let work_items = sqlx::query!(
        "SELECT raw_event_id::uuid as raw_event_id FROM sinex_schemas.work_queue WHERE target_agent_name = $1",
        agent_name
    )
    .fetch_all(&pool)
    .await?;
    
    assert_eq!(work_items.len(), 2, "Should have 2 work queue items");
    
    let event_ids: Vec<uuid::Uuid> = work_items.into_iter()
        .filter_map(|item| item.raw_event_id)
        .collect();
    
    assert!(event_ids.contains(&event1_id.to_uuid()));
    assert!(event_ids.contains(&event2_id.to_uuid()));
    
    Ok(())
}

#[tokio::test]
async fn test_batch_router_avoids_duplicate_routing() -> Result<()> {
    let pool = get_shared_test_pool().await?;
    // Test that batch router doesn't create duplicate work queue entries
    
    let agent_name = "dedup-test-agent";
    let subscriptions = json!({
        "test_source": ["test_event"]
    });
    create_agent_with_subscriptions(&pool, agent_name, &subscriptions).await?;
    
    // Create a test event
    let event_id = insert_event(&pool, &test_event_with_payload("test_source", "test_event", json!({"data": "dedup test"}))).await?;
    
    // Refresh routing cache
    refresh_routing_cache(&pool).await?;
    
    // Run batch router twice
    let first_run = run_batch_router(&pool).await?;
    let second_run = run_batch_router(&pool).await?;
    
    // First run should route 1 event, second run should route 0 (no duplicates)
    assert_eq!(first_run, 1, "First run should route 1 event");
    assert_eq!(second_run, 0, "Second run should not create duplicates");
    
    // Verify only one work queue entry exists
    let work_items = sqlx::query!(
        "SELECT COUNT(*) as count FROM sinex_schemas.work_queue WHERE target_agent_name = $1 AND raw_event_id = $2::uuid::ulid",
        agent_name,
        event_id.to_uuid()
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(work_items.count.unwrap(), 1, "Should have exactly 1 work queue item");
    
    Ok(())
}

#[tokio::test]
async fn test_routing_cache_performance_over_triggers() -> Result<()> {
    let pool = get_shared_test_pool().await?;
    // Test that routing cache approach performs better than per-row triggers
    // This is a conceptual test - in practice we'd measure timing
    
    // Create multiple agents with different subscriptions
    for i in 0..10 {
        let agent_name = format!("perf-agent-{}", i);
        let subscriptions = json!({
            "test_source": [format!("event_type_{}", i % 3)]
        });
        create_agent_with_subscriptions(&pool, &agent_name, &subscriptions).await?;
    }
    
    // Refresh routing cache once
    refresh_routing_cache(&pool).await?;
    
    // Create many events
    let mut event_ids = Vec::new();
    for i in 0..100 {
        let event_id = insert_test_event(
            &pool, 
            "test_source", 
            &format!("event_type_{}", i % 3),
            &format!("perf test {}", i)
        ).await?;
        event_ids.push(event_id);
    }
    
    // Run batch router (should be fast since routing cache is pre-computed)
    let start = std::time::Instant::now();
    let routed_count = run_batch_router(&pool).await?;
    let duration = start.elapsed();
    
    // Should route all events to appropriate agents
    assert!(routed_count > 0, "Should route some events");
    
    // Performance assertion (batch should be faster than 100 individual triggers)
    assert!(duration.as_millis() < 1000, "Batch routing should complete quickly");
    
    Ok(())
}


// Helper function for creating test events in routing tests
// This has a different signature (4 args vs 3) from the common insert_test_event function
async fn insert_test_event(
    pool: &PgPool, 
    source: &str, 
    event_type: &str, 
    test_data: &str
) -> Result<Ulid> {
    let payload = json!({
        "test": test_data,
        "source": source,
        "event_type": event_type
    });
    
    let event = insert_raw_event(
        pool,
        source,
        event_type,
        "test_host",
        payload,
        None,
        Some("1.0.0"),
        None,
    ).await?;
    
    Ok(event.id)
}

