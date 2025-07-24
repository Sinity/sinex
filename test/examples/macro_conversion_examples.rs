//! Example conversions showing how to modernize tests with macros
//! 
//! This file demonstrates before/after examples of converting verbose
//! test implementations to use the powerful test macro system.

use sinex_test_utils::prelude::*;
use sinex_test_utils::test_macros::*;

// =============================================================================
// EXAMPLE 1: Simple Event Insertion
// =============================================================================

// BEFORE: Verbose manual implementation (15 lines)
#[sinex_test]
async fn test_file_created_event_old(ctx: TestContext) -> TestResult {
    let event = EventFactory::new("fs")
        .create_event("file.created", json!({"path": "/test/file.txt", "size": 1024}));
    
    let inserted_id = sinex_db::insert_event_with_validator(ctx.pool(), &event, None)
        .await?
        .id;
    
    let retrieved = sqlx::query!("SELECT * FROM core.events WHERE id = $1", inserted_id.to_uuid())
        .fetch_one(ctx.pool())
        .await?;
    
    assert_eq!(retrieved.source, "fs");
    assert_eq!(retrieved.event_type, "file.created");
    assert_eq!(retrieved.payload.get("path").unwrap().as_str().unwrap(), "/test/file.txt");
    
    Ok(())
}

// AFTER: Clean macro usage (5 lines)
test_event_insertion!(
    test_file_created_event_new,
    "fs",
    "file.created",
    json!({"path": "/test/file.txt", "size": 1024})
);

// =============================================================================
// EXAMPLE 2: Batch Event Operations
// =============================================================================

// BEFORE: Complex concurrent insertion (30+ lines)
#[sinex_test]
async fn test_bulk_import_old(ctx: TestContext) -> TestResult {
    let event_count = 100;
    let mut handles = vec![];
    
    for i in 0..event_count {
        let pool = ctx.pool().clone();
        let handle = tokio::spawn(async move {
            let event = EventFactory::new("import")
                .create_event("item.imported", json!({"index": i, "data": format!("item_{}", i)}));
            sinex_db::insert_event_with_validator(&pool, &event, None).await
        });
        handles.push(handle);
    }
    
    let results: Vec<_> = futures::future::try_join_all(handles).await?;
    let successful = results.iter().filter(|r| r.is_ok()).count();
    
    assert_eq!(successful, event_count);
    
    // Verify all events exist
    let count: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM core.events WHERE source = 'import'")
        .fetch_one(ctx.pool())
        .await?
        .unwrap_or(0);
    
    assert!(count >= event_count as i64);
    
    Ok(())
}

// AFTER: Expressive macro usage (10 lines)
test_batch_events!(
    test_bulk_import_new,
    "import",
    "item.imported",
    100,
    |pool, events| async move {
        let count: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM core.events WHERE source = 'import'")
            .fetch_one(pool)
            .await?
            .unwrap_or(0);
        assert!(count >= 100);
        Ok(())
    }
);

// =============================================================================
// EXAMPLE 3: Checkpoint Flow Testing
// =============================================================================

// BEFORE: Repetitive checkpoint management (25+ lines)
#[sinex_test]
async fn test_automaton_progress_old(ctx: TestContext) -> TestResult {
    use sinex_satellite_sdk::checkpoint::CheckpointManager;
    
    let checkpoint_manager = CheckpointManager::new(
        ctx.pool().clone(),
        "test_automaton",
        "default_group",
        "test_consumer",
    );
    
    // Initial state
    let mut checkpoint = checkpoint_manager.load_checkpoint().await?;
    checkpoint.processed_count = 0;
    checkpoint_manager.save_checkpoint(&checkpoint).await?;
    
    // Process some events
    checkpoint.processed_count = 50;
    checkpoint.set_last_processed_id(Some("event_50".to_string()));
    checkpoint_manager.save_checkpoint(&checkpoint).await?;
    
    // Verify
    let loaded = checkpoint_manager.load_checkpoint().await?;
    assert_eq!(loaded.processed_count, 50);
    assert_eq!(loaded.last_processed_id(), Some("event_50"));
    
    Ok(())
}

// AFTER: Declarative checkpoint testing (5 lines)
test_checkpoint_flow!(
    test_automaton_progress_new,
    "test_automaton",
    0,    // initial count
    50    // updated count
);

// =============================================================================
// EXAMPLE 4: Concurrent Operations Testing
// =============================================================================

// BEFORE: Manual concurrency management (35+ lines)
#[sinex_test]
async fn test_concurrent_queries_old(ctx: TestContext) -> TestResult {
    let worker_count = 50;
    let queries_per_worker = 10;
    let pool = Arc::new(ctx.pool().clone());
    let mut handles = vec![];
    
    for worker_id in 0..worker_count {
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            let mut results = vec![];
            for query_id in 0..queries_per_worker {
                let result: i32 = sqlx::query_scalar("SELECT $1::int + $2::int")
                    .bind(worker_id)
                    .bind(query_id)
                    .fetch_one(pool_clone.as_ref())
                    .await?;
                results.push(result);
            }
            Ok::<Vec<i32>, sqlx::Error>(results)
        });
        handles.push(handle);
    }
    
    let all_results = futures::future::try_join_all(handles).await?;
    
    // Verify all workers completed successfully
    assert_eq!(all_results.len(), worker_count);
    for (worker_id, worker_results) in all_results.iter().enumerate() {
        let results = worker_results.as_ref().unwrap();
        assert_eq!(results.len(), queries_per_worker);
        for (query_id, &result) in results.iter().enumerate() {
            assert_eq!(result, (worker_id + query_id) as i32);
        }
    }
    
    Ok(())
}

// AFTER: Clear concurrent testing (15 lines)
test_concurrent_operations!(
    test_concurrent_queries_new,
    50, // worker count
    |pool, worker_id| async move {
        let mut results = vec![];
        for query_id in 0..10 {
            let result: i32 = sqlx::query_scalar("SELECT $1::int + $2::int")
                .bind(worker_id)
                .bind(query_id)
                .fetch_one(pool.as_ref())
                .await?;
            results.push(result);
        }
        Ok(results)
    },
    |_pool, results| async move {
        for (worker_id, worker_results) in results.iter().enumerate() {
            let results = worker_results.as_ref().unwrap();
            for (query_id, &result) in results.iter().enumerate() {
                assert_eq!(result, (worker_id + query_id) as i32);
            }
        }
        Ok(())
    }
);

// =============================================================================
// EXAMPLE 5: Time-based Query Testing
// =============================================================================

// BEFORE: Manual time range setup (20+ lines)
#[sinex_test]
async fn test_event_time_filtering_old(ctx: TestContext) -> TestResult {
    use chrono::{Duration, Utc};
    
    let now = Utc::now();
    let events_to_insert = 20;
    
    // Insert events across time range
    for i in 0..events_to_insert {
        let event = EventFactory::new("timed")
            .create_event("test.event", json!({"index": i}));
        let time_offset = Duration::hours(i as i64 - 10); // -10 to +9 hours
        // Would need custom insertion with timestamp here...
        sinex_db::insert_event_with_validator(ctx.pool(), &event, None).await?;
    }
    
    // Query specific range
    let start = now - Duration::hours(5);
    let end = now + Duration::hours(5);
    
    let events = get_events_in_time_range(ctx.pool(), start, end).await?;
    
    // Should get roughly half the events
    assert!(events.len() >= 8 && events.len() <= 12);
    
    Ok(())
}

// AFTER: Declarative time-based testing (8 lines)
test_time_range_query!(
    test_event_time_filtering_new,
    20,                               // total events
    chrono::Duration::hours(1),       // spacing between events
    chrono::Duration::hours(-5),      // range start offset
    chrono::Duration::hours(5),       // range end offset
    10                                // expected events in range
);

// =============================================================================
// EXAMPLE 6: Redis Stream Operations
// =============================================================================

// BEFORE: Complex Redis stream setup (40+ lines)
#[sinex_test]
async fn test_redis_event_streaming_old(_ctx: TestContext) -> TestResult {
    use redis::{cmd, AsyncCommands};
    
    let redis_client = redis::Client::open("redis://localhost:6379")?;
    let mut conn = redis_client.get_async_connection().await?;
    
    let stream_key = "test:stream";
    let consumer_group = "test-group";
    
    // Cleanup
    let _: Result<i32, _> = conn.del(stream_key).await;
    
    // Create consumer group
    match cmd("XGROUP")
        .arg("CREATE").arg(stream_key).arg(consumer_group).arg("0").arg("MKSTREAM")
        .query_async::<_, ()>(&mut conn).await {
        Ok(_) => {},
        Err(e) if e.to_string().contains("BUSYGROUP") => {},
        Err(e) => return Err(e.into()),
    }
    
    // Add messages
    for i in 0..10 {
        let _: String = conn.xadd(stream_key, "*", &[
            ("index", i.to_string()),
            ("data", format!("message_{}", i)),
        ]).await?;
    }
    
    // Read messages
    let result = cmd("XREADGROUP")
        .arg("GROUP").arg(consumer_group).arg("consumer1")
        .arg("COUNT").arg(10)
        .arg("STREAMS").arg(stream_key).arg(">")
        .query_async::<_, redis::streams::StreamReadReply>(&mut conn).await?;
    
    assert_eq!(result.keys.len(), 1);
    assert_eq!(result.keys[0].ids.len(), 10);
    
    // Cleanup
    let _: Result<i32, _> = conn.del(stream_key).await;
    
    Ok(())
}

// AFTER: Simplified Redis testing (10 lines)
test_redis_stream_operations!(
    test_redis_event_streaming_new,
    "test:stream",
    "test-group",
    10, // message count
    |conn, stream_key, result, message_ids| async move {
        assert_eq!(result.keys.len(), 1);
        assert_eq!(result.keys[0].ids.len(), 10);
        assert_eq!(message_ids.len(), 10);
        Ok(())
    }
);

// =============================================================================
// EXAMPLE 7: Schema Validation Testing
// =============================================================================

// BEFORE: Verbose schema validation (30+ lines)
#[sinex_test]
async fn test_event_schema_enforcement_old(ctx: TestContext) -> TestResult {
    let schema = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "user_id": {"type": "string", "format": "uuid"},
            "action": {"type": "string", "enum": ["login", "logout", "update"]},
            "timestamp": {"type": "string", "format": "date-time"}
        },
        "required": ["user_id", "action"],
        "additionalProperties": false
    });
    
    // Register schema (would need actual registration code)
    // ...
    
    // Test valid payload
    let valid_payload = json!({
        "user_id": "550e8400-e29b-41d4-a716-446655440000",
        "action": "login",
        "timestamp": "2025-01-01T12:00:00Z"
    });
    
    // This should pass validation
    // ... validation code ...
    
    // Test invalid payload
    let invalid_payload = json!({
        "user_id": "not-a-uuid",
        "action": "invalid_action"
    });
    
    // This should fail validation
    // ... validation code ...
    
    Ok(())
}

// AFTER: Declarative schema testing (12 lines)
test_schema_validation!(
    test_event_schema_enforcement_new,
    json!({
        "user_id": "550e8400-e29b-41d4-a716-446655440000",
        "action": "login",
        "timestamp": "2025-01-01T12:00:00Z"
    }),
    json!({
        "user_id": "not-a-uuid",
        "action": "invalid_action"
    }),
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "user_id": {"type": "string", "format": "uuid"},
            "action": {"type": "string", "enum": ["login", "logout", "update"]},
            "timestamp": {"type": "string", "format": "date-time"}
        },
        "required": ["user_id", "action"],
        "additionalProperties": false
    }),
    "format" // expected error pattern
);

// =============================================================================
// EXAMPLE 8: Event Filtering
// =============================================================================

// BEFORE: Manual filtering implementation (25 lines)
#[sinex_test]
async fn test_source_filtering_old(ctx: TestContext) -> TestResult {
    let sources = ["fs", "terminal", "desktop"];
    let events_per_source = 5;
    
    // Insert events from multiple sources
    for source in &sources {
        for i in 0..events_per_source {
            let event = EventFactory::new(source)
                .create_event("test.event", json!({"index": i}));
            sinex_db::insert_event_with_validator(ctx.pool(), &event, None).await?;
        }
    }
    
    // Query filtered events
    let fs_events = sqlx::query!("SELECT * FROM core.events WHERE source = 'fs'")
        .fetch_all(ctx.pool())
        .await?;
    
    assert_eq!(fs_events.len(), events_per_source);
    for event in &fs_events {
        assert_eq!(event.source, "fs");
    }
    
    Ok(())
}

// AFTER: Clean filtering test (8 lines)
test_event_filter!(
    test_source_filtering_new,
    &["fs", "terminal", "desktop"],
    5,      // events per source
    "fs",   // filter source
    5       // expected count
);

// =============================================================================
// EXAMPLE 9: Parameterized Testing
// =============================================================================

// BEFORE: Repetitive test cases (40+ lines)
#[sinex_test]
async fn test_event_type_validation_old(ctx: TestContext) -> TestResult {
    // Test case 1: Valid event type
    let event1 = EventFactory::new("test").create_event("valid.event.type", json!({}));
    let result1 = sinex_db::insert_event_with_validator(ctx.pool(), &event1, None).await;
    assert!(result1.is_ok());
    
    // Test case 2: Empty event type
    let event2 = EventFactory::new("test").create_event("", json!({}));
    let result2 = sinex_db::insert_event_with_validator(ctx.pool(), &event2, None).await;
    assert!(result2.is_err());
    
    // Test case 3: Invalid characters
    let event3 = EventFactory::new("test").create_event("invalid@type!", json!({}));
    let result3 = sinex_db::insert_event_with_validator(ctx.pool(), &event3, None).await;
    assert!(result3.is_err());
    
    // Test case 4: Too long
    let long_type = "a".repeat(256);
    let event4 = EventFactory::new("test").create_event(&long_type, json!({}));
    let result4 = sinex_db::insert_event_with_validator(ctx.pool(), &event4, None).await;
    assert!(result4.is_err());
    
    Ok(())
}

// AFTER: Parameterized test cases (15 lines)
parameterized_test!(
    test_event_type_validation_new,
    vec![
        ("valid type", ("valid.event.type", true)),
        ("empty type", ("", false)),
        ("invalid chars", ("invalid@type!", false)),
        ("too long", (&"a".repeat(256), false)),
    ],
    |pool, (event_type, should_pass)| async move {
        let event = EventFactory::new("test").create_event(event_type, json!({}));
        let result = sinex_db::insert_event_with_validator(pool, &event, None).await;
        assert_eq!(result.is_ok(), *should_pass);
        Ok(())
    }
);

// =============================================================================
// EXAMPLE 10: Event Flow Testing
// =============================================================================

// BEFORE: Manual flow orchestration (20+ lines)
#[sinex_test]
async fn test_event_processing_flow_old(ctx: TestContext) -> TestResult {
    // Insert event
    let event = EventFactory::new("workflow")
        .create_event("task.created", json!({"task_id": "123", "priority": "high"}));
    let event_id = sinex_db::insert_event_with_validator(ctx.pool(), &event, None).await?.id;
    
    // Simulate processor checkpoint
    let checkpoint_id = Ulid::new();
    sqlx::query!(
        "INSERT INTO core.automaton_checkpoints (id, automaton_name, last_processed_id, processed_count)
         VALUES ($1, $2, $3, $4)",
        checkpoint_id.to_uuid(),
        "task_processor",
        event_id.to_string(),
        1i64
    ).execute(ctx.pool()).await?;
    
    // Verify flow completion
    let checkpoint = sqlx::query!("SELECT * FROM core.automaton_checkpoints WHERE id = $1", checkpoint_id.to_uuid())
        .fetch_one(ctx.pool()).await?;
    
    assert_eq!(checkpoint.last_processed_id.as_deref(), Some(&event_id.to_string()));
    
    Ok(())
}

// AFTER: Declarative flow testing (5 lines)
test_event_flow!(
    test_event_processing_flow_new,
    "workflow",
    "task.created",
    "task_processor"
);

// =============================================================================
// Migration Guide Summary
// =============================================================================

/// This module demonstrates the power of test macros in reducing boilerplate
/// and improving test clarity. Key benefits:
/// 
/// 1. **Code Reduction**: 50-75% fewer lines for common patterns
/// 2. **Consistency**: All tests follow the same patterns
/// 3. **Maintainability**: Changes to test infrastructure are centralized
/// 4. **Readability**: Tests express intent, not implementation
/// 5. **Error Prevention**: Less manual code = fewer bugs
/// 
/// When writing new tests, always check if an existing macro fits your use case
/// before writing verbose implementations.