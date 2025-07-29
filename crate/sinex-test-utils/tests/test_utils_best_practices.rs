//! Comprehensive test suite demonstrating best practices for sinex-test-utils
//!
//! This test suite serves as both validation and documentation for how to
//! properly use the test utilities in real-world scenarios.

use serde_json::json;
use sinex_test_utils::prelude::*;
use std::time::Duration;

/// Test basic event creation patterns
#[sinex_test]
async fn test_event_creation_patterns(ctx: TestContext) -> Result<()> {
    // Basic event creation
    let simple_event = ctx
        .event()
        .source("test-suite")
        .type_("example.basic")
        .insert()
        .await?;

    // Event with custom fields
    let detailed_event = ctx
        .event()
        .source("test-suite")
        .type_("example.detailed")
        .field("user_id", "test-user-123")
        .field("action", "login")
        .field(
            "metadata",
            json!({
                "ip": "192.168.1.1",
                "user_agent": "TestClient/1.0"
            }),
        )
        .timestamp(chrono::Utc::now())
        .insert()
        .await?;

    // Domain-specific builders
    let fs_event = ctx
        .event()
        .filesystem()
        .path("/test/example.txt")
        .size(1024)
        .permissions(0o644)
        .field("owner", "testuser")
        .created()
        .insert()
        .await?;

    // Verify all events were created
    ctx.assert_event_count(3).await?;

    // Verify event properties
    ctx.assert("simple event validation")
        .that(simple_event.source == "test-suite", "source should match")?
        .that(
            simple_event.event_type == "example.basic",
            "type should match",
        )?;

    ctx.assert("detailed event validation")
        .eq(&detailed_event.payload["user_id"], &json!("test-user-123"))?
        .eq(&detailed_event.payload["action"], &json!("login"))?;

    ctx.assert("filesystem event validation")
        .eq(&fs_event.payload["path"], &json!("/test/example.txt"))?
        .eq(&fs_event.payload["size"], &json!(1024))?;

    Ok(())
}

/// Test querying patterns
#[sinex_test]
async fn test_query_patterns(ctx: TestContext) -> Result<()> {
    // Create test data
    for i in 0..10 {
        ctx.event()
            .source("query-test")
            .type_(if i % 2 == 0 { "even" } else { "odd" })
            .field("index", i)
            .field("category", if i < 5 { "low" } else { "high" })
            .insert()
            .await?;
    }

    // Basic queries
    let all_events = ctx.events().fetch().await?;
    ctx.assert("all events").has_size(&all_events, 10)?;

    let even_events = ctx.events().by_type("even").fetch().await?;
    ctx.assert("even events").has_size(&even_events, 5)?;

    // Count queries
    let total = ctx.events().count().await?;
    let low_count = ctx
        .events()
        .by_source("query-test")
        .fetch()
        .await?
        .into_iter()
        .filter(|e| e.payload["category"] == "low")
        .count();

    ctx.assert("counts")
        .eq(&(total as usize), &10)?
        .eq(&low_count, &5)?;

    // Verify ordering
    let ordered = ctx
        .events()
        .by_source("query-test")
        .limit(3)
        .fetch()
        .await?;

    // Events should be in creation order (by ULID)
    for window in ordered.windows(2) {
        ctx.assert("event ordering").that(
            window[0].id < window[1].id,
            "events should be ordered by ID",
        )?;
    }

    Ok(())
}

/// Test schema validation
#[sinex_test]
async fn test_schema_validation_patterns(ctx: TestContext) -> Result<()> {
    // Register a schema
    let user_schema = json!({
        "type": "object",
        "properties": {
            "username": {
                "type": "string",
                "minLength": 3,
                "maxLength": 20,
                "pattern": "^[a-zA-Z0-9_]+$"
            },
            "email": {
                "type": "string",
                "format": "email"
            },
            "age": {
                "type": "integer",
                "minimum": 0,
                "maximum": 150
            }
        },
        "required": ["username", "email"]
    });

    let schema_id = ctx
        .schema()
        .register("auth", "user.created", user_schema)
        .await?;

    // Create valid event using validated builder
    let valid_user = ctx
        .validated_event(schema_id)
        .source("auth")
        .type_("user.created")
        .field("username", "test_user")
        .field("email", "test@example.com")
        .field("age", 25)
        .insert()
        .await?;

    // Verify it was created
    ctx.assert("valid user created")
        .eq(&valid_user.payload["username"], &json!("test_user"))?;

    // Test validation with invalid data
    let invalid_event = ctx
        .event()
        .source("auth")
        .type_("user.created")
        .field("username", "a") // Too short
        .field("email", "not-an-email") // Invalid format
        .build()?;

    // Should fail validation
    ctx.schema()
        .assert_invalid(&invalid_event, schema_id)
        .await?;

    // Test missing required field
    let missing_field = ctx
        .event()
        .source("auth")
        .type_("user.created")
        .field("username", "valid_user")
        // Missing email!
        .build()?;

    ctx.schema()
        .assert_invalid(&missing_field, schema_id)
        .await?;

    Ok(())
}

/// Test timing and synchronization utilities
#[sinex_test]
async fn test_timing_patterns(ctx: TestContext) -> Result<()> {
    // Test barrier synchronization
    let barrier = ctx.timing().barrier(3);
    let barrier = std::sync::Arc::new(barrier);

    let mut handles = vec![];
    for i in 0..3 {
        let b = barrier.clone();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(i * 10)).await;
            b.wait(Duration::from_secs(1)).await
        });
        handles.push(handle);
    }

    // All should complete successfully
    for handle in handles {
        handle.await??;
    }

    // Test event counter
    let counter = ctx.timing().event_counter(5);
    for _ in 0..5 {
        counter.increment();
    }
    ctx.assert("counter reached target")
        .eq(&counter.get(), &5)?;

    // Test synchronizer
    let sync = ctx.timing().synchronizer(Duration::from_secs(1));
    let sync = std::sync::Arc::new(sync);

    let sync_clone = sync.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        sync_clone.signal();
    });

    // Should not timeout
    sync.wait().await?;

    Ok(())
}

/// Test concurrent operations
#[sinex_test]
async fn test_concurrent_patterns(ctx: TestContext) -> Result<()> {
    // Run concurrent tasks with proper isolation
    let results = ctx
        .run_concurrent(5, |ctx, i| async move {
            // Each task gets its own database
            let event = ctx
                .event()
                .source("concurrent")
                .type_("worker.task")
                .field("worker_id", i)
                .field("timestamp", chrono::Utc::now())
                .insert()
                .await?;

            // Simulate some work
            tokio::time::sleep(Duration::from_millis(10)).await;

            // Return the event ID
            Ok(event.id)
        })
        .await?;

    // Should have all results
    ctx.assert("concurrent results").has_size(&results, 5)?;

    // Verify all IDs are unique
    let unique_ids: std::collections::HashSet<_> = results.iter().collect();
    ctx.assert("unique IDs").eq(&unique_ids.len(), &5)?;

    Ok(())
}

/// Test batch operations
#[sinex_test]
async fn test_batch_patterns(ctx: TestContext) -> Result<()> {
    // Create batch of events
    let batch = ctx.create_event_batch("batch-test", 10);
    ctx.assert("batch size").has_size(&batch, 10)?;

    // Insert them with custom modifications
    for (i, builder) in batch.into_iter().enumerate() {
        builder
            .field("batch_index", i)
            .field("processed", false)
            .insert()
            .await?;
    }

    // Use batch insert for pre-built events
    let pre_built: Vec<_> = (0..5)
        .map(|i| {
            ctx.event()
                .source("pre-built")
                .type_("batch.item")
                .field("item_id", format!("item_{}", i))
                .build()
        })
        .collect::<Result<Vec<_>, _>>()?;

    let inserted = ctx.insert_events(&pre_built).await?;
    ctx.assert("batch insert").has_size(&inserted, 5)?;

    // Verify all events
    let total = ctx.events().count().await?;
    ctx.assert("total after batches")
        .eq(&(total as usize), &15)?;

    Ok(())
}

/// Test error handling patterns
#[sinex_test]
async fn test_error_handling_patterns(ctx: TestContext) -> Result<()> {
    // Test validation errors
    let empty_source_result = ctx.event().source("").type_("test").insert().await;

    ctx.assert("empty source fails")
        .error_contains(&empty_source_result, "source")?;

    // Test error aggregation in concurrent operations
    let results = ctx
        .run_concurrent(3, |ctx, i| async move {
            if i == 1 {
                // Intentionally fail one task
                Err(anyhow::anyhow!("Task {} intentionally failed", i))
            } else {
                ctx.event()
                    .source("error-test")
                    .type_("success")
                    .field("task", i)
                    .insert()
                    .await
                    .map(|e| e.id)
            }
        })
        .await;

    // Should have aggregated error
    ctx.assert("concurrent with errors")
        .error_contains(&results, "Task 1 intentionally failed")?;

    Ok(())
}

/// Test fixture usage patterns
#[sinex_test]
async fn test_fixture_patterns(ctx: TestContext) -> Result<()> {
    // Access standard fixtures
    let user_session = ctx.fixtures().user_session().await?;
    ctx.assert("user session fixture")
        .not_empty(&user_session.event_ids)?;

    // Fixtures are cached within a test
    let session2 = ctx.fixtures().user_session().await?;
    ctx.assert("fixture caching")
        .eq(&user_session.event_ids.len(), &session2.event_ids.len())?;

    // Access specialized fixtures
    let checkpoints = ctx.fixtures().scenarios().populated_checkpoints().await?;

    ctx.assert("checkpoint fixture")
        .that(checkpoints.checkpoint_count > 0, "should have checkpoints")?;

    Ok(())
}

/// Test wait conditions and assertions
#[sinex_test]
async fn test_wait_and_assert_patterns(ctx: TestContext) -> Result<()> {
    // Spawn a task that creates events after a delay
    tokio::spawn({
        let ctx_clone = TestContext::with_name("async_producer").await?;
        async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            for i in 0..5 {
                ctx_clone
                    .event()
                    .source("async-producer")
                    .type_("delayed")
                    .field("index", i)
                    .insert()
                    .await?;
            }
            Ok::<_, anyhow::Error>(())
        }
    });

    // Wait for events to appear
    ctx.wait_for_condition(|| async {
        let count = ctx.events().by_source("async-producer").count().await?;
        Ok(count >= 5)
    })
    .await?;

    // Verify they're all there
    let events = ctx.events().by_source("async-producer").fetch().await?;
    ctx.assert("async events arrived").has_size(&events, 5)?;

    Ok(())
}

/// Test snapshot functionality
#[sinex_test]
async fn test_snapshot_patterns(ctx: TestContext) -> Result<()> {
    // Create complex data structure
    let complex_data = json!({
        "version": "1.0",
        "features": ["auth", "storage", "api"],
        "config": {
            "timeout": 30,
            "retries": 3,
            "endpoints": {
                "api": "https://api.example.com",
                "auth": "https://auth.example.com"
            }
        },
        "metadata": {
            "created": "2024-01-01",
            "author": "test"
        }
    });

    // Assert snapshot (creates on first run, compares on subsequent runs)
    ctx.assert_snapshot("complex_config", &complex_data).await?;

    // Test event snapshot
    let event = ctx
        .event()
        .source("snapshot-test")
        .type_("config.loaded")
        .field("config", complex_data.clone())
        .insert()
        .await?;

    ctx.assert_snapshot("config_event_payload", &event.payload)
        .await?;

    Ok(())
}

/// Test measurement and performance patterns
#[sinex_test]
async fn test_measurement_patterns(ctx: TestContext) -> Result<()> {
    // Measure operation time
    let (result, duration) = ctx
        .measure(async {
            // Simulate some work
            tokio::time::sleep(Duration::from_millis(50)).await;

            // Create events
            let mut ids = vec![];
            for i in 0..3 {
                let event = ctx
                    .event()
                    .source("measured")
                    .type_("operation")
                    .field("step", i)
                    .insert()
                    .await?;
                ids.push(event.id);
            }
            Ok(ids)
        })
        .await?;

    // Verify timing
    ctx.assert("operation timing")
        .that(
            duration >= Duration::from_millis(50),
            "should take at least 50ms",
        )?
        .that(
            duration < Duration::from_millis(200),
            "should not take too long",
        )?;

    // Verify result
    ctx.assert("operation result").has_size(&result, 3)?;

    Ok(())
}

/// Test Redis integration patterns
#[sinex_test]
async fn test_redis_patterns(ctx: TestContext) -> Result<()> {
    let mut redis = ctx.redis().await?;

    // Track keys for automatic cleanup
    let key = format!("test:{}:counter", ctx.test_name());
    ctx.track_redis_key(key.clone());

    // Use Redis for test coordination
    redis::cmd("SET")
        .arg(&key)
        .arg(0)
        .query_async(&mut redis)
        .await?;

    // Increment counter
    let new_val: i32 = redis::cmd("INCR").arg(&key).query_async(&mut redis).await?;

    ctx.assert("redis counter").eq(&new_val, &1)?;

    // Key will be automatically cleaned up when test ends

    Ok(())
}

/// Test complex query patterns
#[sinex_test]
async fn test_advanced_query_patterns(ctx: TestContext) -> Result<()> {
    // Create diverse test data
    let sources = ["service-a", "service-b", "service-c"];
    let types = ["startup", "heartbeat", "shutdown"];

    for source in &sources {
        for event_type in &types {
            for i in 0..3 {
                ctx.event()
                    .source(source)
                    .type_(event_type)
                    .field("instance", i)
                    .field("timestamp", chrono::Utc::now())
                    .insert()
                    .await?;
            }
        }
    }

    // Complex filtering
    let service_a_heartbeats = ctx
        .events()
        .by_source("service-a")
        .by_type("heartbeat")
        .fetch()
        .await?;

    ctx.assert("filtered query")
        .has_size(&service_a_heartbeats, 3)?;

    // Verify all are correct
    for event in &service_a_heartbeats {
        ctx.assert("event properties")
            .eq(&event.source, &"service-a")?
            .eq(&event.event_type, &"heartbeat")?;
    }

    Ok(())
}

/// Integration test showing real-world usage
#[sinex_test]
async fn test_real_world_scenario(ctx: TestContext) -> Result<()> {
    // Simulate a user session with multiple activities

    // 1. User login
    let login_event = ctx
        .event()
        .source("auth-service")
        .type_("user.login")
        .field("user_id", "user-123")
        .field("ip", "192.168.1.100")
        .field("success", true)
        .insert()
        .await?;

    // 2. User performs actions
    let actions = ["view_dashboard", "update_profile", "upload_file"];
    for action in &actions {
        ctx.event()
            .source("app-service")
            .type_("user.action")
            .field("user_id", "user-123")
            .field("action", action)
            .field("session_id", login_event.id.to_string())
            .insert()
            .await?;
    }

    // 3. System generates events
    ctx.event()
        .filesystem()
        .path("/uploads/user-123/file.pdf")
        .size(1024 * 1024) // 1MB
        .created()
        .insert()
        .await?;

    // 4. Verify the session
    let user_events = ctx
        .events()
        .fetch()
        .await?
        .into_iter()
        .filter(|e| {
            e.payload
                .get("user_id")
                .and_then(|v| v.as_str())
                .map(|s| s == "user-123")
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();

    ctx.assert("user session events")
        .has_size(&user_events, 4)?; // login + 3 actions

    // 5. Test concurrent activity
    let results = ctx
        .run_concurrent(3, |ctx, i| async move {
            ctx.event()
                .source("background-service")
                .type_("task.completed")
                .field("task_id", format!("task-{}", i))
                .field("user_id", "user-123")
                .insert()
                .await
        })
        .await?;

    ctx.assert("background tasks").has_size(&results, 3)?;

    Ok(())
}
