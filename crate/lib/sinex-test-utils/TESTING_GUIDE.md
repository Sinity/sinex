# Sinex Test Utilities - Comprehensive Testing Guide

This guide provides comprehensive documentation for using the sinex-test-utils crate to write robust, efficient tests for the Sinex event system.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Core Concepts](#core-concepts)
3. [Test Context](#test-context)
4. [Database Management](#database-management)
5. [Fixtures](#fixtures)
6. [Assertions](#assertions)
7. [Timing and Synchronization](#timing-and-synchronization)
8. [Property Testing](#property-testing)
9. [Error Testing](#error-testing)
10. [Performance Testing](#performance-testing)
11. [Best Practices](#best-practices)
12. [Troubleshooting](#troubleshooting)

## Quick Start

### Basic Test Structure

```rust
use sinex_test_utils::prelude::*;

#[sinex_test]
async fn test_basic_event_creation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Create a test event using production APIs
    let event = ctx.create_test_event(
        "fs-watcher",
        "file.created",
        json!({
            "path": "/data/test.txt",
            "size": 1024
        })
    ).await?;
    
    // Query events using direct repository access
    let events = ctx.pool.events()
        .get_by_source(&EventSource::from_static("fs-watcher"), Some(10), None)
        .await?;
    
    // Make assertions with rich context
    ctx.assert("event creation")
        .eq(&events.len(), &1)?
        .that(events[0].payload["size"] == json!(1024), "size should match")?;
    
    Ok(())
}
```

### Test with Tracing

```rust
#[sinex_test(trace = true)]
async fn test_with_logging(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Starting test with tracing enabled");
    
    let event = ctx.create_test_event("test", "logged.event", json!({})).await?;
    tracing::debug!("Created event: {:?}", event.id);
    
    // Verify log messages were captured
    ctx.assert_logged("Starting test with tracing enabled")?;
    
    Ok(())
}
```

## Core Concepts

### The `#[sinex_test]` Macro

**Always use `#[sinex_test]` instead of `#[test]`**. This macro provides:

- Automatic TestContext creation and injection
- Database isolation per test
- Timeout handling
- Tracing integration
- Progress indicators
- Integration with property testing

#### Macro Options

```rust
#[sinex_test]                    // Basic test
#[sinex_test(timeout = 30)]      // Custom timeout in seconds
#[sinex_test(trace = true)]      // Enable tracing
```

### Production API Usage

The test utilities are designed to work with production APIs directly, not wrapper APIs:

```rust
// ✅ Good: Direct production API usage
let event = Event::new(FileCreatedPayload {
    path: "/data/file.txt".into(),
    size: 1024,
    created_at: Utc::now(),
    permissions: Some(0o644),
}).into();
ctx.pool.events().insert(event).await?;

// ✅ Good: Convenience helper for simple tests
let event = ctx.create_test_event("fs", "file.created", json!({"path": "/test"})).await?;

// ❌ Avoid: Don't look for wrapper builder APIs - use production code
```

## Test Context

`TestContext` is the main entry point for all test functionality:

### Database Access

```rust
// Direct pool access for repositories
let events = ctx.pool.events().get_recent(10).await?;
let count = ctx.pool.events().count_all().await?;

// Blob management
let blob = ctx.pool.blobs().get_by_id(&blob_id).await?;

// Checkpoints
let checkpoint = ctx.pool.checkpoints().get_by_processor("worker-1").await?;
```

### Event Creation

```rust
// Simple test events
let event = ctx.create_test_event(
    "fs-watcher",
    "file.modified", 
    json!({
        "path": "/tmp/test.log",
        "size": 2048,
        "modified_at": "2024-01-01T12:00:00Z"
    })
).await?;

// Batch event creation
let events = vec![
    RawEvent::test_event("fs", "file.created", json!({"path": "/a"})),
    RawEvent::test_event("fs", "file.created", json!({"path": "/b"})),
    RawEvent::test_event("fs", "file.created", json!({"path": "/c"})),
];
ctx.insert_events(&events).await?;
```

### Test Metadata

```rust
let test_name = ctx.test_name();  // Get test name for scoping
let elapsed = ctx.elapsed();      // Time since test start
```

## Database Management

### Automatic Isolation

Each test gets its own isolated database from a 64-database pool:

```rust
#[sinex_test]
async fn test_isolation_example(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // This test cannot see data from other tests
    let count = ctx.pool.events().count_all().await?;
    assert_eq!(count, 0); // Always starts empty
    
    // Create test data
    ctx.create_test_event("test", "isolation", json!({})).await?;
    
    let count = ctx.pool.events().count_all().await?;
    assert_eq!(count, 1);
    
    Ok(())
    // Database automatically cleaned up
}
```

### Manual Database Operations

```rust
use sinex_test_utils::db_common;

// Get row counts for verification
let counts = db_common::get_row_counts(&ctx.pool).await?;
for (table, count) in counts {
    println!("{}: {} rows", table, count);
}

// Verify clean state
db_common::verify_clean_state(&ctx.pool).await?;

// Reset database manually (usually not needed)
db_common::reset_database(&ctx.pool).await?;
```

## Fixtures

Fixtures provide reusable test data with proper lifecycle management:

### Standard Fixtures

```rust
use sinex_test_utils::fixtures;

#[sinex_test]
async fn test_with_user_session(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Create a realistic user session with mixed event types
    let session = fixtures::standard_user_session(&ctx).await?;
    
    assert!(!session.event_ids.is_empty());
    assert!(session.checkpoint_id.is_some());
    
    // Session includes filesystem, terminal, and clipboard events
    let fs_events = ctx.pool.events()
        .get_by_source(&EventSource::from("filesystem"), Some(100), None)
        .await?;
    assert!(!fs_events.is_empty());
    
    Ok(())
}
```

### Performance Fixtures

```rust
#[sinex_test]
async fn test_with_performance_dataset(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Create a large dataset for performance testing
    let dataset = fixtures::performance_dataset_with_size(&ctx, 10_000).await?;
    
    assert_eq!(dataset.event_count, 10_000);
    assert!(!dataset.source_distribution.is_empty());
    assert!(!dataset.type_distribution.is_empty());
    
    // Dataset includes size statistics
    println!("Payload stats: {:?}", dataset.payload_size_stats);
    
    Ok(())
}
```

### Error Scenario Fixtures

```rust
#[sinex_test]
async fn test_error_handling(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let errors = fixtures::error_scenarios(&ctx).await?;
    
    // Test with known invalid data
    assert!(!errors.invalid_event_ids.is_empty());
    assert!(!errors.error_messages.is_empty());
    
    // Verify error handling works correctly
    for error_msg in &errors.error_messages {
        assert!(error_msg.contains("error") || error_msg.contains("invalid"));
    }
    
    Ok(())
}
```

### Custom Fixtures

```rust
#[sinex_test]
async fn test_custom_fixture(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Create parameterized fixture
    let session = fixtures::user_session_with_params(&ctx, 100, 10).await?;
    assert_eq!(session.event_ids.len(), 100);
    
    // Create concurrency test fixture
    let concurrency = fixtures::concurrency_test_fixture(&ctx, 5, 20).await?;
    assert_eq!(concurrency.worker_events.len(), 5);
    assert!(!concurrency.synchronization_points.is_empty());
    
    Ok(())
}
```

## Assertions

Rich assertion helpers with context and clear error messages:

### Basic Assertions

```rust
#[sinex_test]
async fn test_assertions(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let events = vec![1, 2, 3];
    let name = Some("test".to_string());
    let empty: Vec<i32> = vec![];
    
    ctx.assert("collection validation")
        .eq(&events.len(), &3)?                      // Equality
        .not_empty(&events)?                         // Non-empty check
        .has_size(&events, 3)?                       // Exact size
        .some(&name)?                                // Option has value
        .that(events[0] == 1, "first element is 1")?; // Custom condition
    
    // This assertion should fail
    let result = ctx.assert("empty check").not_empty(&empty);
    assert!(result.is_err());
    
    Ok(())
}
```

### Event-Specific Assertions

```rust
#[sinex_test]
async fn test_event_assertions(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let event = ctx.create_test_event("test", "validation", json!({})).await?;
    
    // Verify event exists in database
    let event_id = event.id.expect("Event should have ID");
    let exists = ctx.pool.events().exists_by_id(&event_id).await?;
    assert!(exists);
    
    // Count-based assertions
    let count = ctx.pool.events().count_all().await?;
    ctx.assert("event count").eq(&count, &1)?;
    
    Ok(())
}
```

### Error Assertions

```rust
#[sinex_test]
async fn test_error_assertions(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test that invalid operations fail
    let result = ctx.create_test_event("", "invalid", json!({})).await;
    
    ctx.assert("validation error")
        .error_contains(&result, "source")?; // Error message contains "source"
    
    Ok(())
}
```

## Timing and Synchronization

### Basic Timing

```rust
#[sinex_test]
async fn test_timing(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let start_time = ctx.elapsed();
    
    // Do some work
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let elapsed = ctx.elapsed();
    assert!(elapsed > start_time);
    
    // Measure specific operations
    let (result, duration) = ctx.measure(async {
        expensive_operation().await
    }).await?;
    
    println!("Operation took: {:?}", duration);
    
    Ok(())
}
```

### Waiting for Conditions

```rust
#[sinex_test]
async fn test_waiting(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Start background task
    let pool = ctx.pool.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Create event after delay
        let event = RawEvent::test_event("async", "delayed", json!({}));
        let _ = pool.events().insert(event).await;
    });
    
    // Wait for event to appear
    ctx.timing().wait_for_event_count(1).await?;
    
    let count = ctx.pool.events().count_all().await?;
    assert_eq!(count, 1);
    
    Ok(())
}
```

### Synchronization Primitives

```rust
#[sinex_test]
async fn test_synchronization(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    
    let counter = Arc::new(AtomicUsize::new(0));
    let barrier = ctx.timing().barrier(3);
    
    // Spawn concurrent tasks
    let mut handles = vec![];
    for i in 0..3 {
        let counter = counter.clone();
        let barrier = barrier.clone();
        
        let handle = tokio::spawn(async move {
            // All tasks increment counter
            counter.fetch_add(1, Ordering::SeqCst);
            
            // Wait for all tasks to reach this point
            barrier.wait().await;
            
            // All tasks should see count = 3
            assert_eq!(counter.load(Ordering::SeqCst), 3);
        });
        handles.push(handle);
    }
    
    // Wait for all tasks
    for handle in handles {
        handle.await.map_err(|e| SinexError::service(format!("Task failed: {}", e)))??;
    }
    
    Ok(())
}
```

## Property Testing

Combine proptest with database operations:

```rust
use proptest::prelude::*;

#[sinex_test]
async fn test_property_based(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Define test cases
    let test_cases = vec![
        ("short", "x".repeat(10)),
        ("medium", "x".repeat(100)),
        ("long", "x".repeat(1000)),
        ("unicode", "Hello 世界 🌍".to_string()),
        ("empty", "".to_string()),
    ];
    
    for (name, data) in test_cases {
        let event = ctx.create_test_event(
            "proptest",
            name,
            json!({"data": data})
        ).await?;
        
        // Verify event was stored correctly
        let event_id = event.id.expect("Event should have ID");
        let retrieved = ctx.pool.events().get_by_id(&event_id).await?;
        assert!(retrieved.is_some());
        
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.event_type.as_str(), name);
        assert_eq!(retrieved.payload["data"], json!(data));
    }
    
    Ok(())
}
```

## Error Testing

Test error conditions and recovery:

```rust
use sinex_test_utils::error_testing::*;

#[sinex_test]
async fn test_validation_errors(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let tester = ValidationTester::new(&ctx);
    
    // Test various validation scenarios
    let test_cases = vec![
        ("empty_source", "", "valid_type"),
        ("empty_type", "valid_source", ""),
        ("null_payload", "source", "type"),
    ];
    
    for (case_name, source, event_type) in test_cases {
        let result = tester.test_validation_case(case_name, || async {
            ctx.create_test_event(source, event_type, json!(null)).await
        }).await;
        
        // Should fail validation
        assert!(result.is_err());
    }
    
    Ok(())
}
```

## Performance Testing

Use built-in benchmarking support:

```rust
#[cfg(all(test, feature = "bench"))]
mod benches {
    use super::*;
    use sinex_test_utils::prelude::*;
    
    #[sinex_bench]
    async fn bench_event_insertion() -> color_eyre::eyre::Result<()> {
        // Setup large dataset
        let dataset = standard_fixtures::time_series(DatasetSize::Medium);
        
        // Benchmark insertion
        let events = generate_test_events(1000);
        insert_events_batch(ctx.pool(), &events).await?;
        
        Ok(())
    }
    
    #[sinex_bench(args = [100, 1000, 10000])]
    async fn bench_batch_insert(count: usize) -> color_eyre::eyre::Result<()> {
        let events = generate_test_events(count);
        insert_events_batch(ctx.pool(), &events).await?;
        Ok(())
    }
}
```

## Best Practices

### Test Organization

```rust
// ✅ Good: Descriptive test names
#[sinex_test]
async fn test_filesystem_events_trigger_processing_pipeline(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Clear intent from name
}

// ✅ Good: Group related tests in modules
mod filesystem_tests {
    use super::*;
    
    #[sinex_test]
    async fn test_file_creation_events(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        // Related tests together
    }
    
    #[sinex_test]
    async fn test_file_deletion_events(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        // Related tests together
    }
}
```

### Error Handling

```rust
// ✅ Good: Proper error propagation
#[sinex_test]
async fn test_with_proper_errors(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let event = ctx.create_test_event("test", "proper", json!({})).await?;
    let events = ctx.pool.events().get_recent(10).await?;
    ctx.assert("event count").eq(&events.len(), &1)?;
    Ok(())
}

// ❌ Avoid: Panicking in tests
#[sinex_test]
async fn test_bad_error_handling(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let events = ctx.pool.events().get_recent(10).await.unwrap(); // Don't do this
    assert_eq!(events.len(), 1); // Don't use assert! for business logic
    Ok(())
}
```

### Resource Management

```rust
// ✅ Good: Use fixtures for expensive setup
#[sinex_test]
async fn test_with_fixtures(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let session = fixtures::standard_user_session(&ctx).await?;
    // Fixture automatically cached and cleaned up
    test_user_behavior(&session).await?;
    Ok(())
}

// ✅ Good: Scope fixtures appropriately
#[sinex_test]
async fn test_transaction_scoped(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    fixtures::with_transaction_fixture(&ctx, |tx| {
        Box::pin(async move {
            // Work with transaction-scoped data
            Ok("result")
        })
    }).await?;
    Ok(())
}
```

## Troubleshooting

### Common Issues

#### Test Timeouts

```rust
// If tests are timing out, increase timeout
#[sinex_test(timeout = 60)] // 60 seconds
async fn test_slow_operation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Slow operation
    Ok(())
}
```

#### Database Connection Issues

```rust
// Check database state
#[sinex_test]
async fn test_debug_database(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let counts = db_common::get_row_counts(&ctx.pool).await?;
    println!("Table counts: {:?}", counts);
    
    // Verify database is accessible
    db_common::verify_clean_state(&ctx.pool).await?;
    
    Ok(())
}
```

#### Fixture Issues

```rust
// Debug fixture creation
#[sinex_test]
async fn test_debug_fixtures(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let session = fixtures::standard_user_session(&ctx).await?;
    println!("Session created with {} events", session.event_ids.len());
    
    // Check what was actually created
    let count = ctx.pool.events().count_all().await?;
    println!("Total events in database: {}", count);
    
    Ok(())
}
```

### Performance Issues

```rust
// Profile slow tests
#[sinex_test]
async fn test_profile_performance(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let (result, duration) = ctx.measure(async {
        // Operation to profile
        let dataset = fixtures::performance_dataset_with_size(&ctx, 1000).await?;
        Ok(dataset.event_count)
    }).await?;
    
    println!("Created {} events in {:?}", result?, duration);
    
    Ok(())
}
```

### Debugging Test Failures

```rust
// Add debug output to failing tests
#[sinex_test(trace = true)]
async fn test_debug_failure(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Starting debug test");
    
    let event = ctx.create_test_event("debug", "test", json!({})).await?;
    tracing::debug!("Created event: {:?}", event);
    
    let events = ctx.pool.events().get_recent(10).await?;
    tracing::debug!("Found {} events", events.len());
    
    // Check captured logs if test fails
    let logs = ctx.captured_logs();
    println!("Captured logs: {:?}", logs);
    
    Ok(())
}
```

## Advanced Examples

### Complex Integration Test

```rust
#[sinex_test]
async fn test_complete_event_pipeline(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // 1. Create initial filesystem event
    let file_event = ctx.create_test_event(
        "fs-watcher",
        "file.created",
        json!({
            "path": "/data/input.csv",
            "size": 1024 * 1024,
            "permissions": "0644"
        })
    ).await?;
    
    // 2. Simulate processing trigger
    let processor_event = ctx.create_test_event(
        "data-processor",
        "processing.started",
        json!({
            "input_file": "/data/input.csv",
            "processor_id": "csv-analyzer",
            "started_at": chrono::Utc::now().to_rfc3339()
        })
    ).await?;
    
    // 3. Wait for processing completion
    ctx.timing().wait_for_condition(|| async {
        let count = ctx.pool.events()
            .count_by_event_type(&EventType::from("processing.completed"))
            .await?;
        Ok(count >= 1)
    }).await?;
    
    // 4. Verify results
    let completed_events = ctx.pool.events()
        .get_by_event_type(&EventType::from("processing.completed"), Some(10), None)
        .await?;
    
    ctx.assert("processing pipeline")
        .not_empty(&completed_events)?
        .that(
            completed_events[0].payload["input_file"] == json!("/data/input.csv"),
            "processed correct file"
        )?;
    
    // 5. Verify checkpoints were created
    let checkpoints = ctx.pool.checkpoints()
        .get_by_processor("csv-analyzer")
        .await?;
    assert!(checkpoints.is_some());
    
    Ok(())
}
```

### Concurrent Testing

```rust
#[sinex_test]
async fn test_concurrent_event_processing(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    
    let success_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));
    
    // Spawn multiple workers
    let mut handles = vec![];
    for worker_id in 0..10 {
        let success_count = success_count.clone();
        let error_count = error_count.clone();
        let pool = ctx.pool.clone();
        
        let handle = tokio::spawn(async move {
            for i in 0..50 {
                let event = RawEvent::test_event(
                    EventSource::from(format!("worker-{}", worker_id)),
                    EventType::from("concurrent.test"),
                    json!({
                        "worker_id": worker_id,
                        "iteration": i,
                        "timestamp": chrono::Utc::now().to_rfc3339()
                    })
                );
                
                match pool.events().insert(event).await {
                    Ok(_) => { success_count.fetch_add(1, Ordering::SeqCst); }
                    Err(_) => { error_count.fetch_add(1, Ordering::SeqCst); }
                }
            }
        });
        handles.push(handle);
    }
    
    // Wait for all workers
    for handle in handles {
        handle.await.map_err(|e| SinexError::service(format!("Worker failed: {}", e)))?;
    }
    
    let successes = success_count.load(Ordering::SeqCst);
    let errors = error_count.load(Ordering::SeqCst);
    
    println!("Concurrent test: {} successes, {} errors", successes, errors);
    assert_eq!(successes, 500); // 10 workers * 50 operations
    assert_eq!(errors, 0);
    
    // Verify all events were stored
    let total_events = ctx.pool.events().count_all().await?;
    assert_eq!(total_events, 500);
    
    Ok(())
}
```

This guide provides comprehensive coverage of the sinex-test-utils capabilities. For specific use cases not covered here, refer to the individual module documentation and the test files in the `tests/` directory for more examples.