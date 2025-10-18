# Sinex Test Utilities

> The workspace-wide testing handbook lives at [`TESTING.md`](../../TESTING.md).
> Use it for quick-start commands, suite layout, and property-testing guidance.
> This README focuses on the crate itself; API-level details are in
> `doc/overview.md` and `doc/testing_quality_overview.md`.

A comprehensive testing framework for the Sinex event-driven data capture system, providing database isolation, fixture management, and robust testing patterns.

## Quick Start

```rust
use sinex_test_utils::prelude::*;

#[sinex_test]
async fn test_event_creation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Create test event using production APIs
    let event = ctx.create_test_event(
        "fs-watcher",
        "file.created",
        json!({"path": "/test.txt", "size": 1024})
    ).await?;
    
    // Query using direct repository access
    let events = ctx.pool.events().get_recent(10).await?;
    
    // Rich assertions with context
    ctx.assert("event creation")
        .eq(&events.len(), &1)?
        .that(events[0].payload["size"] == json!(1024), "size should match")?;
    
    Ok(())
}
```

## Key Features

- **Database Isolation**: Each test gets its own isolated database from a 64-database pool
- **Production API Usage**: Tests use real production APIs, not mocks or wrappers
- **Rich Fixtures**: Pre-built fixtures for common testing scenarios
- **Comprehensive Assertions**: Context-aware assertions with clear error messages
- **Timing & Synchronization**: Tools for testing concurrent operations
- **Property Testing**: Integration with proptest for robust edge case testing
- **Tracing Integration**: Automatic log capture and verification
- **Performance Testing**: Built-in benchmarking and performance measurement

## Architecture

### Database Pool Strategy

The test utilities use a sophisticated 64-database pool system that provides true isolation:

```
┌─────────────┐ ┌─────────────┐ ┌─────────────┐     ┌─────────────┐
│   Test 1    │ │   Test 2    │ │   Test 3    │ ... │   Test N    │
│  Database   │ │  Database   │ │  Database   │     │  Database   │
│   Pool 1    │ │   Pool 2    │ │   Pool 3    │     │   Pool N    │
└─────────────┘ └─────────────┘ └─────────────┘     └─────────────┘
```

- Each test acquires an exclusive database via PostgreSQL advisory locks
- Parallel test execution with no interference
- Automatic cleanup after test completion
- Pool recycling for optimal performance

### Production API Integration

Tests work directly with production code:

```rust
// ✅ Direct production API usage
let event = Event::new(FileCreatedPayload { /* ... */ }).into();
ctx.pool.events().insert(event).await?;

// ✅ Repository access
let events = ctx.pool.events().get_by_source(&source, limit, offset).await?;

// ✅ Test utilities for convenience
let event = ctx.create_test_event("source", "type", json!({})).await?;
```

## Core Components

### TestContext

Central coordination point providing:

```rust
pub struct TestContext {
    pub pool: DbPool,           // Direct database access
    // ... private fields for lifecycle management
}

impl TestContext {
    // Event creation helpers
    pub async fn create_test_event(&self, source: &str, event_type: &str, payload: JsonValue) -> Result<RawEvent>;
    
    // Rich assertions
    pub fn assert(&self, context: &str) -> AssertionBuilder;
    
    // Timing and measurement
    pub fn elapsed(&self) -> Duration;
    pub async fn measure<F, T>(&self, operation: F) -> Result<(T, Duration)>;
    
    // Tracing integration
    pub fn assert_logged(&self, message: &str) -> Result<()>;
    pub fn captured_logs(&self) -> Vec<String>;
}
```

### Fixtures

Reusable test data with proper lifecycle management:

```rust
// Standard fixtures
let session = fixtures::standard_user_session(&ctx).await?;
let dataset = fixtures::performance_dataset(&ctx).await?;
let errors = fixtures::error_scenarios(&ctx).await?;

// Parameterized fixtures
let large_dataset = fixtures::performance_dataset_with_size(&ctx, 10_000).await?;
let custom_session = fixtures::user_session_with_params(&ctx, 100, 10).await?;

// Specialized fixtures
let concurrency = fixtures::concurrency_test_fixture(&ctx, 5, 20).await?;
let validation = fixtures::schema_validation_fixture(&ctx).await?;
```

### Rich Assertions

Context-aware assertions with clear error messages:

```rust
ctx.assert("user session validation")
    .not_empty(&session.event_ids)?           // Non-empty check
    .has_size(&session.event_ids, 50)?        // Exact size
    .some(&session.checkpoint_id)?             // Option has value
    .that(                                     // Custom condition
        session.event_ids.len() >= 10,
        "should have at least 10 events"
    )?;
```

## Testing Patterns

### Basic Event Testing

```rust
#[sinex_test]
async fn test_filesystem_event_processing(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Create filesystem event
    let event = ctx.create_test_event(
        "fs-watcher",
        "file.created",
        json!({
            "path": "/data/important.csv",
            "size": 2048,
            "permissions": "0644"
        })
    ).await?;
    
    // Verify event was stored
    let stored_events = ctx.pool.events()
        .get_by_source(&EventSource::from("fs-watcher"), Some(10), None)
        .await?;
    
    ctx.assert("filesystem event storage")
        .eq(&stored_events.len(), &1)?
        .that(
            stored_events[0].payload["path"] == json!("/data/important.csv"),
            "path should be preserved"
        )?;
    
    Ok(())
}
```

### Concurrent Operations

```rust
#[sinex_test]
async fn test_concurrent_event_insertion(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let barrier = ctx.timing().barrier(3);
    let mut handles = vec![];
    
    for worker_id in 0..3 {
        let pool = ctx.pool.clone();
        let barrier = barrier.clone();
        
        let handle = tokio::spawn(async move {
            // Wait for all workers to be ready
            barrier.wait().await;
            
            // Insert events concurrently
            for i in 0..10 {
                let event = RawEvent::test_event(
                    EventSource::from(format!("worker-{}", worker_id)),
                    EventType::from("concurrent.test"),
                    json!({"iteration": i})
                );
                pool.events().insert(event).await?;
            }
            
            Result::<(), SinexError>::Ok(())
        });
        handles.push(handle);
    }
    
    // Wait for all workers
    for handle in handles {
        handle.await.map_err(|e| SinexError::service(format!("Task failed: {}", e)))??;
    }
    
    // Verify all events were stored
    let total_count = ctx.pool.events().count_all().await?;
    ctx.assert("concurrent insertion").eq(&total_count, &30)?;
    
    Ok(())
}
```

### Integration Testing

```rust
#[sinex_test]
async fn test_full_event_pipeline(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Load realistic test scenario
    let session = fixtures::standard_user_session(&ctx).await?;
    
    // Verify pipeline components
    assert!(!session.event_ids.is_empty());
    assert!(session.checkpoint_id.is_some());
    
    // Test query operations
    let recent_events = ctx.pool.events().get_recent(10).await?;
    let checkpoint = ctx.pool.checkpoints()
        .get_by_id(&session.checkpoint_id.unwrap())
        .await?;
    
    ctx.assert("pipeline integration")
        .not_empty(&recent_events)?
        .some(&checkpoint)?;
    
    Ok(())
}
```

## Error Testing

```rust
use sinex_test_utils::error_testing::*;

#[sinex_test]
async fn test_validation_errors(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let tester = ValidationTester::new(&ctx);
    
    // Test invalid event source
    let result = tester.test_validation_case("empty_source", || async {
        ctx.create_test_event("", "valid.type", json!({})).await
    }).await;
    
    assert!(result.is_err());
    ctx.assert("validation error")
        .error_contains(&result, "source")?;
    
    Ok(())
}
```

## Performance Testing

```rust
#[cfg(all(test, feature = "bench"))]
mod benchmarks {
    use super::*;
    
    #[sinex_bench(args = [100, 1000, 10000])]
    async fn bench_batch_insert(count: usize) -> color_eyre::eyre::Result<()> {
        let events = generate_test_events(count);
        let (_, duration) = measure_operation(|| async {
            insert_events_batch(&events).await
        }).await?;
        
        println!("Inserted {} events in {:?}", count, duration);
        Ok(())
    }
}
```

## Configuration

The test utilities can be configured via environment variables:

```bash
# Database settings (usually handled by nix develop)
export DATABASE_URL="postgresql://sinex:password@localhost/sinex_test_template"

# Test-specific settings
export SINEX_TEST_TIMEOUT=30        # Default test timeout in seconds
export SINEX_TEST_DB_POOL_SIZE=64   # Number of test databases in pool
export SINEX_TEST_CLEANUP=true      # Auto-cleanup after tests
```

## Feature Flags

- `bench` - Enable benchmarking support
- `proptest` - Property-based testing integration
- `tracing` - Enhanced tracing and logging

## Documentation

- **[doc/overview.md](./doc/overview.md)** - API reference, fixtures, timing utilities, assertions
- **[API Documentation](https://docs.rs/sinex-test-utils)** - Generated API docs
- **[Examples](./tests/)** - Test examples and integration tests

## Installation

Add to your `Cargo.toml`:

```toml
[dev-dependencies]
sinex-test-utils = { path = "../sinex-test-utils" }

# For property testing
proptest = "1.0"

# For async testing  
tokio-test = "0.4"
```

## Best Practices

1. **Always use `#[sinex_test]`** instead of `#[test]` for database-dependent tests
2. **Use production APIs directly** rather than creating test-specific wrappers
3. **Leverage fixtures** for expensive setup rather than recreating data
4. **Use rich assertions** with context for clear failure messages
5. **Test error conditions** explicitly using the error testing utilities
6. **Measure performance** for operations that may impact production

## Common Pitfalls

- ❌ Using `#[test]` instead of `#[sinex_test]` for database tests
- ❌ Creating wrapper APIs instead of testing production code directly  
- ❌ Not using fixtures for expensive test data
- ❌ Using `unwrap()` or `panic!` in test code
- ❌ Not testing error conditions and edge cases

## Contributing

See the main [Sinex documentation](../../README.md) for contribution guidelines.

## License
