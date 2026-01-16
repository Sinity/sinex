# Sinex Test Utilities

> The workspace-wide testing handbook lives at [`TESTING.md`](../../../TESTING.md).
> Use it for quick-start commands, suite layout, and property-testing guidance.
> This README focuses on the crate itself; API-level details are in
> `docs/overview.md` and `docs/testing_quality_overview.md`.

A comprehensive testing framework for the Sinex event-driven data capture system, providing database isolation, dataset seeding, and robust testing patterns.

## Quick Start

```rust
use sinex_test_utils::prelude::*;

#[sinex_test]
async fn test_event_creation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;

    // Create test event using the real pipeline
    let event = ctx
        .publish_json_event(
            "fs-watcher",
            "file.created",
            json!({"path": "/test.txt", "size": 1024})
        )
        .await?;
    
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

- **Database Isolation**: Each test gets its own isolated database from a pooled set of databases
- **Production API Usage**: Tests use real production APIs, not mocks or wrappers
- **Dataset Seeding**: Repeatable seed helpers for common testing scenarios
- **Comprehensive Assertions**: Context-aware assertions with clear error messages
- **Timing & Synchronization**: Tools for testing concurrent operations
- **Property Testing**: Integration with proptest for robust edge case testing
- **Tracing Integration**: Automatic log capture and verification
- **Performance Testing**: Built-in benchmarking and performance measurement

## Logging

The harness prints a compact progress line (`🔄` while running, then `✅/❌`
with elapsed time) for every test. Use `ctx.captured_logs()` inside a test if
you need to assert on emitted tracing lines. When a test fails the harness
records a JSON artifact under `target/test-artifacts/` (override via
`SINEX_TEST_FAIL_DIR`) that includes the error, current pool statistics, and
captured logs when a `TestContext` is present.

## Architecture

### Database Pool Strategy

The test utilities use a pooled-database strategy that provides true isolation while keeping setup
costs amortized across the suite:

```
┌─────────────┐ ┌─────────────┐ ┌─────────────┐     ┌─────────────┐
│   Test 1    │ │   Test 2    │ │   Test 3    │ ... │   Test N    │
│  Database   │ │  Database   │ │  Database   │     │  Database   │
│   Pool 1    │ │   Pool 2    │ │   Pool 3    │     │   Pool N    │
└─────────────┘ └─────────────┘ └─────────────┘     └─────────────┘
```

- Each test acquires an exclusive database via PostgreSQL advisory locks
- Parallel test execution with no interference
- Databases are reset/cleaned on acquisition (so each test starts from a known-clean state)
- Pool recycling for optimal performance

#### Pool sizing

- Pool size defaults to 2× Nextest test threads (num-cpus by default), with a minimum of `64`.
- The pool shrinks automatically if PostgreSQL `max_connections` would be exceeded.
- Per-test DB pools cap at 4 connections; the admin pool caps at 8.

Under Nextest, pool DBs are lazily provisioned (created from a shared template DB on-demand). Use
`cargo xtask test --prime` (or `cargo run -p sinex-test-utils --bin db_prime`) to pre-provision
the pool before running the suite.

### Production API Integration

Tests work directly with production code:

```rust
// ✅ Direct production API usage
let event = Event::new(FileCreatedPayload { /* ... */ }).into();
ctx.pool.events().insert(event).await?;

// ✅ Repository access
let events = ctx.pool.events().get_by_source(&source, limit, offset).await?;

// ✅ Test utilities for convenience
let event = ctx.publish_json_event("source", "type", json!({})).await?;
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
    // Pipeline event helpers
    pub async fn publish_json_event(&self, source: &str, event_type: &str, payload: JsonValue) -> TestResult<Event<JsonValue>>;
    pub async fn publish_test_event(&self, event: &Event<JsonValue>) -> TestResult<Ulid>;
    
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

### Dataset Seeding

Reusable dataset seeds for repeatable setups:

```rust
use sinex_test_utils::dataset_seeds::{seed_events_via_pipeline, EventSpec, SeedClock};

let clock = SeedClock::default();
let specs = vec![
    EventSpec::new("fs-watcher", "file.created", json!({"path": "/tmp/a"})),
    EventSpec::new("terminal", "command.executed", json!({"command": "ls"})),
];
let ctx = ctx.with_nats().await?;
let pipeline = ctx.pipeline().await?;
let ids = seed_events_via_pipeline(&pipeline, &clock, &specs).await?;
assert_eq!(ids.len(), 2);
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
async fn test_filesystem_event_processing(ctx: TestContext) -> TestResult<()> {
    // Create filesystem event
    let event = ctx.publish_json_event(
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
async fn test_concurrent_event_insertion(ctx: TestContext) -> TestResult<()> {
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
async fn test_full_event_pipeline(ctx: TestContext) -> TestResult<()> {
    use sinex_test_utils::dataset_seeds::{seed_events_via_pipeline, EventSpec, SeedClock};

    let clock = SeedClock::default();
    let specs = vec![
        EventSpec::new("fs-watcher", "file.created", json!({"path": "/tmp/a"})),
        EventSpec::new("terminal", "command.executed", json!({"command": "ls"})),
    ];
    let ctx = ctx.with_nats().await?;
    let pipeline = ctx.pipeline().await?;
    let ids = seed_events_via_pipeline(&pipeline, &clock, &specs).await?;
    assert!(!ids.is_empty());

    let recent_events = ctx.pool.events().get_recent(10).await?;
    ctx.assert("pipeline integration").not_empty(&recent_events)?;

    Ok(())
}
```

## Performance Testing

```rust
#[cfg(all(test, feature = "bench"))]
mod benchmarks {
    use super::*;
    
    #[sinex_bench(args = [100, 1000, 10000])]
    async fn bench_batch_insert(count: usize) -> TestResult<()> {
        let events = generate_test_events(count);
        let (_, duration) = measure_operation(|| async {
            seed_events_via_scope(ctx.pipeline().await?, &SeedClock::default(), &events).await
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
export DATABASE_URL="postgresql://sinex:***REDACTED_PASSWORD***@localhost/sinex_test_template"

# Test harness overrides (optional)
export SINEX_PROPTEST_CASES=256              # Override proptest cases
export SINEX_PROPTEST_DIR=target/proptest-regressions
export SINEX_TEST_FAIL_DIR=/tmp/sinex-failures
```

## Feature Flags

- `bench` - Enable benchmarking support
- `proptest` - Property-based testing integration
- `tracing` - Enhanced tracing and logging

## Documentation

- **[docs/overview.md](./docs/overview.md)** - API reference, dataset seeding, timing utilities, assertions
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
3. **Leverage dataset seeding helpers** for repeatable setup rather than ad hoc inserts
4. **Use rich assertions** with context for clear failure messages
5. **Test error conditions** explicitly using production error types and assertions
6. **Measure performance** for operations that may impact production

## Common Pitfalls

- ❌ Using `#[test]` instead of `#[sinex_test]` for database tests
- ❌ Creating wrapper APIs instead of testing production code directly  
- ❌ Not using dataset seeding helpers for expensive test data
- ❌ Using `unwrap()` or `panic!` in test code
- ❌ Not testing error conditions and edge cases

## Contributing

See the main [Sinex documentation](../../../README.md) for contribution guidelines.

## License
