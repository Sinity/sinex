# Sinex Testing Guide

This guide covers testing patterns, best practices, and advanced techniques for the Sinex event system using the sinex-test-utils crate.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Core Concepts](#core-concepts)
3. [Testing Patterns](#testing-patterns)
4. [Advanced Techniques](#advanced-techniques)
5. [Performance Testing](#performance-testing)
6. [Mock Usage](#mock-usage)
7. [Troubleshooting](#troubleshooting)

## Quick Start

### Basic Test Structure

```rust
use sinex_test_utils::prelude::*;

#[sinex_test]
async fn test_basic_event_flow(ctx: TestContext) -> TestResult<()> {
    // Create an event
    let event = ctx.event()
        .source("my-service")
        .type_("action.performed")
        .field("user_id", 123)
        .insert()
        .await?;
    
    // Query it back
    let events = ctx.events()
        .by_source("my-service")
        .fetch()
        .await?;
    
    // Assert
    ctx.assert("event created")
        .eq(&events.len(), &1)?
        .event_eq(&events[0], &event)?;
    
    Ok(())
}
```

### Key Rules

1. **Always use `#[sinex_test]`** - Never use `#[tokio::test]` directly
2. **Accept `TestContext` parameter** - All test functionality flows through it
3. **Return `TestResult<()>`** - Consistent error handling
4. **Use fluent builders** - Chainable methods for readability

## Core Concepts

### TestContext - Your Testing Swiss Army Knife

TestContext provides everything you need:

```rust
ctx.event()       // Create events
ctx.events()      // Query events
ctx.assert()      // Rich assertions
ctx.timing()      // Synchronization
ctx.mocks()       // Mock objects
ctx.scenarios()   // Test fixtures
```

### Event Creation Patterns

#### Domain-Specific Builders

```rust
// Filesystem events
ctx.event()
    .filesystem()
    .path("/etc/config.yml")
    .size(2048)
    .permissions(0o600)
    .modified()
    .insert()
    .await?;

// Terminal commands
ctx.event()
    .terminal()
    .command("docker-compose up -d")
    .working_dir("/app")
    .duration_ms(3500)
    .success()
    .insert()
    .await?;

// System events
ctx.event()
    .system()
    .service("postgresql")
    .started()
    .insert()
    .await?;
```

#### Custom Events

```rust
// Build fields incrementally
let event = ctx.event()
    .source("analytics")
    .type_("metric.recorded")
    .field("metric_name", "api_latency")
    .field("value", 42.5)
    .field("tags", json!(["production", "api-v2"]))
    .insert()
    .await?;

// Batch field insertion
ctx.event()
    .source("monitoring")
    .type_("alert.triggered")
    .fields(vec![
        ("severity", json!("critical")),
        ("service", json!("payment-gateway")),
        ("threshold", json!(99.5)),
        ("current_value", json!(100.0))
    ])
    .insert()
    .await?;
```

### Query Patterns

#### Basic Queries

```rust
// Get all events
let all = ctx.events().fetch().await?;

// Limit results
let recent = ctx.events().limit(10).fetch().await?;

// Filter by source
let fs_events = ctx.events().by_source("fs").fetch().await?;

// Filter by type
let errors = ctx.events().by_type("error.occurred").fetch().await?;

// Get single event
let event = ctx.events().by_id(event_id).fetch_one().await?;
```

#### Complex Queries

```rust
// Multiple filters
let terminal_failures = ctx.events()
    .by_source("shell-kitty")
    .by_type("shell.command.failed")
    .limit(20)
    .fetch()
    .await?;

// Count operations
let total = ctx.events().count().await?;
let fs_count = ctx.events().by_source("fs").count().await?;
```

### Assertion Patterns

#### Contextual Assertions

```rust
// Chain multiple assertions
ctx.assert("user validation")
    .eq(&user.status, &"active")?
    .that(user.age >= 18, "must be adult")?
    .not_empty(&user.roles)?
    .has_size(&user.permissions, 3)?;

// Event-specific assertions
ctx.assert("event comparison")
    .event_eq(&actual, &expected)?;

// Error assertions
ctx.assert("error handling")
    .error_contains(&result, "permission denied")?;

// Async operation timing
ctx.assert("performance")
    .completes_within(
        async { expensive_operation().await },
        Duration::from_secs(2),
        "operation should be fast"
    ).await?;
```

## Testing Patterns

### Data-Driven Tests

Use `parameterized!` for testing multiple cases with the same logic:

```rust
#[sinex_test]
async fn test_file_operations(ctx: TestContext) -> TestResult<()> {
    parameterized!([
        // (name, path, size, expected_result)
        ("empty file", "/tmp/empty.txt", 0, true),
        ("small file", "/tmp/small.txt", 1024, true),
        ("large file", "/tmp/large.bin", 10485760, true),
        ("invalid path", "", 0, false),
    ], |(name, path, size, should_succeed)| {
        let result = ctx.event()
            .filesystem()
            .path(path)
            .size(size)
            .created()
            .insert()
            .await;
        
        if should_succeed {
            assert!(result.is_ok(), "{} should succeed", name);
        } else {
            assert!(result.is_err(), "{} should fail", name);
        }
        Ok(())
    });
    Ok(())
}
```

### Property Testing

For pure functions, use proptest within `#[sinex_test]`:

```rust
#[sinex_test]
async fn test_event_builder_properties(ctx: TestContext) -> TestResult<()> {
    use proptest::prelude::*;
    
    // Test with database operations - use reasonable iterations
    parameterized!([
        ("alphanum", "[a-zA-Z0-9]{5,20}"),
        ("with-dash", "[a-zA-Z][a-zA-Z0-9-]{4,19}"),
        ("unicode", ".*{1,50}"),
    ], |(name, pattern)| {
        // Limited iterations for database tests
        for _ in 0..10 {
            let source = thread_rng().sample_iter(&Alphanumeric)
                .take(10)
                .map(char::from)
                .collect::<String>();
            
            let event = ctx.event()
                .source(&source)
                .type_("test.property")
                .insert()
                .await?;
            
            assert_eq!(event.source, source);
        }
        Ok(())
    });
    
    Ok(())
}
```

### Testing Event Flows

```rust
#[sinex_test]
async fn test_event_processing_pipeline(ctx: TestContext) -> TestResult<()> {
    // 1. Create source event
    let source_event = ctx.event()
        .terminal()
        .command("git commit -m 'Initial commit'")
        .success()
        .insert()
        .await?;
    
    // 2. Simulate processing (would be done by automaton)
    let processed = ctx.event()
        .source("git-analyzer")
        .type_("git.commit.analyzed")
        .field("source_event_id", source_event.id)
        .field("commit_type", "feat")
        .field("scope", "core")
        .insert()
        .await?;
    
    // 3. Verify linkage
    ctx.assert("event linkage")
        .eq(&processed.payload["source_event_id"], &json!(source_event.id))?;
    
    Ok(())
}
```

### Testing with Fixtures

```rust
#[sinex_test]
async fn test_with_user_session(ctx: TestContext) -> TestResult<()> {
    // Get pre-built user session
    let session = ctx.scenarios().user_session().await?;
    
    // Session contains filesystem, terminal, and clipboard events
    let fs_events = ctx.events()
        .by_source("fs")
        .fetch()
        .await?;
    
    assert!(fs_events.len() >= 5, "Session should have filesystem activity");
    
    // Access specific fixture data directly
    assert!(!session.event_ids.is_empty());
    assert!(session.checkpoint_id.is_some());
    
    Ok(())
}
```

## Advanced Techniques

### Schema Validation

```rust
#[sinex_test]
async fn test_with_schema_validation(ctx: TestContext) -> TestResult<()> {
    // Define schema
    let schema = json!({
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "pattern": "^/[a-zA-Z0-9_/.-]+$"
            },
            "size": {
                "type": "integer",
                "minimum": 0,
                "maximum": 1073741824  // 1GB
            },
            "checksum": {
                "type": "string",
                "pattern": "^[a-f0-9]{64}$"
            }
        },
        "required": ["path", "size"],
        "additionalProperties": false
    });
    
    // Register schema
    let schema_id = ctx.schema()
        .register("fs", "file.validated", schema)
        .await?;
    
    // Create valid event
    let valid = ctx.validated_event(schema_id)
        .field("path", "/data/report.pdf")
        .field("size", 2048576)
        .field("checksum", "a".repeat(64))
        .insert()
        .await?;
    
    // Invalid event will fail
    let invalid = ctx.validated_event(schema_id)
        .field("path", "not/absolute")  // Missing leading /
        .field("size", -1)              // Negative size
        .insert()
        .await;
    
    assert!(invalid.is_err());
    
    Ok(())
}
```

### Timing and Synchronization

```rust
#[sinex_test]
async fn test_concurrent_event_generation(ctx: TestContext) -> TestResult<()> {
    use tokio::task::JoinSet;
    
    // Create barrier for synchronized start
    let barrier = Arc::new(ctx.timing().barrier(5));
    let mut tasks = JoinSet::new();
    
    // Spawn 5 concurrent tasks
    for i in 0..5 {
        let barrier_clone = barrier.clone();
        let ctx_clone = ctx.clone();  // TestContext is Clone
        
        tasks.spawn(async move {
            // Wait for all tasks to be ready
            barrier_clone.wait(Duration::from_secs(5)).await?;
            
            // Generate events
            for j in 0..10 {
                ctx_clone.event()
                    .source(format!("worker-{}", i))
                    .type_("work.completed")
                    .field("task_id", j)
                    .insert()
                    .await?;
            }
            
            Ok::<_, CoreError>(i)
        });
    }
    
    // Wait for all tasks
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    
    // Verify all events were created
    ctx.wait_for_event_count(50).await?;
    
    let total = ctx.events().count().await?;
    assert_eq!(total, 50);
    
    Ok(())
}
```

### Custom Assertions

```rust
#[sinex_test]
async fn test_event_ordering(ctx: TestContext) -> TestResult<()> {
    // Create events with timestamps
    let mut events = vec![];
    for i in 0..5 {
        let event = ctx.event()
            .source("ordered")
            .type_("sequence")
            .field("index", i)
            .insert()
            .await?;
        events.push(event);
        
        // Small delay to ensure ordering
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    
    // Custom assertion for ordering
    for window in events.windows(2) {
        let (first, second) = (&window[0], &window[1]);
        
        ctx.assert("event ordering")
            .that(
                first.ts_received < second.ts_received,
                &format!("Event {} should be before {}", 
                    first.payload["index"], 
                    second.payload["index"]
                )
            )?;
    }
    
    Ok(())
}
```

## Performance Testing

### Load Testing with Large Datasets

```rust
#[sinex_test(timeout = 60)]
async fn test_high_volume_ingestion(ctx: TestContext) -> TestResult<()> {
    // Use performance fixtures
    let dataset = ctx.performance()
        .large_dataset_with(10_000)
        .await?;
    
    // Measure ingestion rate
    let (_, duration) = ctx.measure(async {
        // Batch insert events
        for batch in dataset.events.chunks(100) {
            ctx.insert_events(batch).await?;
        }
        Ok::<_, CoreError>(())
    }).await?;
    
    let events_per_second = 10_000.0 / duration.as_secs_f64();
    
    ctx.assert("performance")
        .that(
            events_per_second > 1000.0,
            &format!("Should ingest >1000 events/sec, got {:.0}", events_per_second)
        )?;
    
    Ok(())
}
```

### Stress Testing

```rust
#[sinex_test(timeout = 120)]
async fn test_concurrent_stress(ctx: TestContext) -> TestResult<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    
    let success_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));
    
    // Run concurrent operations
    let results = ctx.run_concurrent(20, |ctx, worker_id| {
        let success = success_count.clone();
        let errors = error_count.clone();
        
        async move {
            for i in 0..100 {
                match ctx.event()
                    .source(format!("stress-{}", worker_id))
                    .type_("load.test")
                    .field("iteration", i)
                    .insert()
                    .await
                {
                    Ok(_) => success.fetch_add(1, Ordering::Relaxed),
                    Err(_) => errors.fetch_add(1, Ordering::Relaxed),
                };
            }
            Ok(())
        }
    }).await?;
    
    let total_success = success_count.load(Ordering::Relaxed);
    let total_errors = error_count.load(Ordering::Relaxed);
    
    println!("Stress test: {} successful, {} errors", total_success, total_errors);
    
    ctx.assert("stress test")
        .that(total_success > 1900, "Should have >95% success rate")?
        .that(total_errors < 100, "Should have <5% error rate")?;
    
    Ok(())
}
```

## Mock Usage

### Filesystem Mocking

```rust
#[sinex_test]
async fn test_filesystem_operations(ctx: TestContext) -> TestResult<()> {
    let fs = ctx.mocks().filesystem();
    
    // Create mock files
    fs.create_file("/app/config.json", br#"{"version": "1.0"}"#).await?;
    fs.create_directory("/app/logs").await?;
    
    // Simulate file operations
    assert!(fs.exists("/app/config.json").await);
    
    let content = fs.read_file("/app/config.json").await?;
    assert_eq!(content, br#"{"version": "1.0"}"#);
    
    // Test error scenarios
    fs.inject_error("/app/secure.key", std::io::ErrorKind::PermissionDenied);
    
    let result = fs.read_file("/app/secure.key").await;
    assert!(result.is_err());
    
    Ok(())
}
```

### Database Mocking with Failure Injection

```rust
#[sinex_test]
async fn test_database_resilience(ctx: TestContext) -> TestResult<()> {
    let db = ctx.mocks()
        .database()
        .with_failure_rate(0.2)  // 20% failure rate
        .with_latency(Duration::from_millis(50));
    
    let mut successes = 0;
    let mut failures = 0;
    
    // Run operations with intermittent failures
    for i in 0..100 {
        match db.execute("INSERT INTO events ...").await {
            Ok(_) => successes += 1,
            Err(_) => failures += 1,
        }
    }
    
    // Should have roughly 80% success rate
    ctx.assert("failure injection")
        .that(failures > 10, "Should have some failures")?
        .that(failures < 30, "Failure rate should be ~20%")?;
    
    Ok(())
}
```

### Network Mocking

```rust
#[sinex_test]
async fn test_network_behavior(ctx: TestContext) -> TestResult<()> {
    let net = ctx.mocks().network();
    
    // Configure latency and packet loss
    net.configure()
        .latency(Duration::from_millis(100))
        .packet_loss(0.05)  // 5% loss
        .bandwidth_limit(1_000_000);  // 1MB/s
    
    // Create mock connection
    let conn = net.connect("api.example.com", 443).await?;
    
    // Test timeout behavior
    let result = tokio::time::timeout(
        Duration::from_millis(50),
        conn.send(b"GET / HTTP/1.1\r\n\r\n")
    ).await;
    
    assert!(result.is_err(), "Should timeout with 100ms latency");
    
    Ok(())
}
```

## Troubleshooting

### Common Issues

#### 1. Database Connection Timeouts

**Symptom**: Tests fail with "connection timeout" errors

**Solution**:
```rust
// Increase timeout for slow systems
#[sinex_test(timeout = 60)]
async fn test_needing_more_time(ctx: TestContext) -> TestResult<()> {
    // Test code
    Ok(())
}
```

#### 2. Flaky Tests

**Symptom**: Tests pass/fail inconsistently

**Solution**:
```rust
// Use proper synchronization
ctx.timing().wait_for_event_count(expected).await?;

// Or use explicit delays for external systems
ctx.timing().delay(Duration::from_millis(100)).await;
```

#### 3. Foreign Key Violations

**Symptom**: Cleanup fails with FK constraint errors

**Solution**: Ensure proper event relationships:
```rust
// Create parent before child
let parent = ctx.event().source("parent").insert().await?;
let child = ctx.event()
    .source("child")
    .field("parent_id", parent.id)
    .insert()
    .await?;
```

### Debugging Techniques

#### Enable Verbose Output

```bash
# Run single test with full output
cargo test test_name -- --nocapture

# Enable debug logging
RUST_LOG=sinex_test_utils=debug cargo test
```

#### Inspect Database State

```rust
#[sinex_test]
async fn test_with_inspection(ctx: TestContext) -> TestResult<()> {
    // ... test operations ...
    
    // Dump current state for debugging
    if std::env::var("DEBUG_TEST").is_ok() {
        let events = ctx.events().fetch().await?;
        for event in &events {
            eprintln!("Event: {} {} - {:?}", 
                event.source, 
                event.event_type, 
                event.payload
            );
        }
    }
    
    Ok(())
}
```

#### Check Pool Health

```rust
#[test]
fn check_database_pool_health() {
    let stats = sinex_test_utils::database_pool::get_pool_stats();
    
    println!("Pool Statistics:");
    println!("  Total acquisitions: {}", stats.total_acquisitions);
    println!("  Average wait time: {}ms", stats.average_wait_time_ms);
    println!("  Cleanup failures: {}", stats.cleanup_failures);
    println!("  Template recreations: {}", stats.template_recreations);
}
```

## Best Practices

1. **Use Domain Builders**: Prefer `ctx.event().filesystem()` over manual construction
2. **Leverage Fixtures**: Reuse common scenarios via `ctx.scenarios()`
3. **Assert with Context**: Use `ctx.assert("description")` for better error messages
4. **Clean Test Names**: Use descriptive names that explain what's being tested
5. **Appropriate Timeouts**: Set realistic timeouts based on test type
6. **Batch Operations**: Use `insert_batch()` for multiple similar events
7. **Mock External Dependencies**: Use mocks for filesystem, network, etc.
8. **Property Test Wisely**: Limit iterations for database operations

## Conclusion

The sinex-test-utils framework provides a comprehensive, performant, and ergonomic testing experience. By following these patterns and leveraging the provided utilities, you can write reliable, maintainable tests for the Sinex event system.

For more examples, see the test files throughout the Sinex codebase, particularly in the `test/` directories.