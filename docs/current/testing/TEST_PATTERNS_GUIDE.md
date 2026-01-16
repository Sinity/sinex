# Test Patterns and Best Practices - Quick Start Guide

This guide helps you quickly find and apply the reusable test patterns documented in `TEST_PATTERNS.md`.

## Quick Navigation

### By Test Type
- **Unit Tests**: See [Section 9: Template Tests - Complete Unit Test](#template-tests)
- **Integration Tests**: See [Section 9: Template Tests - Complete Integration Test](#template-tests)
- **Property Tests**: See [Section 9: Template Tests - Complete Property Test](#template-tests)
- **Error Tests**: See [Section 5.2: Error Matching Patterns](#error-matching-patterns)

### By Concern
- **Database Isolation**: See [Section 1: Database Test Patterns](#database-test-patterns)
- **Async Operations**: See [Section 2: Async Test Patterns](#async-test-patterns)
- **Assertions**: See [Section 5: Assertion Patterns](#assertion-patterns)
- **Timing & Synchronization**: See [Section 11: Timing & Synchronization Patterns](#timing--synchronization-patterns)
- **Performance**: See [Section 12: Performance Optimization](#performance-optimization)

## Core Infrastructure

### The #[sinex_test] Macro
```rust
#[sinex_test]
async fn my_test(ctx: TestContext) -> Result<()> {
    // Automatic TestContext creation
    // Automatic cleanup
    Ok(())
}
```

**What it does**:
- Creates isolated test database
- Provides TestContext with pool access
- Sets 30s timeout (configurable)
- Automatic cleanup on drop
- Integrates with rstest and proptest

**Configuration**:
```rust
#[sinex_test(timeout = 60)]  // Custom timeout
async fn slow_test(ctx: TestContext) -> Result<()> { }

#[sinex_test(trace = true)]  // Enable tracing
async fn traced_test(ctx: TestContext) -> Result<()> { }
```

### TestContext - Your Main Tool
```rust
// Enable NATS / ingestd access for pipeline-first events
let ctx = ctx.with_shared_nats().await?;

// Create events via the real pipeline
let event = ctx.publish_json_event("source", "type", json!({})).await?;

// Query events directly
let events = ctx.pool.events()
    .get_by_source(&EventSource::from("source"), Some(10), None)
    .await?;

// Make assertions
ctx.assert("description")
    .eq(&a, &b)?
    .that(condition, "message")?
    .not_empty(&vec)?;

// Timing utilities
ctx.timing().wait_for_event_count(5).await?;

// Elapsed time tracking
let elapsed = ctx.elapsed();
```

### Pipeline Quick Start (L2-L4)

Use PipelineScope for pipeline boundaries (L2/L3/L4) so tests exercise NATS → ingestd → DB with
namespace isolation. L1 tests stay on `TestContext`, and L0 tests require no harness.

```rust
#[sinex_test]
async fn pipeline_flow(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    let scope = ctx.pipeline_scope().await?;

    scope
        .publish("fs-watcher", "file.created", json!({"path": "/tmp/demo"}))
        .await?;
    scope.wait_for_event_count(1).await?;

    let events = ctx
        .pool
        .events()
        .get_by_source(&EventSource::from("fs-watcher"), Some(10), None)
        .await?;

    ctx.assert("pipeline persisted").not_empty(&events)?;
    Ok(())
}
```

When you must provision streams or consumers manually, derive names from
`ctx.pipeline_namespace()` instead of building subjects/streams directly.

### Production Invariants

Use these guardrails when writing assertions:

- **Anchor uniqueness (hard)** — every material anchor belongs to exactly one live event chain.
- **Provenance XOR (hard)** — events must have either material or synthesis provenance, never both.
- **Single writer (hard)** — only ingestd writes to `core.events`; nodes publish to JetStream.
- **Temporal ledger completeness (hard)** — ledger rows exist for every material offset persisted.
- **Idempotency (best-effort)** — duplicate submissions may replay but must not corrupt state; do
  not fabricate data inside tests to hide regressions.

Call out explicitly when you test best-effort behavior so future changes keep the same semantics.

## 5-Minute Test Template

Start with this structure for any test:

```rust
#[sinex_test]
async fn test_my_feature(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().await?;

    // ARRANGE: Set up test data via pipeline
    ctx.publish_json_event(
        "my-source",
        "my.event",
        json!({"key": "value"}),
    ).await?;

    // ACT: Perform the operation
    let result = ctx.pool.events()
        .get_by_source(&EventSource::from("my-source"), Some(10), None)
        .await?;

    // ASSERT: Verify results
    ctx.assert("test description")
        .not_empty(&result)?
        .has_size(&result, 1)?;

    assert_eq!(result[0].payload["key"], json!("value"));
    
    Ok(())
}
```

## Pattern Lookup by Scenario

### "I need to test event creation"
See: [Section 1.3: Fixture Insertion Patterns](#fixture-insertion-patterns)

```rust
let event = ctx.publish_json_event(
    "fs-watcher",
    "file.created",
    json!({"path": "/test.txt"}),
).await?;
```

### "I need to test error cases"
See: [Section 5.2: Error Matching Patterns](#error-matching-patterns)

```rust
let result = some_operation().await;
result
    .assert_contains_error("validation")?
    .assert_fails()?;
```

### "I need to test with different inputs"
See: [Section 3: Property Test Patterns](#property-test-patterns)

```rust
// Define filesystem_event_strategy() locally (see Section 3.1).

#[sinex_prop(cases = 20)]
async fn test_inputs(
    ctx: &TestContext,
    #[strategy(filesystem_event_strategy())] event: (String, String, Value),
) -> TestResult<()> {
    let (source, ty, payload) = event;
    ctx.publish_json_event(&source, &ty, payload).await?;
    Ok(())
}
```

### "I need to test concurrent operations"
See: [Section 2.2: Concurrent Test Execution](#concurrent-test-execution)

```rust
#[sinex_test]
async fn test_concurrent(ctx: TestContext) -> Result<()> {
    let barrier = Arc::new(tokio::sync::Barrier::new(5));
    let mut handles = vec![];
    
    for i in 0..5 {
        let barrier_clone = barrier.clone();
        let handle = tokio::spawn(async move {
            let ctx = TestContext::with_name(&format!("task_{i}")).await?;
            barrier_clone.wait().await;
            // Do work
            Ok::<(), SinexError>(())
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.await??;
    }
    Ok(())
}
```

### "I need to verify event ordering"
See: [Section 5.4: Temporal Assertions](#temporal-assertions-event-ordering)

```rust
let first = ctx.publish_json_event("src", "type1", json!({})).await?;
let second = ctx.publish_json_event("src", "type2", json!({})).await?;

ctx.assert("ordering")
    .that(
        first.id.as_ref().map(|id| id.as_ulid().timestamp())
            < second.id.as_ref().map(|id| id.as_ulid().timestamp()),
        "events should be ordered",
    )?;
```

### "I need parameterized tests"
See: [Section 2.1: Test Macro](#test-macro-with-automatic-context-creation)

```rust
#[sinex_test]
#[case("source1", "type1")]
#[case("source2", "type2")]
async fn test_params(
    ctx: TestContext,
    #[case] source: &str,
    #[case] event_type: &str,
) -> Result<()> {
    // Test with each case
    Ok(())
}
```

### "I need reusable inputs"

```rust
#[fixture]
pub fn test_sources() -> Vec<&'static str> {
    vec!["source1", "source2", "source3"]
}

#[rstest]
async fn test_with_fixture(
    #[from(test_sources)] sources: Vec<&str>,
    ctx: TestContext,
) -> Result<()> {
    for source in sources {
        ctx.publish_json_event(source, "type", json!({})).await?;
    }
    Ok(())
}
```

## Common Mistakes to Avoid

| Mistake | Problem | Solution |
|---------|---------|----------|
| `tokio::time::sleep(Duration::from_millis(100))` | Flaky tests, no error handling | Use `ctx.timing().wait_for_event_count()` |
| Mocking Event types | Bypasses real validation | Use `Event::builder`/`Event::new`; `Event::dynamic`/`test_event` only for dynamic JSON |
| Assuming event order | Tests fail randomly | Use ULID timestamp comparisons |
| Not testing errors | Missing coverage | Assert on error types/messages explicitly |
| Custom database cleanup | Interferes with pool | Trust TestContext drop |

## Performance Tips

1. **Pool sizing**: Keep Nextest test threads aligned with Postgres capacity (pool defaults to
   2× Nextest test threads, minimum 64).
2. **Template cache**: Delete `target/sinex-test-utils/template_stamp.json` to force rebuild
3. **Batch operations**: Insert events in groups when possible
4. **Avoid sleep**: Use polling with timeouts
5. **Connection limits**: Slot pools cap at 4; admin pool caps at 8
6. **Profiles**: Keep perf/stress/external suites gated behind explicit nextest profiles

## Debugging Tips

### "Test hangs"
Check if database is stuck:
```bash
psql -l | grep sinex_test
```

### "Pool exhausted"
Reduce concurrency or raise PostgreSQL `max_connections`:
```bash
cargo xtask test --profile fast
```

### "Events not appearing"
Use polling with wait:
```rust
ctx.timing().wait_for_event_count(expected).await?;
```

### "Unclear assertion failure"
Add context to assertion:
```rust
ctx.assert("specific scenario description")
    .eq(&actual, &expected)?;
```

## See Also

- **Full Documentation**: `TEST_PATTERNS.md`
- **Database Pool**: `src/database_pool.rs` (1790+ lines)
- **Test Context**: `src/test_context.rs` (617 lines)
- **Assertions**: `src/test_context.rs` → ContextualAssert
- **Property Testing**: `src/property_testing.rs` (735 lines)
- **Dataset Seeds**: `src/dataset_seeds.rs`

## Key Files Structure

```
sinex-test-utils/
├── src/
│   ├── lib.rs                    # Prelude
│   ├── database_pool.rs          # Database isolation
│   ├── test_context.rs           # TestContext and assertions
│   ├── property_testing.rs       # Property test strategies
│   ├── dataset_seeds.rs          # Dataset seeding helpers
│   ├── test_macros.rs            # Test helper macros
│   └── nats.rs                   # NATS test utilities
├── macros/
│   └── src/lib.rs                # #[sinex_test] macro
└── tests/
    ├── integration/               # Integration test examples
    └── *.rs                       # Feature demonstrations
```

## Quick Command Reference

```bash
# Run tests for sinex-test-utils
cargo xtask test --profile reliable --prime -- -p sinex-test-utils

# Rebuild template database
rm target/sinex-test-utils/template_stamp.json && cargo xtask test --profile reliable --prime -- -p sinex-test-utils

# Run with tracing
RUST_LOG=debug cargo xtask test --profile reliable --prime -- -p sinex-test-utils

# Run specific test
cargo xtask test --profile reliable -- -p sinex-test-utils -E 'test(test_name)'

# Update snapshots
INSTA_UPDATE=always cargo xtask test --profile reliable --prime -- -p sinex-test-utils

# Check pool health (in test code)
let report = check_pool_health().await?;
println!("Healthy: {}/{}", report.healthy_slots, report.total_slots);
```

---

For complete details on each pattern, see `TEST_PATTERNS.md`.
