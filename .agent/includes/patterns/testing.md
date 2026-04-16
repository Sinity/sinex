## Testing

### Policy

- **Default:** `#[sinex_test]` for all tests. Raw `#[test]`/`#[tokio::test]` only for trybuild and proc-macro tests.
- **Location:** Per-crate `tests/` directory. Inline `#[cfg(test)]` only when extraction would force visibility changes.
- **DB isolation:** Each test gets its own database (slot pool, not transactions). FK constraints work normally.
- **Events in tests:** Always `ctx.publish(payload)` — handles FK constraints correctly. Never manual insert.

### TestContext Quick Reference

```rust
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn my_test(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();                                      // Exclusive DB slot
    let ctx = ctx.with_nats().shared().await?;                  // Or .ephemeral()
    ctx.publish(FileCreatedPayload { .. }).await?;              // Typed event
    ctx.publish(DynamicPayload::new("src", "type", json!({}))).await?;  // Dynamic
    ctx.timing().wait_for_event_count(1).await?;                // Deterministic wait
    ctx.timing().wait_for_condition(|| async { check() }).await?;
    ctx.assert("context").eq(a, b)?;                            // Rich assertions
    Ok(())
}

#[sinex_test(timeout = 60)]  // Custom timeout
async fn slow_test(ctx: TestContext) -> TestResult<()> { .. }
```

### Timeout Constants (not magic numbers)

| Constant | Duration | Use for |
|----------|----------|---------|
| `Timeouts::QUICK` | 5s | Fast operations |
| `Timeouts::SHORT` | 10s | Typical unit tests |
| `Timeouts::STANDARD` | 30s | Default for most tests |
| `Timeouts::LONG` | 60s | Integration tests |
| `Timeouts::STRESS` | 90s | Heavy stress tests |
| `Timeouts::CI` | 180s | Slow CI environments |

### Dataset Seeding

```rust
use xtask::sandbox::dataset_seeds::*;
let clock = SeedClock::new();
seed_events_via_scope(&scope, &clock, vec![
    EventSpec::new("fs-watcher", "file.created").at(clock.now()),
]).await?;
```

### Property Testing

```rust
use xtask::sandbox::{sinex_prop, prelude::*};

#[sinex_prop(cases = 100, timeout = "30s")]
async fn prop_roundtrip(ctx: &TestContext) -> TestResult<()> { .. }

// Built-in strategies: nats_message_sequence_strategy(), nats_subject_strategy()
// Reproducible: set SINEX_PROPTEST_SEED
```

### Pipeline Test Helpers

```rust
use xtask::sandbox::{
    PipelineScope,         // Test isolation with NATS
    JetStreamTestHelper,   // Stream/consumer operations
    TestNodePublisher,     // Simulate node publishing
    EphemeralNats,         // Standalone NATS instance
};
```
