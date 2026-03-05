## Test Policy (`#[sinex_test]` by default)

Use `#[sinex_test]` for regular tests.

Raw `#[test]` / `#[tokio::test]` are allowlisted only for:

- `trybuild`/compile-fail harnesses
- proc-macro-internal tests that cannot use sandbox runtime

If a raw test is kept for another reason, include a short in-file exception note.

If a test doesn't need `TestContext`, omit it:

```rust
#[sinex_test]
async fn test_without_context() -> TestResult<()> {
    // No ctx parameter - still works
    Ok(())
}
```

## Test Location Policy (prefer `tests/`)

Prefer per-crate `tests/` files by default, including most unit tests.

Inline `#[cfg(test)]` is exception-only: use it only when testing truly local internals and extracting to `tests/` would force undesirable visibility changes. Keep these inline blocks small and focused.

Test utilities are available via the `sandbox` feature in `xtask`:

```rust
// crate/lib/my-crate/tests/integration_test.rs
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn my_test(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();  // Isolated test database
    // ... test code
    Ok(())
}

#[sinex_test(timeout = 60)]  // With timeout
async fn slow_test(ctx: TestContext) -> TestResult<()> { ... }
```

---

## Database Isolation (how it actually works)

```
Pool of 64+ pre-created databases (slot_0, slot_1, ...)
  ↓
Test acquires exclusive slot via advisory lock
  → Gets full separate database, NOT a transaction
  → Cloned from: CREATE DATABASE slot_N WITH TEMPLATE sinex_test_template
  ↓
On Drop: database cleaned/reset, slot returned to pool
```

---

## TestContext Capabilities

```rust
ctx.pool()                          // Database pool (exclusive slot)
ctx.with_nats().shared().await?     // NATS connection
ctx.assert("context").eq(a, b)?     // Rich assertions
ctx.timing().wait_for_event_count(n).await?     // Wait helpers
ctx.timing().wait_for_condition(|| async { ... }).await?  // Custom waits
ctx.publish(payload).await?         // Publish event (Publishable trait)
```

---

## Dataset Seeding

```rust
use xtask::sandbox::dataset_seeds::*;

let clock = SeedClock::new();
seed_events_via_scope(&scope, &clock, vec![
    EventSpec::new("fs-watcher", "file.created").at(clock.now()),
]).await?;
```

---

## Timing Utilities (USE instead of magic numbers)

```rust
use xtask::sandbox::timing::{Timeouts, WaitHelpers};

// Standard timeout constants
Timeouts::QUICK      // 5s   - Fast operations, simple checks
Timeouts::SHORT      // 10s  - Typical unit test operations
Timeouts::MEDIUM     // 15s  - Moderate operations
Timeouts::STANDARD   // 30s  - Default for most tests
Timeouts::LONG       // 60s  - Integration tests
Timeouts::STRESS     // 90s  - Heavy stress test operations
Timeouts::EXTENDED   // 120s - Very slow operations
Timeouts::CI         // 180s - Slow CI environments

// Wait helpers (use instead of sleep)
ctx.timing().wait_for_event_count(5).await?;
ctx.timing().wait_for_condition(|| async { check() }).await?;
```

---

## Test Events (integration tests with DB)

```rust
use sinex_primitives::prelude::*;

// Typed payload (preferred — source/type from trait, compile-time checked)
let event = ctx.publish(FileCreatedPayload {
    path: "/test/file.txt".into(),
    size: 1024,
    ..Default::default()
}).await?;

// Dynamic payload (escape hatch — runtime source/type)
let event = ctx.publish(DynamicPayload::new(
    "fs-watcher", "file.created",
    json!({ "path": "/test/file.txt", "size": 1024 }),
)).await?;
```

---

## Pipeline Testing (with NATS)

```rust
#[sinex_test]
async fn test_with_nats(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;  // Shared NATS (faster)
    // OR
    let ctx = ctx.with_nats().ephemeral().await?;  // Isolated NATS

    // Publish and wait for processing
    ctx.publish(DynamicPayload::new("source", "type", json!({}))).await?;
    ctx.timing().wait_for_event_count(1).await?;
    Ok(())
}

// Additional test helpers
use xtask::sandbox::{
    PipelineScope,           // Test isolation with NATS
    JetStreamTestHelper,     // JetStream stream/consumer operations
    TestNodePublisher,       // Simulate node event publishing
    EphemeralNats,           // Standalone NATS instance for tests
};
```

---

## Property Testing

Property-based testing for fuzzing and invariant verification.

```rust
use sinex_primitives::prelude::*;
use xtask::sandbox::{sinex_prop, prelude::*};

// Property test with TestContext
#[sinex_prop(cases = 100, timeout = "30s")]
async fn prop_event_roundtrip(
    ctx: &TestContext,
) -> TestResult<()> {
    let event_id: Id<Event<JsonValue>> = Id::new(); // UUIDv7-backed typed ID
    let _storage_id = *event_id.as_uuid();
    let _ = ctx;
    // Test invariants with generated inputs
    Ok(())
}

// Builtin strategies
nats_message_sequence_strategy(1, 10)     // NATS message batches
nats_subject_strategy()                   // Valid NATS subjects
```

**Environment variables:**

- `SINEX_PROPTEST_SEED` - Fixed seed for reproducibility

Reference: `xtask/docs/sandbox/property_testing.md`
