# Timing and Synchronization Patterns

Reliable tests require deterministic coordination, not arbitrary sleeps. The test utilities
provide synchronization primitives that avoid race conditions and complete as fast as possible.

## The Anti-Pattern: Fixed Sleeps

```rust
// ❌ BAD: Fixed sleep is either too short (flaky) or too long (slow)
tokio::time::sleep(Duration::from_millis(500)).await;
let events = ctx.pool.events().count().await?;
assert_eq!(events, 5);  // May fail if ingestion takes 600ms

// ❌ NEVER DO THIS: Blocks the async executor
use std::thread;
thread::sleep(Duration::from_millis(100));  // Blocks entire runtime!
```

**Problems**:
- Too short → flaky tests that fail under load
- Too long → slow test suite
- Blocking sleeps → executor starvation

## Adaptive Polling: WaitHelpers

Wait for database state changes with minimal latency:

```rust
// ✅ GOOD: Adaptive polling completes as soon as condition met
ctx.timing().wait_for_event_count(5).await?;
```

### Available Helpers

```rust
// Wait for specific event count
ctx.timing().wait_for_event_count(expected_count).await?;

// Wait for events from specific source
ctx.timing()
    .wait_for_source_events(&source, expected_count, timeout)
    .await?;

// Generic condition polling
ctx.timing()
    .wait_for_condition(
        || async {
            let count = ctx.pool.events().count().await?;
            Ok(count >= 5)
        },
        Duration::from_secs(30)
    )
    .await?;
```

### Implementation Details

- **Initial interval**: 10ms
- **Backoff**: Increases to 100ms maximum
- **Completion**: Returns immediately when condition met
- **Timeout**: Returns clear error with last observed state
- **CI-friendly**: Generous timeouts, fast on success

## TestSynchronizer: One-Shot Signals

Deterministic wait points for background tasks without race conditions.

**Use Case**: Waiting for a background task to reach a specific state (checkpoint saved, leader
elected, material finalized).

**Mechanism**: Uses `tokio::sync::watch` channel for efficient one-shot signaling.

```rust
use sinex_test_utils::timing_utils::TestSynchronizer;

#[sinex_test]
async fn test_background_checkpoint(ctx: TestContext) -> Result<()> {
    let sync = ctx.timing().synchronizer(Duration::from_secs(5));

    // Background task
    let sync_clone = sync.clone();
    tokio::spawn(async move {
        // ... do work ...
        sync_clone.signal();
    });

    // Wait for signal or timeout
    sync.wait().await?;

    // Proceed with assertions after known synchronization point
    Ok(())
}
```

### Why TestSynchronizer?

- **No race conditions** — unlike sleep + check loops
- **Fails fast** — clear error on timeout
- **Zero busy-waiting** — no CPU overhead while waiting

## TestBarrier: Coordinating Multiple Tasks

Ensures N tasks all reach a synchronization point before proceeding.

**Use Case**: Thundering herd tests, concurrent access verification, coordinated writes.

**Mechanism**: Wraps `tokio::sync::Barrier` with timeout support.

```rust
use sinex_test_utils::timing_utils::TestBarrier;

#[sinex_test]
async fn test_concurrent_ingestion(ctx: TestContext) -> Result<()> {
    let barrier = ctx.timing().barrier(3);
    let timeout = Duration::from_secs(10);

    // Launch 3 concurrent tasks
    let mut handles = vec![];
    for i in 0..3 {
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            // Setup work...

            // All tasks wait here
            barrier.wait(timeout).await?;

            // Now all proceed simultaneously
            // ... coordinated work ...
            Ok::<_, SinexError>(())
        }));
    }

    // Wait for all to complete
    for handle in handles {
        handle.await??;
    }

    Ok(())
}
```

### Best Practices for Barriers

- Use for concurrency stress tests
- Verify system behavior under simultaneous load
- Test lock contention and ordering guarantees

## Concurrent Test Execution

Pool-based isolation enables safe concurrent tests:

```rust
#[sinex_test]
async fn test_concurrent_execution(ctx: TestContext) -> Result<()> {
    const TASKS: usize = 5;
    let barrier = Arc::new(tokio::sync::Barrier::new(TASKS));
    let mut handles = vec![];

    for i in 0..TASKS {
        let barrier_clone = barrier.clone();
        let handle = tokio::spawn(async move {
            // Each task gets own TestContext (separate DB)
            let ctx = TestContext::with_name(&format!("concurrent_{i}")).await?;

            // Synchronize all tasks
            barrier_clone.wait().await;

            // Concurrent operations
            for j in 0..10 {
                ctx.publish_event(
                    &format!("task_{i}"),
                    "concurrent.test",
                    json!({"iteration": j}),
                ).await?;
            }

            // Wait with retry loop (adaptive polling)
            const MAX_ATTEMPTS: usize = 20;
            const RETRY_DELAY_MS: u64 = 100;
            for attempt in 0..MAX_ATTEMPTS {
                let events = ctx.pool.events()
                    .get_by_source(&EventSource::from(format!("task_{i}")), Some(100), None)
                    .await?;
                if events.len() == 10 {
                    break;
                }
                if attempt + 1 < MAX_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                }
            }

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

### Key Patterns

- **Barriers** — synchronize task starts
- **Separate TestContext** — each task gets own database
- **Polling with backoff** — wait for flushes without fixed delays
- **Never hardcode waits** — use adaptive polling

## Temporal Assertions: Event Ordering

Use ULID timestamps for event ordering verification:

```rust
#[sinex_test]
async fn test_event_ordering(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;

    let first = ctx.publish_event(
        "timeline",
        "event.first",
        json!({"seq": 1}),
    ).await?;

    // Small delay to ensure different ULID timestamps
    tokio::time::sleep(Duration::from_millis(10)).await;

    let second = ctx.publish_event(
        "timeline",
        "event.second",
        json!({"seq": 2}),
    ).await?;

    // Verify ordering via ULID timestamps (always monotonic)
    ctx.assert("event ordering")
        .that(
            first.id.as_ref().map(|id| id.as_ulid().timestamp())
                < second.id.as_ref().map(|id| id.as_ulid().timestamp()),
            "events should be ordered by ULID timestamp",
        )?;

    Ok(())
}
```

### Key Points

- **ULIDs provide monotonic ordering** — always increase
- **Never rely on wall-clock time** — system clocks can jump
- **Use event ID timestamps** — built-in ordering guarantee
- **Timing utilities help verification** — wait for expected state

## Optional Database: SINEX_EDGE_MODE

### Architecture (as of Jan 2025)

- **Checkpoints**: ALWAYS stored in NATS KV (`KV_sinex_checkpoints`)
- **DATABASE_URL**: Optional — only needed for processors that query events
- **`SINEX_EDGE_MODE=1`**: Suppresses DATABASE_URL requirement + enables schema cache

### Database Dependency by Processor Type

| Type | Needs DATABASE_URL? | Example |
|------|---------------------|---------|
| **Ingestors** | No | fs-watcher, terminal-node, desktop-node |
| **Automata** | Usually yes | analytics-automaton, search-automaton |

**Ingestors** only capture and publish events to NATS. **Automata** query historical events.

### Testing Ingestors Without Database

```rust
#[sinex_test]
async fn test_ingestor_without_database(ctx: TestContext) -> Result<()> {
    std::env::set_var("SINEX_EDGE_MODE", "1");
    std::env::remove_var("DATABASE_URL");

    let ctx = ctx.with_nats().shared().await?;

    // Initialize ingestor - works without DATABASE_URL
    let processor = MyIngestor::new(/* ... */);
    let runner = StreamProcessorRunner::new(/* ... */).await?;

    // Checkpoints work (always NATS KV)
    let checkpoint = runner.current_checkpoint().await?;
    assert!(checkpoint.is_some());

    std::env::remove_var("SINEX_EDGE_MODE");
    Ok(())
}
```

### Testing Automata With Database

```rust
#[sinex_test]
async fn test_automaton_queries_events(ctx: TestContext) -> Result<()> {
    // DATABASE_URL present via TestContext
    let ctx = ctx.with_nats().shared().await?;

    let processor = MyAutomaton::new(/* ... */);
    let runner = StreamProcessorRunner::new(/* ... */).await?;

    // Automaton can query events via db_pool handle
    Ok(())
}
```

## Measurement Utilities

### Elapsed Time

```rust
let elapsed = ctx.elapsed();
println!("Test running for {:?}", elapsed);
```

### Operation Measurement

```rust
let (result, duration) = ctx.measure(|| async {
    expensive_operation().await
}).await?;
println!("Operation took {:?}", duration);
```

## Quick Reference

| Need | Pattern |
|------|---------|
| Wait for event count | `ctx.timing().wait_for_event_count(n)` |
| Wait for condition | `ctx.timing().wait_for_condition(...)` |
| One-shot signal | `ctx.timing().synchronizer(timeout)` |
| Coordinate N tasks | `ctx.timing().barrier(n)` |
| Event ordering | Compare ULID timestamps |
| Measure duration | `ctx.measure(|| async {...})` |

## Summary: DO and DON'T

| DO | DON'T |
|----|-------|
| Use `wait_for_event_count()` | Use `tokio::time::sleep()` |
| Use TestSynchronizer for signals | Use sleep + check loops |
| Use TestBarrier for coordination | Use hardcoded delays |
| Compare ULID timestamps | Assume insertion order |
| Use adaptive polling | Use `std::thread::sleep()` |
