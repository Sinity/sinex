# Sinex Test Utilities

A comprehensive testing framework for the Sinex event-driven data capture system, providing
database isolation, dataset seeding, and robust testing patterns.

## Quick Start

```rust
use sinex_test_utils::prelude::*;

#[sinex_test]
async fn test_event_creation(ctx: TestContext) -> TestResult<()> {
    // Create test event using production APIs
    let event = ctx.publish_json_event(
        "fs-watcher",
        "file.created",
        json!({"path": "/test.txt", "size": 1024})
    ).await?;

    // Query using direct repository access - no mocks or wrappers
    let events = ctx.pool.events().get_recent(10).await?;

    // Rich assertions with context and clear error messages
    ctx.assert("event creation")
        .eq(&events.len(), &1)?
        .that(events[0].payload["size"] == json!(1024), "size should match")?;

    Ok(())
}

## Core Features

### Database Isolation

Each test gets its own isolated database from a pool sized at 2× Nextest test threads (minimum 64,
and reduced if Postgres `max_connections` would be exceeded) using PostgreSQL advisory locks:

- True isolation between parallel tests.
- Automatic cleanup after test completion.
- No interference between concurrent test runs.
- Production-like database environment.

### Production API Usage

Tests work directly with production APIs rather than mocks:

- Use real repository interfaces: `ctx.pool.events().insert(event).await?`.
- Test actual business logic and database interactions.
- Catch integration issues early.
- Simplified test maintenance (no mock synchronization).

### Dataset Seeding

Repeatable dataset seeds for common scenarios:

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

Pipeline seeding helpers (`seed_events_via_pipeline`, `seed_events_via_scope`) enforce
`SeedClock::fixed()` to keep pipeline suites deterministic.

### Context-Aware Assertions

Clear error messages with context:

```rust
ctx.assert("user session validation")
    .not_empty(&session.event_ids)?
    .has_size(&checkpoints, 5)?
    .that(condition, "custom message")?;
```

### Timing & Synchronization

Tools for testing concurrent operations:

```rust
let barrier = ctx.timing().barrier(3);  // Coordinate 3 tasks
ctx.timing().wait_for_event_count(10).await?;  // Wait for condition
let (result, duration) = ctx.measure(operation).await?;  // Measure timing
```

### Tracing Integration

Automatic log capture and verification:

```rust
#[sinex_test(trace = true)]
async fn test_with_logging(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Starting test");
    // ... test logic ...
    ctx.assert_logged("Starting test")?;  // Verify log message
    Ok(())
}
```

### JetStream / NATS Harness

Need to exercise the real message bus in a test? Spin up an ephemeral JetStream-enabled server
with `EphemeralNats`:

```rust
use sinex_test_utils::EphemeralNats;

#[sinex_test]
async fn test_stream_roundtrip() -> color_eyre::Result<()> {
    // Launch a scoped nats-server (looks at $NATS_SERVER_BIN if you need a custom binary)
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let jetstream = async_nats::jetstream::new(client.clone());

    // Normal test logic – create streams, publish, consume…
    // ...

    Ok(())
} // server is torn down automatically when EphemeralNats is dropped
```

By default the helper picks an open localhost port and stores state in a temp directory. Override
the binary path with `NATS_SERVER_BIN=/path/to/nats-server` when you need a custom build.

# Core Concepts

## TestContext – Single Entry Point

All test functionality is accessed through `TestContext`, providing:

- Isolated database per test.
- Event creation helpers.
- Query abstractions.
- Assertion helpers.
- Timing utilities.

## The `#[sinex_test]` Macro

**Always use `#[sinex_test]` instead of `#[test]`.** This macro:

- Creates and injects TestContext.
- Manages database lifecycle.
- Handles timeouts intelligently.
- Provides progress indicators.
- Integrates with proptest.

## Event Creation

Direct production API usage – no wrapper builders:

```rust
// Using convenience helper for simple test events
ctx.publish_json_event("fs-watcher", "file.modified", json!({"path": "/tmp/test"})).await?;

// Using production Event::new() with actual payload types
let event = Event::new(FileCreatedPayload {
    path: "/data/document.pdf".to_string(),
    size: 1024,
    created_at: Utc::now(),
    permissions: Some(0o644),
})?;
ctx.pool.events().insert(event).await?;

// For quick tests without specific payload types
ctx.publish_json_event(
    "my-service",
    "user.action",
    json!({"user_id": 123, "action": "login"})
).await?;
```

## Direct Repository Access

Use production repository methods directly:

```rust
// Direct repository calls - no wrapper query builders
let recent = ctx.pool.events().get_recent(5).await?;
let by_source = ctx.pool.events().get_by_source(&EventSource::from_static("fs-watcher"), Some(10), None).await?;
let count = ctx.pool.events().count_by_event_type(&EventType::from("file.created")).await?;
let single = ctx.pool.events().get_by_id(&event_id).await?;

// Use repository methods directly - no wrapper helpers
```

```
