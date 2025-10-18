# Sinex Test Utilities

A comprehensive testing framework for the Sinex event-driven data capture system, providing
database isolation, fixture management, and robust testing patterns.

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

    // Query using direct repository access - no mocks or wrappers
    let events = ctx.pool.events().get_recent(10).await?;

    // Rich assertions with context and clear error messages
    ctx.assert("event creation")
        .eq(&events.len(), &1)?
        .that(events[0].payload["size"] == json!(1024), "size should match")?;

    Ok(())
}
```

## Core Features

### Database Isolation

Each test gets its own isolated database from a 64-database pool using PostgreSQL advisory locks:

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

### Rich Fixtures

Pre-built test data for common scenarios:

```rust
// Standard user session with mixed event types
let session = fixtures::standard_user_session(&ctx).await?;

// Large dataset for performance testing
let dataset = fixtures::performance_dataset_with_size(&ctx, 10_000).await?;

// Error scenarios for edge case testing
let errors = fixtures::error_scenarios(&ctx).await?;
```

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
async fn test_with_logging(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Starting test");
    // ... test logic ...
    ctx.assert_logged("Starting test")?;  // Verify log message
    Ok(())
}
```

# Core Concepts

## TestContext – Single Entry Point

All test functionality is accessed through `TestContext`, providing:

- Isolated database per test.
- Event creation builders.
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
ctx.create_test_event("fs-watcher", "file.modified", json!({"path": "/tmp/test"})).await?;

// Using production Event::new() with actual payload types
let event = Event::new(FileCreatedPayload {
    path: "/data/document.pdf".to_string(),
    size: 1024,
    created_at: Utc::now(),
    permissions: Some(0o644),
})?;
ctx.pool.events().insert(event).await?;

// For quick tests without specific payload types
ctx.create_test_event(
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

## Fixtures

Access reusable test scenarios through the unified fixture manager:

```rust
// Access fixtures through ctx.fixtures() namespace
let session = ctx.fixtures().user_session().await?;
let dataset = ctx.fixtures().large_dataset().await?;
let errors = ctx.fixtures().validation_failures().await?;

// Or use the nested namespaces for better organization
```
