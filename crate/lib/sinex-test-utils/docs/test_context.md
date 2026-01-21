# TestContext API Reference

TestContext is the central coordination point for all test utilities. It provides database access,
NATS connectivity, assertions, timing utilities, background task management, and automatic cleanup.

## Overview

```rust
pub struct TestContext {
    pub pool: DbPool,           // Direct database access via repositories
    // ... private fields for lifecycle management
}
```

TestContext is automatically created by the `#[sinex_test]` macro and cleaned up when the test
completes. Each test gets an isolated database from the pool.

## Creation

### Automatic via Macro (Recommended)

```rust
#[sinex_test]
async fn test_example(ctx: TestContext) -> Result<()> {
    // ctx is ready to use
    Ok(())
}
```

### Manual Creation

```rust
// Default name derived from test function
let ctx = TestContext::new().await?;

// Custom name (useful for concurrent test spawning)
let ctx = TestContext::with_name("my_test").await?;
```

## NATS Initialization

NATS is not started by default. Use one of these methods to enable:

### `with_shared_nats()` (Recommended)

Reuses a process-wide NATS instance with namespace isolation:

```rust
let ctx = ctx.with_shared_nats().await?;
```

### `with_secure_shared_nats()`

Shared NATS with TLS enabled:

```rust
let ctx = ctx.with_secure_shared_nats().await?;
```

### `with_nats()` (Deprecated)

Creates a dedicated ephemeral NATS instance (slower, use only when isolation is required):

```rust
let ctx = ctx.with_nats().await?;
```

### `with_nats_builder()`

Custom NATS configuration:

```rust
let builder = EphemeralNatsBuilder::new()
    .with_jetstream(true)
    .with_auth_token("secret");
let ctx = ctx.with_nats_builder(builder).await?;
```

### `with_shared_nats_builder()`

Shared NATS with custom builder:

```rust
let ctx = ctx.with_shared_nats_builder(builder).await?;
```

## NATS Access

After initializing NATS:

```rust
// Raw NATS client
let client: NatsClient = ctx.nats_client();

// JetStream context
let js: jetstream::Context = ctx.jetstream().await?;

// Checkpoint KV store
let kv: jetstream::kv::Store = ctx.checkpoint_kv().await?;

// NATS URL (for external processes)
let url: Option<String> = ctx.nats_url();

// Underlying EphemeralNats handle
let handle: Arc<EphemeralNats> = ctx.nats_handle()?;
```

### Lazy Initialization (Ensure Methods)

These methods lazily initialize shared NATS if not already done:

```rust
// Ensure NATS is available, initializing shared if needed
let client = ctx.ensure_nats().await?;

// Ensure JetStream is available
let js = ctx.ensure_jetstream().await?;

// Ensure checkpoint KV is available
let kv = ctx.ensure_checkpoint_kv().await?;
```

## Database Access

### Direct Pool Access

```rust
// Repository methods
let events = ctx.pool.events().get_recent(10).await?;
let count = ctx.pool.events().count_all().await?;

// Insert directly (bypasses pipeline)
let event = Event::<JsonValue>::test_event("source", "type", json!({}));
ctx.pool.events().insert(event).await?;
```

### Database Info

```rust
let url: &str = ctx.database_url();
let name: &str = ctx.database_name();
```

### Database Reset

```rust
// Reset database to clean state (called automatically by PipelineScope)
ctx.reset_database_slot().await?;

// Verify database is clean
ctx.ensure_clean().await?;
```

## Environment Access

```rust
let env: &SinexEnvironment = ctx.env();
```

## Pipeline Testing

### Namespace Isolation

```rust
let namespace: &PipelineNamespace = ctx.pipeline_namespace();

// Derive isolated stream/subject names
let stream = namespace.stream("MY_STREAM");
let subject = namespace.subject("events.>");
```

### PipelineScope (Full Pipeline with Ingestd)

```rust
let scope = ctx.pipeline_scope().await?;
scope.publish("source", "type", json!({})).await?;
scope.wait_for_event_count(1).await?;
```

### PipelineHarness (Lower-Level)

```rust
let harness = ctx.pipeline().await?;
```

## Event Publishing

### JSON Events (Pipeline-First)

```rust
let ctx = ctx.with_shared_nats().await?;

// Basic publish
let event = ctx.publish_json_event("fs-watcher", "file.created", json!({
    "path": "/test.txt"
})).await?;

// With explicit timestamp
let event = ctx.publish_json_event_with_timestamp(
    "fs-watcher",
    "file.created",
    json!({"path": "/test.txt"}),
    Timestamp::now(),
).await?;
```

### TestEventBuilder (Fluent API)

```rust
let event = ctx.event("fs-watcher", "file.created")
    .payload(json!({"path": "/test.txt"}))
    .at(Timestamp::now())
    .publish()
    .await?;
```

### Raw Event Publishing

```rust
let event = Event::<JsonValue>::test_event("source", "type", json!({}));
let ulid = ctx.publish_test_event(&event).await?;
```

## Source Material Management

Events require source materials for FK constraints:

```rust
// Ensure material exists (creates if needed)
ctx.ensure_source_material(material_id, Some("identifier")).await?;

// Create new material
let material = ctx.create_source_material("identifier").await?;

// Ensure specific material with custom fields
ctx.ensure_specific_material(id, kind, identifier).await?;

// Ensure schema material
ctx.ensure_schema_material(id, schema_name).await?;
```

## Assertions

### Fluent Assertion API

```rust
ctx.assert("validation context")
    .eq(&actual, &expected)?
    .that(condition, "message")?
    .not_empty(&collection)?
    .has_size(&collection, 5)?
    .some(&option)?
    .none(&option)?
    .error_contains(&result, "expected error")?;
```

### Event Assertions

```rust
// Assert exact event count
let count = ctx.assert_event_count(5).await?;

// Assert minimum event count
let count = ctx.assert_event_count_at_least(3).await?;

// Assert by source
let count = ctx.assert_event_count_by_source("fs-watcher", 5).await?;
let count = ctx.assert_event_count_by_source_at_least("fs-watcher", 3).await?;

// Assert unique IDs
ctx.assert_unique_event_ids(&events)?;

// Compare events
ctx.assert_event_eq(&event1, &event2, &["field1", "field2"])?;
```

### Log Assertions

```rust
// Assert specific log was captured
ctx.assert_logged("checkpoint saved")?;

// Assert no errors logged
ctx.assert_no_errors_logged()?;
```

## Timing and Measurement

### TimingUtils Access

```rust
let timing = ctx.timing();

// Wait helpers
timing.wait_for_event_count(5).await?;
timing.wait_for_condition(|| async { Ok(check()) }, timeout).await?;

// Synchronization
let sync = timing.synchronizer(timeout);
let barrier = timing.barrier(3);
```

### Direct Measurement

```rust
// Elapsed since test start
let elapsed: Duration = ctx.elapsed();

// Measure operation
let (result, duration) = ctx.measure(|| async {
    expensive_operation().await
}).await?;

// Event count tracking
let baseline = ctx.baseline_event_count();
let current = ctx.current_event_count().await?;
let delta = ctx.event_delta().await?;
```

## Tracing and Logging

### Enable Tracing

```rust
// Via macro
#[sinex_test(trace = true)]
async fn test(ctx: TestContext) -> Result<()> { ... }

// Programmatically
let ctx = ctx.with_tracing("debug");

// Static initialization (for tests without TestContext)
TestContext::init_tracing("info");
```

### Log Capture

```rust
// Capture a log message
ctx.capture_log("custom message".to_string());

// Get all captured logs
let logs: Vec<String> = ctx.captured_logs();
```

## Snapshot Testing

```rust
// JSON snapshot
ctx.assert_inline_snapshot(&value);

// Named snapshot
ctx.snapshot(&value, Some("snapshot_name"));
ctx.snapshot(&value, None);  // auto-named

// Event snapshot (excludes volatile fields)
ctx.snapshot_event(&event, Some("event_name"));
```

## Background Task Management

### Register Tasks

```rust
// Register a JoinHandle
ctx.register_background_task("label", handle).await;

// Register any handle implementing AbortOnDrop
ctx.register_background_handle("label", handle);

// Spawn and register in one call
ctx.spawn_background("label", async {
    // background work
});

// Register shutdown hook
ctx.register_shutdown_hook("label", || async {
    // cleanup work
    Ok(())
}).await;
```

### Coordination

```rust
// Wait for all background tasks to complete
ctx.quiesce_background_tasks().await?;

// Assert no background tasks are running
ctx.assert_idle().await?;

// Get snapshot of background state
let snapshot: BackgroundSnapshot = ctx.background_snapshot();
```

## Test Info

```rust
let name: &str = ctx.test_name();
let elapsed: Duration = ctx.elapsed();
let baseline: i64 = ctx.baseline_event_count();
```

## Failure Diagnostics

```rust
// Get failure snapshot for debugging
let snapshot: TestContextFailureSnapshot = ctx.failure_snapshot();
```

## Cleanup

### Automatic (Default)

TestContext cleans up automatically via Drop:
- Database advisory lock released
- Connection pool closed
- NATS resources cleaned
- Background tasks aborted

### Explicit (Diagnostics Only)

```rust
ctx.force_cleanup().await?;
```

## Helper Structs

### ContextualAssert

Fluent assertion builder with context:

```rust
pub struct ContextualAssert {
    // Created via ctx.assert("context")
}

impl ContextualAssert {
    pub fn eq<T: Debug + PartialEq>(self, left: &T, right: &T) -> TestResult<Self>;
    pub fn that(self, condition: bool, message: &str) -> TestResult<Self>;
    pub fn not_empty<T>(self, collection: &[T]) -> TestResult<Self>;
    pub fn has_size<T>(self, collection: &[T], size: usize) -> TestResult<Self>;
    pub fn some<T>(self, option: &Option<T>) -> TestResult<Self>;
    pub fn none<T>(self, option: &Option<T>) -> TestResult<Self>;
    pub fn error_contains<T, E: Display>(self, result: &Result<T, E>, msg: &str) -> TestResult<Self>;
}
```

### TestEventBuilder

Fluent event builder:

```rust
pub struct TestEventBuilder<'a> {
    // Created via ctx.event(source, event_type)
}

impl TestEventBuilder<'_> {
    pub fn payload(self, payload: JsonValue) -> Self;
    pub fn at<T: Into<Timestamp>>(self, timestamp: T) -> Self;
    pub async fn publish(self) -> TestResult<Event<JsonValue>>;
}
```

### BackgroundSnapshot

Snapshot of registered background tasks:

```rust
pub struct BackgroundSnapshot {
    pub pending: usize,
    pub labels: Vec<String>,
}
```

### TestContextFailureSnapshot

Diagnostic info captured on test failure:

```rust
pub struct TestContextFailureSnapshot {
    // test_name, baseline_events, elapsed_ms, captured_logs, background_snapshot
}
```

## Utility Functions

### Payload Sanitization

```rust
// Sanitize JSON payload (removes/replaces problematic values)
TestContext::sanitize_payload(&mut json_value);
```

### TestContextHandle

Thread-local access to current TestContext:

```rust
// Get handle to current context (if any)
if let Some(handle) = TestContextHandle::try_current() {
    let pool = handle.pool();
}
```
