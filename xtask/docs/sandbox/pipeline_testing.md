# Pipeline Testing

Pipeline tests exercise the same flow that production uses: nodes publish to NATS JetStream,
sinex-ingestd consumes, and the database observes the persisted events. This ensures tests
validate real behavior, not just mock wiring.

## The Pipeline-First Rule

> Before seeding any events, call `ctx.with_nats().shared().await?` and use
> `ctx.publish_event(...)` so every test exercises the actual ingestion path.

```rust
#[sinex_test]
async fn test_pipeline(ctx: TestContext) -> TestResult<()> {
    // Step 1: Enable shared NATS (required)
    let ctx = ctx.with_nats().shared().await?;

    // Step 2: Publish through the pipeline
    let event = ctx.publish_event(
        "fs-watcher",
        "file.created",
        json!({"path": "/tmp/test.txt"})
    ).await?;

    // Step 3: Assert on database state
    let events = ctx.pool.events().get_recent(10).await?;
    ctx.assert("pipeline").not_empty(&events)?;

    Ok(())
}
```

## Shared NATS Architecture

`with_shared_nats()` reuses a process-wide EphemeralNats instance:

```
┌─────────────────────────────────────────────────────────────────┐
│                   Process-Wide EphemeralNats                     │
├─────────────────────────────────────────────────────────────────┤
│  Test A ── namespace_a ──► SINEX_TEST_EVENTS_a                  │
│  Test B ── namespace_b ──► SINEX_TEST_EVENTS_b                  │
│  Test C ── namespace_c ──► SINEX_TEST_EVENTS_c                  │
└─────────────────────────────────────────────────────────────────┘
```

Benefits:
- **One NATS startup** — expensive server startup happens once per test process
- **Namespace isolation** — each test gets unique stream/subject prefixes
- **No interference** — parallel tests operate on isolated JetStream resources

## Namespace Isolation

Each test gets a unique namespace derived from the test name:

```rust
let namespace = ctx.pipeline_namespace();

// Derive stream names
let events_stream = namespace.stream("SINEX_TEST_EVENTS");
// Result: "SINEX_TEST_EVENTS_<unique_suffix>"

// Derive subject patterns
let events_subject = namespace.subject("events.raw.>");
// Result: "<unique_prefix>.events.raw.>"
```

**Critical**: Always use `namespace.stream()` and `namespace.subject()` instead of hardcoding
names. This is the only safe way to share NATS across parallel tests.

## PipelineScope

PipelineScope provides full pipeline testing with in-process ingestd:

```rust
#[sinex_test]
async fn test_full_pipeline(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    // Start in-process ingestd
    let scope = ctx.pipeline_scope().await?;

    // Publish events
    scope.publish("fs-watcher", "file.created", json!({"path": "/a"})).await?;
    scope.publish("terminal", "command.executed", json!({"cmd": "ls"})).await?;

    // Wait for persistence
    scope.wait_for_event_count(2).await?;

    // Query database
    let events = ctx.pool.events().get_recent(10).await?;
    assert_eq!(events.len(), 2);

    Ok(())
}
```

### PipelineScope Lifecycle

1. **Creation**: Calls `ctx.reset_database_slot()` to ensure clean state
2. **Ingestd**: Starts in-process using test's database URL and NATS context
3. **Work Directory**: Reuses per-database directory under `/tmp/sinex-ingestd-shared/`
4. **Cleanup**: Stops ingestd on drop

### PipelineScope API

```rust
impl PipelineScope {
    /// Publish an event through the pipeline
    pub async fn publish(
        &self,
        source: &str,
        event_type: &str,
        payload: Value,
    ) -> Result<()>;

    /// Wait for events to be persisted
    pub async fn wait_for_event_count(&self, count: usize) -> Result<()>;

    /// Get the underlying pipeline harness
    pub fn harness(&self) -> &PipelineHarness;

    /// Explicitly shut down the scope
    pub async fn shutdown(self) -> Result<()>;
}
```

## Manual JetStream Provisioning

PipelineScope provisions ingestd streams automatically. For additional streams or custom
consumers, use the namespace helper:

```rust
use async_nats::jetstream;

let ctx = ctx.with_nats().shared().await?;
let namespace = ctx.pipeline_namespace();
let js = ctx.jetstream().await?;

// Create custom stream with namespaced name
let stream_name = namespace.stream("MY_CUSTOM_STREAM");
let subject_pattern = namespace.subject("custom.events.>");

js.get_or_create_stream(jetstream::stream::Config {
    name: stream_name,
    subjects: vec![subject_pattern],
    ..Default::default()
}).await?;
```

**Do not** call `env.nats_stream_name(...)` or construct stream names manually.

## TestNodePublisher

For tests that need to simulate node behavior directly:

```rust
use xtask::sandbox::TestNodePublisher;

let ctx = ctx.with_nats().shared().await?;
let namespace = ctx.pipeline_namespace().prefix().to_string();

let publisher = TestNodePublisher::with_namespace(
    ctx.nats_client(),
    "fs-watcher",          // source name
    Some(namespace),       // namespace for isolation
);

// Publish like a real node would
publisher.publish_event("file.created", json!({"path": "/tmp/demo"})).await?;
```

TestNodePublisher wraps the node SDK with test defaults. It publishes slices, payloads, and
confirmations just like a production node.

## NATS Client Access

After `with_shared_nats()`:

```rust
// Raw NATS client
let client: async_nats::Client = ctx.nats_client();

// JetStream context
let js: async_nats::jetstream::Context = ctx.jetstream().await?;

// Key-Value store
let kv = js.get_key_value("KV_sinex_checkpoints").await?;
```

## Concurrency Guard

A process-wide semaphore caps how many PipelineScope instances run in parallel:

```
Default limit = available_parallelism / 6, clamped to 1..6
```

This prevents JetStream-heavy suites from starving ingestd. Additional pipeline tests wait
for a permit instead of hitting timeouts.

**Note**: If `jetstream_dlq_test` or `jetstream_e2e_integration_test` exceed 30s, check the
concurrency guard before increasing individual test timeouts.

## Complete Pipeline Test Example

```rust
#[sinex_test]
async fn test_complete_workflow(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;

    // 1. Create events using production event creation
    let fs_event = ctx.publish_event(
        "fs-watcher",
        "file.created",
        json!({"path": "/data/report.pdf", "size": 2048}),
    ).await?;

    let term_event = ctx.publish_event(
        "terminal",
        "command.executed",
        json!({
            "command": "process-report /data/report.pdf",
            "working_dir": "/app",
            "exit_code": 0
        }),
    ).await?;

    // 2. Query using direct repository access
    let events = ctx.pool.events()
        .get_by_source(&EventSource::from_static("fs-watcher"), Some(10), None)
        .await?;
    assert!(!events.is_empty());

    // 3. Use timing utilities to ensure ordering
    ctx.timing().wait_for_event_count(2).await?;

    // 4. Assert with rich context
    ctx.assert("workflow validation")
        .eq(&events[0].event_type.as_str(), &"file.created")?
        .that(
            fs_event.id.as_ref().map(|id| id.as_ulid().timestamp())
                < term_event.id.as_ref().map(|id| id.as_ulid().timestamp()),
            "file should be created before processing (ULID ordering)",
        )?;

    Ok(())
}
```

## Quick Reference

| Need | Helper |
|------|--------|
| Start NATS | `ctx.with_nats().shared().await?` |
| JetStream context | `ctx.jetstream().await?` |
| Namespace | `ctx.pipeline_namespace()` |
| Stream name | `namespace.stream("STREAM")` |
| Subject pattern | `namespace.subject("subject.>")` |
| Publish event | `ctx.publish_event(...)` |
| Full pipeline + ingestd | `ctx.pipeline_scope().await?` |
| Node-style publisher | `TestNodePublisher::with_namespace(...)` |
| Wait for persistence | `scope.wait_for_event_count(n)` |

## When to Use What

| Scenario | Approach |
|----------|----------|
| Unit test, no pipeline | Direct repository: `ctx.pool.events().insert()` |
| Integration test | `ctx.publish_event()` |
| Full pipeline with ingestd | `ctx.pipeline_scope()` |
| Simulating node behavior | `TestNodePublisher` |
| Custom JetStream setup | `ctx.jetstream()` + namespace |

## Troubleshooting

### "NATS not initialized"

**Cause**: Forgot to call `with_shared_nats()`.

**Solution**: Add `let ctx = ctx.with_nats().shared().await?;` before publishing.

### "Stream already exists with different config"

**Cause**: Hardcoded stream name conflicts with another test.

**Solution**: Use `namespace.stream("NAME")` instead of hardcoding.

### "Pipeline test timeout"

**Cause**: Ingestd not consuming fast enough, or too many concurrent pipeline tests.

**Solutions**:
- Check concurrency guard (max 6 concurrent PipelineScope instances)
- Reduce parallel test count
- Use `wait_for_event_count()` instead of fixed sleeps
