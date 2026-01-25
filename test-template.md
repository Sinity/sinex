# Sinex Test Templates

Quick-start boilerplate for common test patterns. All templates use the pipeline-first approach (NATS → ingestd → DB).

**Reference Documentation:**
- Full patterns: [`docs/current/testing/TEST_PATTERNS.md`](/realm/project/sinex/docs/current/testing/TEST_PATTERNS.md)
- Quick guide: [`docs/current/testing/TEST_PATTERNS_GUIDE.md`](/realm/project/sinex/docs/current/testing/TEST_PATTERNS_GUIDE.md)

---

## Basic Unit Test

Simple test with event creation and verification.

```rust
use sinex_core::{EventSource, JsonValue};
use sinex_test_utils::{sinex_test, TestContext, TestResult};
use serde_json::json;

#[sinex_test]
async fn test_basic_event_flow(ctx: TestContext) -> TestResult<()> {
    // Enable NATS/ingestd (pipeline-first)
    let ctx = ctx.with_nats().shared().await?;

    // Create event via pipeline
    let event = ctx.publish_event(
        "test-source",
        "test.event",
        json!({"key": "value"}),
    ).await?;

    // Verify event was persisted
    let events = ctx.pool.events()
        .get_by_source(&EventSource::from("test-source"), Some(10), None)
        .await?;

    ctx.assert("event creation")
        .not_empty(&events)?
        .has_size(&events, 1)?;

    assert_eq!(events[0].payload["key"], json!("value"));
    Ok(())
}
```

---

## Parameterized Test (rstest)

Test multiple cases with different inputs.

```rust
use sinex_test_utils::{sinex_test, TestContext, TestResult};
use rstest::rstest;
use serde_json::json;

#[sinex_test]
#[rstest]
#[case("fs-watcher", "file.created", json!({"path": "/test.txt"}))]
#[case("terminal", "command.executed", json!({"cmd": "ls"}))]
#[case("desktop", "window.focused", json!({"title": "Editor"}))]
async fn test_multiple_sources(
    ctx: TestContext,
    #[case] source: &str,
    #[case] event_type: &str,
    #[case] payload: serde_json::Value,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    let event = ctx.publish_event(source, event_type, payload.clone()).await?;

    let events = ctx.pool.events()
        .get_by_source(&EventSource::from(source.to_string()), Some(10), None)
        .await?;

    ctx.assert("parameterized test")
        .not_empty(&events)?;

    assert_eq!(events[0].source.as_str(), source);
    assert_eq!(events[0].event_type.as_str(), event_type);
    Ok(())
}
```

---

## Integration Test with Multiple Events

Test workflows with multiple related events.

```rust
use sinex_core::{EventSource, JsonValue};
use sinex_test_utils::{sinex_test, TestContext, TestResult};
use serde_json::json;

#[sinex_test]
async fn test_multi_event_workflow(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    // Create sequence of related events
    let file_event = ctx.publish_event(
        "fs-watcher",
        "file.created",
        json!({
            "path": "/data/report.pdf",
            "size": 2048
        }),
    ).await?;

    let cmd_event = ctx.publish_event(
        "terminal",
        "command.executed",
        json!({
            "command": "process-report /data/report.pdf",
            "exit_code": 0
        }),
    ).await?;

    // Wait for events to be persisted
    ctx.timing().wait_for_event_count(2).await?;

    // Query all events
    let all_events = ctx.pool.events()
        .count_all()
        .await?;

    ctx.assert("workflow")
        .that(all_events >= 2, "should have at least 2 events")?;

    // Verify ordering via ULID timestamps
    ctx.assert("event ordering")
        .that(
            file_event.id.as_ref().map(|id| id.as_ulid().timestamp())
                < cmd_event.id.as_ref().map(|id| id.as_ulid().timestamp()),
            "file event should precede command event",
        )?;

    Ok(())
}
```

---

## Property Test with Generated Inputs

Fuzz test with randomly generated valid inputs.

```rust
use sinex_test_utils::{sinex_test, TestContext, TestResult};
use proptest::prelude::*;
use serde_json::json;

// Define strategy locally
fn file_path_strategy() -> BoxedStrategy<String> {
    prop_oneof![
        Just("/tmp/test.txt".to_string()),
        Just("/home/user/document.pdf".to_string()),
        "/[a-z0-9/._-]{1,100}\\.[a-z]{1,5}".prop_map(|s| s.to_string()),
    ]
    .boxed()
}

fn filesystem_event_strategy() -> BoxedStrategy<(String, String, serde_json::Value)> {
    (
        Just("fs-watcher".to_string()),
        prop_oneof![
            Just("file.created".to_string()),
            Just("file.modified".to_string()),
            Just("file.deleted".to_string()),
        ],
        (file_path_strategy(), any::<u64>()).prop_map(|(path, size)| json!({
            "path": path,
            "size": size,
            "modified_time": "2025-01-01T00:00:00Z"
        })),
    )
    .boxed()
}

#[sinex_prop(cases = 64)]
async fn property_filesystem_events_roundtrip(
    ctx: &TestContext,
    #[strategy(filesystem_event_strategy())] event: (String, String, serde_json::Value),
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (source, event_type, payload) = event;

    let inserted = ctx.publish_event(&source, &event_type, payload.clone()).await?;

    let fetched = ctx.pool.events()
        .get_by_source(&EventSource::from(source.clone()), Some(10), None)
        .await?;

    prop_assert!(!fetched.is_empty(), "should retrieve inserted event");
    prop_assert_eq!(inserted.payload, payload, "payload should match");
    Ok(())
}
```

---

## Error Handling Test

Test expected failure cases.

```rust
use sinex_test_utils::{sinex_test, TestContext, TestResult};
use serde_json::json;

#[sinex_test]
async fn test_invalid_event_source(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    // Empty source should fail validation
    let result = ctx.publish_event(
        "",  // Invalid: empty source
        "valid.type",
        json!({"data": "value"}),
    ).await;

    assert!(result.is_err(), "empty source should be rejected");

    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("source") || err.to_string().contains("empty"),
        "error should mention validation failure"
    );

    Ok(())
}
```

---

## Concurrent Operations Test

Test system behavior under concurrent load.

```rust
use sinex_test_utils::{sinex_test, TestContext, TestResult};
use std::sync::Arc;
use tokio::time::Duration;
use serde_json::json;

#[sinex_test]
async fn test_concurrent_ingestion(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    const TASKS: usize = 5;
    const EVENTS_PER_TASK: usize = 10;

    let barrier = Arc::new(tokio::sync::Barrier::new(TASKS));
    let mut handles = vec![];

    for task_id in 0..TASKS {
        let barrier_clone = barrier.clone();
        let ctx_clone = ctx.clone();

        let handle = tokio::spawn(async move {
            // Wait for all tasks to be ready
            barrier_clone.wait().await;

            // Each task publishes events
            for i in 0..EVENTS_PER_TASK {
                ctx_clone.publish_event(
                    &format!("concurrent-task-{task_id}"),
                    "concurrent.test",
                    json!({"task": task_id, "index": i}),
                ).await?;
            }

            Ok::<_, sinex_core::Error>(())
        });

        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle.await??;
    }

    // Verify all events were persisted
    let expected_total = TASKS * EVENTS_PER_TASK;
    ctx.timing().wait_for_event_count(expected_total).await?;

    let total = ctx.pool.events().count_all().await?;
    ctx.assert("concurrent ingestion")
        .eq(&total, &expected_total)?;

    Ok(())
}
```

---

## Timing and Synchronization Test

Test background tasks with deterministic synchronization.

```rust
use sinex_test_utils::{sinex_test, TestContext, TestResult};
use tokio::time::Duration;
use serde_json::json;

#[sinex_test]
async fn test_background_processing(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let sync = ctx.timing().synchronizer(Duration::from_secs(5));

    // Spawn background task
    let sync_clone = sync.clone();
    let ctx_clone = ctx.clone();
    tokio::spawn(async move {
        // Simulate background work
        ctx_clone.publish_event(
            "background-worker",
            "work.completed",
            json!({"status": "done"}),
        ).await?;

        // Signal completion
        sync_clone.signal();
        Ok::<_, sinex_core::Error>(())
    });

    // Wait for background task to signal
    sync.wait().await?;

    // Verify background work completed
    let events = ctx.pool.events()
        .get_by_source(&EventSource::from("background-worker"), Some(10), None)
        .await?;

    ctx.assert("background task")
        .not_empty(&events)?;

    Ok(())
}
```

---

## Pipeline Scope Test (L2-L4)

Test with isolated pipeline namespace for stream/consumer isolation.

```rust
use sinex_test_utils::{sinex_test, TestContext, TestResult};
use serde_json::json;

#[sinex_test]
async fn test_pipeline_scope(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline_scope().await?;

    // Publish via pipeline scope (isolated namespace)
    scope.publish(
        "fs-watcher",
        "file.created",
        json!({"path": "/tmp/scoped"}),
    ).await?;

    // Wait for event to be persisted
    scope.wait_for_event_count(1).await?;

    // Verify via database
    let events = ctx.pool.events()
        .get_by_source(&EventSource::from("fs-watcher"), Some(10), None)
        .await?;

    ctx.assert("pipeline scope")
        .not_empty(&events)?;

    Ok(())
}
```

---

## Event Ordering Verification Test

Verify temporal ordering using ULID timestamps.

```rust
use sinex_test_utils::{sinex_test, TestContext, TestResult};
use tokio::time::Duration;
use serde_json::json;

#[sinex_test]
async fn test_event_temporal_ordering(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    let first = ctx.publish_event(
        "timeline",
        "event.first",
        json!({"sequence": 1}),
    ).await?;

    // Small delay to ensure different ULID timestamps
    tokio::time::sleep(Duration::from_millis(10)).await;

    let second = ctx.publish_event(
        "timeline",
        "event.second",
        json!({"sequence": 2}),
    ).await?;

    tokio::time::sleep(Duration::from_millis(10)).await;

    let third = ctx.publish_event(
        "timeline",
        "event.third",
        json!({"sequence": 3}),
    ).await?;

    // Verify ULID ordering
    let first_ts = first.id.as_ref().unwrap().as_ulid().timestamp();
    let second_ts = second.id.as_ref().unwrap().as_ulid().timestamp();
    let third_ts = third.id.as_ref().unwrap().as_ulid().timestamp();

    ctx.assert("temporal ordering")
        .that(first_ts < second_ts, "first < second")?
        .that(second_ts < third_ts, "second < third")?;

    Ok(())
}
```

---

## Snapshot Test

Use insta for deterministic snapshot comparisons.

```rust
use sinex_test_utils::{sinex_test, TestContext, TestResult};
use serde_json::json;

#[sinex_test]
async fn test_event_snapshot(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    let event = ctx.publish_event(
        "snapshot-test",
        "test.snapshot",
        json!({
            "field1": "value1",
            "field2": 42,
            "nested": {
                "key": "data"
            }
        }),
    ).await?;

    // Snapshot the payload (ID will change, so only check payload)
    insta::assert_json_snapshot!(event.payload, {
        // Optional: redact dynamic fields
        ".timestamp" => "[timestamp]",
    });

    Ok(())
}
```

---

## Custom Typed Event Test

Use typed payloads instead of dynamic JSON (when schema is known).

```rust
use sinex_core::{Event, EventSource, EventType};
use sinex_test_utils::{sinex_test, TestContext, TestResult};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct FileCreatedPayload {
    path: String,
    size: u64,
    mime_type: String,
}

#[sinex_test]
async fn test_typed_event(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    let payload = FileCreatedPayload {
        path: "/tmp/test.txt".to_string(),
        size: 1024,
        mime_type: "text/plain".to_string(),
    };

    // Create typed event
    let event = Event::new(
        EventSource::from("fs-watcher"),
        EventType::from("file.created"),
        payload.clone(),
    )?;

    // Insert via repository
    let inserted = ctx.pool.events().insert(event).await?;

    // Type safety at query time
    let fetched: Event<FileCreatedPayload> = ctx.pool.events()
        .get_by_id(inserted.id.as_ref().unwrap())
        .await?
        .expect("event should exist");

    ctx.assert("typed event")
        .eq(&fetched.payload.path, &payload.path)?
        .eq(&fetched.payload.size, &payload.size)?;

    Ok(())
}
```

---

## Best Practices Summary

### DO
- ✅ Use `ctx.with_nats().shared().await?` before creating events (pipeline-first)
- ✅ Use `ctx.publish_event()` for simple test events
- ✅ Use `ctx.timing().wait_for_event_count()` instead of `sleep()`
- ✅ Use ULID timestamps for event ordering assertions
- ✅ Define property test strategies locally in the test module
- ✅ Use `ctx.assert()` for fluent, contextual assertions
- ✅ Test both success and error cases

### DON'T
- ❌ Don't use `tokio::time::sleep()` for synchronization (use `ctx.timing()` helpers)
- ❌ Don't directly insert into DB without pipeline (bypass ingestd)
- ❌ Don't mock production types (Event, EventSource, etc.)
- ❌ Don't assume event ordering without ULID verification
- ❌ Don't ignore cleanup (trust Drop trait and TestContext)
- ❌ Don't use `std::thread::sleep()` (blocks executor)

### Pipeline-First Rule
Before seeding any events, call:
```rust
let ctx = ctx.with_nats().shared().await?;
```

Then use `ctx.publish_event(...)` so every test exercises the actual ingestion path (NATS → ingestd → DB).

Direct database fabrication bypasses ingestd and should be avoided.

---

## Quick Reference

| Pattern | Template |
|---------|----------|
| Basic unit test | [Basic Unit Test](#basic-unit-test) |
| Multiple test cases | [Parameterized Test](#parameterized-test-rstest) |
| Multi-event workflow | [Integration Test](#integration-test-with-multiple-events) |
| Fuzz testing | [Property Test](#property-test-with-generated-inputs) |
| Error cases | [Error Handling](#error-handling-test) |
| Concurrent operations | [Concurrent Operations](#concurrent-operations-test) |
| Background tasks | [Timing and Synchronization](#timing-and-synchronization-test) |
| Pipeline isolation | [Pipeline Scope](#pipeline-scope-test-l2-l4) |
| Event ordering | [Event Ordering](#event-ordering-verification-test) |
| Snapshot comparison | [Snapshot Test](#snapshot-test) |
| Typed payloads | [Custom Typed Event](#custom-typed-event-test) |

For comprehensive documentation, see:
- [`TEST_PATTERNS.md`](/realm/project/sinex/docs/current/testing/TEST_PATTERNS.md)
- [`TEST_PATTERNS_GUIDE.md`](/realm/project/sinex/docs/current/testing/TEST_PATTERNS_GUIDE.md)
