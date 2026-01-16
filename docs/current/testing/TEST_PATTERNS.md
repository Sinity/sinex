# Sinex Test Patterns and Best Practices

A comprehensive guide to reusable test patterns extracted from the sinex-test-utils crate.

> Pipeline-first rule: before seeding any events, call `let ctx = ctx.with_nats().await?;` (or
> `with_shared_nats()`) and use `ctx.publish_json_event(...)` so every test exercises the actual
> ingestion path. Direct database fabrication helpers (formerly `ctx.db_only()`) have been
> removed to avoid bypassing ingestd.

---

## 1. Database Test Patterns

### 1.1 Isolated Database Acquisition from Pool

**Pattern**: Each test gets a clean, isolated database from the pool with advisory lock protection.

```rust
// TestDatabase struct provides automatic cleanup via Drop
pub struct TestDatabase {
    name: String,
    pool: DbPool,
    slot: Arc<DatabaseSlot>,
    lock_id: i64,
    acquired_at: Instant,
    acquisition_process_id: u32,
}

// Acquisition with automatic cleanup on drop
let test_db = acquire_test_database().await?;
// Database is automatically returned to pool when test_db is dropped
```

**Best Practices**:
- Use advisory locks for inter-process coordination
- Pool size defaults to 2× Nextest test threads, minimum 64, and shrinks if Postgres
  `max_connections` would be exceeded
- Automatic cleanup with background manager to avoid blocking
- Slot pools cap at 4 connections; admin pool caps at 8

**Key Files**:
- `/realm/project/sinex/crate/lib/sinex-test-utils/src/database_pool.rs` (1790+ lines)

---

### 1.2 Transaction Rollback Patterns

**Pattern**: Automatic cleanup between tests via database cleaning, not transactions.

```rust
// Clean database for reuse
async fn clean_database(pool: &DbPool, db_name: &str) -> Result<()> {
    // Uses shared db_common implementation
    crate::db_common::reset_database(pool).await?;
    crate::db_common::verify_clean_state(pool).await?;
}

// Database cleanup verified before test starts
let baseline_events = pool.events().count_all().await?;
```

**Best Practices**:
- Call `verify_clean_state()` to ensure database is ready
- Track baseline event count for delta calculations
- For checkpoint state, purge the NATS KV entries (e.g., via test helpers) instead of modifying database constraints.
- Retry cleanup once on failure before giving up

**Key Functions**:
- `reset_database()` - clears all data
- `verify_clean_state()` - confirms all tables empty
- `get_row_counts()` - diagnostic inspection

---

### 1.3 Fixture Insertion Patterns

**Pattern**: Use pipeline-first approach (`ctx.publish_json_event`) to exercise the full ingestion
path. Direct repository access is available for rare cases where pipeline isolation isn't needed.

```rust
// PREFERRED: Pipeline-first approach (exercises NATS → ingestd → DB)
let event = ctx.publish_json_event(
    "fs-watcher",
    "file.created",
    json!({"path": "/test/file.txt", "size": 1024})
).await?;

// ALTERNATIVE: Direct repository access (for unit tests that don't need pipeline)
let event = Event::<JsonValue>::test_event(
    "fs-watcher",
    "file.created",
    json!({"path": "/test/file.txt", "size": 1024})
);
ctx.pool.events().insert(event).await?;

// Batch insertion via pipeline
let events = vec![
    ("fs-watcher", "file.changed", json!({"path": "/test.file"})),
    ("terminal", "command.executed", json!({"command": "ls"})),
];
for (source, event_type, payload) in events {
    ctx.publish_json_event(source, event_type, payload).await?;
}
```

**Key Pattern**:
- Always use `ctx.with_shared_nats().await?` before creating events
- `ctx.publish_json_event()` exercises the real ingestion pipeline
- Direct repository access (`ctx.pool.events().insert()`) bypasses pipeline (use sparingly)
- Payload sanitization and source material registration handled automatically

---

### 1.4 Migration Test Patterns

**Pattern**: Template database created once per process, reused for all tests.

```rust
// Template is created with all migrations applied
async fn ensure_template_database(
    admin_url: &str,
    base_url: &str,
    slot_max_connections: u32,
) -> Result<String>

// Template creation checked via fingerprint of migrations
fn migrations_fingerprint() -> Option<String> {
    // Hashes all files in migrations/ directory
    // Invalidates cache when migrations change
}

// Test databases created from template via PostgreSQL cloning
CREATE DATABASE test_db WITH TEMPLATE template_db
```

**Benefits**:
- Migrations run once per process, not per test
- Test databases created via fast cloning (seconds vs minutes)
- Extension versions tracked for drift detection
- Automatic recreation on migration changes

**Cache Invalidation**:
- Stored in `target/sinex-test-utils/template_stamp.json` (managed automatically; delete only when debugging a corrupted local template)
- Detects extension version changes
- Checks for required schema elements
- Rebuilds if TimescaleDB version changes

---

## 2. Async Test Patterns

### 2.1 Test Macro with Automatic Context Creation

**Pattern**: Use `#[sinex_test]` macro for automatic TestContext lifecycle.

```rust
#[sinex_test]
async fn test_example(ctx: TestContext) -> Result<()> {
    // ctx is automatically created
    let event = ctx.publish_json_event("source", "type", json!({})).await?;
    // ctx is automatically cleaned up
    Ok(())
}

// With rstest parameterization
#[sinex_test]
#[case("source1", "type1")]
#[case("source2", "type2")]
async fn test_parameterized(
    ctx: TestContext,
    #[case] source: &str,
    #[case] event_type: &str,
) -> Result<()> {
    Ok(())
}

// With custom timeout
#[sinex_test(timeout = 60)]
async fn test_long_running(ctx: TestContext) -> Result<()> {
    // 60-second timeout
    Ok(())
}
```

**Macro Features**:
- Default timeout: 30s for async, 10s for sync
- Automatic proptest integration
- Rstest case parameterization
- Optional tracing: `#[sinex_test(trace = true)]`

---

### 2.2 Concurrent Test Execution

**Pattern**: Pool-based isolation enables safe concurrent tests.

```rust
#[sinex_test]
async fn test_concurrent_test_execution(
    ctx: TestContext
) -> Result<()> {
    const TASKS: usize = 5;
    let barrier = Arc::new(tokio::sync::Barrier::new(TASKS));
    let mut handles = vec![];

    for i in 0..TASKS {
        let barrier_clone = barrier.clone();
        let handle = tokio::spawn(async move {
            let ctx = TestContext::with_name(&format!("concurrent_{i}"))
                .await?;

            // Synchronize all tasks to start at same time
            barrier_clone.wait().await;

            // Each performs operations
            for j in 0..10 {
                ctx.publish_json_event(
                    &format!("task_{i}"),
                    "concurrent.test",
                    json!({"iteration": j}),
                ).await?;
            }

            // Wait for flushes with retry loop
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

    // Wait for all to complete
    for handle in handles {
        handle.await??;
    }
    Ok(())
}
```

**Key Patterns**:
- Use barriers to synchronize task starts
- Each task gets own TestContext (separate DB)
- Polling with exponential backoff for flushes
- Never hardcode waits - use polling

---

### 2.3 Cleanup and Teardown Patterns

**Pattern**: Automatic cleanup via Drop trait and background manager.

```rust
impl Drop for TestDatabase {
    fn drop(&mut self) {
        let lock_id = self.lock_id;

        // Safe, non-blocking cleanup via background manager
        CLEANUP_MANAGER.schedule_cleanup(CleanupTask {
            lock_id,
            pool: self.pool.clone(),
            slot_name: self.name.clone(),
        });

        // Immediately return slot to available
        self.slot.in_use.store(false, Ordering::Release);
    }
}

// Force cleanup available for tests that need it
pub async fn force_cleanup(&self) -> Result<()> {
    self.db.force_cleanup().await
}
```

**Background Cleanup**:
- Removes advisory lock
- Closes connection pool
- Non-blocking so Drop finishes immediately
- Timeout: 5s for lock release, 2s for pool close

---

### 2.4 Pipeline Harness Concurrency Guard

**Pattern**: A process-wide semaphore caps how many PipelineHarness instances run in parallel, preventing JetStream-heavy suites from starving ingestd.

- Default limit scales with CPU count (`available_parallelism / 6`, clamped to 1..6).
- Additional pipeline tests wait for a permit instead of hitting the 30 s timeout, keeping DLQ/e2e suites deterministic.
- Permits are released automatically when `scope.shutdown()` completes or when the harness drops during panic unwinding.

Use this guard before increasing individual test timeouts when `jetstream_dlq_test` or `jetstream_e2e_integration_test` exceed 30 s.

---

## 3. Property Test Patterns

### 3.1 Event-Focused Strategies

**Pattern**: Define proptest strategies locally in the test module to keep scope tight.

```rust
fn file_path_strategy() -> BoxedStrategy<String> {
    prop_oneof![
        Just("/tmp/test.txt".to_string()),
        Just("/home/user/document.pdf".to_string()),
        "/[a-z0-9/._-]{1,100}\\.[a-z]{1,5}".prop_map(|s| s.to_string()),
    ]
    .boxed()
}

fn event_source_strategy() -> BoxedStrategy<String> {
    prop_oneof![
        Just("filesystem".to_string()),
        Just("shell.kitty".to_string()),
        "[a-z][a-z0-9._]*".prop_map(|s| s.to_string()),
    ]
    .boxed()
}

fn json_payload_strategy() -> BoxedStrategy<Value> {
    let leaf = prop_oneof![
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        ".*".prop_map(Value::from),
    ];

    leaf.prop_recursive(
        8,   // max depth
        256, // max nodes
        10,  // max items per collection
        |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..10).prop_map(Value::from),
                prop::collection::hash_map(".*", inner, 0..10)
                    .prop_map(|map| Value::from(map.into_iter().collect::<serde_json::Map<_, _>>())),
            ]
        },
    )
    .boxed()
}

fn filesystem_event_strategy() -> BoxedStrategy<(String, String, Value)> {
    (
        Just("filesystem".to_string()),
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
```

**Key Characteristics**:
- Strategies generate valid test data
- Deterministic with proptest runners
- Domain-aware (filenames, commands, etc)
- Include malicious inputs for security testing

---

### 3.2 Macro Harness (`#[sinex_prop]` / `sinex_proptest!`)

**Pattern**: Let the macros drive proptest with deterministic runners, optional
`TestContext`, and progress output. They support cases/timeout/seed overrides
and automatically persist failing seeds to `target/proptest-regressions/`.

```rust
#[sinex_prop(cases = 64, timeout = "45s", seed = 1337)]
async fn filesystem_property(
    ctx: &TestContext,
    #[strategy(filesystem_event_strategy())] event: (String, String, Value),
) -> TestResult<()> {
    let (source, ty, payload) = event;
    let inserted = ctx.publish_json_event(&source, &ty, payload).await?;
    assert_eq!(inserted.source.as_str(), source);
    Ok(())
}

sinex_proptest! {
    fn ulid_roundtrip(value in json_payload_strategy()) -> TestResult<()> {
        let text = value.to_string();
        let decoded: Value = serde_json::from_str(&text)?;
        prop_assert_eq!(decoded, value);
        Ok(())
    }
}
```

**When to use**:
- Tests that need `TestContext` + database access
- Want cases/seed/timeout overrides with no harness boilerplate
- Need snapshots/log capture if a case fails

**Environment overrides**:
- `SINEX_PROPTEST_CASES` – force a runner case count (CI can raise to 1024+)
- `SINEX_PROPTEST_SEED` – replay a recorded failure deterministically
- `SINEX_PROPTEST_DIR` – override the default `target/proptest-regressions` path
- `SINEX_TEST_FAIL_DIR` – path for JSON failure artifacts (default `target/test-artifacts/`)

**Predefined Properties**
- Event creation works for all valid inputs
- Inserted events are retrievable by ID, source, type
- Malicious inputs are safely handled
- Event relationships preserved

---

### 3.3 Custom Generators (ULIDs, Events)

**Pattern**: Use production types directly, leverage dataset seeding helpers for complex objects.

```rust
// ULID generation via production API
let id = Id::<Event<JsonValue>>::new();
let ulid = id.as_ulid();

// Event generation with test helper
let event = Event::<JsonValue>::test_event(
    source.as_ref(),
    event_type.as_ref(),
    sanitized_payload,
);

// Source material registration (for FK constraints)
pub async fn ensure_source_material(
    &self,
    id: Id<SourceMaterial>,
    source_identifier: Option<&str>,
) -> Result<()> {
    let material_ulid_uuid = id.to_uuid();
    let identifier = source_identifier.unwrap_or_else(|| {
        if id.to_string() == BOOTSTRAP_MATERIAL_ID {
            BOOTSTRAP_MATERIAL_IDENTIFIER.to_string()
        } else {
            format!("test-material-{id}")
        }
    });

    sqlx::query!(
        r#"
            INSERT INTO raw.source_material_registry 
                (id, material_kind, source_identifier, status, timing_info_type)
            VALUES ($1::uuid::ulid, $2, $3, $4, $5)
            ON CONFLICT (id) DO NOTHING
        "#,
        material_ulid_uuid,
        "annex",
        identifier,
        "completed",
        "realtime"
    )
    .execute(&self.pool)
    .await?;

    Ok(())
}
```

---

## 4. Integration Test Patterns

### 4.1 Service Startup/Shutdown

**Pattern**: TestIngestdHandle manages service lifecycle.

```rust
pub async fn start_test_ingestd_with_config(
    config: TestIngestdConfig,
    ctx: Option<&TestContext>,
) -> Result<TestIngestdHandle> {
    // Starts ingestd service for integration testing
    // Returns handle that stops service on drop
}

pub struct TestIngestdHandle {
    // Service state and cleanup
}

impl Drop for TestIngestdHandle {
    fn drop(&mut self) {
        // Shutdown service
    }
}

// Usage
#[sinex_test]
async fn test_ingestd_integration(ctx: TestContext) -> Result<()> {
    let ingestd_handle = start_test_ingestd_with_config(
        TestIngestdConfig::default(),
        Some(&ctx),
    ).await?;
    
    // Service is running
    // ...
    
    // Automatic cleanup on handle drop
    Ok(())
}
```

---

### 4.2 Service Client Setup

**Pattern**: Use the SDK transport or NATS clients; there are no gRPC clients.

```rust
// Services publish events/materials over JetStream
// Clients use the SDK or direct NATS publishers

let publisher = runtime.transport().nats_publisher()?;
publisher.publish_event(event).await?;
```

---

### 4.3 NATS Connection Management

**Pattern**: EphemeralNats provides temporary NATS server.

```rust
pub struct EphemeralNats {
    // Manages temporary NATS server
    // Automatic cleanup on drop
}

// Usage
#[sinex_test]
async fn test_with_nats(ctx: TestContext) -> Result<()> {
    let nats = EphemeralNats::new().await?;
    // NATS server is running
    // ...
    // Automatic cleanup
    Ok(())
}
```

---

### 4.4 End-to-End Flow Validation

**Pattern**: Complete workflow from event capture to query.

```rust
#[sinex_test]
async fn test_complete_workflow(ctx: TestContext) -> Result<()> {
    // 1. Create events using production event creation
    let fs_event = ctx.publish_json_event(
        "fs-watcher",
        "file.created",
        json!({"path": "/data/report.pdf", "size": 2048}),
    ).await?;

    let term_event = ctx.publish_json_event(
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

**Key Elements**:
- Real event creation (not mocks)
- Direct repository queries
- Timing utilities for coordination
- Contextual assertions with ordering checks

---

## 5. Assertion Patterns

### 5.1 Custom Assertion Helpers

**Pattern**: ContextualAssert provides fluent assertion API.

```rust
pub struct ContextualAssert<'ctx> {
    ctx: &'ctx TestContext,
    context: String,
}

impl<'ctx> ContextualAssert<'ctx> {
    /// Assert two values are equal
    pub fn eq<T: Debug + PartialEq>(
        self,
        left: &T,
        right: &T,
    ) -> Result<Self> { }

    /// Assert a condition is true
    pub fn that(self, condition: bool, message: &str) -> Result<Self> { }

    /// Assert collection is not empty
    pub fn not_empty<T>(self, collection: &[T]) -> Result<Self> { }

    /// Assert collection has specific size
    pub fn has_size<T>(self, collection: &[T], expected_size: usize) -> Result<Self> { }

    /// Assert option is Some
    pub fn some<T>(self, option: &Option<T>) -> Result<Self> { }

    /// Assert option is None
    pub fn none<T>(self, option: &Option<T>) -> Result<Self> { }

    /// Assert result contains error
    pub fn error_contains<T, E: Display>(
        self,
        result: &Result<T, E>,
        expected_error: &str,
    ) -> Result<Self> { }
}

// Usage
ctx.assert("database validation")
    .eq(&actual_count, &expected_count)?
    .that(!events.is_empty(), "should have events")?
    .has_size(&events, 5)?;

// Chainable for multiple assertions
ctx.assert("complex check")
    .some(&optional_value)?
    .that(
        value_ref.as_ref() > 0,
        "value must be positive",
    )?;
```

---

### 5.2 Error Matching Patterns

**Pattern**: Explicit error matching without helper traits.

```rust
let result = some_operation().await;
let err = result.expect_err("expected failure");
assert!(err.to_string().contains("validation"));

// If you need to inspect variants:
match err {
    SinexError::Validation { .. } => { /* expected */ }
    other => panic!("unexpected error: {other}"),
}
```

---

### 5.3 Snapshot Testing

**Pattern**: Insta snapshots for deterministic comparisons.

```rust
// Macro-based with automatic naming
#[sinex_test]
async fn test_event_snapshot(ctx: TestContext) -> Result<()> {
    let event = ctx.publish_json_event(
        "snapshot-test",
        "test.snapshot",
        json!({"key": "value"}),
    ).await?;

    insta::assert_json_snapshot!(event);
    Ok(())
}

// Custom snapshot naming
#[sinex_test]
async fn test_custom_snapshot(ctx: TestContext) -> Result<()> {
    let data = some_complex_data();
    
    ctx.assert_inline_snapshot(&data);
    // or
    insta::assert_yaml_snapshot!("custom_name", data);
    
    Ok(())
}
```

**Features**:
- Auto-generated baseline snapshots
- Update mode: `INSTA_UPDATE=always cargo xtask test --profile reliable --prime -- -p sinex-test-utils`
- YAML, JSON, debug, and inline formats
- Git-friendly diffs for review

---

### 5.4 Temporal Assertions (Event Ordering)

**Pattern**: Use ULID timestamps for event ordering verification.

```rust
#[sinex_test]
async fn test_event_ordering(ctx: TestContext) -> Result<()> {
    // Create events in sequence
    let first = ctx.publish_json_event(
        "timeline",
        "event.first",
        json!({"seq": 1}),
    ).await?;

    tokio::time::sleep(Duration::from_millis(10)).await;

    let second = ctx.publish_json_event(
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

**Key Pattern**:
- ULIDs provide monotonic ordering
- Never rely on wall-clock time
- Use event ID timestamps for assertions
- Test utilities provide timing helpers

---

## 6. Reusable Fixtures

### 6.1 TestContext Fixtures

**Pattern**: AsyncTest contexts for database access.

```rust
#[fixture]
pub async fn test_context_fixture() -> TestContext {
    TestContext::new()
        .await
        .expect("Failed to create test context")
}

#[fixture]
pub async fn test_context_with_tracing() -> TestContext {
    TestContext::new()
        .await
        .expect("Failed to create test context")
        .with_tracing("debug")
}

// rstest usage
#[rstest]
async fn test_with_fixture(test_context_fixture: TestContext) -> Result<()> {
    // fixture automatically managed by rstest
    Ok(())
}
```

---

### 6.2 Rstest Fixtures

**Pattern**: Rstest fixtures for common test data.

```rust
#[fixture]
pub fn test_sources() -> Vec<&'static str> {
    vec!["fs-watcher", "terminal", "desktop", "system"]
}

#[fixture]
pub fn test_event_types() -> Vec<(&'static str, &'static str)> {
    vec![
        ("fs-watcher", "file.created"),
        ("fs-watcher", "file.modified"),
        ("terminal", "command.executed"),
        ("desktop", "window.focused"),
        ("system", "service.started"),
    ]
}

#[fixture]
pub fn test_paths() -> Vec<Utf8PathBuf> {
    vec![
        Utf8PathBuf::from("/tmp/test.txt"),
        Utf8PathBuf::from("/home/user/document.pdf"),
        Utf8PathBuf::from("/var/log/system.log"),
        Utf8PathBuf::from("/opt/app/config.toml"),
    ]
}

// Usage with rstest
#[rstest]
async fn test_all_sources(
    #[from(test_sources)] sources: Vec<&str>,
    ctx: TestContext,
) -> Result<()> {
    for source in sources {
        ctx.publish_json_event(source, "test.type", json!({})).await?;
    }
    Ok(())
}
```

---

## 7. Best Practices

### 7.1 DO: Use Production APIs

- Use `Event::<JsonValue>::test_event()` directly
- Use repository methods from `ctx.pool.events()`
- Trust production validation and error handling

### 7.2 DO: Isolate Test Databases

- Each test gets its own database via pool
- Advisory locks prevent cross-process interference
- Automatic cleanup via Drop trait

### 7.3 DO: Use Timing Utilities for Coordination

```rust
ctx.timing().wait_for_event_count(expected).await?;
```

- Prefer polling over sleeps
- Use exponential backoff
- Respect timing metadata

### 7.4 DO: Test Error Cases Explicitly

```rust
let result = ctx.publish_json_event("", "valid.type", json!({})).await;
assert!(result.is_err());
```

### 7.5 DO: Use `#[sinex_prop]` / `sinex_proptest!` For Edge Cases

```rust
#[sinex_prop(cases = 64)]
async fn fuzz_create(
    ctx: &TestContext,
    #[strategy(filesystem_event_strategy())] event: (String, String, Value),
) -> TestResult<()> {
    let (source, ty, payload) = event;
    ctx.publish_json_event(&source, &ty, payload).await?;
    Ok(())
}

sinex_proptest! {
    fn ulid_roundtrip(value in ulid_strategy()) -> TestResult<()> {
        let encoded = value.to_string();
        let decoded = Ulid::from_string(&encoded)?;
        prop_assert_eq!(decoded, value);
        Ok(())
    }
}
```

### 7.6 DON'T: Hardcode Sleeps

Instead use timing utilities:
```rust
// Bad
tokio::time::sleep(Duration::from_millis(100)).await;

// Good
ctx.timing().wait_for_event_count(1).await?;
```

### 7.7 DON'T: Mock Production Types

- Always use real Event, EventSource, etc.
- Use `Event::new` or explicit helpers for complex objects
- Trust production APIs

### 7.8 DON'T: Assume Event Ordering

- Always verify with ULID timestamps
- Never rely on insertion order
- Use temporal assertions

### 7.9 DON'T: Ignore Cleanup

- Let TestContext drop automatically
- Use force_cleanup() only for diagnostics
- Trust background cleanup manager

### 7.10 DON'T: Skip Error Testing

- Test both success and failure paths
- Assert on error types or messages explicitly
- Verify error messages are meaningful

---

## 8. Common Patterns Summary

| Pattern | File | Key Struct | Usage |
|---------|------|-----------|-------|
| Database Isolation | database_pool.rs | TestDatabase | `acquire_test_database().await?` |
| Test Context | test_context.rs | TestContext | `#[sinex_test] async fn test(ctx: TestContext)` |
| Assertions | test_context.rs | ContextualAssert | `ctx.assert("msg").eq(&a, &b)?` |
| Properties | property_testing.rs | Local strategies | `#[sinex_prop]` / `sinex_proptest!` |
| Dataset Seeding | dataset_seeds.rs | EventSpec / SeedClock | `seed_events_via_db(&ctx, &clock, &specs)` |
| Service Management | node_management_utils.rs | TestIngestdHandle | `start_test_ingestd_with_config()` |
| NATS | nats.rs | EphemeralNats | `EphemeralNats::new().await?` |
| Macros | macros/src/lib.rs | #[sinex_test] | `#[sinex_test] async fn test()` |

---

## 9. Template Tests

### Complete Unit Test
```rust
#[sinex_test]
#[case("source1", "type1")]
#[case("source2", "type2")]
async fn test_unit_example(
    ctx: TestContext,
    #[case] source: &str,
    #[case] event_type: &str,
) -> Result<()> {
    // Arrange
    let event = ctx.publish_json_event(
        source,
        event_type,
        json!({"test_key": "test_value"}),
    ).await?;

    // Act
    let events = ctx.pool.events()
        .get_by_source(&EventSource::new(source.to_string()), Some(10), None)
        .await?;

    // Assert
    ctx.assert("unit test")
        .not_empty(&events)?
        .has_size(&events, 1)?;
    
    assert_eq!(events[0].source.as_str(), source);
    Ok(())
}
```

### Complete Integration Test
```rust
#[sinex_test]
async fn test_integration_example(ctx: TestContext) -> Result<()> {
    // Setup: Create test data
    for i in 0..5 {
        ctx.publish_json_event(
            "integration-test",
            "test.event",
            json!({"index": i}),
        ).await?;
    }

    // Execution: Perform operations
    let all_events = ctx.pool.events()
        .get_by_source(&EventSource::from("integration-test"), Some(100), None)
        .await?;

    // Verification: Assert results
    ctx.assert("integration test")
        .has_size(&all_events, 5)?;

    for (i, event) in all_events.iter().enumerate() {
        assert_eq!(event.payload["index"], json!(i));
    }

    Ok(())
}
```

### Complete Property Test
```rust
// See section 3.1 for strategy helpers used below.

#[sinex_prop(cases = 128, timeout = "60s")]
async fn property_events_roundtrip(
    ctx: &TestContext,
    #[strategy(filesystem_event_strategy())] event: (String, String, Value),
) -> TestResult<()> {
    let (source, ty, payload) = event;
    let inserted = ctx.publish_json_event(&source, &ty, payload.clone()).await?;
    let fetched = ctx
        .pool
        .events()
        .get_by_source(&EventSource::from(source.clone()), Some(10), None)
        .await?;

    prop_assert!(!fetched.is_empty());
    prop_assert_eq!(inserted.payload, payload);
    Ok(())
}

sinex_proptest! {
    #![cases = 64]
    #[timeout = "45s"]
    fn property_ulids_roundtrip(value in json_payload_strategy()) -> TestResult<()> {
        let body = value.to_string();
        let decoded: Value = serde_json::from_str(&body)?;
        prop_assert_eq!(decoded, value);
        Ok(())
    }
}
```

---

## 10. Troubleshooting

### Issue: "Database pool exhausted"
**Solution**: Reduce concurrent tests or raise PostgreSQL `max_connections`. The pool size is
derived from Nextest test threads (minimum 64) and will auto-shrink if the server cap is too low.
Use `cargo xtask test --profile fast` for fewer concurrent tests, or adjust
`.config/nextest.toml` if you need a lower ceiling.

### Issue: "Advisory lock timeout"
**Solution**: Database may be stuck, check system load
```bash
# Check PostgreSQL connections
psql -l
# Kill stuck backends
SELECT pg_terminate_backend(pid) FROM pg_stat_activity 
WHERE datname LIKE 'sinex_test%';
```

### Issue: "Migration fingerprint mismatch"
**Solution**: The harness automatically rebuilds the shared template whenever
migrations or required extensions change. Manual deletion should only be
necessary when debugging local Postgres issues:
```bash
# Only needed if the cached template is corrupted locally
rm target/sinex-test-utils/template_stamp.json
cargo xtask test --profile reliable --prime -- -p sinex-test-utils
```

### Issue: "Tests hang on cleanup"
**Solution**: Background cleanup manager is waiting, increase timeout
```bash
# Check cleanup process
ps aux | grep sqlx
# Allow more time for cleanup during shutdown
```

---

## 11. Timing & Synchronization Patterns

### 11.1 TestSynchronizer - Race-Free One-Shot Signals

**Pattern**: Deterministic wait points for background tasks without race conditions.

**Use Case**: Waiting for a background task to reach a specific state (e.g., checkpoint saved, leader elected, material finalized).

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

**Why This Matters**:
- No race conditions (unlike sleep + check loops)
- Fails fast on timeout with clear error
- Zero busy-waiting overhead

---

### 11.2 TestBarrier - Coordinating Multiple Concurrent Tasks

**Pattern**: Ensures N tasks all reach a synchronization point before proceeding.

**Use Case**: Thundering herd tests, concurrent access verification, coordinated writes.

**Mechanism**: Wraps `tokio::sync::Barrier` with timeout support.

```rust
use sinex_test_utils::timing_utils::TestBarrier;

#[sinex_test]
async fn test_concurrent_ingestion(ctx: TestContext) -> Result<()> {
    let barrier = ctx.timing().barrier(3);
    let timeout = Duration::from_secs(10);

    // Launch 3 concurrent ingestors
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

**Best Practices**:
- Use for concurrency stress tests
- Verify system behavior under simultaneous load
- Test lock contention and ordering guarantees

---

### 11.3 WaitHelpers - Adaptive Polling

**Pattern**: Wait for database state changes with minimal latency and no fixed sleeps.

**Why Not `tokio::time::sleep`**:
```rust
// ❌ BAD: Fixed sleep is either too short (flaky) or too long (slow)
tokio::time::sleep(Duration::from_millis(500)).await;
let events = ctx.pool.events().count().await?;
assert_eq!(events, 5);  // May fail if ingestion takes 600ms

// ✅ GOOD: Adaptive polling completes as soon as condition met
ctx.timing().wait_for_event_count(5).await?;
```

**Available Helpers**:

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

**Implementation Details**:
- Starts with 10ms intervals, backs off to 100ms
- Completes immediately when condition met
- Returns clear timeout error with last observed state
- CI-friendly: generous timeouts, fast completion on success

**Anti-Pattern to Avoid**:
```rust
// ❌ NEVER DO THIS
use std::thread;
thread::sleep(Duration::from_millis(100));  // Blocks executor!

// ❌ AVOID THIS
tokio::time::sleep(Duration::from_secs(1)).await;  // Fixed delay
```

**Always use adaptive polling instead**:
```rust
// ✅ DO THIS
ctx.timing().wait_for_event_count(n).await?;
```

---

### 11.4 Testing Processors With Optional Database Dependency

**Architecture** (as of Jan 2025):
- **Checkpoints**: ALWAYS stored in NATS KV (`KV_sinex_checkpoints`)
- **DATABASE_URL**: Optional dependency - only needed for processors that query events
- **`SINEX_EDGE_MODE=1`**: Not a "mode" - just suppresses DATABASE_URL requirement error + enables schema cache

**Database Dependency by Processor Type**:

| Type | Needs DATABASE_URL? | Example |
|------|---------------------|---------|
| **Ingestors** | ❌ No | fs-watcher, terminal-node, desktop-node |
| **Automata** | ✅ Usually yes | analytics-automaton, search-automaton, pkm-automaton |

**Why Automata Need DATABASE_URL**:
- Query historical events from `core.events` table
- Aggregate data across event streams
- Build derived views and indexes

**Why Ingestors Don't**:
- Only capture and publish events to NATS
- Never query the event database

**What `SINEX_EDGE_MODE=1` Actually Does**:
1. Suppresses configuration error when DATABASE_URL is missing
2. Enables schema broadcast cache (subscribes to `system.schemas.active` for validation)

**Testing Ingestors Without Database**:

```rust
#[sinex_test]
async fn test_ingestor_without_database(ctx: TestContext) -> Result<()> {
    // Set SINEX_EDGE_MODE to allow missing DATABASE_URL
    std::env::set_var("SINEX_EDGE_MODE", "1");
    std::env::remove_var("DATABASE_URL");

    let ctx = ctx.with_shared_nats().await?;

    // Initialize ingestor - works without DATABASE_URL
    let processor = MyIngestor::new(/* ... */);
    let runner = StreamProcessorRunner::new(/* ... */).await?;

    // Checkpoints work regardless (always NATS KV)
    let checkpoint = runner.current_checkpoint().await?;
    assert!(checkpoint.is_some());

    std::env::remove_var("SINEX_EDGE_MODE");
    Ok(())
}
```

**Testing Automata With Database**:

```rust
#[sinex_test]
async fn test_automaton_queries_events(ctx: TestContext) -> Result<()> {
    // DATABASE_URL present via TestContext
    let ctx = ctx.with_shared_nats().await?;

    let processor = MyAutomaton::new(/* ... */);
    let runner = StreamProcessorRunner::new(/* ... */).await?;

    // Automaton can query events via db_pool handle
    // ... processor queries core.events ...

    Ok(())
}
```

**Key Points**:
1. **There is no "checkpoint mode"** - checkpoints are always NATS KV
2. **`SINEX_EDGE_MODE` is not a mode** - it's a permission flag + schema cache enabler
3. **DATABASE_URL is just an optional dependency** - present or absent, everything else works the same
4. **Test coverage**: Verify ingestors work without DATABASE_URL; automata work with it

---

## 12. Performance Optimization

### Baseline Metrics
- Template creation: ~5-15 minutes (first run)
- Template reuse: <1 second per test database
- Database acquisition: <100ms typical
- Test execution: Parallelized via Nextest

### Optimization Strategies
1. **Template caching**: Leverage migrations fingerprint
2. **Pool sizing**: Keep Nextest test threads aligned with Postgres capacity
3. **Connection limits**: Keep slot pools small (4) and admin pool small (8)
4. **Minimal datasets**: Only create what you need
5. **Batch operations**: Insert events in groups when possible

### Monitoring
```rust
// Check pool health
let report = check_pool_health().await?;
println!("Healthy: {}/{}", report.healthy_slots, report.total_slots);

// Get statistics
let stats = get_pool_stats();
println!("Avg wait: {}ms", stats.average_wait_time_ms);
```
