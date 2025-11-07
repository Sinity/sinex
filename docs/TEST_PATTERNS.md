# Sinex Test Patterns and Best Practices

A comprehensive guide to reusable test patterns extracted from the sinex-test-utils crate.

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
- Pool size configurable via `SINEX_TESTUTILS_POOL_SIZE` environment variable
- Automatic cleanup with background manager to avoid blocking
- Connection limits tunable via `SINEX_TESTUTILS_CONN_BUDGET`

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
- Relax strict FKs that conflict with synthetic test IDs:
  ```sql
  ALTER TABLE core.processor_checkpoints 
    DROP CONSTRAINT IF EXISTS processor_checkpoints_last_processed_id_fkey
  ```
- Retry cleanup once on failure before giving up

**Key Functions**:
- `reset_database()` - clears all data
- `verify_clean_state()` - confirms all tables empty
- `get_row_counts()` - diagnostic inspection

---

### 1.3 Fixture Insertion Patterns

**Pattern**: Use production Event creation APIs with TestContext convenience methods.

```rust
// Direct production API - no wrappers
let event = Event::<JsonValue>::test_event(
    "fs-watcher",
    "file.created",
    json!({"path": "/test/file.txt", "size": 1024})
);

// Direct repository access via pool
ctx.pool.events().insert(event).await?;

// TestContext convenience for common patterns
let event = ctx.create_test_event(
    "fs-watcher",
    "file.created",
    json!({"path": "/test/file.txt", "size": 1024})
).await?;

// Batch insertion
let events = vec![/* Event instances */];
ctx.insert_events(&events).await?;
```

**Key Pattern**:
- Use production APIs directly when possible
- TestContext wrapper adds test-specific concerns (sanitization, provenance)
- Payload sanitization removes malicious patterns automatically
- Source material registration automatic for FK satisfaction

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
- Stored in `target/sinex-test-utils/template_stamp.json`
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
    let event = ctx.create_test_event("source", "type", json!({})).await?;
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
                ctx.create_test_event(
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

## 3. Property Test Patterns

### 3.1 Event-Focused Strategies

**Pattern**: Reusable proptest strategies for common Sinex types.

```rust
pub struct SinexStrategies;

impl SinexStrategies {
    /// Strategy for valid event sources
    pub fn event_source() -> BoxedStrategy<String> {
        prop_oneof![
            Just("filesystem".to_string()),
            Just("shell.kitty".to_string()),
            "[a-z][a-z0-9._]*".prop_map(|s| s.to_string()),
        ].boxed()
    }

    /// Strategy for valid event types
    pub fn event_type() -> BoxedStrategy<String> {
        prop_oneof![
            Just("file.created".to_string()),
            Just("file.modified".to_string()),
            "[a-z][a-z0-9._]*\\.[a-z][a-z0-9._]*".prop_map(|s| s.to_string()),
        ].boxed()
    }

    /// Strategy for JSON payloads
    pub fn json_payload() -> BoxedStrategy<Value> {
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
        ).boxed()
    }

    /// Domain-specific strategies
    pub fn filesystem_event() -> BoxedStrategy<(String, String, Value)> {
        (
            Just("filesystem".to_string()),
            prop_oneof![
                Just("file.created".to_string()),
                Just("file.modified".to_string()),
                Just("file.deleted".to_string()),
            ],
            (Self::file_path(), any::<u64>())
                .prop_map(|(path, size)| json!({
                    "path": path,
                    "size": size,
                    "modified_time": "2025-01-01T00:00:00Z"
                })),
        ).boxed()
    }

    /// Malicious input testing
    pub fn malicious_payload() -> BoxedStrategy<Value> {
        prop_oneof![
            // SQL injection attempts
            Just(json!({
                "path": "'; DROP TABLE events; --",
                "command": "$(rm -rf /)"
            })),
            // XSS attempts
            Just(json!({
                "content": "<script>alert('xss')</script>",
                "html": "<img src=x onerror=alert(1)>"
            })),
            // Path traversal
            Just(json!({
                "path": "../../etc/passwd",
                "file": "../../../root/.ssh/id_rsa"
            })),
        ].boxed()
    }
}
```

**Key Characteristics**:
- Strategies generate valid test data
- Deterministic with proptest runners
- Domain-aware (filenames, commands, etc)
- Include malicious inputs for security testing

---

### 3.2 Property Test Runner Integration

**Pattern**: PropertyTester struct bridges TestContext with proptest.

```rust
pub struct PropertyTester<'ctx> {
    ctx: &'ctx TestContext,
    runner: proptest::test_runner::TestRunner,
}

impl<'ctx> PropertyTester<'ctx> {
    pub fn new(ctx: &'ctx TestContext) -> Self {
        Self {
            ctx,
            runner: proptest::test_runner::TestRunner::deterministic(),
        }
    }

    /// Run property test with custom strategy
    pub async fn test_property<S, T, F, Fut>(
        &mut self,
        strategy: S,
        test_cases: u32,
        property: F,
    ) -> Result<()>
    where
        S: Strategy<Value = T>,
        F: Fn(&TestContext, T) -> Fut,
        Fut: std::future::Future<Output = Result<()>> + 'ctx,
        T: 'ctx,
    {
        for case_num in 0..test_cases {
            let tree = strategy.new_tree(&mut self.runner)
                .map_err(|e| SinexError::unknown(format!("Tree generation failed: {e:?}")))?;
            let value = tree.current();

            property(self.ctx, value).await
                .map_err(|e| SinexError::validation(format!("Case {case_num} failed: {e}")))?;
        }
        Ok(())
    }

    /// Predefined property tests
    pub async fn test_event_creation_property(&mut self, test_cases: u32) -> Result<()> {
        // Property: All valid events should be creatable
        // ...
    }

    pub async fn test_event_querying_property(&mut self, test_cases: u32) -> Result<()> {
        // Property: Inserted events should be retrievable
        // ...
    }

    pub async fn test_malicious_input_rejection(&mut self, test_cases: u32) -> Result<()> {
        // Property: Malicious payloads should be rejected or sanitized
        // ...
    }
}

// Usage in tests
#[sinex_test]
async fn test_with_properties(ctx: TestContext) -> Result<()> {
    let mut tester = ctx.property_tester();
    tester.test_event_creation_property(20).await?;
    Ok(())
}
```

**Predefined Properties**:
- Event creation works for all valid inputs
- Inserted events are retrievable by ID, source, type
- Malicious inputs are safely handled
- Event relationships preserved

---

### 3.3 Custom Generators (ULIDs, Events)

**Pattern**: Use production types directly, leverage fixtures for complex objects.

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
    config: TestIngestdConfig
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
        TestIngestdConfig::default()
    ).await?;
    
    // Service is running
    // ...
    
    // Automatic cleanup on handle drop
    Ok(())
}
```

---

### 4.2 gRPC Client Setup

**Pattern**: Clients created from live ingestd service.

```rust
// Services provide gRPC endpoints
// Clients connect to live services
// Automatic reconnection on transient failures

// Example pattern (service-specific)
let client = YourGrpcClient::connect(service_url).await?;
let response = client.your_method(request).await?;
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
    let fs_event = ctx.create_test_event(
        "fs-watcher",
        "file.created",
        json!({"path": "/data/report.pdf", "size": 2048}),
    ).await?;

    let term_event = ctx.create_test_event(
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

**Pattern**: ErrorAssertions trait for error testing.

```rust
pub trait ErrorAssertions<T> {
    /// Assert result contains specific error text
    fn assert_contains_error(self, text: &str) -> Result<T, SinexError>;

    /// Assert result is specific error type
    fn assert_error_type<E: Error + 'static + Send + Sync>(self) -> Result<T, SinexError>;

    /// Assert result fails with any error
    fn assert_fails(self) -> Result<SinexError, SinexError>;

    /// Assert result succeeds
    fn assert_succeeds(self) -> Result<T, SinexError>;
}

// Usage
let result = some_operation().await;
result
    .assert_contains_error("validation")?
    .assert_fails()?;

// Or with flow control
let error = some_operation()
    .await
    .assert_fails()?;
assert!(error.to_string().contains("expected message"));
```

---

### 5.3 Snapshot Testing

**Pattern**: Insta snapshots for deterministic comparisons.

```rust
// Macro-based with automatic naming
#[sinex_test]
async fn test_event_snapshot(ctx: TestContext) -> Result<()> {
    let event = ctx.create_test_event(
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
- Update mode: `INSTA_UPDATE=always cargo test`
- YAML, JSON, debug, and inline formats
- Git-friendly diffs for review

---

### 5.4 Temporal Assertions (Event Ordering)

**Pattern**: Use ULID timestamps for event ordering verification.

```rust
#[sinex_test]
async fn test_event_ordering(ctx: TestContext) -> Result<()> {
    // Create events in sequence
    let first = ctx.create_test_event(
        "timeline",
        "event.first",
        json!({"seq": 1}),
    ).await?;

    tokio::time::sleep(Duration::from_millis(10)).await;

    let second = ctx.create_test_event(
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

### 6.2 Data Fixtures

**Pattern**: Standard fixtures for common test data.

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
        ctx.create_test_event(source, "test.type", json!({})).await?;
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

### 7.4 DO: Test Error Cases with ErrorAssertions

```rust
result
    .assert_contains_error("expected message")?
    .assert_fails()?;
```

### 7.5 DO: Use Property Testing for Edge Cases

```rust
let mut tester = ctx.property_tester();
tester.test_event_creation_property(20).await?;
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
- Use builders for complex objects
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
- Use ErrorAssertions for consistency
- Verify error messages are meaningful

---

## 8. Common Patterns Summary

| Pattern | File | Key Struct | Usage |
|---------|------|-----------|-------|
| Database Isolation | database_pool.rs | TestDatabase | `acquire_test_database().await?` |
| Test Context | test_context.rs | TestContext | `#[sinex_test] async fn test(ctx: TestContext)` |
| Assertions | test_context.rs | ContextualAssert | `ctx.assert("msg").eq(&a, &b)?` |
| Properties | property_testing.rs | PropertyTester | `ctx.property_tester().test_*()` |
| Fixtures | fixtures.rs | UserSessionFixture | `#[fixture] pub fn fixture()` |
| Builders | builders.rs | TestCheckpointBuilder | `TestCheckpointBuilder::new()` |
| Error Testing | error_testing.rs | ErrorAssertions | `result.assert_fails()?` |
| Service Management | satellite_management_utils.rs | TestIngestdHandle | `start_test_ingestd_with_config()` |
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
    let event = ctx.create_test_event(
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
        ctx.create_test_event(
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
#[sinex_test]
async fn test_property_example(ctx: TestContext) -> Result<()> {
    let mut tester = ctx.property_tester();

    // Property: All valid events should be creatable
    tester.test_event_creation_property(50).await?;

    // Property: All created events should be queryable
    tester.test_event_querying_property(50).await?;

    Ok(())
}
```

---

## 10. Troubleshooting

### Issue: "Database pool exhausted"
**Solution**: Increase pool size or reduce concurrent tests
```bash
export SINEX_TESTUTILS_POOL_SIZE=20
```

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
rm target/sinex-test-utils/template_stamp.json
cargo test
```

### Issue: "Tests hang on cleanup"
**Solution**: Background cleanup manager is waiting, increase timeout
```bash
# Check cleanup process
ps aux | grep sqlx
# Allow more time for cleanup during shutdown
```

---

## 11. Performance Optimization

### Baseline Metrics
- Template creation: ~5-15 minutes (first run)
- Template reuse: <1 second per test database
- Database acquisition: <100ms typical
- Test execution: Parallelized via Nextest

### Optimization Strategies
1. **Template caching**: Leverage migrations fingerprint
2. **Pool sizing**: Match concurrent test count
3. **Connection limits**: Tune per `SINEX_TESTUTILS_CONN_BUDGET`
4. **Minimal fixtures**: Only create what you need
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
