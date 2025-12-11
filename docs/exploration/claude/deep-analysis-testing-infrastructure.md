# Deep Analysis: Testing Infrastructure

## Phase 12 - Comprehensive Testing Infrastructure Analysis

**Analysis Date**: 2025-11-18
**Scope**: Testing infrastructure, fixtures, property testing, test utilities
**Files Analyzed**: 15+ test infrastructure files
**Previous Issues**: 101 (Issues 1-101 from Phases 1-11)
**New Issues This Phase**: 23 (Issues 102-124)

---

## Executive Summary

This phase analyzes Sinex's testing infrastructure across three major dimensions:

1. **Fixture Management System** - Sophisticated caching and lifecycle management
2. **Property-Based Testing** - Integration with proptest for randomized testing
3. **Database Pool Architecture** - 64-database parallel test execution (covered in Phase 6)

The testing infrastructure is **well-architected** with several advanced patterns:

- Global fixture registry with reference counting
- Parameterized fixtures with caching
- Comprehensive property testing strategies
- Parallel test execution with database isolation

However, several **critical issues** were identified:

- **Cleanup ordering dependencies** (Issue 102)
- **Reference count leak potential** (Issue 103)
- **Insufficient panic safety in cleanup** (Issue 104)
- **Missing property test coverage** for critical invariants (Issues 110-114)
- **No fuzzing integration** despite malicious payload infrastructure (Issue 119)

**Risk Assessment**: MEDIUM-HIGH

- Testing infrastructure bugs can mask production bugs
- Fixture leaks can cause test pollution
- Missing coverage can leave bugs undiscovered

---

## Table of Contents

1. [Fixture Management System](#1-fixture-management-system)
2. [Property-Based Testing Infrastructure](#2-property-based-testing-infrastructure)
3. [Test Context and Lifecycle](#3-test-context-and-lifecycle)
4. [Database Testing Patterns](#4-database-testing-patterns)
5. [Property Test Coverage Analysis](#5-property-test-coverage-analysis)
6. [Issue Catalog (102-124)](#6-issue-catalog-issues-102-124)
7. [Recommendations](#7-recommendations)
8. [Cross-References](#8-cross-references)

---

## 1. Fixture Management System

### 1.1 Global Fixture Registry Architecture

**File**: `crate/lib/sinex-test-utils/src/fixtures.rs:1-100`

The fixture system uses a global singleton registry with OnceCell:

```rust
static FIXTURE_REGISTRY: OnceCell<Arc<Mutex<FixtureRegistry>>> = OnceCell::const_new();

struct FixtureRegistry {
    cache: HashMap<FixtureKey, Arc<dyn Any + Send + Sync>>,
    cleanups: HashMap<CleanupKey, CleanupTask>,
    ref_counts: HashMap<FixtureKey, usize>,
}

fn registry() -> Arc<Mutex<FixtureRegistry>> {
    FIXTURE_REGISTRY
        .get_or_init(|| Arc::new(Mutex::new(FixtureRegistry::new())))
        .clone()
}
```

**Pattern Analysis**:

- ✅ **Good**: OnceCell ensures singleton initialization safety
- ✅ **Good**: Arc<Mutex> allows concurrent access from multiple tests
- ⚠️ **Issue 102**: Cleanup ordering not guaranteed (see below)

### 1.2 Reference Counting Lifecycle

**File**: `crate/lib/sinex-test-utils/src/fixtures.rs:150-250`

Fixtures use reference counting for shared lifetime management:

```rust
async fn get_or_create<T, F, Fut>(&mut self, key: String, creator: F) -> TestResult<Arc<T>>
where
    T: Send + Sync + 'static,
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let cache_key = FixtureKey {
        type_name: std::any::type_name::<T>().to_string(),
        params: key,
    };

    // Check cache
    if let Some(cached) = self.cache.get(&cache_key) {
        // INCREMENT REFERENCE COUNT
        self.ref_counts.entry(cache_key.clone()).and_modify(|c| *c += 1);
        return cached.clone().downcast::<T>().map_err(|_| /* ... */);
    }

    // Create new fixture
    let fixture = creator().await?;
    let arc_fixture = Arc::new(fixture);

    // Store in cache with initial ref count of 1
    self.cache.insert(cache_key.clone(), arc_fixture.clone() as Arc<dyn Any + Send + Sync>);
    self.ref_counts.insert(cache_key, 1);

    Ok(arc_fixture)
}

async fn release<T: 'static>(&mut self, key: String) -> TestResult<()> {
    let cache_key = FixtureKey {
        type_name: std::any::type_name::<T>().to_string(),
        params: key,
    };

    // DECREMENT REFERENCE COUNT
    let should_cleanup = if let Some(count) = self.ref_counts.get_mut(&cache_key) {
        *count -= 1;
        *count == 0  // Cleanup when count reaches 0
    } else {
        false
    };

    if should_cleanup {
        self.ref_counts.remove(&cache_key);
        self.cache.remove(&cache_key);

        // Run cleanup if registered
        if let Some(cleanup) = self.cleanups.remove(&cache_key.into()) {
            cleanup.run().await?;
        }
    }

    Ok(())
}
```

**🔴 Issue 102: Cleanup Ordering Not Guaranteed**

- **Severity**: HIGH
- **Location**: `fixtures.rs:200-220`
- **Problem**: HashMap iteration order is non-deterministic, but fixtures may have dependencies
- **Example**: DatabaseFixture must be cleaned up BEFORE TestContextFixture
- **Impact**: Cleanup failures, resource leaks, test pollution
- **Fix**: Use dependency graph or explicit ordering mechanism

**🔴 Issue 103: Reference Count Leak on Panic**

- **Severity**: MEDIUM
- **Location**: `fixtures.rs:150-180`
- **Problem**: If `get_or_create` panics after incrementing ref count, count is never decremented
- **Code Path**:

  ```rust
  self.ref_counts.entry(cache_key.clone()).and_modify(|c| *c += 1);
  // PANIC HERE - ref count leaked
  return cached.clone().downcast::<T>().map_err(|_| /* ... */);
  ```

- **Impact**: Fixture never cleaned up, resource leak across test runs
- **Fix**: Use RAII guard that decrements on drop

**🔴 Issue 104: Cleanup Panic Safety**

- **Severity**: MEDIUM
- **Location**: `fixtures.rs:210-215`
- **Problem**: If cleanup.run() panics, fixture remains in cache with ref count 0
- **Code**:

  ```rust
  if should_cleanup {
      self.ref_counts.remove(&cache_key);  // REMOVED
      self.cache.remove(&cache_key);        // REMOVED

      if let Some(cleanup) = self.cleanups.remove(&cache_key.into()) {
          cleanup.run().await?;  // PANIC - cache already cleared
      }
  }
  ```

- **Impact**: Fixture removed from tracking but cleanup incomplete, partial resource leak
- **Fix**: Remove from cache AFTER successful cleanup

### 1.3 Parameterized Fixtures

**File**: `crate/lib/sinex-test-utils/src/fixtures.rs:300-400`

Fixtures support parameterization with caching by parameter values:

```rust
pub async fn test_database_with_name(name: &str) -> TestResult<Arc<TestDatabase>> {
    let key = format!("test_db_{}", name);

    registry()
        .lock()
        .await
        .get_or_create(key.clone(), || async {
            let db = TestDatabase::new(name).await?;
            Ok(db)
        })
        .await
}

pub async fn test_context_with_config(config: TestConfig) -> TestResult<Arc<TestContext>> {
    // Serialize config to create unique key
    let key = serde_json::to_string(&config)?;

    registry()
        .lock()
        .await
        .get_or_create(key, || async {
            TestContext::with_config(config).await
        })
        .await
}
```

**⚠️ Issue 105: No Parameter Validation**

- **Severity**: LOW
- **Location**: `fixtures.rs:300-350`
- **Problem**: No validation that parameter values are safe for use as cache keys
- **Example**: Config with NaN floats, non-canonical JSON
- **Impact**: Duplicate fixtures created for semantically identical configs
- **Fix**: Canonical serialization or explicit validation

**⚠️ Issue 106: Cache Key Collision Risk**

- **Severity**: MEDIUM
- **Location**: `fixtures.rs:320`
- **Problem**: Simple string concatenation for cache keys
- **Example**:

  ```rust
  let key = format!("test_db_{}", name);
  // "test_db_foo_bar" vs "test_db_foo" + "_bar"
  ```

- **Impact**: Different fixture requests might collide
- **Fix**: Use structured key with type information and parameter hash

### 1.4 Cleanup Function Registration

**File**: `crate/lib/sinex-test-utils/src/fixtures.rs:450-550`

Cleanup functions are stored separately from fixtures:

```rust
pub enum CleanupTask {
    Sync(Box<dyn FnOnce() -> TestResult<()> + Send>),
    Async(Pin<Box<dyn Future<Output = TestResult<()>> + Send>>),
}

impl CleanupTask {
    async fn run(self) -> TestResult<()> {
        match self {
            CleanupTask::Sync(f) => f(),
            CleanupTask::Async(fut) => fut.await,
        }
    }
}

pub async fn register_cleanup<F>(key: String, cleanup: F)
where
    F: FnOnce() -> TestResult<()> + Send + 'static,
{
    let cleanup_key = CleanupKey {
        fixture_key: key,
        cleanup_id: Ulid::new().to_string(),
    };

    registry()
        .lock()
        .await
        .cleanups
        .insert(cleanup_key, CleanupTask::Sync(Box::new(cleanup)));
}
```

**⚠️ Issue 107: No Cleanup Timeout**

- **Severity**: MEDIUM
- **Location**: `fixtures.rs:470-480`
- **Problem**: Cleanup can hang indefinitely
- **Impact**: Test suite hangs on cleanup, CI timeout
- **Fix**: Add timeout with `tokio::time::timeout`

**⚠️ Issue 108: Cleanup Errors Swallowed**

- **Severity**: MEDIUM
- **Location**: `fixtures.rs:210`
- **Problem**: Cleanup errors propagated as `TestResult` but often ignored
- **Code**:

  ```rust
  cleanup.run().await?;  // Error propagated but...

  // In test cleanup:
  let _ = release::<TestDatabase>("db").await;  // ERROR IGNORED
  ```

- **Impact**: Silent cleanup failures, resource leaks
- **Fix**: Log cleanup errors, consider cleanup failure registry

### 1.5 Composite Fixtures

**File**: `crate/lib/sinex-test-utils/src/fixtures.rs:600-700`

Fixtures can depend on other fixtures:

```rust
pub async fn test_context() -> TestResult<Arc<TestContext>> {
    // Get or create database fixture
    let db = test_database().await?;

    // Create context that uses database
    registry()
        .lock()
        .await
        .get_or_create("test_context".to_string(), || async {
            let ctx = TestContext::new(db.clone()).await?;
            Ok(ctx)
        })
        .await
}
```

**🔴 Issue 109: No Dependency Tracking**

- **Severity**: HIGH
- **Location**: `fixtures.rs:600-650`
- **Problem**: Composite fixtures hold Arc to dependencies, but registry doesn't track relationship
- **Example**:

  ```rust
  // TestContext holds Arc<TestDatabase>
  // But registry doesn't know TestContext depends on TestDatabase
  // If TestDatabase cleaned up first, TestContext has dangling reference
  ```

- **Impact**: Use-after-cleanup, potential panics or corrupted state
- **Fix**: Explicit dependency graph, reference counting includes dependents

---

## 2. Property-Based Testing Infrastructure

### 2.1 Strategy Builders for Domain Types

**File**: `crate/lib/sinex-test-utils/src/property_testing.rs:1-200`

Custom strategies for Sinex domain types:

```rust
pub struct SinexStrategies;

impl SinexStrategies {
    /// Generate valid EventSource strings
    pub fn event_source() -> BoxedStrategy<String> {
        prop_oneof![
            // Common real sources
            Just("filesystem".to_string()),
            Just("shell.kitty".to_string()),
            Just("desktop.hyprland".to_string()),

            // Random valid sources
            "[a-z][a-z0-9._]*".prop_map(|s| s.to_string()),

            // Edge cases
            Just("".to_string()),  // Empty source
            Just("a".to_string()),  // Single char
        ]
        .boxed()
    }

    /// Generate valid EventType strings
    pub fn event_type() -> BoxedStrategy<String> {
        prop_oneof![
            // Common event types
            Just("file.created".to_string()),
            Just("command.executed".to_string()),

            // Random valid types
            "[a-z][a-z0-9._]*".prop_map(|s| s.to_string()),
        ]
        .boxed()
    }

    /// Generate valid JSON payloads
    pub fn json_payload() -> BoxedStrategy<Value> {
        prop_oneof![
            // Primitives
            any::<String>().prop_map(Value::String),
            any::<i64>().prop_map(|n| json!(n)),
            any::<bool>().prop_map(Value::Bool),
            Just(Value::Null),

            // Objects
            prop::collection::hash_map(
                "[a-z_]+",
                any::<String>().prop_map(Value::String),
                0..10
            ).prop_map(|m| Value::Object(m.into_iter().collect())),
        ]
        .boxed()
    }
}
```

**⚠️ Issue 110: Insufficient Edge Case Coverage**

- **Severity**: MEDIUM
- **Location**: `property_testing.rs:1-100`
- **Problem**: Strategies don't cover important edge cases
- **Missing Cases**:
  - Unicode in event sources (e.g., "测试.source")
  - Very long strings (>1MB)
  - Control characters in JSON
  - Deeply nested JSON (>100 levels)
  - Circular references (if using serde_json::Value)
- **Impact**: Bugs not caught by property tests
- **Fix**: Add explicit edge case generators

**⚠️ Issue 111: No ULID Strategy**

- **Severity**: LOW
- **Location**: `property_testing.rs:50-150`
- **Problem**: No strategy for generating arbitrary ULIDs
- **Impact**: Can't test ULID-dependent code with property tests
- **Fix**: Add `SinexStrategies::ulid()` strategy

### 2.2 Malicious Payload Generation

**File**: `crate/lib/sinex-test-utils/src/property_testing.rs:250-400`

Strategies for security/adversarial testing:

```rust
impl SinexStrategies {
    /// Generate malicious payloads for security testing
    pub fn malicious_payload() -> BoxedStrategy<Value> {
        prop_oneof![
            // Extremely large strings (potential DoS)
            prop::collection::vec(any::<u8>(), 1_000_000..2_000_000)
                .prop_map(|bytes| {
                    Value::String(String::from_utf8_lossy(&bytes).to_string())
                }),

            // SQL injection attempts
            Just(json!({"path": "'; DROP TABLE events; --"})),
            Just(json!({"path": "' OR '1'='1"})),

            // XSS attempts
            Just(json!({"content": "<script>alert('xss')</script>"})),
            Just(json!({"content": "javascript:alert('xss')"})),

            // Path traversal
            Just(json!({"path": "../../../../etc/passwd"})),
            Just(json!({"path": "..\\..\\..\\windows\\system32\\config\\sam"})),

            // Null byte injection
            Just(json!({"path": "/etc/passwd\0.txt"})),

            // Format string attacks
            Just(json!({"format": "%s%s%s%s%s%s%s%s%s%s"})),

            // Deeply nested JSON (potential stack overflow)
            Self::deeply_nested_json(100),
        ]
        .boxed()
    }

    fn deeply_nested_json(depth: usize) -> BoxedStrategy<Value> {
        if depth == 0 {
            Just(json!("deep")).boxed()
        } else {
            Self::deeply_nested_json(depth - 1)
                .prop_map(|inner| json!({"nested": inner}))
                .boxed()
        }
    }

    /// Generate payloads that might trigger integer overflow
    pub fn overflow_payload() -> BoxedStrategy<Value> {
        prop_oneof![
            Just(json!({"size": i64::MAX})),
            Just(json!({"size": u64::MAX})),
            Just(json!({"count": usize::MAX})),
        ]
        .boxed()
    }
}
```

**✅ Excellent**: Comprehensive malicious payload coverage

- SQL injection, XSS, path traversal, null bytes, format strings
- DoS via large payloads and deep nesting
- Integer overflow cases

**🔴 Issue 112: Malicious Payloads Not Tested in CI**

- **Severity**: HIGH
- **Location**: `property_testing.rs:250-400` (infrastructure exists but not used)
- **Problem**: Malicious payload strategies defined but no tests actually use them
- **Evidence**: Grep for `malicious_payload()` shows only definition, no usage
- **Impact**: Security vulnerabilities not tested despite infrastructure existing
- **Fix**: Add adversarial property tests using malicious strategies

**⚠️ Issue 113: No Fuzzing Integration**

- **Severity**: MEDIUM
- **Location**: `property_testing.rs` (entire file)
- **Problem**: No integration with cargo-fuzz or other fuzzing tools
- **Impact**: Missing continuous fuzzing in CI
- **Fix**: Add `fuzz/` directory with libFuzzer harnesses

### 2.3 PropertyTester Integration

**File**: `crate/lib/sinex-test-utils/src/property_testing.rs:500-650`

PropertyTester integrates with TestContext for stateful testing:

```rust
pub struct PropertyTester {
    ctx: Arc<TestContext>,
    config: ProptestConfig,
}

impl PropertyTester {
    pub fn new(ctx: Arc<TestContext>) -> Self {
        Self {
            ctx,
            config: ProptestConfig::default(),
        }
    }

    pub fn with_cases(mut self, cases: u32) -> Self {
        self.config = self.config.with_cases(cases);
        self
    }

    pub async fn run<F, Fut>(&self, property: F) -> TestResult<()>
    where
        F: Fn(Arc<TestContext>) -> Fut,
        Fut: Future<Output = TestResult<()>>,
    {
        // Run property test with TestContext
        proptest!(self.config.clone(), |(_,)| {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(async {
                    property(self.ctx.clone()).await
                })
                .unwrap();
        });

        Ok(())
    }
}
```

**⚠️ Issue 114: No Shrinking for Async Properties**

- **Severity**: MEDIUM
- **Location**: `property_testing.rs:600-650`
- **Problem**: Proptest shrinking doesn't work well with async properties
- **Example**: If property fails with large payload, shrinking may not find minimal failing case
- **Impact**: Harder to debug property test failures
- **Fix**: Use `TestCaseError::fail()` with proper shrinking hints

**⚠️ Issue 115: Runtime Created Per Test Case**

- **Severity**: LOW (performance)
- **Location**: `property_testing.rs:620`
- **Problem**: New Tokio runtime created for each property test case
- **Code**:

  ```rust
  tokio::runtime::Runtime::new()  // EXPENSIVE - called 100+ times
      .unwrap()
      .block_on(async { /* ... */ })
  ```

- **Impact**: Slow property tests (runtime creation ~1ms per case)
- **Fix**: Reuse runtime or use `#[tokio::test]` directly

---

## 3. Test Context and Lifecycle

### 3.1 TestContext Architecture

**File**: `crate/lib/sinex-test-utils/src/lib.rs:1-150`

TestContext provides integrated test environment:

```rust
pub struct TestContext {
    pub db: TestDatabase,
    pub config: TestConfig,
    temp_dir: TempDir,
    cleanup_tasks: Arc<Mutex<Vec<CleanupTask>>>,
}

impl TestContext {
    pub async fn new() -> TestResult<Self> {
        let db = TestDatabase::new().await?;
        let temp_dir = TempDir::new()?;

        Ok(Self {
            db,
            config: TestConfig::default(),
            temp_dir,
            cleanup_tasks: Arc::new(Mutex::new(Vec::new())),
        })
    }

    pub async fn with_config(config: TestConfig) -> TestResult<Self> {
        let mut ctx = Self::new().await?;
        ctx.config = config;
        Ok(ctx)
    }

    pub fn temp_path(&self) -> &Path {
        self.temp_dir.path()
    }

    pub async fn register_cleanup<F>(&self, cleanup: F)
    where
        F: FnOnce() -> TestResult<()> + Send + 'static,
    {
        self.cleanup_tasks
            .lock()
            .await
            .push(CleanupTask::Sync(Box::new(cleanup)));
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        // Run all cleanup tasks
        let tasks = std::mem::take(&mut *self.cleanup_tasks.blocking_lock());

        for task in tasks {
            if let Err(e) = tokio::runtime::Handle::current().block_on(task.run()) {
                eprintln!("Cleanup error: {}", e);
            }
        }
    }
}
```

**⚠️ Issue 116: Cleanup in Drop May Panic**

- **Severity**: HIGH
- **Location**: `lib.rs:80-95`
- **Problem**: Drop implementation calls `block_on` which may panic if no runtime
- **Code**:

  ```rust
  tokio::runtime::Handle::current()  // PANIC if no runtime
      .block_on(task.run())
  ```

- **Impact**: Test cleanup panics, resources leaked
- **Fix**: Use `Handle::try_current()` and spawn blocking task

**⚠️ Issue 117: TempDir Not Cleaned on Panic**

- **Severity**: LOW
- **Location**: `lib.rs:20-40`
- **Problem**: If test panics before Drop, TempDir cleanup may not run
- **Impact**: `/tmp` filled with test directories over time
- **Fix**: Use `defer!` macro or explicit cleanup guard

### 3.2 TestDatabase Pool Architecture

**Reference**: Phase 6 analysis (`docs/deep-analysis-database-patterns.md`)

The 64-database pool mechanism is thoroughly analyzed in Phase 6. Key points:

- **Template Database**: Single template database cloned for each test
- **Advisory Locks**: PostgreSQL advisory locks for pool coordination
- **Migration Fingerprinting**: Hash-based migration tracking
- **Parallel Execution**: Up to 64 tests run concurrently

**Issues from Phase 6** (already cataloged):

- Issue 50: No detection of concurrent schema changes
- Issue 51: Lease timeout too short (5 seconds)
- Issue 52: No recovery from template corruption

---

## 4. Database Testing Patterns

### 4.1 Transaction-Scoped Tests

**File**: `crate/lib/sinex-test-utils/src/lib.rs:200-300`

Tests can run in transactions that rollback automatically:

```rust
impl TestContext {
    pub async fn with_transaction<F, Fut, T>(&self, f: F) -> TestResult<T>
    where
        F: FnOnce(&mut sqlx::Transaction<'_, Postgres>) -> Fut,
        Fut: Future<Output = TestResult<T>>,
    {
        let mut tx = self.db.pool().begin().await?;

        let result = f(&mut tx).await;

        // Always rollback (even on success)
        tx.rollback().await?;

        result
    }
}

// Usage:
#[sinex_test]
async fn test_event_insertion() -> TestResult<()> {
    let ctx = TestContext::new().await?;

    ctx.with_transaction(|tx| async move {
        // Insert event
        let event = Event::test_event(/* ... */);
        event.insert(tx).await?;

        // Verify insertion
        let count = sqlx::query_scalar("SELECT COUNT(*) FROM core.events")
            .fetch_one(tx)
            .await?;
        assert_eq!(count, 1);

        Ok(())
        // Transaction automatically rolled back
    }).await
}
```

**✅ Excellent**: Transaction rollback prevents test pollution

**⚠️ Issue 118: Transaction Timeout Not Configurable**

- **Severity**: LOW
- **Location**: `lib.rs:200-250`
- **Problem**: No way to extend transaction timeout for slow tests
- **Impact**: Long-running tests may hit timeout
- **Fix**: Add `.with_timeout(duration)` configuration

### 4.2 Event Factories

**File**: `crate/lib/sinex-test-utils/src/factories.rs:1-200`

Test data factories for creating events:

```rust
pub struct EventFactory;

impl EventFactory {
    pub fn fs_event(path: &str) -> Event<JsonValue> {
        Event::test_event(
            EventSource::from_static("filesystem"),
            EventType::from_static("file.created"),
            json!({
                "path": path,
                "size": 1024,
                "timestamp": Utc::now().to_rfc3339(),
            }),
        )
    }

    pub fn terminal_event(command: &str) -> Event<JsonValue> {
        Event::test_event(
            EventSource::from_static("shell.kitty"),
            EventType::from_static("command.executed"),
            json!({
                "command": command,
                "exit_code": 0,
                "duration_ms": 123,
            }),
        )
    }

    pub fn random_event() -> Event<JsonValue> {
        Event::test_event(
            EventSource::new(&format!("source_{}", Ulid::new())),
            EventType::new(&format!("type_{}", Ulid::new())),
            json!({"random": true}),
        )
    }
}
```

**✅ Good**: Convenient factory methods reduce boilerplate

**⚠️ Issue 119: No Builder Pattern**

- **Severity**: LOW
- **Location**: `factories.rs:1-200`
- **Problem**: Factory methods not chainable, hard to customize
- **Example**: Want fs_event with custom size - must duplicate factory
- **Fix**: Use builder pattern:

  ```rust
  EventFactory::fs_event("path")
      .with_size(2048)
      .with_timestamp(custom_time)
      .build()
  ```

---

## 5. Property Test Coverage Analysis

### 5.1 Existing Property Tests

**File**: `crate/lib/sinex-core/tests/property_tests.rs:1-511`

Current property tests cover:

1. **ULID Properties** (lines 21-82)
   - Uniqueness
   - Timestamp ordering
   - Transitivity
   - String round-trip

2. **Event Creation Properties** (lines 124-151)
   - Field preservation
   - Timestamp bounds

3. **Event Serialization Properties** (lines 153-193)
   - JSON round-trip
   - Field preservation after serde

4. **Domain Type Properties** (lines 199-252)
   - EventSource, EventType, HostName preservation
   - Clone equality

5. **Generic ID Properties** (lines 258-312)
   - Uniqueness
   - String representation (26 chars, alphanumeric)
   - ULID conversion round-trip

6. **Unicode Handling Properties** (lines 327-360)
   - Unicode preservation in all fields
   - Serialization with Unicode

7. **Large Payload Properties** (lines 363-407)
   - Large strings (1KB-100KB)
   - Large arrays (100-10K elements)
   - Serialization of large events

8. **Concurrent Operation Properties** (lines 410-450)
   - ULID uniqueness across threads
   - Concurrent generation correctness

9. **Validation Properties** (lines 457-483)
   - Domain types accept any length
   - Events creatable with any valid domain types

### 5.2 Coverage Gaps

**🔴 Issue 120: No Database Property Tests**

- **Severity**: HIGH
- **Location**: `property_tests.rs` (entire file)
- **Missing Coverage**:
  - Event insertion preserves all fields
  - Query ordering matches ULID ordering
  - Batch insertion atomicity
  - Concurrent inserts don't violate constraints
- **Impact**: Database bugs not caught by property tests
- **Fix**: Add database property tests using TestContext

**🔴 Issue 121: No NATS Property Tests**

- **Severity**: HIGH
- **Location**: Test suite (no NATS property tests found)
- **Missing Coverage**:
  - Message ordering guarantees
  - Consumer acknowledgment correctness
  - Message size limits
  - Connection failure recovery
- **Impact**: Message bus bugs not tested
- **Fix**: Add NATS property tests

**⚠️ Issue 122: No Satellite Property Tests**

- **Severity**: MEDIUM
- **Location**: Test suite (no satellite property tests found)
- **Missing Coverage**:
  - StatefulStreamProcessor state transitions
  - Checkpoint persistence and recovery
  - Event deduplication
  - Graceful shutdown completeness
- **Impact**: Satellite bugs not tested with randomized inputs
- **Fix**: Add satellite property tests

**⚠️ Issue 123: No Schema Validation Property Tests**

- **Severity**: MEDIUM
- **Location**: `property_tests.rs:1-511` (no schema tests)
- **Missing Coverage**:
  - Valid events always pass validation
  - Invalid events always fail validation
  - Schema evolution preserves compatibility
- **Impact**: Schema validation bugs not caught
- **Fix**: Add property tests for pg_jsonschema validation

**⚠️ Issue 124: No Adversarial Property Tests Using Malicious Payloads**

- **Severity**: MEDIUM
- **Location**: Test suite (malicious strategies defined but not used)
- **Missing Coverage**:
  - SQL injection attempts properly escaped
  - XSS payloads properly sanitized
  - Path traversal blocked
  - DoS payloads handled gracefully
- **Impact**: Security vulnerabilities not tested
- **Fix**: Add adversarial property tests using `SinexStrategies::malicious_payload()`

---

## 6. Issue Catalog (Issues 102-124)

### Critical Issues (1)

**Issue 109: No Dependency Tracking in Composite Fixtures** (HIGH)

- **File**: `fixtures.rs:600-650`
- **Problem**: Composite fixtures hold Arc to dependencies, but registry doesn't track relationship
- **Impact**: Use-after-cleanup, potential panics or corrupted state
- **Fix**: Explicit dependency graph, reference counting includes dependents

### High-Severity Issues (5)

**Issue 102: Cleanup Ordering Not Guaranteed** (HIGH)

- **File**: `fixtures.rs:200-220`
- **Problem**: HashMap iteration order is non-deterministic, but fixtures may have dependencies
- **Impact**: Cleanup failures, resource leaks, test pollution
- **Fix**: Use dependency graph or explicit ordering mechanism

**Issue 112: Malicious Payloads Not Tested in CI** (HIGH)

- **File**: `property_testing.rs:250-400`
- **Problem**: Malicious payload strategies defined but no tests actually use them
- **Impact**: Security vulnerabilities not tested despite infrastructure existing
- **Fix**: Add adversarial property tests using malicious strategies

**Issue 116: Cleanup in Drop May Panic** (HIGH)

- **File**: `lib.rs:80-95`
- **Problem**: Drop implementation calls `block_on` which may panic if no runtime
- **Impact**: Test cleanup panics, resources leaked
- **Fix**: Use `Handle::try_current()` and spawn blocking task

**Issue 120: No Database Property Tests** (HIGH)

- **File**: `property_tests.rs` (missing tests)
- **Problem**: No property tests for database operations
- **Impact**: Database bugs not caught by property tests
- **Fix**: Add database property tests using TestContext

**Issue 121: No NATS Property Tests** (HIGH)

- **File**: Test suite (missing tests)
- **Problem**: No property tests for NATS operations
- **Impact**: Message bus bugs not tested
- **Fix**: Add NATS property tests

### Medium-Severity Issues (11)

**Issue 103: Reference Count Leak on Panic** (MEDIUM)

- **File**: `fixtures.rs:150-180`
- **Problem**: If `get_or_create` panics after incrementing ref count, count is never decremented
- **Impact**: Fixture never cleaned up, resource leak across test runs
- **Fix**: Use RAII guard that decrements on drop

**Issue 104: Cleanup Panic Safety** (MEDIUM)

- **File**: `fixtures.rs:210-215`
- **Problem**: If cleanup.run() panics, fixture remains in cache with ref count 0
- **Impact**: Fixture removed from tracking but cleanup incomplete, partial resource leak
- **Fix**: Remove from cache AFTER successful cleanup

**Issue 106: Cache Key Collision Risk** (MEDIUM)

- **File**: `fixtures.rs:320`
- **Problem**: Simple string concatenation for cache keys
- **Impact**: Different fixture requests might collide
- **Fix**: Use structured key with type information and parameter hash

**Issue 107: No Cleanup Timeout** (MEDIUM)

- **File**: `fixtures.rs:470-480`
- **Problem**: Cleanup can hang indefinitely
- **Impact**: Test suite hangs on cleanup, CI timeout
- **Fix**: Add timeout with `tokio::time::timeout`

**Issue 108: Cleanup Errors Swallowed** (MEDIUM)

- **File**: `fixtures.rs:210`
- **Problem**: Cleanup errors propagated as `TestResult` but often ignored
- **Impact**: Silent cleanup failures, resource leaks
- **Fix**: Log cleanup errors, consider cleanup failure registry

**Issue 110: Insufficient Edge Case Coverage in Property Strategies** (MEDIUM)

- **File**: `property_testing.rs:1-100`
- **Problem**: Strategies don't cover important edge cases (Unicode, very long strings, deeply nested JSON)
- **Impact**: Bugs not caught by property tests
- **Fix**: Add explicit edge case generators

**Issue 113: No Fuzzing Integration** (MEDIUM)

- **File**: `property_testing.rs` (entire file)
- **Problem**: No integration with cargo-fuzz or other fuzzing tools
- **Impact**: Missing continuous fuzzing in CI
- **Fix**: Add `fuzz/` directory with libFuzzer harnesses

**Issue 114: No Shrinking for Async Properties** (MEDIUM)

- **File**: `property_testing.rs:600-650`
- **Problem**: Proptest shrinking doesn't work well with async properties
- **Impact**: Harder to debug property test failures
- **Fix**: Use `TestCaseError::fail()` with proper shrinking hints

**Issue 122: No Satellite Property Tests** (MEDIUM)

- **File**: Test suite (missing tests)
- **Problem**: No property tests for satellite operations
- **Impact**: Satellite bugs not tested with randomized inputs
- **Fix**: Add satellite property tests

**Issue 123: No Schema Validation Property Tests** (MEDIUM)

- **File**: `property_tests.rs:1-511`
- **Problem**: No property tests for schema validation
- **Impact**: Schema validation bugs not caught
- **Fix**: Add property tests for pg_jsonschema validation

**Issue 124: No Adversarial Property Tests** (MEDIUM)

- **File**: Test suite (malicious strategies defined but not used)
- **Problem**: Security vulnerabilities not tested
- **Impact**: SQL injection, XSS, path traversal not tested
- **Fix**: Add adversarial property tests using `SinexStrategies::malicious_payload()`

### Low-Severity Issues (6)

**Issue 105: No Parameter Validation in Parameterized Fixtures** (LOW)

- **File**: `fixtures.rs:300-350`
- **Problem**: No validation that parameter values are safe for use as cache keys
- **Impact**: Duplicate fixtures created for semantically identical configs
- **Fix**: Canonical serialization or explicit validation

**Issue 111: No ULID Strategy** (LOW)

- **File**: `property_testing.rs:50-150`
- **Problem**: No strategy for generating arbitrary ULIDs
- **Impact**: Can't test ULID-dependent code with property tests
- **Fix**: Add `SinexStrategies::ulid()` strategy

**Issue 115: Runtime Created Per Test Case** (LOW - performance)

- **File**: `property_testing.rs:620`
- **Problem**: New Tokio runtime created for each property test case
- **Impact**: Slow property tests
- **Fix**: Reuse runtime or use `#[tokio::test]` directly

**Issue 117: TempDir Not Cleaned on Panic** (LOW)

- **File**: `lib.rs:20-40`
- **Problem**: If test panics before Drop, TempDir cleanup may not run
- **Impact**: `/tmp` filled with test directories over time
- **Fix**: Use `defer!` macro or explicit cleanup guard

**Issue 118: Transaction Timeout Not Configurable** (LOW)

- **File**: `lib.rs:200-250`
- **Problem**: No way to extend transaction timeout for slow tests
- **Impact**: Long-running tests may hit timeout
- **Fix**: Add `.with_timeout(duration)` configuration

**Issue 119: No Builder Pattern in Factories** (LOW)

- **File**: `factories.rs:1-200`
- **Problem**: Factory methods not chainable, hard to customize
- **Impact**: Boilerplate duplication when customizing factories
- **Fix**: Implement builder pattern

---

## 7. Recommendations

### 7.1 Immediate Actions (This Sprint)

1. **Fix Fixture Cleanup Ordering** (Issue 102)
   - Implement dependency graph for fixtures
   - Topological sort for cleanup order
   - Priority: HIGH

2. **Fix Drop Panic in TestContext** (Issue 116)
   - Use `Handle::try_current()` instead of `current()`
   - Spawn blocking task for cleanup
   - Priority: HIGH

3. **Add Adversarial Property Tests** (Issues 112, 124)
   - Create `test/adversarial_property_tests.rs`
   - Use existing `malicious_payload()` strategies
   - Test SQL injection, XSS, path traversal
   - Priority: HIGH (security)

4. **Add Database Property Tests** (Issue 120)
   - Event insertion preserves fields
   - Query ordering correctness
   - Batch insertion atomicity
   - Priority: HIGH

### 7.2 Medium-Term Actions (Next Quarter)

1. **Implement Fixture Dependency Tracking** (Issue 109)
   - Track fixture dependencies in registry
   - Reference counting includes dependents
   - Prevent use-after-cleanup

2. **Add NATS Property Tests** (Issue 121)
   - Message ordering guarantees
   - Consumer acknowledgment correctness
   - Connection failure recovery

3. **Integrate Fuzzing** (Issue 113)
   - Add `fuzz/` directory
   - Implement libFuzzer harnesses
   - Run in CI nightly

4. **Improve Property Test Edge Cases** (Issue 110)
   - Unicode coverage
   - Very long strings (>1MB)
   - Deeply nested JSON (>100 levels)
   - Control characters

5. **Fix Reference Count Leak on Panic** (Issue 103)
   - Implement RAII guard for ref count
   - Automatic decrement on panic

### 7.3 Long-Term Actions (6+ Months)

1. **Comprehensive Satellite Property Tests** (Issue 122)
   - StatefulStreamProcessor state transitions
   - Checkpoint persistence and recovery
   - Event deduplication
   - Graceful shutdown completeness

2. **Schema Validation Property Tests** (Issue 123)
   - Valid events always pass
   - Invalid events always fail
   - Schema evolution compatibility

3. **Fixture Performance Optimization**
   - Per-test-file fixture scopes
   - Lazy initialization
   - Parallel fixture creation

4. **Test Infrastructure Monitoring**
   - Fixture leak detection
   - Cleanup failure tracking
   - Test performance metrics

### 7.4 Testing Recommendations

**Fixture Testing**:

```rust
#[test]
fn test_fixture_cleanup_ordering() {
    // Track cleanup order
    let order = Arc::new(Mutex::new(Vec::new()));

    // Create fixtures with dependencies
    let db = test_database_with_cleanup(order.clone(), "db").await;
    let ctx = test_context_with_cleanup(order.clone(), "ctx", db).await;

    // Drop fixtures
    drop(ctx);
    drop(db);

    // Verify cleanup order: ctx before db
    assert_eq!(order.lock().unwrap(), vec!["ctx", "db"]);
}
```

**Adversarial Property Testing**:

```rust
#[sinex_test]
fn test_sql_injection_resistance() -> TestResult {
    proptest!(|(
        payload in SinexStrategies::malicious_payload()
    )| {
        let ctx = TestContext::new().await?;

        let event = Event::test_event(
            EventSource::from_static("test"),
            EventType::from_static("malicious"),
            payload,
        );

        // Should not panic or corrupt database
        event.insert(&ctx.db.pool()).await?;

        // Verify database still healthy
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM core.events")
            .fetch_one(&ctx.db.pool())
            .await?;
        prop_assert!(count >= 0);
    });
    Ok(())
}
```

**Database Property Testing**:

```rust
#[sinex_test]
fn test_event_insertion_preserves_fields() -> TestResult {
    proptest!(|(
        source in SinexStrategies::event_source(),
        event_type in SinexStrategies::event_type(),
        payload in SinexStrategies::json_payload()
    )| {
        let ctx = TestContext::new().await?;

        let original = Event::test_event(
            EventSource::new(&source),
            EventType::new(&event_type),
            payload.clone(),
        );

        // Insert
        original.insert(&ctx.db.pool()).await?;

        // Retrieve
        let retrieved = Event::get_by_id(&ctx.db.pool(), original.id.unwrap()).await?;

        // Verify all fields preserved
        prop_assert_eq!(retrieved.source.as_str(), source.as_str());
        prop_assert_eq!(retrieved.event_type.as_str(), event_type.as_str());
        prop_assert_eq!(retrieved.payload, payload);
    });
    Ok(())
}
```

---

## 8. Cross-References

### Related Analysis Documents

- **Phase 6: Database Patterns** (`docs/deep-analysis-database-patterns.md`)
  - 64-database pool architecture
  - Migration fingerprinting
  - Template database cloning
  - Issues 50-52 (pool coordination, lease timeout, template corruption)

- **Phase 10: Concurrency Patterns** (`docs/deep-analysis-concurrency-patterns.md`)
  - Channel sizing patterns
  - Lock contention analysis
  - tokio::spawn management
  - Race conditions
  - Related to testing async code in property tests (Issue 114)

### Related Issues by Category

**Fixture Management**:

- Issue 102: Cleanup ordering not guaranteed
- Issue 103: Reference count leak on panic
- Issue 104: Cleanup panic safety
- Issue 105: No parameter validation
- Issue 106: Cache key collision risk
- Issue 107: No cleanup timeout
- Issue 108: Cleanup errors swallowed
- Issue 109: No dependency tracking in composite fixtures

**Property Testing**:

- Issue 110: Insufficient edge case coverage
- Issue 111: No ULID strategy
- Issue 112: Malicious payloads not tested
- Issue 113: No fuzzing integration
- Issue 114: No shrinking for async properties
- Issue 115: Runtime created per test case

**Test Context**:

- Issue 116: Cleanup in Drop may panic
- Issue 117: TempDir not cleaned on panic
- Issue 118: Transaction timeout not configurable
- Issue 119: No builder pattern in factories

**Coverage Gaps**:

- Issue 120: No database property tests
- Issue 121: No NATS property tests
- Issue 122: No satellite property tests
- Issue 123: No schema validation property tests
- Issue 124: No adversarial property tests

### Files by Risk Level

**High Risk** (bugs affect test reliability):

- `fixtures.rs` - Cleanup ordering, reference leaks, panic safety
- `lib.rs` (TestContext) - Drop panic, temp dir cleanup
- `property_testing.rs` - Missing adversarial tests

**Medium Risk** (performance, usability):

- `factories.rs` - No builder pattern
- `property_tests.rs` - Coverage gaps

**Low Risk** (nice-to-have improvements):

- Test organization
- Documentation

---

## Summary Statistics

**Total Issues This Phase**: 23 (Issues 102-124)

- Critical: 1
- High: 5
- Medium: 11
- Low: 6

**Total Issues Across All Phases**: 124 (Issues 1-124)

**Analysis Metrics**:

- Files analyzed: 15+
- Lines of code reviewed: ~3,500
- Test infrastructure patterns identified: 8
- Property test strategies analyzed: 10
- Coverage gaps identified: 5

**Top Priorities**:

1. Fix fixture cleanup ordering (Issue 102)
2. Fix Drop panic in TestContext (Issue 116)
3. Add adversarial property tests (Issues 112, 124)
4. Add database property tests (Issue 120)
5. Implement fixture dependency tracking (Issue 109)

---

**End of Phase 12 Analysis**

**Next Phase**: To be determined based on remaining codebase areas
