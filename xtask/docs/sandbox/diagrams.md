# Testing Infrastructure Diagrams

## 64-Database Parallel Test Pool

```
┌─────────────────────────────────────────────────────────────────────┐
│                       PostgreSQL Server                              │
│                                                                       │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │              Template Database                                  │ │
│  │  test_db_template                                               │ │
│  │                                                                  │ │
│  │  - All migrations applied                                       │ │
│  │  - Schema matches production                                    │ │
│  │  - Migration fingerprint stored: SHA256(migrations)             │ │
│  │  - Used as template for fast cloning                            │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                                                                       │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │              Test Database Pool (64 databases)                  │ │
│  │                                                                  │ │
│  │  test_db_00  ←─ Advisory Lock: 1000                            │ │
│  │  test_db_01  ←─ Advisory Lock: 1001                            │ │
│  │  test_db_02  ←─ Advisory Lock: 1002                            │ │
│  │  ...                                                            │ │
│  │  test_db_63  ←─ Advisory Lock: 1063                            │ │
│  │                                                                  │ │
│  │  Each database:                                                  │ │
│  │  - Cloned from test_db_template (fast!)                         │ │
│  │  - Isolated (no cross-test pollution)                           │ │
│  │  - Coordinated via advisory locks                               │ │
│  └────────────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────────────┘
```

## Pool Acquisition Flow

```
Test Process                 DatabasePool               PostgreSQL
    │                             │                          │
    │ acquire_slot()              │                          │
    ├────────────────────────────>│                          │
    │                             │                          │
    │                             │ Check migration hash     │
    │                             ├─────────────────────────>│
    │                             │ SELECT fingerprint       │
    │                             │   FROM test_db_template  │
    │                             │<─────────────────────────┤
    │                             │                          │
    │                             │ Hash matches?            │
    │                             │ YES: Use template        │
    │                             │ NO:  Rebuild template    │
    │                             │                          │
    │                             │ Try slot 0               │
    │                             ├─────────────────────────>│
    │                             │ CONNECT test_db_00       │
    │                             │<─────────────────────────┤
    │                             │                          │
    │                             │ pg_try_advisory_lock(1000)
    │                             ├─────────────────────────>│
    │                             │<─────── true ────────────┤
    │                             │ LOCK ACQUIRED ✓          │
    │                             │                          │
    │<─ TestDatabase(slot=0) ─────┤                          │
    │                             │                          │
    │ Run test...                 │                          │
    │ INSERT/UPDATE/DELETE        │                          │
    │ Test assertions             │                          │
    │                             │                          │
    │ Drop TestDatabase           │                          │
    │ (automatic cleanup)         │                          │
    ├────────────────────────────>│                          │
    │                             │ pg_advisory_unlock(1000) │
    │                             ├─────────────────────────>│
    │                             │ LOCK RELEASED            │
    │                             │                          │
    │                             │ pool.close()             │
    │                             ├─────────────────────────>│
    │                             │ CONNECTION CLOSED        │
    │<─ () ───────────────────────┤                          │
```

## Parallel Test Execution

```
┌─────────────────────────────────────────────────────────────────────┐
│                     cargo nextest run (64 threads)                   │
│                                                                       │
│  Thread 1 → acquire_slot() → test_db_00 ─┐                          │
│  Thread 2 → acquire_slot() → test_db_01  │                          │
│  Thread 3 → acquire_slot() → test_db_02  │  All tests run           │
│  ...                                      ├─ in parallel             │
│  Thread 63 → acquire_slot() → test_db_62 │  No interference         │
│  Thread 64 → acquire_slot() → test_db_63─┘                          │
│                                                                       │
│  Thread 65 → acquire_slot() → [WAIT]                                 │
│                  │                                                    │
│                  │ Sleep 50ms, retry...                              │
│                  │                                                    │
│                  ↓ (Thread 1 finishes)                               │
│               test_db_00 released                                    │
│                  │                                                    │
│                  ↓                                                    │
│  Thread 65 → acquire_slot() → test_db_00 ✓                          │
└───────────────────────────────────────────────────────────────────────┘

Benefits:
✅ Up to 64 tests in parallel (vs 1 with shared DB)
✅ No test pollution (isolated databases)
✅ Fast startup (template cloning ~100ms)
✅ Automatic cleanup (advisory locks)
✅ No manual teardown needed
```

## Migration Fingerprinting

```
Purpose: Detect when migrations change, trigger template rebuild

┌───────────────────────────────────────────────────────────────────┐
│ 1. Hash all migration files                                        │
│    SHA256(m001.sql + m002.sql + ... + m050.sql)                   │
│    → "a3f9c2e1..."                                                 │
│                                                                    │
│ 2. Store in test_db_template                                       │
│    CREATE TABLE _test_metadata (                                   │
│      migration_fingerprint TEXT                                    │
│    );                                                              │
│    INSERT VALUES ('a3f9c2e1...');                                  │
│                                                                    │
│ 3. On next test run:                                               │
│    - Compute current hash                                          │
│    - Compare with template hash                                    │
│    - Match?    → Use template (fast)                               │
│    - Mismatch? → Rebuild template (one-time cost)                  │
│                                                                    │
│ 4. Rebuild procedure:                                              │
│    DROP DATABASE IF EXISTS test_db_template;                       │
│    CREATE DATABASE test_db_template;                               │
│    \c test_db_template                                             │
│    -- Apply all migrations                                         │
│    -- Store new fingerprint                                        │
│                                                                    │
│ Result: Template always matches current schema                     │
└───────────────────────────────────────────────────────────────────┘
```

## Fixture Management

```
┌─────────────────────────────────────────────────────────────────────┐
│              Global Fixture Registry (Singleton)                     │
│                                                                       │
│  static FIXTURE_REGISTRY: OnceCell<Arc<Mutex<FixtureRegistry>>>     │
│                                                                       │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │ FixtureRegistry:                                              │  │
│  │                                                                │  │
│  │ cache:      HashMap<FixtureKey, Arc<dyn Any>>                 │  │
│  │ ref_counts: HashMap<FixtureKey, usize>                        │  │
│  │ cleanups:   HashMap<CleanupKey, CleanupTask>                  │  │
│  │                                                                │  │
│  │ FixtureKey = (type_name, params)                              │  │
│  │   e.g., ("TestDatabase", "test_db_05")                        │  │
│  │        ("TestContext", "{config_json}")                       │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                       │
│  Reference Counting:                                                  │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │ Test A calls: test_database("mydb")                           │  │
│  │   → Create fixture, ref_count = 1                             │  │
│  │                                                                │  │
│  │ Test B calls: test_database("mydb")  (same key!)              │  │
│  │   → Return cached fixture, ref_count = 2                      │  │
│  │                                                                │  │
│  │ Test A drops fixture                                           │  │
│  │   → ref_count = 1 (no cleanup yet)                            │  │
│  │                                                                │  │
│  │ Test B drops fixture                                           │  │
│  │   → ref_count = 0 → Run cleanup → Remove from cache           │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                       │
│  Cleanup Tasks:                                                       │
│  - Database: pool.close(), remove advisory lock                      │
│  - Temp files: fs::remove_dir_all(path)                              │
│  - NATS connections: conn.close()                                    │
└───────────────────────────────────────────────────────────────────────┘
```

## Property-Based Testing Strategies

```
Strategy Builders:
┌───────────────────────────────────────────────────────────────────┐
│ SinexStrategies::event_source()                                    │
│ → Generates: "filesystem", "shell.kitty", "", "a"                  │
│                                                                    │
│ SinexStrategies::json_payload()                                    │
│ → Generates: null, strings, objects, arrays (0-10 elements)        │
│                                                                    │
│ SinexStrategies::malicious_payload()  ← ADVERSARIAL                │
│ → Generates:                                                       │
│   - SQL injection: "'; DROP TABLE events; --"                     │
│   - XSS: "<script>alert('xss')</script>"                          │
│   - Path traversal: "../../../../etc/passwd"                      │
│   - DoS: 1MB-2MB strings                                          │
│   - Deeply nested JSON (100 levels)                               │
│   - Integer overflow: i64::MAX                                    │
└───────────────────────────────────────────────────────────────────┘

Runs 100 random test cases, shrinks failures to minimal repro
```

## See Also

- Patterns: [patterns.md](./patterns.md)
- Test docs: [README.md](./README.md)
