# Database Testing Architecture

The test utilities provide a sophisticated database pooling system optimized for parallel test
execution. Each test gets an isolated database that is cleaned and reused between tests.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         PostgreSQL Server                                │
├─────────────────────────────────────────────────────────────────────────┤
│  sinex_test_template_shared (migrated template)                         │
│    ├── Schema fingerprint                                               │
│    └── Extension versions (TimescaleDB, etc.)                           │
├─────────────────────────────────────────────────────────────────────────┤
│  sinex_test_pool_0   ──── advisory lock 0 ────► Test A                  │
│  sinex_test_pool_1   ──── advisory lock 1 ────► Test B                  │
│  sinex_test_pool_2   ──── advisory lock 2 ────► Test C                  │
│  ...                                                                     │
│  sinex_test_pool_N-1 ──── advisory lock N-1 ──► Test N                  │
└─────────────────────────────────────────────────────────────────────────┘
```

### Layers

1. **Template Database** — A shared, migrated template (`sinex_test_template_shared`) tagged with
   schema fingerprint and extension versions. Created once per migration set.

2. **Database Pool** — `sinex_test_pool_0..N-1` cloned from the template via PostgreSQL's
   `CREATE DATABASE ... WITH TEMPLATE` for fast provisioning.

3. **Advisory Locks** — PostgreSQL advisory locks coordinate exclusive access across test
   processes.

4. **Smart Cleanup** — Efficient reset/truncate with verification and slot quarantine on failure.

## Pool Sizing

Pool size is derived deterministically from the active Nextest profile:

```
pool_size = max(64, test_threads × 2)
slot_max_connections = 4
admin_max_connections = 8
```

If PostgreSQL `max_connections` is lower than required, the pool auto-shrinks:

```
effective_pool = floor((max_connections - admin_max_connections) / slot_max_connections)
```

`test_threads` comes from `.config/nextest.toml` when running under Nextest (via
`NEXTEST_PROFILE` or `NEXTEST_PROFILE_NAME`); otherwise it falls back to CPU count.

## Database Lifecycle

### 1. Template Creation

The first test process creates the migrated template:

```rust
async fn ensure_template_database(
    admin_url: &str,
    base_url: &str,
    slot_max_connections: u32,
) -> Result<String>
```

Template creation:
- Runs all migrations
- Records schema fingerprint (hash of migration files)
- Tracks extension versions (TimescaleDB, etc.)
- Stores metadata in `target/xtask sandbox/template_stamp.json`

Later processes reuse the template if the fingerprint matches.

### 2. Pool Initialization

- **Nextest runs**: Pool databases are lazily created on-demand (first test needing slot N
  creates `sinex_test_pool_N`)
- **Non-Nextest runs**: May eagerly create pool databases

Pre-provision the pool before test runs:

```bash
xtask test --prime
# or
cargo run -p xtask sandbox --bin db_prime
```

### 3. Test Acquisition

```rust
pub struct TestDatabase {
    name: String,
    pool: DbPool,
    slot: Arc<DatabaseSlot>,
    lock_id: i64,
    acquired_at: Instant,
    acquisition_process_id: u32,
}

let test_db = acquire_test_database().await?;
let pool = test_db.pool();
```

Acquisition sequence:
1. Find available slot
2. Acquire PostgreSQL advisory lock (lock ID = hash of database name)
3. Reset database (truncate all tables)
4. Verify clean state
5. Return TestDatabase handle

### 4. Test Execution

The test has exclusive access to its database via the advisory lock. Database operations
are fully isolated from other tests.

### 5. Release

On TestDatabase drop:
1. Advisory lock released (non-blocking via background manager)
2. Connection pool closed
3. Slot marked as available

## Foreign Key Handling

Database cleanup respects foreign key constraints:

```rust
async fn clean_database(pool: &DbPool, db_name: &str) -> Result<()> {
    // 1. Disable FK checks temporarily
    // 2. Truncate in dependency order
    // 3. Re-enable FK checks
    // 4. Verify referential integrity
}
```

The cleanup process:
1. Discovers FK relationships via `information_schema`
2. Builds dependency graph
3. Truncates tables in topological order
4. Verifies all tables are empty

## Advisory Lock Management

Advisory locks prevent race conditions in multi-process test execution:

```
Lock ID = hash(database_name) % 2^31
```

Lock types:
- **Slot locks** — Exclusive during test acquisition and execution
- **Template locks** — Shared during template use, exclusive during template recreation

```rust
// Acquire slot lock
SELECT pg_advisory_lock($1);

// Release slot lock
SELECT pg_advisory_unlock($1);

// Try acquire (non-blocking)
SELECT pg_try_advisory_lock($1);
```

### Stuck Lock Recovery

If a test process crashes, locks may be orphaned. Detection and recovery:

```sql
-- Check for stuck locks
SELECT * FROM pg_stat_activity WHERE datname LIKE 'sinex_test%';

-- Force release
SELECT pg_terminate_backend(pid) FROM pg_stat_activity
WHERE datname LIKE 'sinex_test%';
```

## Cache Invalidation

The template is rebuilt when:
- Migration files change (detected via fingerprint)
- TimescaleDB version changes
- Required schema elements are missing
- Explicit cache clear

Manual cache invalidation (only for debugging):

```bash
rm target/xtask sandbox/template_stamp.json
xtask test --prime
```

## Performance Characteristics

| Operation | Typical Duration |
|-----------|------------------|
| Template creation | 5-15 minutes (first run) |
| Template reuse check | <1 second |
| Database cloning | <1 second |
| Database acquisition | <100ms |
| Database cleanup | <50ms |

### Optimization Strategies

1. **Template caching** — Migrations run once per fingerprint change
2. **Pool sizing** — Match Nextest test threads to available capacity
3. **Connection limits** — Small per-slot pools (4) prevent exhaustion
4. **Lazy provisioning** — Databases created on demand under Nextest
5. **Batch operations** — Insert events in groups when possible

## Monitoring

```rust
// Check pool health
let report = check_pool_health().await?;
println!("Healthy: {}/{}", report.healthy_slots, report.total_slots);
println!("Quarantined: {}", report.quarantined_slots);

// Get acquisition statistics
let stats = get_pool_stats();
println!("Total acquisitions: {}", stats.total_acquisitions);
println!("Avg wait time: {}ms", stats.average_wait_time_ms);
println!("Cleanup failures: {}", stats.cleanup_failures);
```

## Usage Patterns

### Automatic via TestContext (Recommended)

```rust
#[sinex_test]
async fn test_something(ctx: TestContext) -> Result<()> {
    // Database automatically acquired and cleaned
    ctx.pool.events().insert(event).await?;
    Ok(())
}
```

### Manual Acquisition (Rare)

```rust
let db = acquire_test_database().await?;
let pool = db.pool();
// ... use pool for queries
// Automatically returned to pool on drop
```

### Verify Clean State

```rust
#[sinex_test]
async fn test_clean_state(ctx: TestContext) -> Result<()> {
    // Database is guaranteed clean at start
    let count = ctx.pool.events().count_all().await?;
    assert_eq!(count, 0);
    Ok(())
}
```

## Fixtures and Seeding

### Direct Repository Insertion

```rust
let event = Event::<JsonValue>::test_event(
    "fs-watcher",
    "file.created",
    json!({"path": "/test.txt"})
);
ctx.pool.events().insert(event).await?;
```

### Pipeline Insertion (Preferred)

```rust
let ctx = ctx.with_nats().shared().await?;
let event = ctx.publish_event(
    "fs-watcher",
    "file.created",
    json!({"path": "/test.txt"})
).await?;
```

### Batch Seeding

```rust
use xtask::sandbox::dataset_seeds::{seed_events_via_pipeline, EventSpec, SeedClock};

let clock = SeedClock::default();
let specs = vec![
    EventSpec::new("fs-watcher", "file.created", json!({"path": "/a"})),
    EventSpec::new("terminal", "command.executed", json!({"cmd": "ls"})),
];
let ctx = ctx.with_nats().shared().await?;
let pipeline = ctx.pipeline_scope().await?;
let ids = seed_events_via_pipeline(&pipeline, &clock, &specs).await?;
```

## Troubleshooting

### "Database pool exhausted"

**Cause**: More concurrent tests than available slots.

**Solutions**:
- Reduce concurrent tests: `xtask test --debug`
- Raise PostgreSQL `max_connections`
- Adjust `.config/nextest.toml` test threads

### "Advisory lock timeout"

**Cause**: Database stuck, previous test crashed.

**Solutions**:
```bash
# Check PostgreSQL connections
psql -c "SELECT * FROM pg_stat_activity WHERE datname LIKE 'sinex_test%'"

# Kill stuck backends
psql -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname LIKE 'sinex_test%'"
```

### "Migration fingerprint mismatch"

**Cause**: Template out of sync with migration files.

**Solution**: The harness auto-rebuilds; for manual reset:
```bash
rm target/xtask sandbox/template_stamp.json
xtask test --prime
```

### "Tests hang on cleanup"

**Cause**: Background cleanup manager waiting on locks.

**Solution**: Check for orphaned processes:
```bash
ps aux | grep sqlx
```

## Key Files

- `database_pool.rs` (~1800 lines) — Pool implementation
- `db_common.rs` — Shared cleanup utilities
- `target/xtask sandbox/template_stamp.json` — Template metadata cache
