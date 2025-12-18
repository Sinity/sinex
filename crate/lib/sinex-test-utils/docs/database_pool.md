# Database Pool – High-Performance Test Database Isolation

Provides a sophisticated database pooling system optimized for parallel test execution. It maintains
a pool of migrated databases that are cleaned and reused between tests for optimal performance.

## Architecture

The pool uses a multi-layered approach:

1. **Template Database** – a shared migrated template (`sinex_test_template_shared`) tagged with a
   schema fingerprint and extension versions.
2. **Database Pool** – `sinex_test_pool_0..N-1` cloned from the template (size is configurable).
3. **Advisory Locks** – PostgreSQL advisory locks for inter-process coordination.
4. **Smart Cleanup** – efficient reset/truncate with verification and slot quarantine on failure.

## Performance Characteristics

The pool is designed so that:

- Database creation happens rarely (first run, or when the schema fingerprint changes).
- Each test acquires a DB via advisory locks, resets it, then runs assertions.
- Under `cargo nextest`, pool DBs are lazily created on-demand to avoid heavy DDL in every per-test
  process.

## Usage Pattern

```rust
// Automatic through TestContext (recommended)
#[sinex_test]
async fn test_something(ctx: TestContext) -> Result<()> {
    // Database automatically acquired (from the pool) and cleaned before use
    ctx.create_test_event("test", "test.event", json!({})).await?;
    Ok(())
}

// Manual acquisition (for special cases)
let db = acquire_test_database().await?;
let pool = db.pool();
// ... use pool for queries
// Automatically returned to pool on drop
```

## Implementation Details

### Database Lifecycle

1. Template creation – first process creates the migrated template; later processes reuse it if the
   schema fingerprint matches.
2. Pool initialization
   - Non-nextest runs may eagerly create pool DBs.
   - Nextest runs lazily create pool DBs on-demand.
3. Test acquisition – DB acquired with an advisory lock and reset/verified clean.
4. Test execution – isolated database operations.
5. Release – advisory lock is released and the connection pool is closed.

### Foreign Key Handling

The cleanup process respects foreign key constraints:

1. Disable FK checks temporarily.
2. Truncate in dependency order.
3. Re-enable FK checks.
4. Verify referential integrity.

### Lock Management

Advisory locks prevent race conditions:

- Lock ID = `hash(database_name) % 2^31`.
- Exclusive slot locks during acquisition/use.
- Shared/exclusive template locks to prevent template recreation during cloning.

## Monitoring

```rust
let stats = get_pool_stats();
println!("Total acquisitions: {}", stats.total_acquisitions);
println!("Avg wait time: {}ms", stats.average_wait_time_ms);
println!("Cleanup failures: {}", stats.cleanup_failures);
```
