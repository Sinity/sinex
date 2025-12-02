# Database Pool – High-Performance Test Database Isolation

Provides a sophisticated database pooling system optimized for parallel test execution. It maintains
a pool of pre-warmed, migrated databases that are cleaned and reused between tests for optimal
performance.

## Architecture

The pool uses a multi-layered approach:

1. **Template Database** – single migrated template created once per test run.
2. **Database Pool** – 64 pre-created databases cloned from the template.
3. **Advisory Locks** – PostgreSQL advisory locks for inter-process coordination.
4. **Smart Cleanup** – efficient truncation with foreign key awareness.

## Performance Characteristics

- Acquisition time: ~5–10 ms per database (after initial warmup).
- Cleanup time: ~20–30 ms with optimised truncation.
- Parallelism: supports 64 concurrent tests without contention.
- Memory usage: ~50 MB per database (configurable).

## Usage Pattern

```rust
// Automatic through TestContext (recommended)
#[sinex_test]
async fn test_something(ctx: TestContext) -> Result<()> {
    // Database automatically acquired and cleaned
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

1. Template creation – first test creates migrated template.
2. Pool initialization – 64 databases created from template.
3. Test acquisition – clean database acquired with advisory lock.
4. Test execution – isolated database operations.
5. Cleanup & return – data truncated, returned to pool.

### Foreign Key Handling

The cleanup process respects foreign key constraints:

1. Disable FK checks temporarily.
2. Truncate in dependency order.
3. Re-enable FK checks.
4. Verify referential integrity.

### Lock Management

Advisory locks prevent race conditions:

- Lock ID = `hash(database_name) % 2^31`.
- Exclusive locks during acquisition/cleanup.
- Automatic release on connection drop.

## Monitoring

```rust
let stats = get_pool_stats();
println!("Total acquisitions: {}", stats.total_acquisitions);
println!("Avg wait time: {}ms", stats.average_wait_time_ms);
println!("Cleanup failures: {}", stats.cleanup_failures);
```
