# Database Patterns

Database layer patterns for Sinex.

## Repository Pattern

Exemplary implementation with compile-time safety:

```rust
pub trait Repository<'a> {
    fn pool(&self) -> &'a PgPool;
    fn new(pool: &'a PgPool) -> Self;
}
```

**Lifetime-Based Ownership:**
- Repositories borrow pool with lifetime `'a`
- No owned pool = no connection leaks
- Zero-cost abstraction (compiles to direct function calls)

**DbPoolExt for Ergonomic Access:**
```rust
pub trait DbPoolExt {
    fn events(&self) -> EventRepository<'_>;
    fn checkpoints(&self) -> CheckpointRepository<'_>;
    fn source_materials(&self) -> SourceMaterialRepository<'_>;
}

// Usage:
let event = pool.events().get_by_id(event_id).await?;
```

## SQLX Compile-Time Validation

```rust
pub async fn insert<T>(&self, event: Event<T>) -> DbResult<Event<JsonValue>> {
    let record = sqlx::query_as!(
        EventRecord,
        r#"
        INSERT INTO core.events (id, source, event_type, payload, ...)
        VALUES ($1, $2, $3, $4, ...)
        RETURNING id as "id!: Uuid", ...
        "#,
        id.to_uuid(),
        event.source.as_str(),
        // ...
    )
    .fetch_one(self.pool)
    .await?;

    Ok(record.try_to_event()?)
}
```

**Benefits:**
- Compile-time SQL validation (catches typos, schema mismatches)
- Type-safe bindings with proper nullability
- SQL injection protection via parameterized queries

## TimescaleDB Integration

**Hypertable Partitioning:**
```sql
SELECT create_hypertable(
    'core.events',
    by_range('id', partition_func => 'uuid_extract_timestamp'::regproc),
    if_not_exists => TRUE
);
```

**Partition Strategy:**
- Partition column: `id` (UUIDv7)
- Partition function: `uuid_extract_timestamp` (extracts timestamp from UUIDv7)
- Partition interval: Automatic (~7 days default)

**Benefits:**
- UUIDv7 as partition key (clever design synergy)
- Time-series optimizations automatic
- Native time-bucketing support

## Test Database Pool

64 pre-created databases for parallel testing:

```rust
pub async fn acquire_slot(&self) -> DbResult<TestDatabase> {
    for i in 0..self.slots.len() {
        // Try to acquire advisory lock (non-blocking)
        let lock_acquired: bool = sqlx::query_scalar(
            "SELECT pg_try_advisory_lock($1)"
        )
        .bind(slot.advisory_lock_key)
        .fetch_one(&pool)
        .await?;

        if lock_acquired {
            return Ok(TestDatabase { pool, slot_number: i, ... });
        }
    }
    // All slots busy, sleep and retry
}
```

**Benefits:**
- Up to 64 parallel tests
- No test pollution (isolated databases)
- Fast test startup (template cloning)
- Automatic migration management

## See Also

- Schema design: `crate/lib/sinex-schema/docs/schema_design.md`
