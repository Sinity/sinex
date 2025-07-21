# Test Migration Guide

This guide shows how to migrate tests from raw SQL queries to the new query builder patterns.

## Before and After Examples

### Example 1: Basic Event Insertion

**Before (Raw SQL):**
```rust
let event = events::filesystem_event("file.created", "/test.txt");
let inserted_id: Ulid = sqlx::query_scalar!(
    r#"
    INSERT INTO core.events (source, event_type, host, payload)
    VALUES ($1, $2, $3, $4)
    RETURNING event_id as "event_id: Ulid"
    "#,
    event.source,
    event.event_type,
    event.host,
    event.payload
)
.fetch_one(&pool)
.await?;
```

**After (Query Builder):**
```rust
let event = TestEvents::filesystem("/test.txt")
    .insert(&pool)
    .await?;
```

### Example 2: Checkpoint Query

**Before (Raw SQL):**
```rust
let checkpoint: CheckpointRecord = sqlx::query_as!(
    CheckpointRecord,
    r#"
    SELECT automaton_name, consumer_group, consumer_name, 
           last_processed_id::text, processed_count
    FROM core.automaton_checkpoints
    WHERE automaton_name = $1 
      AND consumer_group = $2 
      AND consumer_name = $3
    "#,
    automaton_name,
    consumer_group,
    consumer_name
)
.fetch_one(&pool)
.await?;
```

**After (Query Builder):**
```rust
let checkpoint = TestQueries::get_checkpoint(&pool, automaton_name)
    .await?
    .expect("Checkpoint should exist");
```

### Example 3: Batch Event Insertion

**Before (Raw SQL):**
```rust
for i in 0..100 {
    let event = events::filesystem_event("file.created", &format!("/file{}.txt", i));
    sqlx::query!(
        r#"
        INSERT INTO core.events (source, event_type, host, payload, ts_orig)
        VALUES ($1, $2, $3, $4, $5)
        "#,
        event.source,
        event.event_type,
        event.host,
        event.payload,
        event.ts_orig
    )
    .execute(&pool)
    .await?;
}
```

**After (Query Builder):**
```rust
let events = BatchEventBuilder::new("fs", "file.created", 100)
    .with_payload_generator(|i| json!({
        "path": format!("/file{}.txt", i)
    }))
    .insert(&pool)
    .await?;
```

### Example 4: ULID/UUID Conversion

**Before (Raw SQL with manual conversion):**
```rust
let event_id = Ulid::new();
let uuid = Uuid::from_str(&event_id.to_string())?;
sqlx::query!(
    "INSERT INTO core.events (event_id, source, event_type, host, payload) 
     VALUES ($1::uuid, $2, $3, $4, $5)",
    uuid,
    source,
    event_type,
    host,
    payload
)
.execute(&pool)
.await?;
```

**After (Automatic conversion):**
```rust
let event = TestEventBuilder::new(source, event_type)
    .with_payload(payload)
    .insert(&pool)
    .await?;
```

### Example 5: Complex Query with Joins

**Before (Raw SQL):**
```rust
let results = sqlx::query!(
    r#"
    SELECT e.event_id::text as id, e.source, e.event_type, 
           ac.processed_count
    FROM core.events e
    LEFT JOIN core.automaton_checkpoints ac 
        ON ac.last_processed_id = e.event_id::text
    WHERE e.source = $1 
      AND e.ts_orig > $2
    ORDER BY e.ts_orig DESC
    LIMIT $3
    "#,
    source,
    start_time,
    limit as i64
)
.fetch_all(&pool)
.await?;
```

**After (Using multiple query builders):**
```rust
// Get events
let events = TestQueries::get_events_by_source(&pool, source, Some(limit))
    .await?;

// Get related checkpoints if needed
for event in &events {
    if let Some(checkpoint) = TestQueries::get_checkpoint_by_event(&pool, &event.id).await? {
        // Process checkpoint data
    }
}
```

## Migration Checklist

1. **Replace raw SQL queries:**
   - [ ] Search for `sqlx::query` and replace with appropriate query builders
   - [ ] Use `TestQueries` for common operations
   - [ ] Use `TestEventBuilder` for creating test data

2. **Remove manual ULID/UUID conversions:**
   - [ ] Query builders handle conversions automatically
   - [ ] No need for `.to_uuid()` or manual string conversions

3. **Use test macros for common patterns:**
   - [ ] Replace repetitive test code with macros
   - [ ] Use `test_event_insertion!` for simple insertion tests
   - [ ] Use `test_batch_events!` for batch operations

4. **Simplify test setup:**
   - [ ] Use `TestScenarioBuilder` for complex test scenarios
   - [ ] Use `BatchEventBuilder` for creating multiple events

5. **Improve assertions:**
   - [ ] Use structured types instead of tuples
   - [ ] Leverage builder patterns for clearer test intent

## Benefits After Migration

1. **Type Safety**: No more SQL syntax errors at runtime
2. **Maintainability**: Schema changes only require updates in query builders
3. **Readability**: Tests express intent clearly without SQL noise
4. **Performance**: Query builders use prepared statements
5. **Consistency**: All tests follow the same patterns

## Common Pitfalls to Avoid

1. **Don't mix patterns**: Either use query builders or raw SQL, not both
2. **Don't bypass abstractions**: Use TestQueries instead of calling EventQueries directly in tests
3. **Don't forget cleanup**: Use `TestQueries::cleanup_*` methods
4. **Don't hardcode values**: Use builders to generate test data dynamically

## Next Steps

1. Start with simple tests (basic CRUD operations)
2. Move to complex tests (joins, aggregations)
3. Update test documentation
4. Remove unused test utilities
5. Add new test cases using the improved patterns