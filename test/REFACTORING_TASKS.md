# Parallel Refactoring Task Instructions

## Overview
We have 331 raw SQL queries across 32 test files to refactor. These tasks can be executed in parallel to maximize efficiency before context runs out.

## Task Distribution

### Task 1: Database Integration Tests
**Files:** 
- `test/integration/database_test.rs` (45 queries)
- `test/integration/checkpoint_consistency_test.rs` (12 queries)
- `test/integration/checkpoint_persistence_test.rs` (8 queries)

**Instructions:**
1. Replace all `sqlx::query*` with appropriate `TestQueries` methods
2. Use `TestEventBuilder` for event creation
3. Use `TestCheckpointBuilder` for checkpoint operations
4. Remove all manual ULID/UUID conversions
5. Ensure all tests use `#[sinex_test]` macro

**Key Patterns:**
```rust
// Replace this:
sqlx::query!("INSERT INTO core.events...")
// With:
TestEventBuilder::new(source, event_type).insert(&pool).await?

// Replace this:
sqlx::query_as!("SELECT ... FROM core.automaton_checkpoints...")
// With:
TestQueries::get_checkpoint(&pool, automaton_name).await?
```

### Task 2: Event and Search Tests
**Files:**
- `test/integration/event_sources_test.rs` (15 queries)
- `test/integration/search_service_test.rs` (18 queries)
- `test/integration/process_event_test.rs` (10 queries)

**Instructions:**
1. Use `BatchEventBuilder` for bulk event creation
2. Replace event queries with `TestQueries::get_events_by_*` methods
3. Use `TestScenarioBuilder` for complex multi-step tests
4. Pay special attention to SQL injection test - keep security focus but use safe patterns

### Task 3: System and Performance Tests
**Files:**
- `test/system/performance_test.rs` (22 queries)
- `test/system/stress_test.rs` (14 queries)
- `test/system/reliability_test.rs` (19 queries)
- `test/performance/*_test.rs` (35 queries total)

**Instructions:**
1. Performance tests should use `BatchEventBuilder` for load generation
2. Keep timing measurements but use query builders
3. For concurrent tests, use Arc<DbPool> pattern with TestQueries
4. Replace raw count queries with `TestQueries::count_events_by_source`

### Task 4: Satellite and Architecture Tests
**Files:**
- `test/integration/satellite_architecture_test.rs` (25 queries)
- `test/integration/system_integration_test.rs` (20 queries)
- `test/integration/end_to_end_workflows_test.rs` (18 queries)

**Instructions:**
1. Use `TestScenarioBuilder` for end-to-end flows
2. Keep architectural validation but use proper abstractions
3. For satellite tests, combine with existing satellite test utilities
4. Ensure event flow tests use `TestEventBuilder::with_source_events`

### Task 5: Adversarial and Property Tests
**Files:**
- `test/adversarial/*.rs` (28 queries total)
- `test/property/*.rs` (15 queries total)
- `test/unit/database_test.rs` (12 queries)

**Instructions:**
1. Maintain test intent for edge cases and adversarial scenarios
2. Use builders but keep payloads that test boundaries
3. For property tests, integrate with proptest while using query builders
4. Keep chaos engineering aspects but use safe database patterns

## Common Refactoring Patterns

### Pattern 1: Event Insertion
```rust
// OLD:
let id: Ulid = sqlx::query_scalar!(
    r#"INSERT INTO core.events (source, event_type, host, payload)
       VALUES ($1, $2, $3, $4)
       RETURNING event_id as "event_id: Ulid""#,
    source, event_type, host, payload
).fetch_one(&pool).await?;

// NEW:
let event = TestEventBuilder::new(source, event_type)
    .with_host(host)
    .with_payload(payload)
    .insert(&pool)
    .await?;
let id = event.id;
```

### Pattern 2: Checkpoint Query
```rust
// OLD:
let checkpoint = sqlx::query_as!(
    CheckpointRecord,
    r#"SELECT * FROM core.automaton_checkpoints WHERE automaton_name = $1"#,
    name
).fetch_one(&pool).await?;

// NEW:
let checkpoint = TestQueries::get_checkpoint(&pool, name)
    .await?
    .expect("Checkpoint should exist");
```

### Pattern 3: Count Query
```rust
// OLD:
let count: i64 = sqlx::query_scalar!(
    "SELECT COUNT(*) FROM core.events WHERE source = $1",
    source
).fetch_one(&pool).await?;

// NEW:
let count = TestQueries::count_events_by_source(&pool, source).await?;
```

### Pattern 4: Batch Operations
```rust
// OLD:
for i in 0..100 {
    sqlx::query!("INSERT INTO core.events...").execute(&pool).await?;
}

// NEW:
BatchEventBuilder::new("source", "type", 100)
    .insert(&pool)
    .await?;
```

## Success Criteria
Each task should:
1. Compile without errors: `cargo check --tests`
2. Pass all tests: `cargo test <specific_test_file>`
3. Have zero raw SQL queries: `! grep -q "sqlx::query" <file>`
4. Follow consistent patterns from examples

## Commit Strategy
Each task should create a separate commit:
```bash
git add <modified_files>
git commit -m "refactor: migrate <component> tests to query builders

- Replace raw SQL with TestQueries methods
- Use TestEventBuilder for event creation
- Remove manual ULID/UUID conversions
- Maintain all test functionality"
```