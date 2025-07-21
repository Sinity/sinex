# Test Suite Refactoring Plan

## Current State Analysis

### Problems Identified
1. **371 raw SQL queries** using `sqlx::query*` instead of centralized query builders
2. **Repetitive test patterns** - similar tests with slight variations
3. **Inconsistent abstractions** - some tests use builders, others use raw SQL
4. **ULID/UUID conversion issues** - manual casting instead of using query builder's automatic conversion
5. **Missing test utilities** - common operations reimplemented in each test

### Key Insights
- The codebase has a sophisticated query builder system in `sinex-db/src/queries/`
- Query builders automatically handle ULID/UUID conversions
- Tests are bypassing this abstraction layer, leading to maintenance issues

## Refactoring Strategy

### Phase 1: Create Test Abstraction Layer

#### 1.1 Test Query Builders
Create test-specific query builders that wrap the production query builders:

```rust
// test/common/query_helpers.rs
pub struct TestQueries;

impl TestQueries {
    pub async fn insert_test_event(
        pool: &DbPool,
        source: &str,
        event_type: &str,
        payload: JsonValue,
    ) -> Result<RawEvent> {
        EventQueries::insert_event(
            source.to_string(),
            event_type.to_string(),
            hostname::get()?.to_string(),
            payload,
            None,
            None,
            None,
            None,
        )
        .fetch_one(pool)
        .await
    }
    
    pub async fn get_checkpoint(
        pool: &DbPool,
        automaton_name: &str,
    ) -> Result<Option<CheckpointRecord>> {
        CheckpointQueries::get_checkpoint(
            automaton_name.to_string(),
            format!("{}-group", automaton_name),
            format!("{}-consumer", automaton_name),
        )
        .fetch_optional(pool)
        .await
    }
}
```

#### 1.2 Test Data Builders
Enhance existing builders with fluent interfaces:

```rust
// test/common/builders.rs
pub struct TestEventBuilder {
    source: String,
    event_type: String,
    payload: JsonValue,
    ts_orig: Option<DateTime<Utc>>,
    source_event_ids: Option<Vec<Ulid>>,
}

impl TestEventBuilder {
    pub fn new(source: &str, event_type: &str) -> Self {
        Self {
            source: source.to_string(),
            event_type: event_type.to_string(),
            payload: json!({}),
            ts_orig: None,
            source_event_ids: None,
        }
    }
    
    pub fn with_payload(mut self, payload: JsonValue) -> Self {
        self.payload = payload;
        self
    }
    
    pub fn with_timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.ts_orig = Some(ts);
        self
    }
    
    pub fn with_source_events(mut self, ids: Vec<Ulid>) -> Self {
        self.source_event_ids = Some(ids);
        self
    }
    
    pub async fn insert(self, pool: &DbPool) -> Result<RawEvent> {
        EventQueries::insert_event(
            self.source,
            self.event_type,
            hostname::get()?.to_string(),
            self.payload,
            self.ts_orig,
            None,
            None,
            self.source_event_ids,
        )
        .fetch_one(pool)
        .await
    }
}
```

### Phase 2: Refactor Test Categories

#### 2.1 Database Tests
- Use `EventQueries`, `CheckpointQueries`, etc. exclusively
- No raw SQL queries
- Test database functionality, not SQL syntax

#### 2.2 Integration Tests
- Use test builders for setup
- Focus on end-to-end flows
- Verify through query builders, not raw SQL

#### 2.3 Performance Tests
- Use batch operations with query builders
- Measure query builder performance, not raw SQL

### Phase 3: Eliminate Repetition

#### 3.1 Parameterized Tests
```rust
#[test_case("fs", "file.created"; "filesystem events")]
#[test_case("shell", "command.executed"; "shell events")]
#[test_case("clipboard", "content.changed"; "clipboard events")]
async fn test_event_insertion(source: &str, event_type: &str) -> Result<()> {
    // Single test implementation for all cases
}
```

#### 3.2 Test Macros
```rust
macro_rules! test_event_flow {
    ($name:ident, $source:expr, $event_type:expr, $payload:expr) => {
        #[sinex_test]
        async fn $name(pool: DbPool) -> TestResult {
            let event = TestEventBuilder::new($source, $event_type)
                .with_payload($payload)
                .insert(&pool)
                .await?;
            
            // Common assertions
            assert_eq!(event.source, $source);
            assert_eq!(event.event_type, $event_type);
            Ok(())
        }
    };
}
```

### Phase 4: Coverage Optimization

#### 4.1 Coverage Matrix
- Map what each test covers
- Identify gaps and overlaps
- Remove redundant tests
- Add missing edge cases

#### 4.2 Test Categories
1. **Unit Tests**: Core logic, no database
2. **Integration Tests**: Query builders, database interactions
3. **System Tests**: Full pipeline, multiple components
4. **Property Tests**: Invariants, edge cases

### Implementation Plan

1. **Week 1**: Create test abstraction layer
   - Test query builders
   - Enhanced test data builders
   - Common test utilities

2. **Week 2**: Refactor database tests
   - Replace all raw SQL with query builders
   - Update assertions to use test utilities
   - Fix ULID/UUID issues

3. **Week 3**: Refactor integration tests
   - Use test builders throughout
   - Eliminate duplicate test logic
   - Add parameterized tests

4. **Week 4**: Optimize coverage
   - Remove redundant tests
   - Add missing edge cases
   - Update documentation

## Success Metrics

1. **Zero raw SQL queries** in tests (down from 371)
2. **50% reduction** in test code volume through deduplication
3. **100% usage** of query builders for database operations
4. **Consistent test patterns** across all test categories
5. **Improved test performance** through batch operations

## Long-term Benefits

1. **Maintainability**: Changes to schema require updates in one place
2. **Type Safety**: Query builders provide compile-time guarantees
3. **Performance**: Prepared statements and connection pooling
4. **Consistency**: All tests follow same patterns
5. **Documentation**: Tests serve as examples of proper API usage