# Test Modernization Guide

This guide demonstrates how to modernize the Sinex test suite using powerful abstractions to achieve ~75% code reduction while increasing coverage.

## Core Modernization Patterns

### 1. Property-Based Testing

**Before**: Multiple hardcoded test cases
```rust
#[test]
fn test_ulid_ordering() {
    let ulid1 = Ulid::new();
    let ulid2 = Ulid::new();
    assert!(ulid2 > ulid1);
}

#[test]
fn test_ulid_string_ordering() {
    let ulid1 = Ulid::new();
    let ulid2 = Ulid::new();
    assert!(ulid2.to_string() > ulid1.to_string());
}

// ... 20 more similar tests
```

**After**: Single comprehensive property test
```rust
sinex_proptest_sync! {
    fn ulid_ordering_properties(ulids in proptest::collection::vec(ulids(), 2..50)) {
        // Tests millions of cases instead of a few hardcoded ones
        let mut sorted = ulids.clone();
        sorted.sort();
        
        // Multiple properties tested together
        for window in sorted.windows(2) {
            prop_assert!(window[1] > window[0]);
            prop_assert!(window[1].to_string() > window[0].to_string());
            prop_assert!(window[1].timestamp_ms() >= window[0].timestamp_ms());
        }
    }
}
```

### 2. Test Macros for Common Patterns

**Before**: Repetitive event insertion tests
```rust
#[sinex_test]
async fn test_filesystem_event(ctx: TestContext) -> TestResult {
    let event = RawEvent {
        id: Ulid::new(),
        source: "filesystem",
        event_type: "file.created",
        payload: json!({"path": "/test.txt"}),
        // ... more fields
    };
    
    let id = sinex_db::insert_event(ctx.pool(), &event).await?;
    let retrieved = sinex_db::get_event_by_id(ctx.pool(), id).await?;
    assert_eq!(retrieved.source, event.source);
    // ... more assertions
    Ok(())
}
```

**After**: Declarative test macro
```rust
test_event_insertion!(
    test_filesystem_event,
    sources::FS,
    event_types::filesystem::FILE_CREATED,
    json!({"path": "/test.txt"})
);
```

### 3. Builder Patterns

**Before**: Manual event construction
```rust
let events = vec![];
for i in 0..100 {
    events.push(RawEvent {
        id: Ulid::new(),
        ts_orig: Some(Utc::now() + Duration::seconds(i)),
        source: "test",
        event_type: "test.event",
        payload: json!({"index": i}),
        // ... many fields
    });
}
```

**After**: Fluent builder API
```rust
let events = BatchEventBuilder::new("test", "test.event", 100)
    .with_time_spacing(Duration::seconds(1))
    .with_payload_generator(|i| json!({"index": i}))
    .build();
```

### 4. Smart Waiting

**Before**: Arbitrary sleeps
```rust
// Insert events
for event in events {
    insert_event(&event).await?;
}
tokio::time::sleep(Duration::from_secs(2)).await; // Hope it's enough
let count = get_event_count().await?;
assert_eq!(count, 100);
```

**After**: Condition-based waiting
```rust
ctx.insert_events(&events).await?;
ctx.wait_for_event_count(100).await?; // Waits exactly as long as needed
```

### 5. Parameterized Tests

**Before**: Copy-pasted test functions
```rust
#[test]
fn test_error_database() {
    let err = CoreError::Database("Connection failed".into());
    assert!(err.to_string().contains("database"));
}

#[test]
fn test_error_validation() {
    let err = CoreError::Validation("Invalid format".into());
    assert!(err.to_string().contains("validation"));
}
// ... 10 more similar tests
```

**After**: Single parameterized test
```rust
parameterized_test!(
    test_error_display_formats,
    vec![
        ("database", CoreError::Database("Connection failed".into())),
        ("validation", CoreError::Validation("Invalid format".into())),
        ("serialization", CoreError::Serialization("Parse error".into())),
        // ... all cases in one place
    ],
    |_pool, (name, error)| async move {
        let display = error.to_string();
        assert!(display.contains(name));
        Ok(())
    }
);
```

### 6. Concurrent Testing Patterns

**Before**: Manual thread management
```rust
let handles = vec![];
for i in 0..10 {
    let handle = tokio::spawn(async move {
        // Complex setup
        let result = do_operation(i).await;
        // Manual verification
        result
    });
    handles.push(handle);
}
for handle in handles {
    handle.await??;
}
```

**After**: Structured concurrent test
```rust
test_concurrent_operations!(
    test_name,
    10, // concurrent tasks
    |pool, task_id| async move { 
        // Operation with automatic error handling
    },
    |pool, results| async move { 
        // Consolidated verification
    }
);
```

### 7. Stateful Property Testing

**Before**: Fixed test sequences
```rust
#[test]
fn test_event_sequence() {
    let mut events = vec![];
    events.push(create_event());
    assert_eq!(events.len(), 1);
    events.push(create_event());
    assert_eq!(events.len(), 2);
    // Limited scenarios
}
```

**After**: Exploring state spaces
```rust
stateful_proptest! {
    name: event_sequence_properties,
    state: Vec<RawEvent>,
    operations: [
        add_event() => {
            let event = arbitrary_event();
            state.push(event);
            // Invariants checked after each operation
            assert!(state.iter().all(|e| e.id != Ulid::nil()));
        },
        clear() => {
            state.clear();
            assert!(state.is_empty());
        }
    ]
}
```

## Migration Strategy

### Phase 1: Identify Patterns
1. Look for repetitive test functions testing similar things
2. Find tests with hardcoded values that could be generalized
3. Identify sleeps and polling loops
4. Find copy-pasted test setup code

### Phase 2: Apply Abstractions
1. Group similar tests into property-based tests
2. Extract common patterns into test macros
3. Replace manual construction with builders
4. Replace sleeps with smart waiting

### Phase 3: Measure Impact
- Count line reduction
- Measure coverage increase
- Document patterns for team

## Example Migration

Let's migrate the database tests as a complete example:

### Original (database_test.rs) - 500+ lines
```rust
#[sinex_test]
async fn test_insert_single_event(ctx: TestContext) -> TestResult {
    let event = // ... 10 lines of manual construction
    let id = insert_event(ctx.pool(), &event).await?;
    let retrieved = get_event_by_id(ctx.pool(), id).await?;
    assert_eq!(retrieved.source, event.source);
    // ... 20 more assertions
    Ok(())
}

// ... 20 more similar tests
```

### Modernized - 150 lines
```rust
// Single property test replaces 20 individual tests
sinex_proptest_async! {
    fn event_persistence_properties(
        event in arbitrary_event()
    ) {
        let ctx = TestContext::new().await;
        
        // Insert and retrieve
        let id = ctx.insert_event(&event).await?;
        let retrieved = ctx.get_event(id).await?;
        
        // All properties verified at once
        prop_assert_events_equivalent(&event, &retrieved);
    }
}

// Parameterized test for edge cases
parameterized_test!(
    test_event_edge_cases,
    vec![
        ("empty_payload", event_with_empty_payload()),
        ("huge_payload", event_with_huge_payload()),
        ("unicode", event_with_unicode()),
    ],
    |pool, (_name, event)| async move {
        test_event_roundtrip(pool, &event).await
    }
);
```

## Benefits Achieved

1. **Coverage**: Property tests explore millions of cases vs dozens
2. **Maintainability**: DRY principles, single source of truth
3. **Readability**: Declarative style shows intent clearly
4. **Performance**: No wasted time on arbitrary sleeps
5. **Debugging**: Better error messages from property test frameworks

## Tool Support

The modernization uses these test utilities from `test/common/`:
- `property_helpers.rs` - Generators for property testing
- `test_macros.rs` - Common test patterns
- `builders.rs` - Fluent builders for test data
- `fixtures.rs` - Realistic test data
- `query_helpers.rs` - Database test utilities

## Next Steps

1. Start with unit tests (highest ROI)
2. Move to integration tests
3. Apply to satellite-specific tests
4. Document new patterns as they emerge

The goal is to make tests a powerful tool for understanding and verifying system behavior, not just a checkbox for coverage.