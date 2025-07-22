# Sinex Test Suite

This comprehensive guide covers the organization, patterns, and best practices for the Sinex test suite.

## 📋 Table of Contents

1. [Test Organization](#test-organization)
2. [Testing Patterns](#testing-patterns)
3. [Running Tests](#running-tests)
4. [Writing Tests](#writing-tests)
5. [Test Infrastructure](#test-infrastructure)
6. [Modernization Guide](#modernization-guide)
7. [Troubleshooting](#troubleshooting)

## Test Organization

The test suite is organized into distinct categories based on scope and purpose:

```
test/
├── unit/           # Unit tests for individual components
├── integration/    # Integration tests for subsystems
├── property/       # Property-based tests using proptest
├── adversarial/    # Stress and chaos testing
├── system/         # End-to-end system tests
├── performance/    # Performance and benchmark tests
├── common/         # Shared test utilities and infrastructure
├── nixos-vm/       # NixOS VM integration tests
└── examples/       # Example test patterns and templates
```

### Test Categories

- **Unit Tests** (`unit/`): Fast, isolated tests for individual functions and modules
- **Integration Tests** (`integration/`): Tests for database operations, API endpoints, and service interactions
- **Property Tests** (`property/`): Comprehensive property-based testing using proptest
- **Adversarial Tests** (`adversarial/`): Chaos engineering, concurrency stress tests, and boundary testing
- **System Tests** (`system/`): Full system integration including all satellites and services
- **Performance Tests** (`performance/`): Benchmarks and performance regression tests

## Testing Patterns

### Property-Based Testing

The test suite heavily leverages property-based testing for comprehensive coverage:

```rust
sinex_proptest_async! {
    fn database_event_persistence_properties(
        event in arbitrary_event()
    ) {
        let ctx = TestContext::new().await;
        let id = ctx.insert_event(&event).await?;
        let retrieved = ctx.get_event(id).await?;
        prop_assert_events_equivalent(&event, &retrieved);
    }
}
```

### Test Macros

Common patterns are abstracted into macros for consistency:

```rust
test_event_insertion!(
    filesystem_event_test,
    sources::FS,
    event_types::filesystem::FILE_CREATED,
    json!({"path": "/test.txt"})
);
```

### Parameterized Tests

Edge cases and specific scenarios use parameterized testing:

```rust
parameterized_test!(
    test_edge_cases,
    vec![
        ("empty_payload", event_with_empty_payload()),
        ("huge_payload", event_with_payload_size(1_000_000)),
        ("unicode_everywhere", event_with_unicode_fields()),
    ],
    |pool: &DbPool, (_name, event): (&str, RawEvent)| async move {
        // Test logic
    }
);
```

### Concurrent Testing

Concurrency patterns for testing race conditions:

```rust
test_concurrent_operations!(
    test_concurrent_inserts,
    20, // concurrent tasks
    |pool: Arc<DbPool>, task_id: usize| async move {
        // Concurrent operation
    }
);
```

## Running Tests

### Basic Commands

```bash
# Run all tests
just test

# Run unit tests only
just test-unit

# Run integration tests
just test-integration

# Run with coverage
just test-coverage

# Run specific test
cargo test test_name

# Run tests in a specific module
cargo test --test unit/database_test
```

### Test Environment

Tests require:
- PostgreSQL with TimescaleDB extension
- Redis for stream processing tests
- Environment variables (set automatically in nix develop shell)

### Performance Testing

```bash
# Run benchmarks
cargo bench

# Run specific benchmark
cargo bench benchmark_name
```

## Writing Tests

### Test Context

All database tests should use `TestContext` for proper isolation:

```rust
#[sinex_test]
async fn test_database_operation(pool: DbPool) -> AnyhowResult<()> {
    let ctx = TestContext::with_pool(pool);
    
    // Test operations
    let event = ctx.event_builder("source", "type").build();
    let id = ctx.insert_event(&event).await?;
    
    // Assertions
    let retrieved = ctx.get_event(id).await?;
    assert_eq!(event.source, retrieved.source);
    
    Ok(())
}
```

### Event Builders

Use builders for consistent event creation:

```rust
let event = EventBuilder::new("fs", "file.created")
    .with_payload(json!({
        "path": "/test.txt",
        "size": 1024
    }))
    .with_timestamp(Utc::now())
    .build();
```

### Timing and Synchronization

Avoid arbitrary sleeps; use deterministic waiting:

```rust
// Bad: arbitrary sleep
tokio::time::sleep(Duration::from_secs(1)).await;

// Good: wait for condition
wait_for_events(pool, "source", 5, Duration::from_secs(10)).await?;
```

## Test Infrastructure

### Common Utilities

The `test/common/` directory provides:

- **`prelude.rs`**: Common imports for all tests
- **`test_context.rs`**: Unified test context with database helpers
- **`event_builders.rs`**: Event creation utilities
- **`database_pool.rs`**: Test database management
- **`timing_optimization/`**: Deterministic wait helpers
- **`mocks/`**: Mock implementations for satellites and services

### Test Macros

Available in `sinex-test-macros`:

- `#[sinex_test]`: Wraps async test with database transaction rollback
- `sinex_proptest_async!`: Property testing with async support
- `test_event_insertion!`: Standard event insertion test
- `parameterized_test!`: Run same test with different inputs
- `test_concurrent_operations!`: Concurrent testing framework

### Property Generators

Located in `test/common/property_helpers.rs`:

- `arbitrary_event()`: Generate random valid events
- `ulids()`: Generate valid ULIDs
- `event_sources()`: Generate valid event sources
- `json_payloads()`: Generate arbitrary JSON payloads

## Modernization Guide

### Converting Legacy Tests

1. **Identify repetitive patterns**
   - Multiple similar test functions
   - Boilerplate setup/teardown
   - Manual test data generation

2. **Apply appropriate pattern**
   - Property-based for comprehensive coverage
   - Parameterized for specific cases
   - Macros for common operations

3. **Example transformation**:

```rust
// Before: 10 individual ULID tests
#[test]
fn test_ulid_string_length() {
    let ulid = Ulid::new();
    assert_eq!(ulid.to_string().len(), 26);
}

// After: 1 property test covering all cases
sinex_proptest_sync! {
    fn ulid_invariants(ulid in ulids()) {
        prop_assert_eq!(ulid.to_string().len(), 26);
        // ... more properties
    }
}
```

### Impact Metrics

Modern patterns achieve:
- **75% code reduction** while testing more cases
- **100x more test coverage** through property testing
- **Faster test execution** with better parallelization
- **Clearer test intent** with declarative patterns

## Troubleshooting

### Common Issues

1. **Database connection errors**
   ```bash
   # Ensure PostgreSQL is running
   systemctl status postgresql
   
   # Check test database exists
   psql -U postgres -c "SELECT 1 FROM pg_database WHERE datname = 'sinex_test'"
   ```

2. **Redis connection errors**
   ```bash
   # Ensure Redis is running
   systemctl status redis
   redis-cli ping
   ```

3. **Flaky tests**
   - Replace sleeps with deterministic waits
   - Use `TestSynchronizer` for coordination
   - Ensure proper test isolation

4. **Compilation errors after cleanup**
   ```bash
   # Clear build cache
   cargo clean
   
   # Rebuild
   cargo check --workspace
   ```

### Debug Helpers

```rust
// Enable debug logging for specific test
#[sinex_test]
async fn test_with_logging(pool: DbPool) -> AnyhowResult<()> {
    env_logger::init();
    // Test logic
}

// Dump database state
ctx.debug_dump_events().await?;

// Check Redis state
ctx.debug_redis_info().await?;
```

## Best Practices

1. **Use TestContext** for all database operations
2. **Prefer property-based tests** over individual test cases
3. **Avoid arbitrary sleeps** - use condition-based waiting
4. **Clean test data** - tests should not depend on external state
5. **Descriptive names** - test names should explain what and why
6. **Fast feedback** - keep unit tests under 100ms
7. **Test one thing** - each test should verify a single behavior
8. **Use builders** - consistent test data creation
9. **Document complex tests** - explain non-obvious test logic
10. **Run tests locally** before pushing - `just test-fast`

## Contributing

When adding new tests:

1. Place in appropriate category directory
2. Use existing patterns and utilities
3. Add property-based tests for new functionality
4. Update this README if adding new patterns
5. Ensure tests pass in CI environment

For questions or improvements, see the main project documentation.