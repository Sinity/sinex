# Sinex Test Framework

## Core Features

The test framework is designed to be simple and reliable:

1. **Database-per-test isolation** - Each test gets its own PostgreSQL database
2. **Automatic cleanup** - Databases are dropped after tests complete  
3. **Timeout protection** - Default 10 seconds (configurable)
4. **Zero configuration** - Just works out of the box

## Basic Usage

```rust
use crate::common::prelude::*;

#[sinex_test]
async fn test_example(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Each test gets a completely isolated database
    let event = ctx.event_builder("test", "example")
        .payload(json!({"data": "test"}))
        .build();
    
    ctx.insert_event(&event).await?;
    
    // No cleanup needed - database drops automatically
    Ok(())
}
```

## Timeout Configuration

```rust
#[sinex_test(timeout = 30)]  // 30 second timeout for slower tests
async fn test_slow_operation(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Long running test
}
```

Default timeout is 10 seconds - this catches most hanging tests while allowing reasonable operations.

## Performance

- Database creation: ~200-300ms per test
- When running in parallel: overhead is amortized  
- Example: 43 tests complete in 1.86s (~43ms per test when parallel)

## Design Philosophy

Following the principle of good test frameworks like Jest, pytest, and Go's testing:

1. **Simple by default** - No configuration needed for common cases
2. **Fast feedback** - Low timeouts catch problems quickly
3. **Real isolation** - No shared state between tests
4. **Let the runner do its job** - cargo test handles progress display

## What We Don't Do (And Why)

- **No test categories** - Use cargo test filtering instead
- **No query counting** - Not useful enough to justify complexity
- **No optional metrics** - If it's worth measuring, measure it always
- **No parallel control** - Every test is already isolated
- **No complex logging** - Use println! and --nocapture when debugging

## Possible Future Enhancements

Based on what successful test frameworks provide:

### Better Assertions
Like Jest's expect() or pytest's rich assertions:
```rust
// Instead of: assert_eq!(events.len(), 5);
// Could have: expect!(events).to_have_length(5);
// With better error messages showing the actual events
```

### Snapshot Testing
For comparing complex outputs:
```rust
#[sinex_test]
async fn test_complex_output(ctx: TestContext) -> Result<()> {
    let result = generate_report(&ctx).await?;
    assert_snapshot!(result); // Compares to saved snapshot
    Ok(())
}
```

### Test Fixtures
Reusable test data setup:
```rust
#[sinex_test]
async fn test_with_users(ctx: TestContext) -> Result<()> {
    ctx.load_fixture("users").await?; // Pre-populate common test data
    // Test with known data
    Ok(())
}
```

But honestly? The current framework is probably good enough. Perfect isolation, fast execution, and simple API covers 95% of needs.