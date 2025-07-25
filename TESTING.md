# Testing Guide for Sinex

> **FOR AI ASSISTANTS**: When adding tests to Sinex crates, always use `#[sinex_test]` macro and access all functionality through `TestContext`. Tests go inline at the bottom of source files in a `#[cfg(test)]` module. See examples below.

## Quick Start - Writing Tests

All Sinex tests use the unified `TestContext` API through the `sinex_test` macro:

```rust
use sinex_test_utils::prelude::*;

#[sinex_test]
async fn test_example(ctx: TestContext) -> TestResult<()> {
    // Create an event
    let event = ctx.event()
        .source("my-component")
        .type_("action.performed")
        .field("user_id", "123")
        .insert()
        .await?;
    
    // Query events
    let events = ctx.events()
        .by_source("my-component")
        .fetch()
        .await?;
    
    // Assert
    assert_eq!(events.len(), 1);
    Ok(())
}
```

## Test Infrastructure

**IMPORTANT**: All test functionality is accessed through `TestContext`. There is no need to manage databases, fixtures, or test utilities directly.

### Available through `ctx`:

1. **Event Creation**: `ctx.event()` - builder pattern for creating test events
2. **Event Queries**: `ctx.events()` - query builder for finding events
3. **Assertions**: `ctx.assert()` - chainable assertions
4. **Timing**: `ctx.timing()` - wait for conditions, measure performance
5. **Fixtures**: `ctx.scenarios()` - pre-built test data
6. **Mocks**: `ctx.mocks()` - mock satellites, filesystems, etc.
7. **Properties**: `ctx.property_tester()` - property-based testing

## Common Test Patterns

### Basic Event Test
```rust
#[sinex_test]
async fn test_event_creation(ctx: TestContext) -> TestResult<()> {
    // Simple event
    let event = ctx.event()
        .source("test")
        .type_("test.event")
        .insert()
        .await?;
    
    assert!(!event.id.to_string().is_empty());
    Ok(())
}
```

### Satellite/Ingestor Tests
```rust
#[sinex_test]
async fn test_filesystem_events(ctx: TestContext) -> TestResult<()> {
    // Create filesystem events
    let event = ctx.event()
        .filesystem()
        .file_created("/tmp/test.txt", 1024)
        .insert()
        .await?;
    
    // Wait for processing
    ctx.timing().wait_for_event_count(1).await?;
    
    Ok(())
}
```

### Property-Based Tests

For property testing, use the appropriate approach based on whether you need database access:

**Pure Functions (No Database)** - Use standard `#[test]` with proptest:
```rust
#[test]
fn test_pure_functions() {
    use proptest::prelude::*;
    
    proptest!(|(value in 0..1000u32)| {
        // Test pure logic - no database operations
        let result = my_pure_function(value);
        prop_assert!(result > 0);
    });
}
```

**Database Operations** - Use `#[sinex_test]` with parameterized! macro:
```rust
#[sinex_test]
async fn test_database_operations(ctx: TestContext) -> TestResult<()> {
    parameterized!([
        (0, "zero"),
        (42, "answer"),
        (u32::MAX, "max"),
    ], |(value, name)| {
        let event = ctx.event()
            .source("test")
            .field("value", value)
            .field("name", name)
            .insert()
            .await?;
        assert_eq!(event.payload["value"], json!(value));
        Ok(())
    });
    Ok(())
}
```

**Why?** Creating a new database connection for each property test iteration would be prohibitively slow (100s of ms per iteration).

### Concurrent Tests
```rust
#[sinex_test]
async fn test_concurrent_operations(ctx: TestContext) -> TestResult<()> {
    let results = ctx.run_concurrent(10, |ctx, i| async move {
        ctx.event()
            .source(&format!("worker-{}", i))
            .insert()
            .await
    }).await?;
    
    assert_eq!(results.len(), 10);
    Ok(())
}
```

## Test Organization

### Unit Tests (inline in source files)
Sinex uses inline tests at the bottom of each source file. This keeps tests close to the code they test:

```rust
// src/my_module.rs
pub fn process_data(input: &str) -> Result<String> {
    // Implementation
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::prelude::*;
    
    // Note: Only import what you need. The prelude provides:
    // - #[sinex_test] macro
    // - TestContext
    // - TestResult
    // - CoreError, RawEvent, ErrorContext
    
    #[sinex_test]
    async fn test_process_data(ctx: TestContext) -> TestResult<()> {
        // Tests have access to private functions from parent module
        let result = process_data("input")?;
        assert_eq!(result, "expected output");
        
        // Use TestContext for database operations
        let event = ctx.event()
            .source("my_module")
            .type_("data.processed")
            .field("input", "input")
            .field("output", &result)
            .insert()
            .await?;
            
        assert_eq!(event.source, "my_module");
        Ok(())
    }
    
    #[sinex_test]
    async fn test_error_handling(ctx: TestContext) -> TestResult<()> {
        // Test error cases
        let result = process_data("");
        assert!(result.is_err());
        Ok(())
    }
}
```

**Important**: If you see old imports like `use sinex_test_utils::{TestContext, TestConfig, database_pool};`, remove them. Everything you need is in the prelude.

### Integration Tests
Place in `test/` directory:
```rust
// test/integration_test.rs
use sinex_test_utils::prelude::*;

#[sinex_test]
async fn test_full_pipeline(ctx: TestContext) -> TestResult<()> {
    // Multi-component test
    Ok(())
}
```

## Key Principles

1. **Everything through TestContext** - Don't import test utilities directly
2. **Automatic cleanup** - Database rollback happens automatically
3. **Isolated tests** - Each test gets its own database
4. **No manual setup** - The `#[sinex_test]` macro handles everything

## Quick Reference

```rust
// Event creation
ctx.event().source("s").type_("t").insert().await?
ctx.quick_event("source", "type").await?
ctx.batch_events(100, "source").await?

// Event queries  
ctx.events().by_source("s").fetch().await?
ctx.events().by_type("t").limit(10).fetch().await?
ctx.events().recent(Duration::hours(1)).fetch().await?

// Assertions
ctx.assert("test").eq(&a, &b)?
ctx.assert_event_count(5).await?
ctx.assert_events_match(&pattern).await?

// Timing
ctx.timing().wait_for_event_count(10).await?
ctx.measure(|| async { /* operation */ }).await?

// Fixtures
ctx.scenarios().user_session().await?
ctx.scenarios().performance_dataset(1000).await?

// Mocks
ctx.mocks().filesystem()
ctx.mocks().satellite("test-sat")
```

## DO NOT

- Import database pools directly
- Create TestContext manually (use the macro)
- Manage cleanup (it's automatic)
- Write `#[tokio::test]` (use `#[sinex_test]`)