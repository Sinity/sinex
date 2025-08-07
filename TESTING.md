# Testing Guide for Sinex

> **FOR AI ASSISTANTS**: When adding tests to Sinex crates, always use `#[sinex_test]` macro and access all functionality through `TestContext`. Tests go inline at the bottom of source files in a `#[cfg(test)]` module.

## Quick Start

All Sinex tests use the unified `TestContext` API:

```rust
use sinex_test_utils::prelude::*;

#[sinex_test]
async fn test_example(ctx: TestContext) -> TestResult<()> {
    // Create an event
    let event = ctx.create_test_event(
        "my-component",
        "action.performed",
        json!({"user_id": "123"})
    ).await?;
    
    // Query events
    let source_ref = sinex_types::domain::EventSource::from("my-component");
    let events = ctx.pool.events().get_by_source(&source_ref, Some(10), None).await?;
    
    // Assert
    assert_eq!(events.len(), 1);
    Ok(())
}
```

## Documentation

For comprehensive testing documentation, see:

- **API Documentation**: `cargo doc --package sinex-test-utils --open`
- **Testing Patterns**: `crate/sinex-test-utils/TESTING.md` 
- **Benchmarking API**: `cargo doc --package sinex-test-utils --features bench --open`

## Key Principles

1. **Everything through TestContext** - Don't import test utilities directly
2. **Automatic cleanup** - Database rollback happens automatically
3. **Isolated tests** - Each test gets its own database
4. **No manual setup** - The `#[sinex_test]` macro handles everything

## Common Commands

```bash
# Run all tests
cargo test

# Run tests for specific crate
cargo test -p sinex-db

# Run single test with output
cargo test test_name -- --nocapture

# Run with debug logging
RUST_LOG=sinex_test_utils=debug cargo test

# Run benchmarks
cargo bench --features bench

# Benchmark commands (via just)
just bench-all              # Run all benchmarks
just bench-quick            # Quick benchmarks with small dataset
just bench-compare          # Compare with main branch
just bench-crate <name>     # Benchmark specific crate