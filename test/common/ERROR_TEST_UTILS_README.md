# Error Test Utilities

## Quick Start

```rust
use crate::common::prelude::*;

// Test that operation fails with specific error type
test_error_case!(
    test_db_error,
    |pool| async move {
        sqlx::query("INVALID SQL").execute(pool).await
            .map_err(|e| CoreError::from(e))
    },
    CoreErrorVariant::Database
);
```

## Available Utilities

### Error Assertions
- `ErrorAssert::is_core_error_variant()` - Check error type
- `ErrorAssert::contains_message()` - Check error message
- `ErrorAssert::has_context_key()` - Check context exists
- `ErrorAssert::chain_contains()` - Check error chain

### Error Builders
- `ErrorScenarioBuilder` - Build complex error scenarios
- `CommonErrorScenarios` - Pre-built common errors

### Test Macros
- `test_error_case!` - Test specific error type
- `test_error_propagation!` - Test error chains
- `test_recovery!` - Test error recovery
- `test_validation_error!` - Test field validation
- `test_concurrent_errors!` - Test concurrent failures
- `test_timeout_error!` - Test timeouts
- And many more...

See `ERROR_HANDLING_PATTERNS.md` for comprehensive documentation.