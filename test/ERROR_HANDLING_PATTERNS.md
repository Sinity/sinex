# Error Handling Test Patterns

This document describes the comprehensive error handling test patterns implemented for Sinex, providing standardized ways to test error scenarios, propagation, recovery, and validation.

## Overview

The error handling test infrastructure consists of:
- **Error Testing Utilities** (`error_test_utils.rs`) - Helper functions and builders
- **Error Test Macros** (`error_test_macros.rs`) - Declarative macros for common patterns
- **Example Tests** - Demonstrations of proper usage

## Core Components

### 1. Error Assertion Utilities

```rust
use crate::common::error_test_utils::{ErrorAssert, CoreErrorVariant};

// Check error variant
assert!(ErrorAssert::is_core_error_variant(&error, CoreErrorVariant::Database));

// Check error message contains text
assert!(ErrorAssert::contains_message(&error, "connection failed"));

// Check error has context key
assert!(ErrorAssert::has_context_key(&error, "host"));

// Check error chain contains message
assert!(ErrorAssert::chain_contains(&error, "root cause"));
```

### 2. Error Scenario Builder

```rust
let error = ErrorScenarioBuilder::new(CoreErrorVariant::Database, "Connection failed")
    .with_context("host", "localhost")
    .with_context("port", 5432)
    .with_source("Network timeout")
    .with_source("Connection pool exhausted")
    .build();
```

### 3. Common Error Scenarios

Pre-built error scenarios for common cases:

```rust
// Database connection failure
let db_error = CommonErrorScenarios::database_connection_failed();

// Validation error
let validation_error = CommonErrorScenarios::validation_field_error("email", "invalid@");

// Timeout error
let timeout_error = CommonErrorScenarios::operation_timeout("query", 5000);

// Resource exhaustion
let resource_error = CommonErrorScenarios::resource_exhausted("connections", 100);

// Permission denied
let permission_error = CommonErrorScenarios::permission_denied("write", "/etc/config");

// Not found
let not_found = CommonErrorScenarios::not_found("user", "12345");

// Cascading failure
let cascade = CommonErrorScenarios::cascading_failure();
```

## Test Macros

### 1. Basic Error Testing

```rust
// Test that operation returns specific error type
test_error_case!(
    test_database_error,
    |pool| async move {
        // Operation that should fail
        sqlx::query("INVALID SQL").execute(pool).await
            .map_err(|e| CoreError::from(e))
    },
    CoreErrorVariant::Database
);

// With custom validation
test_error_case!(
    test_validation_with_check,
    |pool| async move { /* failing operation */ },
    CoreErrorVariant::Validation,
    |error: &CoreError| {
        assert!(error.to_string().contains("field"));
        Ok(())
    }
);
```

### 2. Error Propagation

```rust
test_error_propagation!(
    test_layer_propagation,
    vec![
        ("repository", |pool| async move {
            Err(CoreError::Database("Primary key violation".to_string()))
        }),
        ("service", |pool| async move {
            Err(CoreError::Service("Failed to create user".to_string()))
        }),
        ("handler", |pool| async move {
            Err(CoreError::Other("Request failed".to_string()))
        }),
    ]
);
```

### 3. Error Recovery

```rust
// Basic recovery
test_recovery!(
    test_transient_error_recovery,
    |pool| async move {
        // Initial failure
        Err(CoreError::Database("Connection lost".to_string()))
    },
    |pool, _error| async move {
        // Recovery operation
        Ok(())
    }
);

// Recovery with retries
test_recovery!(
    test_retry_logic,
    |pool| async move { /* operation */ },
    3,     // max retries
    true   // should succeed
);
```

### 4. Validation Errors

```rust
test_validation_error!(
    test_invalid_email,
    "email",           // field name
    json!("invalid"),  // invalid value
    "format"           // expected error reason
);
```

### 5. Concurrent Errors

```rust
test_concurrent_errors!(
    test_concurrent_failures,
    10,  // concurrent operations
    |pool, worker_id| async move {
        if worker_id % 3 == 0 {
            Err(CoreError::ResourceExhausted("Pool full".to_string()))
        } else {
            Ok(())
        }
    },
    3    // expected failures
);
```

### 6. Error Context Preservation

```rust
test_error_context!(
    test_context_preservation,
    |pool| async move {
        CoreError::database("Query failed")
            .with_context("table", "events")
            .with_context("operation", "INSERT")
            .build()
            .into()
    },
    vec![
        ("table", "events"),
        ("operation", "INSERT"),
    ]
);
```

### 7. Constraint Violations

```rust
test_constraint_violation!(
    test_unique_violation,
    |pool| async move {
        // Setup: insert initial record
        insert_test_record(pool).await
    },
    |pool| async move {
        // Violating operation
        insert_duplicate_record(pool).await
    },
    "unique"  // constraint type
);
```

### 8. Timeout Errors

```rust
test_timeout_error!(
    test_slow_operation,
    |pool| async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok(())
    },
    500  // timeout in ms
);
```

### 9. Error Transformation

```rust
test_error_transformation!(
    test_error_conversion,
    sqlx::Error::RowNotFound,
    |e| CoreError::not_found("user", "123"),
    |transformed| {
        assert!(ErrorAssert::is_core_error_variant(&transformed, CoreErrorVariant::NotFound));
    }
);
```

### 10. Idempotency Under Errors

```rust
test_error_idempotency!(
    test_idempotent_operation,
    |pool| async move {
        // Operation that might fail
        update_checkpoint(pool).await
    },
    |pool| async move {
        // Verify state unchanged
        verify_checkpoint_state(pool).await
    }
);
```

### 11. Error with Rollback

```rust
test_error_with_rollback!(
    test_transaction_rollback,
    |pool| async move {
        // Setup initial state
        let initial_count = get_count(pool).await?;
        Ok(initial_count)
    },
    |pool| async move {
        // Operation that fails and should rollback
        perform_failing_transaction(pool).await
    },
    |pool, initial_count| async move {
        // Verify rollback
        let final_count = get_count(pool).await?;
        assert_eq!(initial_count, final_count);
        Ok(())
    }
);
```

### 12. Event Processing Errors

```rust
test_event_processing_error!(
    test_invalid_event_processing,
    "test.event",
    json!({"invalid": true}),
    |pool, event| async move {
        process_event(pool, event).await
    },
    |error| {
        assert!(ErrorAssert::is_core_error_variant(error, CoreErrorVariant::Validation));
        Ok(())
    }
);
```

### 13. Cascading Errors

```rust
test_cascading_errors!(
    test_system_cascade,
    |pool| async move {
        // Initial failure
        Err(CoreError::Network("Connection lost".to_string()))
    },
    vec![
        ("service_a", |pool| async move {
            Err(CoreError::Service("Service A failed".to_string()))
        }),
        ("service_b", |pool| async move {
            Err(CoreError::Service("Service B failed".to_string()))
        }),
    ]
);
```

### 14. Partial Failures

```rust
test_partial_failure!(
    test_batch_partial_success,
    |pool| async move {
        // Batch operation returning Vec<Result<T, E>>
        process_batch(pool).await
    },
    7,   // expected successes
    3    // expected failures
);
```

## Best Practices

### 1. Use Appropriate Error Types

```rust
// ✅ Good - specific error type
CoreError::Database("Connection timeout after 30s".to_string())

// ❌ Bad - generic error
CoreError::Unknown("Something went wrong".to_string())
```

### 2. Add Rich Context

```rust
// ✅ Good - rich context
CoreError::validation("Field validation failed")
    .with_context("field", "email")
    .with_context("value", "invalid@")
    .with_context("reason", "Missing domain")
    .build()

// ❌ Bad - no context
CoreError::Validation("Invalid email".to_string())
```

### 3. Test Error Chains

```rust
// ✅ Good - complete error chain
let error = CoreError::service("Operation failed")
    .with_source("Database error")
    .with_source("Connection timeout")
    .with_source("Network unreachable")
    .build();

// ❌ Bad - no root cause
CoreError::Service("Operation failed".to_string())
```

### 4. Test Recovery Paths

```rust
// ✅ Good - test both failure and recovery
test_recovery!(
    test_with_recovery,
    |pool| async move { /* initial failure */ },
    |pool, error| async move { /* recovery logic */ }
);

// ❌ Bad - only test failure
#[test]
fn test_failure() {
    assert!(operation().is_err());
}
```

### 5. Use Assertion Helpers

```rust
// ✅ Good - specific assertions
assert_error_type!(result, CoreErrorVariant::Database);
assert_error_contains!(result, "connection");
assert_error_context!(error, "host", "localhost");

// ❌ Bad - generic assertions
assert!(result.is_err());
```

## Examples

See the following files for comprehensive examples:
- `test/examples/error_handling_demo.rs` - All patterns demonstrated
- `test/integration/error_handling_patterns_test.rs` - Real-world usage

## Migration Guide

To convert existing error tests:

1. Replace manual error checking with assertion helpers
2. Use error scenario builders for test data
3. Apply appropriate test macros for common patterns
4. Add rich context to all errors
5. Test recovery paths, not just failures

### Before:
```rust
#[test]
async fn test_error() {
    let result = operation().await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("failed"));
}
```

### After:
```rust
test_error_case!(
    test_operation_failure,
    |pool| async move { operation(pool).await },
    CoreErrorVariant::Service,
    |error| {
        assert_error_contains!(Err(error.clone()), "failed");
        assert_error_context!(error, "operation", "test_op");
        Ok(())
    }
);
```