# Sinex Test Macro Usage Guide

This guide provides comprehensive documentation for using the test macro system to write cleaner, more maintainable tests.

## Available Macros

### 1. `test_event_insertion!`
**Purpose**: Test simple event insertion and retrieval

**Usage**:
```rust
test_event_insertion!(
    test_name,
    "source",
    "event.type",
    json!({"key": "value"})
);
```

**When to use**:
- Testing that events are correctly inserted into the database
- Verifying event fields are stored properly
- Simple single-event scenarios

### 2. `test_batch_events!`
**Purpose**: Test batch event operations

**Usage**:
```rust
test_batch_events!(
    test_name,
    "source",
    "event.type",
    100, // count
    |pool, events| async move {
        // Custom verification logic
        assert_eq!(events.len(), 100);
        Ok(())
    }
);
```

**When to use**:
- Testing bulk imports
- Performance testing with multiple events
- Concurrent event insertion scenarios

### 3. `test_checkpoint_flow!`
**Purpose**: Test checkpoint creation and updates

**Usage**:
```rust
test_checkpoint_flow!(
    test_name,
    "automaton_name",
    10,  // initial processed count
    50   // updated processed count
);
```

**When to use**:
- Testing automaton progress tracking
- Checkpoint persistence verification
- State management testing

### 4. `test_concurrent_operations!`
**Purpose**: Test concurrent database operations

**Usage**:
```rust
test_concurrent_operations!(
    test_name,
    50, // number of concurrent tasks
    |pool, task_id| async move {
        // Operation for each task
        let result = do_something(pool, task_id).await?;
        Ok(result)
    },
    |pool, results| async move {
        // Verify all results
        assert_eq!(results.len(), 50);
        Ok(())
    }
);
```

**When to use**:
- Testing connection pool behavior
- Concurrent query execution
- Race condition testing
- Load testing

### 5. `test_time_range_query!`
**Purpose**: Test time-based event queries

**Usage**:
```rust
test_time_range_query!(
    test_name,
    20,                           // total events to create
    Duration::hours(1),           // spacing between events
    Duration::hours(-5),          // query start (relative to now)
    Duration::hours(5),           // query end (relative to now)
    10                            // expected events in range
);
```

**When to use**:
- Testing time-based filtering
- Verifying TimescaleDB functionality
- Testing event windowing

### 6. `test_event_filter!`
**Purpose**: Test event filtering by source

**Usage**:
```rust
test_event_filter!(
    test_name,
    &["fs", "terminal", "desktop"],  // sources to create events for
    5,                               // events per source
    "fs",                            // filter source
    5                                // expected count
);
```

**When to use**:
- Testing source-based filtering
- Multi-source event scenarios
- Query optimization testing

### 7. `test_with_scenario!`
**Purpose**: Test with setup and teardown

**Usage**:
```rust
test_with_scenario!(
    test_name,
    |pool| async move {
        // Setup
        create_test_data(pool).await
    },
    |pool, setup_data| async move {
        // Test body
        run_test(pool, setup_data).await
    },
    |pool| async move {
        // Cleanup (always runs)
        cleanup_test_data(pool).await
    }
);
```

**When to use**:
- Complex test scenarios requiring setup
- Tests that need cleanup regardless of outcome
- Stateful testing scenarios

### 8. `parameterized_test!`
**Purpose**: Run the same test with multiple inputs

**Usage**:
```rust
parameterized_test!(
    test_name,
    vec![
        ("valid input", (input1, expected1)),
        ("edge case", (input2, expected2)),
        ("error case", (input3, expected3)),
    ],
    |pool, (input, expected)| async move {
        let result = process(pool, input).await?;
        assert_eq!(result, expected);
        Ok(())
    }
);
```

**When to use**:
- Testing multiple input variations
- Edge case testing
- Input validation testing

### 9. `test_event_flow!`
**Purpose**: Test event processing flow from insertion to checkpoint

**Usage**:
```rust
test_event_flow!(
    test_name,
    "source",
    "event.type",
    "processor_name"
);
```

**When to use**:
- End-to-end event processing tests
- Testing automaton integration
- Verifying processing pipelines

### 10. `test_redis_stream_operations!`
**Purpose**: Test Redis stream operations

**Usage**:
```rust
test_redis_stream_operations!(
    test_name,
    "stream:key",
    "consumer-group",
    10, // message count
    |conn, stream_key, result, message_ids| async move {
        // Verify stream operations
        assert_eq!(message_ids.len(), 10);
        Ok(())
    }
);
```

**When to use**:
- Testing Redis stream integration
- Consumer group functionality
- Message acknowledgment flows

### 11. `test_schema_validation!`
**Purpose**: Test JSON schema validation

**Usage**:
```rust
// Full version with valid/invalid payloads
test_schema_validation!(
    test_name,
    json!({"valid": "payload"}),      // valid payload
    json!({"invalid": "payload"}),    // invalid payload
    json!({"$schema": "..."}),        // JSON schema
    "expected error"                   // error pattern
);

// Simple version
test_schema_validation!(
    test_name,
    json!({"test": "payload"}),
    json!({"$schema": "..."}),
    true  // should pass?
);
```

**When to use**:
- Testing schema registration
- Payload validation
- Schema evolution testing

## Best Practices

### 1. Choose the Right Macro
- Use the most specific macro for your use case
- Don't force a pattern if it doesn't fit naturally
- Consider writing a new macro for repeated patterns

### 2. Keep Verification Logic Simple
- Put complex assertions in the verification closure
- Use descriptive variable names in closures
- Keep each test focused on one aspect

### 3. Naming Conventions
- Test names should describe what is being tested
- Use consistent prefixes for related tests
- Include the condition being tested in the name

### 4. Error Messages
- Macros provide good default error messages
- Add context in verification closures when needed
- Use `assert_eq!` over `assert!` for better diagnostics

### 5. Performance Considerations
- Batch operations are more efficient than loops
- Use concurrent macros for parallel operations
- Consider test isolation vs. performance tradeoffs

## Migration Checklist

When converting existing tests:

1. **Identify the pattern**: Look for repetitive setup/assertion code
2. **Choose the appropriate macro**: Match the test intent to a macro
3. **Extract parameters**: Identify what varies between tests
4. **Move logic to closures**: Put custom logic in verification closures
5. **Add imports**: Ensure `use crate::common::test_macros::*;`
6. **Test the conversion**: Run the test to ensure it still passes
7. **Remove old code**: Delete the verbose implementation

## Common Pitfalls

### 1. Over-Macroization
Not every test needs a macro. Simple, unique tests can remain as-is.

### 2. Complex Closures
If your verification closure is longer than the original test, reconsider.

### 3. Lost Context
Ensure error messages still provide enough context when tests fail.

### 4. Hidden Behavior
Macros should make tests clearer, not hide important behavior.

## Examples

See `/realm/project/sinex/test/examples/macro_conversion_examples.rs` for comprehensive before/after examples of each macro in use.

## Creating Custom Macros

If you identify a new pattern:

1. Count occurrences (aim for 10+ uses)
2. Design a clear, minimal API
3. Implement in `test/common/test_macros.rs`
4. Document with examples
5. Convert existing tests to validate

## Automation

Use the conversion script for bulk updates:

```bash
# Analyze patterns
./test/scripts/convert_to_macros.py --analyze

# Dry run
./test/scripts/convert_to_macros.py --dry-run

# Convert specific pattern
./test/scripts/convert_to_macros.py --pattern checkpoint

# Convert single file
./test/scripts/convert_to_macros.py --file test/integration/database_test.rs
```

## Support

For questions or new macro suggestions, consult the test architecture team or refer to the existing macro implementations in `test/common/test_macros.rs`.