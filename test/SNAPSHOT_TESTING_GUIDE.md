# Snapshot Testing Guide

## Overview

Snapshot testing provides a powerful way to capture and verify complex test outputs, making it easier to detect regressions and maintain test accuracy over time. Our snapshot testing utilities support JSON structures, events, checkpoints, and custom types with advanced features like redaction, fuzzy matching, and version control integration.

## Benefits

1. **Reduced Manual Assertion Writing**: Instead of writing dozens of `assert_eq!` statements for complex structures, capture the entire output as a snapshot
2. **Visual Regression Detection**: Changes to data structures are immediately visible in diffs
3. **Easy Test Updates**: When intentional changes occur, update all affected snapshots with `UPDATE_SNAPSHOTS=1`
4. **Consistent Test Data**: Redaction features ensure tests are deterministic despite dynamic values

## Basic Usage

### Simple Snapshot Assertion

```rust
use crate::common::snapshot_testing::assert_snapshot;

#[test]
fn test_complex_output() {
    let result = generate_complex_data();
    
    // Capture snapshot with automatic name
    assert_snapshot!(result);
    
    // Or with custom name
    assert_snapshot!(result, "my_complex_data");
}
```

### Inline Snapshots

For smaller data structures, use inline snapshots:

```rust
use crate::common::snapshot_testing::assert_inline_snapshot;

#[test]
fn test_simple_json() {
    let data = json!({
        "status": "success",
        "count": 42
    });
    
    assert_inline_snapshot!(data, @r###"
    {
      "count": 42,
      "status": "success"
    }
    "###);
}
```

### Updating Snapshots

When test outputs change intentionally:

```bash
# Update all snapshots
UPDATE_SNAPSHOTS=1 cargo test

# Update specific test snapshots
UPDATE_SNAPSHOTS=1 cargo test test_name
```

## Advanced Features

### Redaction for Dynamic Data

Remove or replace dynamic values that would cause false failures:

```rust
use crate::common::snapshot_testing::{assert_snapshot, Redaction};

#[test]
fn test_with_timestamps() {
    let data = json!({
        "id": Ulid::new().to_string(),
        "timestamp": Utc::now().to_rfc3339(),
        "process_id": std::process::id(),
        "data": "important stuff"
    });
    
    assert_snapshot!(
        data,
        "test_with_dynamic_values",
        Redaction::timestamps(),    // Replace all timestamps with fixed value
        Redaction::ulids(),         // Replace ULIDs with sequential IDs
        Redaction::dynamic_ids()    // Replace PIDs, window IDs, etc.
    );
}
```

### Custom Redactions

Create specific redactions for your use case:

```rust
// Regex-based redaction
Redaction::regex(r"\d{3}-\d{3}-\d{4}", "XXX-XXX-XXXX")

// Field-specific redaction
Redaction::field("user.email", json!("user@example.com"))

// Multiple redactions
assert_snapshot!(
    data,
    "user_data",
    Redaction::field("user.password", json!("[REDACTED]")),
    Redaction::field("api_key", json!("sk_test_xxx")),
    Redaction::timestamps()
);
```

### Snapshot Builder Pattern

For complex redaction scenarios:

```rust
use crate::common::snapshot_testing::snapshot;

#[test]
fn test_complex_scenario() {
    let data = generate_complex_data();
    
    snapshot(data)
        .name("complex_scenario")
        .redact_timestamps()
        .redact_ulids()
        .redact_field("sensitive_data", json!("[HIDDEN]"))
        .fuzzy_match(FuzzyMatcher::AnyNumber)
        .assert();
}
```

## Integration Examples

### Event Testing

```rust
#[sinex_test]
async fn test_event_sequence_snapshot(ctx: TestContext) -> TestResult {
    // Generate events
    let events = vec![
        create_file_event("/test/file1.txt"),
        create_command_event("vim file1.txt"),
        create_window_event("vim - file1.txt"),
    ];
    
    // Insert and process
    for event in &events {
        EventQueries::insert_raw_event(ctx.pool(), event).await?;
    }
    
    // Capture the sequence
    assert_snapshot!(
        events,
        "file_edit_workflow",
        Redaction::timestamps(),
        Redaction::ulids()
    );
    
    Ok(())
}
```

### System State Snapshots

```rust
#[test]
fn test_system_health_snapshot() {
    let health_state = collect_system_metrics();
    
    snapshot(health_state)
        .name("system_health_baseline")
        .redact_field("cpu_usage", json!(25.0))  // Normalize variable metrics
        .redact_field("memory_mb", json!(500))
        .redact_timestamps()
        .assert();
}
```

### Property Test Integration

```rust
#[test]
fn test_property_patterns_snapshot() {
    let mut runner = TestRunner::new(Config::default());
    let mut results = Vec::new();
    
    for _ in 0..100 {
        let input = arb_complex_input()
            .new_tree(&mut runner)
            .unwrap()
            .current();
        
        let output = process(input);
        results.push(categorize_result(output));
    }
    
    // Snapshot the distribution of results
    let summary = summarize_results(&results);
    assert_snapshot!(summary, "property_test_distribution");
}
```

## Best Practices

### 1. Snapshot Naming

Use descriptive names that indicate what's being tested:

```rust
// Good
assert_snapshot!(result, "user_registration_flow");
assert_snapshot!(config, "default_system_configuration");

// Less clear
assert_snapshot!(result, "test1");
assert_snapshot!(data, "output");
```

### 2. Appropriate Redaction

Redact only what varies between test runs:

```rust
// Good: Redact only dynamic values
assert_snapshot!(
    event,
    "process_started_event",
    Redaction::timestamps(),
    Redaction::field("process_id", json!(12345))
);

// Avoid: Over-redacting loses test value
assert_snapshot!(
    event,
    "process_event",
    Redaction::field("*", json!("[REDACTED]"))  // Don't do this!
);
```

### 3. Snapshot Organization

Snapshots are stored in `test/snapshots/` with a directory structure matching your test modules:

```
test/snapshots/
├── integration/
│   ├── system_integration_test/
│   │   ├── comprehensive_system_configuration.snap
│   │   └── system_health_monitoring_state.snap
│   └── process_event_test/
│       └── process_lifecycle.snap
└── unit/
    └── typed_clipboard_test/
        ├── clipboard_text_operations.snap
        └── clipboard_edge_cases.snap
```

### 4. Version Control

- Commit snapshot files to git
- Review snapshot changes in PRs carefully
- Use snapshot diffs to understand test changes

### 5. When to Use Snapshots

**Good use cases:**
- Complex JSON structures
- Multi-step workflows
- System configuration
- Error message formats
- Performance baselines

**Avoid for:**
- Simple boolean checks
- Single numeric values
- Frequently changing data without stable structure

## Troubleshooting

### Snapshot Mismatches

When a test fails due to snapshot mismatch:

1. **Review the diff**: The error message shows what changed
2. **Determine if intentional**: Is this an expected change?
3. **Update if appropriate**: `UPDATE_SNAPSHOTS=1 cargo test test_name`
4. **Or fix the regression**: If the change is unexpected

### Flaky Snapshots

If snapshots are inconsistent:

1. **Add more redactions**: Identify all dynamic fields
2. **Clear redaction cache**: `clear_redaction_cache()` between tests
3. **Use fuzzy matchers**: For values that vary within acceptable ranges

### Large Snapshots

For very large outputs:

1. **Extract key parts**: Snapshot only the relevant portions
2. **Use summaries**: Create statistical summaries instead of full data
3. **Split into multiple**: Break into logical sections

## Migration Guide

To migrate existing tests to snapshot testing:

1. **Identify candidates**: Look for tests with many assertions on complex data
2. **Capture baseline**: Run with `UPDATE_SNAPSHOTS=1` to create initial snapshots
3. **Add redactions**: Identify and handle dynamic values
4. **Simplify test code**: Remove manual assertions
5. **Document changes**: Explain the migration in commit messages

Example migration:

```rust
// Before: Manual assertions
assert_eq!(result.status, "success");
assert_eq!(result.items.len(), 3);
assert_eq!(result.items[0].name, "item1");
assert_eq!(result.items[0].value, 42);
// ... many more assertions

// After: Snapshot testing
assert_snapshot!(result, "operation_result");
```

## Performance Considerations

- Snapshot comparison is fast (string comparison)
- File I/O is minimized (cached reads)
- Redaction is performed only once per test
- Large snapshots (>1MB) may impact test speed

## Future Enhancements

Planned improvements to snapshot testing:

1. **Binary snapshot support**: For non-text data
2. **Partial snapshots**: Assert on subtrees of large structures
3. **Snapshot analytics**: Track snapshot changes over time
4. **IDE integration**: Better tooling for snapshot review
5. **Compression**: For very large snapshots

## Summary

Snapshot testing dramatically simplifies testing complex outputs while improving test maintainability. By capturing expected outputs and automatically detecting changes, it provides confidence that your system behaves consistently while making it easy to update tests when behavior intentionally changes.