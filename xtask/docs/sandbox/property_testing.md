# Property Testing

Property-based testing with proptest integration, including deterministic runners, TestContext
support, and automatic seed persistence for reproducible failures.

## Quick Start

### `#[sinex_prop]` Macro

For property tests that need TestContext:

```rust
#[sinex_prop(cases = 64, timeout = "45s")]
async fn filesystem_property(
    ctx: &TestContext,
    #[strategy(filesystem_event_strategy())] event: (String, String, Value),
) -> TestResult<()> {
    let (source, ty, payload) = event;
    let ctx = ctx.with_nats().shared().await?;
    let inserted = ctx.publish_event(&source, &ty, payload).await?;
    assert_eq!(inserted.source.as_str(), source);
    Ok(())
}
```

### `sinex_proptest!` Block Macro

For pure property tests without TestContext:

```rust
sinex_proptest! {
    fn ulid_roundtrip(value in json_payload_strategy()) -> TestResult<()> {
        let text = value.to_string();
        let decoded: Value = serde_json::from_str(&text)?;
        prop_assert_eq!(decoded, value);
        Ok(())
    }
}
```

## Macro Configuration

### `#[sinex_prop]` Options

```rust
#[sinex_prop(cases = 64, timeout = "45s", seed = 1337)]
async fn property_test(/* ... */) -> TestResult<()> {
    // ...
}
```

| Option | Default | Description |
|--------|---------|-------------|
| `cases` | 256 | Number of test iterations |
| `timeout` | 30s | Maximum test duration |
| `seed` | random | Fixed seed for reproducibility |

### `sinex_proptest!` Options

```rust
sinex_proptest! {
    #![cases = 64]
    #[timeout = "45s"]
    fn property_name(value in strategy()) -> TestResult<()> {
        // ...
    }
}
```

## Environment Overrides

Environment variables override macro configuration:

| Variable | Purpose |
|----------|---------|
| `SINEX_PROPTEST_SEED` | Replay a recorded failure deterministically |
| `SINEX_TEST_FAIL_DIR` | Path for JSON failure artifacts |

Case counts are controlled via the `#[sinex_prop(cases = N)]` macro attribute (default: 256).

## Writing Strategies

Define proptest strategies locally in the test module:

### File Path Strategy

```rust
fn file_path_strategy() -> BoxedStrategy<String> {
    prop_oneof![
        Just("/tmp/test.txt".to_string()),
        Just("/home/user/document.pdf".to_string()),
        "/[a-z0-9/._-]{1,100}\\.[a-z]{1,5}".prop_map(|s| s.to_string()),
    ]
    .boxed()
}
```

### Event Source Strategy

```rust
fn event_source_strategy() -> BoxedStrategy<String> {
    prop_oneof![
        Just("filesystem".to_string()),
        Just("shell.kitty".to_string()),
        "[a-z][a-z0-9._]*".prop_map(|s| s.to_string()),
    ]
    .boxed()
}
```

### JSON Payload Strategy

```rust
fn json_payload_strategy() -> BoxedStrategy<Value> {
    let leaf = prop_oneof![
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        ".*".prop_map(Value::from),
    ];

    leaf.prop_recursive(
        8,   // max depth
        256, // max nodes
        10,  // max items per collection
        |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..10).prop_map(Value::from),
                prop::collection::hash_map(".*", inner, 0..10)
                    .prop_map(|map| Value::from(
                        map.into_iter().collect::<serde_json::Map<_, _>>()
                    )),
            ]
        },
    )
    .boxed()
}
```

### Filesystem Event Strategy

```rust
fn filesystem_event_strategy() -> BoxedStrategy<(String, String, Value)> {
    (
        Just("filesystem".to_string()),
        prop_oneof![
            Just("file.created".to_string()),
            Just("file.modified".to_string()),
            Just("file.deleted".to_string()),
        ],
        (file_path_strategy(), any::<u64>()).prop_map(|(path, size)| json!({
            "path": path,
            "size": size,
            "modified_time": "2025-01-01T00:00:00Z"
        })),
    )
    .boxed()
}
```

## Strategy Design Principles

### Domain Awareness

Strategies should generate domain-realistic data:

```rust
// Good: Domain-aware sources
prop_oneof![
    Just("fs-watcher".to_string()),
    Just("terminal".to_string()),
    Just("desktop".to_string()),
]

// Bad: Arbitrary strings
any::<String>()  // Generates unrealistic data
```

### Include Edge Cases

Include malicious inputs for security testing:

```rust
fn malicious_path_strategy() -> BoxedStrategy<String> {
    prop_oneof![
        Just("../../../etc/passwd".to_string()),
        Just("/dev/null".to_string()),
        Just("\0null\0byte".to_string()),
        "/[a-z0-9/._-]{1,100}".prop_map(|s| s.to_string()),
    ]
    .boxed()
}
```

### Determinism

Strategies must be deterministic for reproducible failures:

```rust
// Good: Deterministic
Just("fixed_value".to_string())
"[a-z]{1,10}".prop_map(|s| s.to_string())

// Bad: Non-deterministic
Ulid::new().to_string()  // Different each run!
```

## Using Production Types

Use production types directly; leverage dataset seeding for complex objects:

```rust
// ULID generation via production API
let id = Id::<Event<JsonValue>>::new();
let ulid = id.as_ulid();

// Event generation with test helper
let event = Event::<JsonValue>::test_event(
    source.as_ref(),
    event_type.as_ref(),
    sanitized_payload,
);

// Source material registration (for FK constraints)
ctx.ensure_source_material(id, Some("test-material")).await?;
```

## Regression Files

Failing seeds are automatically persisted:

```
target/proptest-regressions/
└── test_module/
    └── property_test_name.proptest-regressions
```

### Manual Replay

```bash
# Replay specific seed
SINEX_PROPTEST_SEED=12345 xtask test -- -p my-crate

# Or via proptest env var
PROPTEST_CASES=1 PROPTEST_SEED=12345 xtask test
```

### Clearing Regressions

```bash
rm -rf target/proptest-regressions/
```

## Complete Property Test Examples

### Unit Test (Pure)

```rust
sinex_proptest! {
    #![cases = 128]

    fn ulid_roundtrip(value in ulid_strategy()) -> TestResult<()> {
        let encoded = value.to_string();
        let decoded = Ulid::from_string(&encoded)?;
        prop_assert_eq!(decoded, value);
        Ok(())
    }
}
```

### Integration Test (With TestContext)

```rust
#[sinex_prop(cases = 64, timeout = "60s")]
async fn property_events_roundtrip(
    ctx: &TestContext,
    #[strategy(filesystem_event_strategy())] event: (String, String, Value),
) -> TestResult<()> {
    let (source, ty, payload) = event;
    let ctx = ctx.with_nats().shared().await?;
    let inserted = ctx.publish_event(&source, &ty, payload.clone()).await?;

    let fetched = ctx
        .pool
        .events()
        .get_by_source(&EventSource::from(source.clone()), Some(10), None)
        .await?;

    prop_assert!(!fetched.is_empty());
    prop_assert_eq!(inserted.payload, payload);
    Ok(())
}
```

### Security Fuzzing

```rust
#[sinex_prop(cases = 256)]
async fn fuzz_path_sanitization(
    ctx: &TestContext,
    #[strategy(malicious_path_strategy())] path: String,
) -> TestResult<()> {
    let result = ctx.publish_event(
        "fs-watcher",
        "file.created",
        json!({"path": path}),
    ).await;

    // Should either succeed with sanitized path or fail gracefully
    match result {
        Ok(event) => {
            // Verify path was sanitized
            prop_assert!(!event.payload["path"].as_str().unwrap().contains(".."));
        }
        Err(e) => {
            // Verify error is validation-related, not a crash
            prop_assert!(e.to_string().contains("validation"));
        }
    }
    Ok(())
}
```

## When to Use Property Tests

**Good candidates**:
- Serialization roundtrips (encode → decode = identity)
- Validation invariants (always reject malformed input)
- Idempotency (applying twice = applying once)
- Ordering guarantees (ULIDs are monotonic)
- Security fuzzing (malicious inputs don't crash)

**Poor candidates**:
- Tests requiring specific known values
- Tests with complex setup/teardown
- Tests requiring external services
- Simple assertions better served by unit tests

## Running Property Tests

```bash
# Run all property tests
xtask test -- --test property_tests

# Replay specific failure
SINEX_PROPTEST_SEED=12345 xtask test

# Generate new regressions
xtask test -- -p sinex-primitives
```

## Predefined Properties to Test

- Event creation works for all valid inputs
- Inserted events are retrievable by ID, source, type
- Malicious inputs are safely handled (sanitized or rejected)
- Event relationships are preserved across operations
- ULID ordering is monotonic
- JSON roundtrips preserve data
