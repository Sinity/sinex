# Obsolete Code to Remove

## Files to Delete Entirely

### 1. coverage_assurance.rs
- **Why obsolete**: Custom coverage tracking replaced by cargo-llvm-cov and modern tools
- **Usage**: Only in its own tests
- **Modern replacement**: Use cargo-llvm-cov, cargo-tarpaulin, or IDE coverage tools

### 2. redis_pool.rs
- **Why obsolete**: Redis being replaced by NATS throughout the system
- **Usage**: Zero external usage found
- **Modern replacement**: NATS JetStream for message passing

## Code to Remove from Existing Files

### 1. lib.rs
- Remove `mod coverage_assurance;`
- Remove `mod redis_pool;`
- Remove any re-exports of CoverageTracker
- Remove any re-exports of redis pool types

### 2. test_context.rs
- Remove the comment "// Removed obsolete assert_snapshot - use snapshot(), snapshot_event(), etc. below"
- This comment is pointless since the code is already gone

### 3. telemetry/accumulator.rs
- Remove `use sinex_test_utils::parameterized;`
- Convert the test to use enhanced sinex_test macro with rstest

## Patterns Made Obsolete

### 1. Custom Coverage Tracking
```rust
// OBSOLETE
track_coverage!(event_type: "fs", "file.created");
CoverageTracker::record_validation_rule("test_rule");

// MODERN: Use code coverage tools
// - cargo llvm-cov
// - cargo tarpaulin
// - IDE integrated coverage
```

### 2. Redis Test Infrastructure
```rust
// OBSOLETE
let redis_db = acquire_test_redis().await?;
let redis_conn = redis_db.connection();

// MODERN: Use NATS for messaging
let nats_client = NatsClient::new(&config).await?;
```

### 3. parameterized! macro
```rust
// OBSOLETE
parameterized!([
    ("case1", value1),
    ("case2", value2),
], |(name, value)| {
    // test body
});

// MODERN: Enhanced sinex_test with rstest
#[sinex_test]
#[case("case1", value1)]
#[case("case2", value2)]
async fn test_name(ctx: TestContext, #[case] name: &str, #[case] value: Type) -> Result<()> {
    // test body
}
```

## Verification Steps

1. Delete coverage_assurance.rs
2. Delete redis_pool.rs
3. Remove their module declarations from lib.rs
4. Fix any compilation errors (there should be none based on usage analysis)
5. Convert the one parameterized! usage in accumulator.rs
6. Remove obsolete comments

## Benefits

- Removes ~600+ lines of unused code
- Eliminates custom implementations replaced by industry-standard tools
- Reduces maintenance burden
- Makes the codebase more approachable for new contributors
- Aligns with modern Rust testing practices