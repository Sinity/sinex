# Troubleshooting and Best Practices

Common issues, debugging strategies, and patterns for writing reliable tests.

## Best Practices Summary

### DO

| Practice | Reason |
|----------|--------|
| Use `#[sinex_test]` | Automatic lifecycle management |
| Use `ctx.with_nats().shared()` | Required for pipeline testing |
| Use `ctx.publish_event()` | Exercises real ingestion path |
| Use `ctx.assert()` | Rich error messages with context |
| Use `ctx.timing().wait_for_*()` | Adaptive polling without flakiness |
| Use production APIs directly | Tests validate real behavior |
| Use proptest for edge cases | Finds bugs fixed-value tests miss |
| Test error cases explicitly | Validates error handling paths |

### DON'T

| Anti-Pattern | Problem |
|--------------|---------|
| `tokio::time::sleep()` | Too short = flaky, too long = slow |
| `std::thread::sleep()` | Blocks async executor |
| Mock production types | Tests don't validate real code |
| Assume event ordering | Use ULID timestamps |
| Ignore cleanup | Let TestContext Drop handle it |
| Skip error testing | Misses failure mode bugs |
| Hardcode stream names | Conflicts in parallel tests |
| Use `unwrap()` in tests | Poor error messages |

## Common Issues

### "Database pool exhausted"

**Symptoms**: Test hangs waiting for database, eventually times out.

**Cause**: More concurrent tests than available slots.

**Solutions**:
```bash
# Use debug mode (single-threaded)
xtask test --debug

# Or increase PostgreSQL max_connections
# (in postgresql.conf or via devenv)
```

The pool size is `max(64, test_threads × 2)` and auto-shrinks if `max_connections` is too low.

### "Advisory lock timeout"

**Symptoms**: Test hangs during database acquisition.

**Cause**: Previous test crashed, leaving lock held.

**Solutions**:
```bash
# Check PostgreSQL connections
psql -c "SELECT pid, datname, state, query
         FROM pg_stat_activity
         WHERE datname LIKE 'sinex_test%'"

# Terminate stuck backends
psql -c "SELECT pg_terminate_backend(pid)
         FROM pg_stat_activity
         WHERE datname LIKE 'sinex_test%'"
```

### "Migration fingerprint mismatch"

**Symptoms**: Tests fail with schema errors despite no code changes.

**Cause**: Template database out of sync with migration files.

**Solution**: The harness auto-rebuilds, but for manual reset:
```bash
rm target/xtask/sandbox/template_stamp.json
xtask test --prime
```

### "Tests hang on cleanup"

**Symptoms**: Test appears complete but doesn't finish.

**Cause**: Background cleanup manager waiting on locks.

**Solutions**:
```bash
# Check for orphaned processes
ps aux | grep sqlx

# Verify cleanup manager timeout
# (default: 5s for lock release, 2s for pool close)
```

### "NATS not initialized"

**Symptoms**: `nats_client()` or `jetstream()` returns error.

**Cause**: Forgot to call `with_shared_nats()`.

**Solution**: Add before publishing:
```rust
let ctx = ctx.with_nats().shared().await?;
```

### "Stream already exists with different config"

**Symptoms**: JetStream operation fails with conflict error.

**Cause**: Hardcoded stream name conflicts with another test.

**Solution**: Use namespace helper:
```rust
let namespace = ctx.pipeline_namespace();
let stream_name = namespace.stream("MY_STREAM");  // Not "MY_STREAM" directly
```

### "Pipeline test timeout"

**Symptoms**: `wait_for_event_count()` times out.

**Causes**:
- Ingestd not consuming fast enough
- Too many concurrent pipeline tests (>6)
- Events not being published correctly

**Solutions**:
- Check concurrency guard (max 6 PipelineScope instances)
- Verify NATS connectivity
- Use `ctx.timing().wait_for_condition()` for custom checks

### "Property test regression not reproducible"

**Symptoms**: Test passes locally but fails in CI (or vice versa).

**Causes**:
- Different proptest case counts
- Non-deterministic strategy

**Solutions**:
```bash
# Reproduce with exact seed
SINEX_PROPTEST_SEED=12345 xtask test

# Verify strategy is deterministic (no Ulid::new(), etc.)
```

## Debugging Strategies

### Enable Tracing

```rust
#[sinex_test(trace = true)]
async fn test_with_logs(ctx: TestContext) -> Result<()> {
    // Tracing captured automatically
    Ok(())
}
```

Or programmatically:
```rust
let ctx = ctx.with_tracing("debug");
```

### Inspect Captured Logs

```rust
let logs = ctx.captured_logs();
for log in &logs {
    eprintln!("{}", log);
}
```

### Check Failure Artifacts

When tests fail, artifacts are written to `target/test-artifacts/` (or `SINEX_TEST_FAIL_DIR`):

```bash
ls target/test-artifacts/
cat target/test-artifacts/test_name_*.json
```

Artifacts contain:
- Error message and backtrace
- Pool statistics
- Captured logs

### Use Debug Mode

```bash
# Default: retries enabled
xtask test

# Debug mode (single-threaded, full output)
xtask test --debug
```

### Snapshot Retries

For flaky integration tests, use `snapshot_helper::retry_with_snapshot`:
```rust
retry_with_snapshot(|| async {
    // Your test code
    Ok(())
}).await?;
```

This captures failure snapshots on first failure, attempts cleanup, then retries once.

## Test Templates

### Complete Unit Test

```rust
#[sinex_test]
#[case("source1", "type1")]
#[case("source2", "type2")]
async fn test_unit_example(
    ctx: TestContext,
    #[case] source: &str,
    #[case] event_type: &str,
) -> Result<()> {
    // Arrange
    let ctx = ctx.with_nats().shared().await?;
    let event = ctx.publish_event(
        source,
        event_type,
        json!({"test_key": "test_value"}),
    ).await?;

    // Act
    let events = ctx.pool.events()
        .get_by_source(&EventSource::new(source.to_string()), Some(10), None)
        .await?;

    // Assert
    ctx.assert("unit test")
        .not_empty(&events)?
        .has_size(&events, 1)?;

    assert_eq!(events[0].source.as_str(), source);
    Ok(())
}
```

### Complete Integration Test

```rust
#[sinex_test]
async fn test_integration_example(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;

    // Setup: Create test data
    for i in 0..5 {
        ctx.publish_event(
            "integration-test",
            "test.event",
            json!({"index": i}),
        ).await?;
    }

    // Wait for persistence
    ctx.timing().wait_for_event_count(5).await?;

    // Execution: Query
    let all_events = ctx.pool.events()
        .get_by_source(&EventSource::from("integration-test"), Some(100), None)
        .await?;

    // Verification
    ctx.assert("integration test")
        .has_size(&all_events, 5)?;

    for (i, event) in all_events.iter().enumerate() {
        assert_eq!(event.payload["index"], json!(i));
    }

    Ok(())
}
```

### Complete Property Test

```rust
#[sinex_prop(cases = 128, timeout = "60s")]
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

### Error Handling Test

```rust
#[sinex_test]
async fn test_error_handling(ctx: TestContext) -> Result<()> {
    // Test validation rejection
    let result = ctx.publish_event("", "valid.type", json!({})).await;
    assert!(result.is_err());

    let err = result.unwrap_err();
    assert!(err.to_string().contains("validation") ||
            err.to_string().contains("source"));

    // Test specific error variant
    match err {
        SinexError::Validation { .. } => { /* expected */ }
        other => panic!("unexpected error type: {other}"),
    }

    Ok(())
}
```

## Diagnostics Commands

### `xtask status --doctor`

Reports toolchain versions, NATS availability, Postgres reachability, and required extensions:

```bash
xtask status --doctor
```

### Pool Health Check

```rust
let report = check_pool_health().await?;
println!("Healthy: {}/{}", report.healthy_slots, report.total_slots);
println!("Quarantined: {}", report.quarantined_slots);
```

### Pool Statistics

```rust
let stats = get_pool_stats();
println!("Total acquisitions: {}", stats.total_acquisitions);
println!("Avg wait time: {}ms", stats.average_wait_time_ms);
println!("Cleanup failures: {}", stats.cleanup_failures);
```

## Flaky Test Prevention

1. **Use adaptive polling** — never `sleep()`
2. **Use namespace isolation** — never hardcode stream names
3. **Use TestBarrier** — coordinate concurrent tasks
4. **Use TestSynchronizer** — wait for background signals
5. **Use ULID ordering** — never assume insertion order
6. **Use proptest** — find edge cases systematically
7. **Use default flags** — retries enabled by default

## CI-Specific Tips

```bash
# CI configuration with priming
xtask test --prime

# Debug CI failures locally
xtask test --debug -- -p failing-crate
```

## Key Files

| File | Purpose |
|------|---------|
| `database_pool.rs` | Pool implementation (~1800 lines) |
| `test_context.rs` | TestContext struct and methods |
| `timing.rs` | Synchronization primitives |
| `nats.rs` | EphemeralNats management |
| `macros/src/lib.rs` | `#[sinex_test]`, `#[sinex_prop]` |
| `.config/nextest.toml` | Nextest profiles |
| `target/test-artifacts/` | Failure snapshots |
