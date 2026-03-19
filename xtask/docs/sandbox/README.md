# Sinex Test Utilities

A comprehensive testing framework for the Sinex event-driven data capture system, providing
database isolation, pipeline testing, and robust testing patterns.

> **Workspace-wide handbook**: See `README.md` for documentation routing
> and `xtask/docs/verification.md` for validation flows. This documentation
> focuses on the test utilities API.

## Quick Start

```rust
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_event_creation(ctx: TestContext) -> TestResult<()> {
    // Enable NATS (required for pipeline testing)
    let ctx = ctx.with_nats().shared().await?;

    // Create test event using the real pipeline
    let event = ctx
        .publish_event(
            "fs-watcher",
            "file.created",
            json!({"path": "/test.txt", "size": 1024})
        )
        .await?;

    // Query using direct repository access
    let events = ctx.pool.events().get_recent(10).await?;

    // Rich assertions with context
    ctx.assert("event creation")
        .eq(&events.len(), &1)?
        .that(events[0].payload["size"] == json!(1024), "size should match")?;

    Ok(())
}
```

## Key Features

| Feature | Description | Documentation |
|---------|-------------|---------------|
| **Database Isolation** | Each test gets an isolated database from a pool | [database_testing.md](database_testing.md) |
| **Pipeline Testing** | Tests exercise real NATS → ingestd → DB flow | [pipeline_testing.md](pipeline_testing.md) |
| **TestContext** | Central coordination with assertions and timing | [test_context.md](test_context.md) |
| **Property Testing** | Proptest integration with `#[sinex_prop]` | [property_testing.md](property_testing.md) |
| **Timing Utilities** | Synchronization, barriers, adaptive polling | [timing_patterns.md](timing_patterns.md) |
| **Troubleshooting** | Common issues and best practices | [troubleshooting.md](troubleshooting.md) |

## The Pipeline-First Rule

Before seeding any events, call `ctx.with_nats().shared().await?` and use
`ctx.publish_event(...)` so every test exercises the actual ingestion path.

```rust
// PREFERRED: Pipeline-first approach (exercises NATS → ingestd → DB)
let ctx = ctx.with_nats().shared().await?;
let event = ctx.publish_event(
    "fs-watcher",
    "file.created",
    json!({"path": "/test/file.txt", "size": 1024})
).await?;

// ALTERNATIVE: Direct repository access (only for unit tests)
let event = pool.events().insert_test_event(
    "fs-watcher",
    "file.created",
    json!({"path": "/test/file.txt", "size": 1024})
).await?;
```

Direct database insertion bypasses ingestd and should be used sparingly.

## Test Macros

### `#[sinex_test]`

The primary test macro providing automatic TestContext lifecycle:

```rust
// Basic usage
#[sinex_test]
async fn test_basic(ctx: TestContext) -> Result<()> {
    Ok(())
}

// With custom timeout (default: 30s async, 10s sync)
#[sinex_test(timeout = 60)]
async fn test_long_running(ctx: TestContext) -> Result<()> {
    Ok(())
}

// With tracing enabled
#[sinex_test(trace = true)]
async fn test_with_logs(ctx: TestContext) -> Result<()> {
    Ok(())
}

// With rstest parameterization
#[sinex_test]
#[case("source1", "type1")]
#[case("source2", "type2")]
async fn test_parameterized(
    ctx: TestContext,
    #[case] source: &str,
    #[case] event_type: &str,
) -> Result<()> {
    Ok(())
}
```

### `#[sinex_prop]` and `sinex_proptest!`

Property testing macros with TestContext integration:

```rust
#[sinex_prop(cases = 64, timeout = "45s")]
async fn property_test(
    ctx: &TestContext,
    #[strategy(any::<u64>())] value: u64,
) -> TestResult<()> {
    // property assertions
    Ok(())
}

sinex_proptest! {
    fn uuid_roundtrip(value in any::<String>()) -> TestResult<()> {
        // pure property test without TestContext
        Ok(())
    }
}
```

## Environment Variables

### Test Infrastructure

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATABASE_URL` | (from devenv) | Primary test database connection |
| `SINEX_TEST_FAIL_DIR` | `target/test-artifacts/` | Failure snapshot directory |
| `SINEX_TEST_USE_TLS` | `false` | Enable TLS in integration tests |
| `SINEX_TEST_NATS_TOKEN` | — | NATS authentication token |
| `SINEX_TEST_NATS_CONFIG_FILE` | — | Custom NATS config path |
| `SINEX_TEST_OPTIMIZATIONS` | `false` | Enable test optimizations |
### Property Testing

| Variable | Default | Purpose |
|----------|---------|---------|
| `SINEX_PROPTEST_SEED` | random | Reproducible seed for debugging |

### Database Testing

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATABASE_URL_SUPERUSER` | — | Superuser connection for setup/teardown |
| `DATABASE_URL_APP` | — | App-specific connection for permission tests |
| `BENCH_DATABASE_URL` | — | Separate database for benchmarks |

### Edge Mode

| Variable | Default | Purpose |
|----------|---------|---------|
| `SINEX_EDGE_MODE` | `false` | Suppress DATABASE_URL requirement, enable schema cache |

## Feature Flags

Enable in `Cargo.toml` for additional functionality:

```toml
[dev-dependencies]
xtask sandbox = { path = "../xtask sandbox", features = ["bench", "proptest"] }
```

| Feature | Purpose |
|---------|---------|
| `bench` | Enable benchmarking support (`#[sinex_bench]`) |
| `proptest` | Property-based testing integration |
| `tracing` | Enhanced tracing and log capture |

## Logging and Diagnostics

The harness prints compact progress (`🔄` running, `✅/❌` result with elapsed time) for every test.

```rust
// Access captured logs inside a test
let logs = ctx.captured_logs();
assert!(logs.iter().any(|l| l.contains("expected message")));

// Assert specific log was emitted
ctx.assert_logged("checkpoint saved")?;
```

When a test fails, the harness records a JSON artifact under `target/test-artifacts/` containing:
- Error message and backtrace
- Pool statistics at failure time
- Captured tracing logs (when TestContext is present)

Override the artifact directory with `SINEX_TEST_FAIL_DIR`.

## Running Tests

```bash
# Fast feedback
xtask test

# Debug mode (single-threaded, full output)
xtask test --debug

# Full workspace with priming (recommended before PR)
xtask test --prime

# Single crate
xtask test -- -p xtask

# Update snapshots
INSTA_UPDATE=always xtask test --prime
```

## Documentation Index

- **[patterns.md](patterns.md)** — Fixture registry, property testing, database pool architecture
- **[test_context.md](test_context.md)** — TestContext API, lifecycle, assertions
- **[database_testing.md](database_testing.md)** — Pool architecture, isolation, cleanup
- **[pipeline_testing.md](pipeline_testing.md)** — NATS, JetStream, PipelineScope
- **[timing_patterns.md](timing_patterns.md)** — Synchronization, barriers, wait helpers
- **[property_testing.md](property_testing.md)** — Proptest integration, strategies
- **[troubleshooting.md](troubleshooting.md)** — Common issues, best practices, templates
