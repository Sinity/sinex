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
| `DATABASE_URL` | (from the devShell) | Primary test database connection |
| `SINEX_TEST_FAIL_DIR` | `.sinex/test-artifacts/` | Failure snapshot directory |
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

Add `xtask` as a dev-dependency in `Cargo.toml`:

```toml
[dev-dependencies]
xtask = { path = "../xtask" }
```

The sandbox harness is available from the `xtask::sandbox` module.

## Logging and Diagnostics

The harness prints compact progress (`🔄` running, `✅/❌` result with elapsed time) for every test.

```rust
// Access captured logs inside a test
let logs = ctx.captured_logs();
assert!(logs.iter().any(|l| l.contains("expected message")));

// Assert specific log was emitted
ctx.assert_logged("checkpoint saved")?;
```

When a test fails, the harness records structured evidence under
`.sinex/test-artifacts/` and prints:

```text
EVIDENCE: <path>.evidence.json SUMMARY: <path>.summary.txt (<error>)
```

The evidence bundle contains the failing test, error, pool state, context state,
process snapshot, timeline events, capture summaries, and artifact references.
Captured tracing logs are attached automatically when present. Runtime
dependencies observed through harness entrypoints are emitted as impact artifacts
under `.sinex/test-artifacts/impact/`; scheduling decisions must come from
machine-derived impact data and exact proof fingerprints, not hand-written test
taxonomy labels.

```rust
#[sinex_test(timeout = 120)]
async fn source_material_row_stream_preserves_anchors(ctx: TestContext) -> Result<()> {
    // test body
    Ok(())
}
```

Use ordinary nextest filters for exact test selection:

```bash
xtask test -p sinexd -E 'test(parse_listener)'
xtask impact explain
```

Resource-shape tests should stay in Rust tests when the metric belongs to
product/runtime behavior rather than the xtask command runner. Keep assertions
limited to correctness invariants, and write measured resource profiles as JSON
evidence artifacts:

```bash
xtask test -p sinexd -E 'test(material_assembler)'
xtask test -p sinex-e2e-tests -E 'test(material_idempotency)'
xtask test -p sinex-workspace-tests -E 'test(weechat_vertical)'
```

Those artifacts are trendable input records. They are not perf contracts unless
a later change deliberately promotes a measured metric into a documented gate.

Migration note: source-material, parser, and restart-recovery coverage lives
in `sinexd`, workspace, and e2e Rust tests, not `xtask exercise` and not taxonomy metadata.
Future `xtask exercise` entries that validate product/runtime behavior rather
than xtask command contracts should be treated as migration-only and moved into
ordinary `#[sinex_test]` coverage.

Tests can opt into richer collectors before failing:

```rust
ctx.record_evidence_event("fixture", "created source material", json!({"source": "terminal"}));
ctx.capture_db_evidence("db").await?;
ctx.capture_nats_evidence("nats").await?;
ctx.capture_material_directory_evidence("spool", spool_dir)?;
```

Use named collectors for source-material, NATS, DB, logs, process, and custom
evidence. `xtask test` surfaces artifact paths and records runtime dependency
edges for impact planning; semantics live in Rust tests and production code
contracts.

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
xtask test -p xtask

# Update snapshots
xtask test --prime --update-snapshots
```

## Documentation Index

- **[patterns.md](patterns.md)** — Fixture registry, property testing, database pool architecture
- **[test_context.md](test_context.md)** — TestContext API, lifecycle, assertions
- **[database_testing.md](database_testing.md)** — Pool architecture, isolation, cleanup
- **[pipeline_testing.md](pipeline_testing.md)** — NATS, JetStream, PipelineScope
- **[timing_patterns.md](timing_patterns.md)** — Synchronization, barriers, wait helpers
- **[property_testing.md](property_testing.md)** — Proptest integration, strategies
- **[troubleshooting.md](troubleshooting.md)** — Common issues, best practices, templates
