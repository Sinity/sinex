# Sinex Testing Guide

This guide outlines the standard testing patterns and utilities available in `sinex-test-utils` for verifying the Sinex distributed system.

## 1. Test Categories

We distinguish between three main types of tests:

### Unit Tests (`#[test]`)

- **Scope**: Single function or module.
- **Location**: Inside `src/` files or `tests/unit`.
- **Dependencies**: minimal mocking; no external services (NATS/DB).

### Integration Tests (`tests/integration/*.rs`)

- **Scope**: Component interaction (e.g. `Satellite` + `NATS` + `DB`).
- **Infrastructure**: Uses `sinex-test-utils` spawners.
- **Key Pattern**: "Black Box" testing via public APIs.

### Pipeline Tests (`tests/integration/pipeline_*.rs`)

- **Scope**: End-to-end data flow (Ingest -> Process -> Emit).
- **Tooling**: `TestPipeline` builder.

## 2. Core Utilities

### `TestEnv`

The fundamental harness for integration tests.

```rust
use sinex_test_utils::*;

#[tokio::test]
async fn test_example() -> Result<()> {
    let mut env = TestEnv::new().await?; // Spawns NATS, DB (if needed) -> Edge Mode compatible
    let nats = env.nats_client().await?;
    // ...
}
```

### `WaitHelpers`

Eliminate strict sleeps (`thread::sleep`) in favor of polling.

```rust
// BAD
tokio::time::sleep(Duration::from_secs(5)).await;

// GOOD
WaitHelpers::wait_for(|| async {
    check_condition().await
}, Duration::from_secs(5)).await?;
```

### `SatelliteRuntime` (Edge Mode)

Runs a satellite in-process for testing.

```rust
let (guard, handle) = SatelliteRuntime::spawn_testing(
    my_satellite_service, 
    &env
).await?;
```

## 3. Best Practices

- **Edge Mode First**: Prefer tests that run without a database (`SINEX_EDGE_MODE=1`). NATS is the primary coordination bus.
- **Unique Subjects**: Use `TestEnv::random_subject()` or UUIDs to avoid collision in parallel tests.
- **Cleanup**: `TestEnv` handles teardown, but ensure explicitly spawned processes (if any) are killed.
