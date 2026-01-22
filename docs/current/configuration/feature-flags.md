# Feature Flags

This document describes compile-time feature flags used across Sinex crates.

## Core Feature Matrix

### sinex-core

| Feature | Default | Dependencies | Description |
|---------|---------|--------------|-------------|
| `full` | Yes (via default) | `macros`, `sqlx`, `nats`, `migrations` | Complete feature set |
| `types-only` | No | `macros` | Just types, validation, errors (no DB/NATS) |
| `macros` | No | `sinex-macros` | Proc macros (`EventPayload` derive, `#[with_context]`) |
| `sqlx` | No | `sqlx`, `jsonschema` | Database types, pools, repositories |
| `nats` | No | `async-nats` | NATS coordination client |
| `migrations` | No | `sea-orm-migration`, `sqlx` | Database migration support |
| `arbitrary` | No | `proptest` | Property testing strategies |
| `testing` | No | - | Test-only methods |
| `slow-tests` | No | - | Long-running test gates |

### sinex-node-sdk

| Feature | Default | Dependencies | Description |
|---------|---------|--------------|-------------|
| `db` | Yes | `sqlx`, `sinex-core/sqlx` | Database access |
| `messaging` | Yes | `async-nats`, `sinex-core/nats` | NATS messaging |
| `preflight` | No | - | System preflight checks |
| `event-source` | No | - | Event source helpers |
| `automaton` | No | - | Automaton infrastructure |
| `macros` | No | `sinex-macros` | Proc macros |
| `external-tests` | No | - | Tests requiring external services |

### sinex-test-utils

| Feature | Default | Description |
|---------|---------|-------------|
| `bench` | No | Benchmarking support (divan, sysinfo) |
| `slow-tests` | No | Long-running test gates |
| `rstest-preview` | No | rstest integration features |
| `internal-tests` | No | Internal test utilities |

## Binary Requirements

Each binary requires specific features to be enabled in its dependencies:

| Binary | Required sinex-core Features | Required sinex-node-sdk Features |
|--------|------------------------------|----------------------------------|
| `sinex-ingestd` | `full` | `db`, `messaging` |
| `sinex-gateway` | `full` | N/A |
| `sinex-fs-ingestor` | `full` | `db`, `messaging` |
| `sinex-terminal-ingestor` | `full` | `db`, `messaging` |
| `sinex-desktop-ingestor` | `full` | `db`, `messaging` |
| `sinex-system-ingestor` | `full` | `db`, `messaging` |
| All automatons | `full` | `db`, `messaging`, `automaton` |

## Minimal Builds

For library consumers that only need types:

```toml
[dependencies]
sinex-core = { version = "0.4", default-features = false, features = ["types-only"] }
```

This provides:
- Event types and payloads
- Domain types (`EventSource`, `EventType`, etc.)
- Error types
- Validation utilities
- No database or NATS dependencies

## CI Feature Testing

The following feature combinations are tested in CI:

1. **Default** - All crates with default features
2. **Minimal** - `sinex-core` with `types-only`
3. **No NATS** - `sinex-node-sdk` with `db` only
4. **Slow tests** - With `slow-tests` feature enabled

To test a specific feature combination locally:

```bash
# Minimal build
cargo build -p sinex-core --no-default-features --features types-only

# Without NATS
cargo build -p sinex-node-sdk --no-default-features --features db

# Run slow tests
cargo nextest run --features slow-tests
```

## Adding New Features

When adding a new feature flag:

1. Document it in this file
2. Update the binary requirements table if needed
3. Add a CI job for the feature combination if it should be officially supported
4. Consider backward compatibility for downstream consumers
