# Feature Flags

This document describes compile-time feature flags used across Sinex crates.

## Core Feature Matrix

### sinex-primitives

| Feature | Default | Dependencies | Description |
|---------|---------|--------------|-------------|
| `full` | Yes (via default) | `macros`, `sqlx`, `nats` | Complete feature set |
| `types-only` | No | `macros` | Just types, validation, errors (no DB/NATS) |
| `macros` | No | `sinex-macros` | Proc macros (`EventPayload` derive) |
| `sqlx` | No | `sqlx`, `jsonschema` | Database types, pools, repositories |
| `nats` | No | `async-nats` | NATS coordination client |
| `arbitrary` | No | `proptest` | Property testing strategies |
| `testing` | No | - | Test-only methods |

### sinex-node-sdk

| Feature | Default | Dependencies | Description |
|---------|---------|--------------|-------------|
| `db` | Yes | `sqlx`, `sinex-db/sqlx` | Database access |
| `messaging` | Yes | `async-nats`, `sinex-primitives/nats` | NATS messaging |
| `preflight` | No | - | System preflight checks |
| `event-source` | No | - | Event source helpers |
| `automaton` | No | - | Automaton infrastructure |
| `macros` | No | `sinex-macros` | Proc macros |
| `external-tests` | No | - | Tests requiring external services |

### xtask (sandbox feature)

| Feature | Default | Description |
|---------|---------|-------------|
| `sandbox` | No | Test infrastructure (TestContext, pool, NATS helpers) |
| `bench` | No | Benchmarking support (divan, sysinfo) |

## Binary Requirements

Each binary requires specific features to be enabled in its dependencies:

| Binary | Required sinex-primitives Features | Required sinex-node-sdk Features |
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
sinex-primitives = { version = "0.4", default-features = false, features = ["types-only"] }
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
2. **Minimal** - `sinex-primitives` with `types-only`
3. **No NATS** - `sinex-node-sdk` with `db` only
4. **Heavy tests** - With `--heavy` flag

To test a specific feature combination locally:

```bash
# Minimal build
cargo build -p sinex-primitives --no-default-features --features types-only

# Without NATS
cargo build -p sinex-node-sdk --no-default-features --features db

# Run heavy/ignored tests
xtask test --heavy
```

## Adding New Features

When adding a new feature flag:

1. Document it in this file
2. Update the binary requirements table if needed
3. Add a CI job for the feature combination if it should be officially supported
4. Coordinate downstream consumer changes before enabling the flag by default
