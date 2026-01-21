# Testing Documentation

## Crate-Level Test Documentation

Detailed test patterns and utilities are documented in the `sinex-test-utils` crate
(`crate/lib/sinex-test-utils/docs/`):

| Document | Content |
|----------|---------|
| `README.md` | Entry point, quick start, environment variables, feature flags |
| `test_context.md` | TestContext API, lifecycle, assertions, diagnostics |
| `database_testing.md` | Pool architecture, isolation, cleanup, FK handling |
| `pipeline_testing.md` | NATS, JetStream, PipelineScope, namespace isolation |
| `timing_patterns.md` | Synchronization, barriers, adaptive polling, SINEX_EDGE_MODE |
| `property_testing.md` | Proptest integration, strategies, regression files |
| `troubleshooting.md` | Common issues, best practices, test templates |

## Global Testing References

- `../../TESTING.md` — Workspace-wide testing handbook (commands, layout, conventions)

## Running Tests

```bash
# Fast feedback (no retries)
cargo xtask test --profile fast

# Full workspace (recommended before PR)
cargo xtask test --profile default --prime

# CI selection
cargo xtask test --profile default --prime

# Specific crate
cargo xtask test --profile default -- -p sinex-test-utils

# Update snapshots
INSTA_UPDATE=always cargo xtask test --profile default --prime
```

## Test Levels

| Level | Scope | Database | NATS |
|-------|-------|----------|------|
| L0 | Pure functions | No | No |
| L1 | Unit + DB | Yes | No |
| L2 | Integration | Yes | Ephemeral |
| L3 | Pipeline | Yes | Shared |
| L4 | System/E2E | Yes | Full |

## See Also

- Testing priorities: `docs/planning/testing-priorities-and-roadmap.md`
- Cross-cutting patterns: `docs/current/architecture/advanced-implementation-patterns.md`
