# Testing Documentation

## Test Utilities

Test infrastructure lives in `xtask/src/sandbox/` and is documented in `xtask/docs/sandbox/`:

| Document | Content |
|----------|---------|
| `README.md` | Entry point, quick start, environment variables |
| `test_context.md` | TestContext API, lifecycle, assertions, diagnostics |
| `database_testing.md` | Pool architecture, isolation, cleanup, FK handling |
| `pipeline_testing.md` | NATS, JetStream, PipelineScope, namespace isolation |
| `timing_patterns.md` | Synchronization, barriers, adaptive polling |
| `property_testing.md` | Proptest integration, strategies, regression files |
| `troubleshooting.md` | Common issues, best practices, test templates |

## Global Testing References

- `../../TESTING.md` — Workspace-wide testing handbook (commands, layout, conventions)

## Running Tests

```bash
# Fast feedback
cargo xtask test

# Debug mode (single-threaded, full output)
cargo xtask test --debug

# Full workspace with priming (recommended before PR)
cargo xtask test --prime

# Specific crate
cargo xtask test -- -p sinex-primitives

# Update snapshots
INSTA_UPDATE=always cargo xtask test --prime
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
