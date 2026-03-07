# Sinex Testing Handbook

This handbook defines workspace-wide testing conventions, layout policy, and command usage.
Detailed sandbox API documentation lives under `xtask/docs/sandbox/` (`xtask::sandbox`).

## Core Policy

- Default to `#[sinex_test]` for regular tests.
- Raw `#[test]` / `#[tokio::test]` are allowlisted only for `trybuild`/compile-fail and proc-macro-internal tests.
- Place tests in per-crate `tests/` by default.
- Keep inline `#[cfg(test)]` modules only when extraction would force undesirable visibility changes, and include a short in-file exception reason.

## Quick Start

```bash
# Quick feedback on changed packages
xtask test

# Debug mode (single-threaded, full output)
xtask test --debug

# Prime DB template before running tests
xtask test --prime

# Targeted package / test name
xtask test -p <package>
xtask test --debug -E 'test(name_fragment)'

# Update snapshots
INSTA_UPDATE=always xtask test --prime
```

## Prerequisites

Run `nix develop` (or `direnv allow`) so PostgreSQL/TimescaleDB, `nats-server`, and the Rust toolchain are available.

Override the NATS binary path with `NATS_SERVER_BIN=/custom/path/nats-server` when needed.

## Diagnostics

- `xtask status --doctor` — toolchain/services health report.
- `xtask history tests failures --output` — inspect failing test output from the most recent run.
- Failure artifacts are written to `target/test-artifacts/` (override with `SINEX_TEST_FAIL_DIR`).

## Test Layout

| Location | What lives here |
|----------|-----------------|
| `crate/**/tests/` | Crate-owned unit, integration, and property tests |
| `xtask/tests/` | xtask command and harness tests |
| `tests/e2e/` | Workspace-level multi-crate and NixOS test suites |
| `tests/e2e/nixos-vm/` | Full VM deployment, chaos, and performance suites |

**Rule:** new tests live with the crate that owns the behavior. Workspace-level tests are for genuinely cross-crate scenarios.

## Test Flags

| Flag | Use case |
|------|----------|
| (none) | Standard runs with retries, excludes heavy ignored tests |
| `--debug` | Single-threaded with full stdout/stderr |
| `--heavy` | Include ignored long/external tests |
| `--prime` | Prime DB template before testing |
| `--all` | Override `--affected` and run all workspace tests |
| `--affected` | Run only changed packages (default) |
| `--bg` | Run in background and retrieve output later via `xtask jobs` |

## Running Heavy Tests

```bash
# Include ignored long/external tests
xtask test --heavy --prime

# Helper script (same intent)
./scripts/run-heavy-tests.sh
```

## Property Testing Conventions

- Keep crate-specific property tests in that crate's `tests/` tree.
- Keep workspace property tests only for truly cross-crate invariants.
- Use `#[sinex_prop]` or `sinex_proptest!`; pin `cases = 256` when stability matters.
- Regression seeds persist to `tests/property/*.proptest-regressions`.

```bash
xtask test -p <package> -E 'test(property_)'
```

## Quality Controls

- `xtask check --full` for fmt + clippy + forbidden pattern scan.
- `.github/workflows/ci.yml` mirrors local workflow with `xtask xtr ci workspace`.
- Coverage flows run via `xtask test --coverage`.

## Authoritative References

**Sandbox docs (`xtask/docs/sandbox/`):**

- `README.md` — overview and setup
- `test_context.md` — `TestContext` API
- `database_testing.md` — database isolation model
- `pipeline_testing.md` — NATS/JetStream pipeline tests
- `timing_patterns.md` — deterministic waits and timing helpers
- `property_testing.md` — property test patterns
- `troubleshooting.md` — failure modes and fixes

**Other references:**

- `tests/e2e/nixos-vm/README.md` — VM harness guide
- `docs/documentation-guidelines.md` — test/doc placement checklist

## If You Only Read One Section

1. Put tests in the owning crate by default.
2. Use `#[sinex_test]` unless an explicit allowlisted exception applies.
3. Prefer `xtask test -p ...` and `-E ...` over passthrough args.
4. Run `xtask check --full` and `xtask test --prime` before opening a PR.
