# Sinex Testing Handbook

This handbook covers workspace-wide testing conventions, test organization, and CI configuration.
For detailed API documentation, see `xtask/docs/sandbox/` (test utilities are in `xtask::sandbox`).

## Quick Start

```bash
# Quick feedback
xtask test

# Debug mode (single-threaded, full output)
xtask test --debug

# Full workspace with priming (recommended before PR)
xtask test --prime

# Targeted runs
xtask test -- -p <package>
xtask test -- --test <binary>

# Update snapshots
INSTA_UPDATE=always xtask test --prime
```

## Prerequisites

Run `nix develop` (or source your local toolchain) so TimescaleDB/PostgreSQL, the `nats-server`
binary, and the Cargo toolchain are available.

If outside Nix, install NATS manually:

```bash
# macOS
brew install nats-server

# Linux (Debian/Ubuntu)
sudo apt-get install -y nats-server

# Or download release binary
curl -L https://github.com/nats-io/nats-server/releases/latest/download/nats-server-amd64.zip -o /tmp/nats.zip
unzip /tmp/nats.zip -d /tmp/nats && sudo mv /tmp/nats/nats-server /usr/local/bin/
```

Override with `NATS_SERVER_BIN=/custom/path/nats-server` if needed.

## Diagnostics

- `xtask doctor` — reports toolchain versions, NATS availability, Postgres reachability
- Failure artifacts written to `target/test-artifacts/` (override with `SINEX_TEST_FAIL_DIR`)

## Test Layout

| Location | What lives here |
|----------|-----------------|
| `crate/*/<crate>/tests/` | Crate-owned unit, integration, and property tests |
| `crate/lib/sinex-primitives/tests/` | Core data-path suites (unit, integration, performance, adversarial) |
| `crate/lib/sinex-node-sdk/tests/` | Node SDK lifecycle, checkpoint, and annex coverage |
| `xtask/tests/` | Test harness demonstrations and xtask command tests |
| `tests/e2e/` | NixOS module assertions and VM harness support |
| `tests/e2e/nixos-vm/` | Full NixOS VM suites (deployment, chaos, performance) |

**Rule**: Put new tests in the crate that owns the behavior. Workspace-level tests are reserved
for scenarios that truly span multiple crates.

## Test Flags

Use xtask flags instead of nextest profiles:

| Flag | Use case |
|------|----------|
| (none) | Standard runs with retries, perf/stress/external excluded |
| `--debug` | Single-threaded with full stdout/stderr |
| `--heavy` | Include `#[ignore]` tests (long-running, external) |
| `--prime` | Prime database template before testing |
| `--affected` | Only test changed packages |

## Running heavy / ignored tests

Some tests are intentionally marked `#[ignore = "long"]` or `#[ignore = "external"]` and are skipped by default to keep quick developer feedback fast. To run those tests locally you can:

```bash
# Run only tests annotated with #[ignore = "long"|"external"] (recommended)
direnv exec /realm/project/sinex xtask test:heavy --prime

# To run *all* ignored tests (including flaky/platform-specific skips):
direnv exec /realm/project/sinex xtask test --include-ignored --prime

# or use the helper script
./scripts/run-heavy-tests.sh
```

There is also a VS Code task named "Run heavy tests (include ignored)" that runs the same command.

## Property Testing Conventions

- **sinex-primitives**: Property tests for event modeling, schema validation, ULID behavior,
  sanitization, and repository invariants live under `crate/lib/sinex-primitives/tests/property/`.

- **sinex-node-sdk**: Cross-node properties use NATS fixtures and live under
  `crate/lib/sinex-node-sdk/tests/property/`.

- **Cross-crate properties**: Keep workspace-level tests only when the scenario genuinely
  spans multiple crates. Document cross-crate dependencies at the top of the file.

Use `#[sinex_prop]` or `sinex_proptest!` macros. Add `cases = 256` to lock runner config.
Failing seeds persist to `tests/property/*.proptest-regressions`.

```bash
xtask test -- --test property_tests
```

## System Watcher Resilience

The `sinex-system-ingestor` crate includes specialized resilience tests:

- **Unit Tests (`unified_processor::tests`):** Use mock watchers and factories to verify restart logic and failure handling without external dependencies (D-Bus/Systemd).
- **Integration Tests:** Verifies standard behavioral contracts but skips D-Bus tests in CI/headless environments where the system bus is unavailable.

## Quality Controls

- **Linting**: `Cargo.toml` (`workspace.lints.*`). Clippy runs with `warn` levels for
  `pedantic`/`nursery` groups. `clippy.toml` bans `std::thread::sleep` in async code.

- **CI**: `.github/workflows/ci.yml` mirrors local workflow: formatting, clippy,
  `xtask test --prime` against TimescaleDB.

- **Coverage**: `cargo tarpaulin` for coverage reports during larger refactors.

## Coverage Backlog

- Add explicit confirmation payload format + Nats-Msg-Id dedup tests
- Add sinex-node-sdk confirmation consumer integration tests
- Add full automaton integration test in sinex-node-sdk
- Add DLQ consumer/replay tests and retention policy coverage
- Add ingestd property tests for idempotency, batch ordering, monotonic offsets
- Add restart resilience coverage for confirmation stream durability
- Add explicit sinex-schema migration tests
- Add JetStream-focused chaos test in sinex-core adversarial coverage

## Authoritative References

**Test Utilities Documentation** (`xtask/docs/sandbox/`):

- `README.md` — Entry point, quick start, environment variables
- `test_context.md` — TestContext API, lifecycle, assertions
- `database_testing.md` — Pool architecture, isolation, cleanup
- `pipeline_testing.md` — NATS, JetStream, PipelineScope
- `timing_patterns.md` — Synchronization, barriers, wait helpers
- `property_testing.md` — Proptest integration, strategies
- `troubleshooting.md` — Common issues, best practices

**Other References**:

- `tests/e2e/nixos-vm/README.md` — VM harness, parallel snapshot runner
- `docs/documentation-guidelines.md` — Documentation checklist

## If You Only Read One Section

1. Put new tests in the crate that owns the behavior.
2. Use `#[sinex_test]` and `TestContext` utilities — avoid bespoke scaffolding.
3. Keep quick-start commands in muscle memory: `xtask test --prime`.
4. Link back to this handbook when opening PRs.
