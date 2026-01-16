# Sinex Testing Handbook

This handbook is the single entry point for everything related to testing in the
Sinex workspace. It links out to crate-level deep dives, explains how the test
suites are organised, and captures the conventions that the CI gate enforces.

## Quick Start

```bash
# Fast feedback (no retries)
cargo xtask test --profile fast

# Full workspace matrix (all crates, Nextest)
cargo xtask test --profile reliable --prime

# CI selection (reliable + CI-only skips)
cargo xtask test --profile ci --prime

# Targeted runs
cargo xtask test --profile <profile-name>
cargo xtask test --profile reliable -- --test <binary>
cargo xtask test --profile reliable -- -p <package>

# Before opening a PR
cargo xtask test --profile reliable --prime
```

## Config Sources & Precedence

**Sources (build + test):**
- **Cargo/tooling:** `Cargo.toml`, `.cargo/config.toml`, `rustfmt.toml`, `clippy.toml`, `deny.toml`, `.cargo-machete.toml`.
- **Dev shell/env:** `.envrc`, `devenv.nix`, `flake.nix`, `.env.example` (manual `.env` overrides only when explicitly sourced).
- **Test runner:** `.config/nextest.toml`, `xtask/src/main.rs`, `tests/e2e/nixos-vm/run-vm-tests.sh`.
- **CI orchestration:** `.github/workflows/*.yml`, `.github/actions/nix-bootstrap/action.yml`.

**Precedence (highest wins):**
1. CLI flags (`cargo`, `xtask`, `nextest`).
2. Environment variables in the running shell (direnv/devenv exports, CI job env, manual exports).
3. Tool config files (Cargo profiles, Nextest profiles, clippy/rustfmt/deny settings).
4. CI workflow steps define the command graph + env for CI runs.

Notes:
- `cargo xtask ci postgres -- <cmd>` injects `PG*` + `DATABASE_URL*` for the wrapped command.
- `flake.nix` builds use an ephemeral Postgres and set `DATABASE_URL`/`PG*` for SQLx checks.

**Prerequisites:** run `nix develop` (or source your local toolchain) so that
TimescaleDB/PostgreSQL, the `nats-server` binary, and the Cargo toolchain are
available. `TestContext` connects to the local Postgres socket at
`/run/postgresql` and spins up an ephemeral JetStream instance by shelling out
to `nats-server`, so both services must be reachable before running the Nextest
suite.

If you are inside the Nix dev shell (`nix develop`), `nats-server` is already
on `PATH` (see `flake.nix` exporting `NATS_SERVER_BIN`). For other toolchains,
install NATS manually:

```bash
# macOS (Homebrew)
brew install nats-server

# Linux (Debian/Ubuntu)
sudo apt-get install -y nats-server

# Or download a release binary
curl -L https://github.com/nats-io/nats-server/releases/latest/download/nats-server-amd64.zip -o /tmp/nats.zip
unzip /tmp/nats.zip -d /tmp/nats
sudo mv /tmp/nats/nats-server /usr/local/bin/
```

You can override the binary path with `NATS_SERVER_BIN=/custom/path/nats-server`
if you prefer to keep it outside `$PATH`.

## Diagnostics & Flake Handling

- `cargo xtask doctor` – reports toolchain versions, NATS binary availability, Postgres reachability, and required extensions for the current DB. Use when service readiness is in doubt.
- `snapshot_helper::retry_with_snapshot` – wrap flaky integration tests to capture failure snapshots (pool stats, context logs) on first failure, attempt cleanup, then retry once. This is now used in dataset seeding, satellite, and timing utilities; mirror the pattern if you add a test that can be sensitive to timing or FK races.

## Satellite Error Handling Conventions

- **Configuration:** use `SatelliteError::Config`/`Configuration` for invalid or missing settings and fail fast during initialization.
- **Lifecycle:** use `SatelliteError::Lifecycle` for missing runtime state, shutdown wiring, or other init/teardown invariants.
- **Processing:** use `SatelliteError::Processing` for per-event data issues (invalid payloads, dropped inputs) that can be skipped or DLQ’d.
- **General:** use `SatelliteError::General` with `eyre::WrapErr`/`eyre!` when bubbling unexpected failures that need context.
- **Logging:** `warn!` for recoverable drops or expected retries; `error!` when operator action is required. Avoid logging the same error twice unless you add new context.

## Benchmarking

- `scripts/bench-builds.sh` – build + SQLx + nix build baselines
- `scripts/bench-nextest.sh` – wrapper for `cargo xtask bench`; see `scripts/bench-nextest.sh --help`

## Test Layout at a Glance

| Location | What lives here | Notes |
| --- | --- | --- |
| `crate/*/<crate>/tests/` | Crate-owned unit, integration, and property tests | Prefer putting new coverage beside the code it exercises |
| `crate/lib/sinex-core/tests/{unit,integration,performance,system,adversarial}` | Core data-path suites and heavy scenarios | Formerly in `tests/`; moved to keep focus with `sinex-core` |
| `crate/lib/sinex-satellite-sdk/tests/{integration,property,system}` | Satellite SDK lifecycle, checkpoint, and annex coverage | Includes property regressions that only touch the SDK |
| `crate/lib/sinex-test-utils/tests/` | Harness demonstrations and helper examples | Includes the migrated `macro_conversion` / `rstest` demos |
| `tests/e2e/` | NixOS module assertions and VM harness support | Hosts the Rust integration test plus shared Nix fixtures |
| `tests/e2e/nixos-vm/` | Full NixOS VM suites (deployment, chaos, performance) | See `tests/e2e/nixos-vm/README.md` for runner details |
| `crate/lib/sinex-core/tests/security/`, `crate/satellites/*/tests/security/` | Hardening suites for core invariants and satellite sandboxes | Security coverage now lives beside the component it protects |

When in doubt, default to the crate’s own `tests/` directory—workspace-level
tests are reserved for scenarios that truly span multiple crates or binaries.

## Writing Tests

- **Always use `#[sinex_test]`** (or helpers built on top of it). The macro
  provisions a `TestContext`, injects an isolated database, wires tracing, and
  enforces timeouts.
- **TestContext first:** reach through `ctx` for repositories, dataset seeds, timing
  utilities, and assertions. Avoid raw `sqlx::query` unless the query under test
  is exactly what you are asserting.
- **Async hygiene:** use bounded concurrency (`buffer_unordered`, semaphores),
  propagate errors rather than ignoring `JoinHandle`s, and avoid `std::thread::sleep`.
- **Dataset seeds:** prefer the helpers under `sinex_test_utils::dataset_seeds`
  rather than re-creating bespoke data builders. When you need a satellite
  runtime for integration coverage, reach for the shared builder exported by
  `sinex_test_utils::TestRuntimeBuilder`. It provisions telemetry emitters,
  checkpoint managers, and NATS wiring automatically so tests exercise the
  production pipeline end-to-end:

  ```rust
  use sinex_test_utils::TestRuntimeBuilder;

  let test_runtime = TestRuntimeBuilder::new(&ctx, "my-service")
      .with_dry_run(true)
      .build()
      .await?;
  ```
- **Property tests:** place proptest suites alongside the crate they fuzz (see
  below). Use `#[sinex_prop]` (or the block-style `sinex_proptest!`) so the
  harness provides tracing, timeouts, context wiring, and seed persistence.
  Add `cases = 256` (or any number) to lock the runner config. Capture any
  failing seeds in that crate’s `tests/property/*.proptest-regressions` files.

## Property Testing Guidelines

- **sinex-core:** property tests that focus on event modelling, schema
  validation, ULID behaviour, sanitisation, or repository invariants live under
  `crate/lib/sinex-core/tests/property/`.
- **sinex-satellite-sdk:** cross-satellite properties require updated NATS
  fixtures and are slated for their own crate-level suite—see the follow-up
  note in `crate/lib/sinex-test-utils/test-analysis.md` before adding new
  coverage.
- **Cross-crate properties:** keep a workspace-level test only when the scenario
  genuinely spans multiple crates (for example, database validation +
  satellite checkpoints + CLI automation in the same property). Document the
  cross-crate dependency at the top of the file so future migrations remain clear.

Run property suites with Nextest like any other test:

```bash
cargo xtask test --profile reliable -- --test property_tests
```

## Tooling & Profiles

Nextest profiles are defined in `.config/nextest.toml`:

| Profile | Use case |
| --- | --- |
| `default` | Baseline selection (perf/stress/external excluded), retry once |
| `fast` | Same selection as default, no retries |
| `reliable` | Same selection, more retries + longer timeout |
| `ci` | Reliable profile with CI-only skips baked in |
| `parallel` | Max throughput on large machines |
| `debug` | Single-threaded with full stdout/stderr |
| `ci-parallel` | High-parallel CI runners |

Invoke with `cargo xtask test --profile <name>` (for example,
`cargo xtask test --profile fast`).

Benchmarks live behind the `bench` feature in `sinex-test-utils`; use
`cargo bench --features bench` for comparisons.

## Authoritative References

- `crate/lib/sinex-test-utils/docs/overview.md` – API reference for
  `TestContext`, dataset seeding, assertions, timing utilities, and the database pool.
- `crate/lib/sinex-test-utils/docs/testing_quality_overview.md` – QA strategy,
  Nextest configuration, and CI expectations.
- `tests/e2e/nixos-vm/README.md` – VM harness, parallel snapshot runner, and helper
  commands.
- `docs/documentation-guidelines.md` – documentation checklist (ensure
  `cargo xtask check` and `cargo xtask test --profile reliable --prime` pass after
  moving or adding tests; use `cargo xtask test --profile ci --prime` to match CI selection).

## Coverage Backlog (carry-forward)

- Add explicit confirmation payload format + Nats-Msg-Id dedup tests (current
  coverage validates confirmation receipt but not message headers or dedup).
- Add sinex-satellite-sdk confirmation consumer integration tests.
- Add a full automaton integration test in sinex-satellite-sdk.
- Add DLQ consumer/replay tests and retention policy coverage (current tests
  only verify routing of invalid payloads into the DLQ).
- Add ingestd property tests for idempotency, batch ordering, and monotonic
  offsets (ingestd tests do not currently use proptest).
- Add restart resilience coverage for outbox/confirmation stream durability
  across ingestd restarts.
- Add explicit sinex-schema migration tests.
- Add a JetStream-focused chaos test in sinex-core adversarial coverage.

## If You Only Read One Section

1. Put new tests in the crate that owns the behaviour.
2. Reach for `TestContext` utilities before writing bespoke scaffolding.
3. Keep the quick-start commands in muscle memory (`cargo xtask test --profile fast` or `cargo xtask test --profile reliable --prime`).
4. Link back to this handbook (or the crate-level docs above) when opening PRs
   so reviewers know which conventions you followed.
