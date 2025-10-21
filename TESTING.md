# Sinex Testing Handbook

This handbook is the single entry point for everything related to testing in the
Sinex workspace. It links out to crate-level deep dives, explains how the test
suites are organised, and captures the conventions that the CI gate enforces.

## Quick Start

```bash
# Fast feedback (unit + library integration)
just test

# Full workspace matrix (all crates, Nextest)
just test-all

# Targeted runs
cargo nextest run --workspace --test <binary>
cargo nextest run --workspace --profile <profile-name>

# Before opening a PR
just pre-commit
```

**Prerequisites:** run `nix develop` (or source your local toolchain) so that
TimescaleDB, NATS, and the Cargo toolchain are available. Test helpers rely on
environment variables such as `DATABASE_URL` that the dev shell provides.

## Test Layout at a Glance

| Location | What lives here | Notes |
| --- | --- | --- |
| `crate/*/<crate>/tests/` | Crate-owned unit, integration, and property tests | Prefer putting new coverage beside the code it exercises |
| `crate/lib/sinex-core/tests/{unit,integration,performance,system,adversarial}` | Core data-path suites and heavy scenarios | Formerly in `tests/`; moved to keep focus with `sinex-core` |
| `crate/lib/sinex-satellite-sdk/tests/{integration,property,system}` | Satellite SDK lifecycle, checkpoint, and annex coverage | Includes property regressions that only touch the SDK |
| `tests/integration/`, `tests/property/`, `tests/examples/` | Remaining cross-crate or documentation-driven scenarios | Gradually shrinking as coverage migrates into crates |
| `tests/nixos-vm/` | Full NixOS VM suites (deployment, chaos, performance) | See `tests/nixos-vm/README.md` for runner details |
| `crate/lib/sinex-core/tests/security/`, `crate/satellites/*/tests/security/` | Hardening suites for core invariants and satellite sandboxes | Security coverage now lives beside the component it protects |

When in doubt, default to the crate’s own `tests/` directory—workspace-level
tests are reserved for scenarios that truly span multiple crates or binaries.

## Writing Tests

- **Always use `#[sinex_test]`** (or helpers built on top of it). The macro
  provisions a `TestContext`, injects an isolated database, wires tracing, and
  enforces timeouts.
- **TestContext first:** reach through `ctx` for repositories, fixtures, timing
  utilities, and assertions. Avoid raw `sqlx::query` unless the query under test
  is exactly what you are asserting.
- **Async hygiene:** use bounded concurrency (`buffer_unordered`, semaphores),
  propagate errors rather than ignoring `JoinHandle`s, and avoid `std::thread::sleep`.
- **Fixtures:** prefer the fixture namespaces under `sinex_test_utils::fixtures`
  rather than re-creating bespoke data builders.
- **Property tests:** place proptest suites alongside the crate they fuzz (see
  below). Capture any new failing seeds in the crate’s
  `tests/property/.proptest-regressions/` directory.

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
cargo nextest run --workspace --test property_tests
```

## Tooling & Profiles

Nextest profiles are defined in `.config/nextest.toml`:

| Profile | Use case |
| --- | --- |
| `default` | CI gate, everyday coverage |
| `fast` | Workstation feedback loop (fewer threads, shorter slow timeout) |
| `reliable` | Flake hunting (fewer threads, more retries, longer timeouts) |
| `parallel` | Max throughput on large machines |
| `debug` | Single-threaded with full stdout/stderr |
| `ci-parallel` | High-parallel CI runners |

Invoke with `cargo nextest run --profile <name>` or use the matching `just`
alias (`just test-fast`, `just test-reliable`, …).

Benchmarks live behind the `bench` feature in `sinex-test-utils`; use
`cargo bench --features bench` or `just bench-*` helpers for comparisons.

## Authoritative References

- `crate/lib/sinex-test-utils/doc/overview.md` – API reference for
  `TestContext`, fixtures, assertions, timing utilities, and the database pool.
- `crate/lib/sinex-test-utils/doc/testing_quality_overview.md` – QA strategy,
  Nextest configuration, and CI expectations.
- `tests/nixos-vm/README.md` – VM harness, parallel snapshot runner, and helper
  commands.
- `docs/documentation-guidelines.md` – documentation checklist (ensure `just
  check` and `just test` pass after moving or adding tests).

## If You Only Read One Section

1. Put new tests in the crate that owns the behaviour.
2. Reach for `TestContext` utilities before writing bespoke scaffolding.
3. Keep the quick-start commands in muscle memory (`just test`, `just test-all`,
   `just pre-commit`).
4. Link back to this handbook (or the crate-level docs above) when opening PRs
   so reviewers know which conventions you followed.
