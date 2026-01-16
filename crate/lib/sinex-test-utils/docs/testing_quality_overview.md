# Testing, Quality Assurance, and Reliability Overview

This note complements the day-to-day guides by highlighting the cross-cutting
quality controls that keep the Sinex test ecosystem healthy. For hands-on
usage, see:

- `TESTING.md` – quick-start guide for writing and running tests with
  `TestContext` and Nextest.
- `docs/overview.md` – API-level documentation for `sinex-test-utils` helpers
  (dataset seeding, timing utilities, assertions, database pool).
- README files under `tests/` for suite-specific notes (e.g., property tests,
  VM smoke tests).

## Test Suite Layout

| Location                     | Focus / Scope                                             | Notes                                                   |
|------------------------------|-----------------------------------------------------------|---------------------------------------------------------|
| `crate/lib/sinex-core/tests/unit/`        | Fast, isolated component tests                           | Keep under ~1 s each; prefer direct API usage           |
| `crate/lib/sinex-core/tests/integration/` | Core repository and schema workflows                     | Former workspace suites now colocated with the crate    |
| `crate/lib/sinex-core/tests/{system,performance,adversarial}/` | Heavy end-to-end, load, and chaos coverage            | Uses real services; slower (~minutes)                   |
| `crate/lib/sinex-satellite-sdk/tests/{integration,property,system}/` | Satellite lifecycle and annex validation        | Property regressions live beside the SDK helpers        |
| `crate/lib/sinex-test-utils/tests/`       | Harness demonstrations and helper examples               | Includes the migrated `macro_conversion` / `rstest` demos|
| `tests/e2e/`                              | NixOS module assertions and VM harness support           | Hosts the Rust integration test plus shared Nix assets  |
| `tests/e2e/nixos-vm/`                     | Full VM deployment and chaos scenarios                   | Optional in CI; document dataset sizes                  |

`TestContext` is the single entry point for database access, dataset seeding,
timing utilities, and assertion helpers. Favour it over one-off utilities when
adding new tests.

## Nextest Profiles

The workspace ships with several profiles (`.config/nextest.toml`):

| Profile       | Use case                               | Tweaks                                              |
|---------------|----------------------------------------|-----------------------------------------------------|
| `default`     | CI / day-to-day baseline                | `test-threads = num-cpus`, retry once               |
| `fast`        | Quick local feedback                    | `test-threads = num-cpus`, no retries               |
| `reliable`    | Flake hunting / soak tests              | `test-threads = num-cpus`, 3 retries, 180s timeout  |
| `ci`          | CI selection                            | reliable + CI-only skips                            |
| `parallel`    | Max throughput on large machines        | `test-threads = num-cpus`, no retries, low output   |
| `debug`       | Single-threaded with full output        | 1 thread, `success-output = immediate-final`        |
| `ci-parallel` | High-parallel CI runners                | `test-threads = num-cpus`, retry once               |
| `bench`       | Benchmark runs                          | `test-threads = num-cpus`, 600s timeout             |
| `perf`        | Explicit perf suites only               | filtered; 600s timeout                              |
| `stress`      | Explicit stress/chaos suites only       | filtered; 900s timeout                              |
| `external`    | External/git-annex suites only          | filtered; 900s timeout                              |

Invoke with `cargo xtask test --profile <name>`.

## Quality Controls

- Linting lives in `Cargo.toml` (`workspace.lints.*`). Clippy runs with `warn`
  levels for the `pedantic`/`nursery` groups and escalates targeted lints
  (`too_many_arguments`, `type_complexity`). The root `clippy.toml` currently
  only bans `std::thread::sleep` so we do not block inside async code.
- CI (`.github/workflows/ci.yml`) mirrors the local workflow: formatting,
  clippy, `cargo xtask test --profile ci --prime` against TimescaleDB,
  and optional coverage via Tarpaulin. SQLx macros always run against the live
  test database during compilation, so no offline cache exists.
- Coverage generation uses `cargo tarpaulin`. Run it during larger refactors or
  before landing riskier changes to confirm we are exercising new execution
  paths.

## Reliability & Observability

- Structured error handling (`sinex_core::types::error::SinexError`) keeps
  failure modes explicit. When adding new domains, extend the error enums rather
  than falling back to opaque strings.
- `IntegrityTester` and related utilities (`docs/database_pool.md`) provide
  repeatable data checks—use them whenever schema or ingestion logic changes.
- Tracing/metrics emit through `sinex-telemetry`; enable `RUST_LOG` and Grafana
  dashboards when diagnosing timing or ordering issues in tests.

## Developer Workflow at a Glance

- `cargo xtask check` – fmt check + `cargo check`.
- `cargo xtask test --profile fast` – quick loop with Nextest.
- `cargo xtask test --profile ci --prime` – CI-equivalent selection.
- `nix develop` – reproducible environment with TimescaleDB, NATS, and compiler
  toolchains pinned.

## Current Focus Areas

- Continue migrating legacy tests away from ad-hoc SQL toward the shared query
  builders exposed by `sinex-core`. This removes duplication and keeps SQLx
  compile-time checks meaningful.
- Consolidate older helper macros and event builders into the modern
  `TestContext` & dataset seeding helpers so new contributors have a single obvious path.
- Monitor flaky suites with the `reliable` profile; promote fixes upstream once
  the repeated retries stop catching regressions.

## Test Suite Streamlining (carry-forward)

- Recompute test counts before any consolidation work; the last audit predates
  the migration of tests into crate-local suites.
- Aim for one comprehensive test per feature area and use parameterized tests
  (or property tests) for variations, rather than duplicating near-identical
  cases.
- Focus streamlining on sinex-test-utils helper modules that historically
  accumulated duplication: `test_context`, `coverage_assurance`, `timing_utils`,
  `satellite_management_utils`, `property_testing`, and `lib.rs`.
- Treat streamlining as a performance lever: removing redundant tests reduces
  database pool pressure and avoids hangs under high parallelism.
