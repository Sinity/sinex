# Testing, Quality Assurance, and Reliability Overview

This note complements the day-to-day guides by highlighting the cross-cutting
quality controls that keep the Sinex test ecosystem healthy. For hands-on
usage, see:

- `TESTING.md` – quick-start guide for writing and running tests with
  `TestContext` and Nextest.
- `doc/overview.md` – API-level documentation for `sinex-test-utils` helpers
  (fixtures, timing utilities, assertions, database pool).
- README files under `tests/` for suite-specific notes (e.g., property tests,
  VM smoke tests).

## Test Suite Layout

| Location                     | Focus / Scope                                             | Notes                                                   |
|------------------------------|-----------------------------------------------------------|---------------------------------------------------------|
| `tests/unit/`                | Fast, isolated component tests                           | Keep under ~1 s each; prefer direct API usage           |
| `tests/integration/`         | Cross-crate workflows and database interactions          | Largest suite; beware legacy raw SQL during migrations  |
| `tests/system/`              | End-to-end flows exercising ingest → automata → gateway  | Uses real services; slower (~minutes)                   |
| `tests/property/`            | proptest-based fuzzing of core invariants                | Commit new `.proptest-regressions` seeds                |
| `tests/adversarial/`         | Chaos/boundary/security scenarios                        | Stress error paths, malformed input                     |
| `tests/performance/`         | Throughput/latency/resource measurements                 | Optional in CI; document dataset sizes                  |
| `tests/security/`, `...`     | Specialised suites (Unicode handling, concurrency, etc.) | Add README if suite-specific setup is required          |

`TestContext` is the single entry point for database access, fixture creation,
timing utilities, and assertion helpers. Favour it over one-off utilities when
adding new tests.

## Nextest Profiles

The workspace ships with several profiles (`.config/nextest.toml`):

| Profile       | Use case                               | Tweaks                                              |
|---------------|----------------------------------------|-----------------------------------------------------|
| `default`     | CI / day-to-day baseline                | `test-threads = num-cpus`, retry once               |
| `fast`        | Quick local feedback                    | 4 threads, shorter slow-timeout                     |
| `reliable`    | Flake hunting / soak tests              | 2 threads, 3 retries, longer slow-timeout           |
| `parallel`    | Max throughput on large machines        | `test-threads = num-cpus`, retries disabled         |
| `debug`       | Single-threaded with full output        | 1 thread, `success-output = immediate-final`        |
| `ci-parallel` | High-parallel CI runners                | 18 threads, retry once, balanced slow-timeout       |

Invoke with `cargo nextest run --profile <name>` or the corresponding `just`
aliases (e.g., `just test`, `just test-fast`, `just test-reliable`).

## Quality Controls

- Linting lives in `Cargo.toml` (`workspace.lints.*`). Clippy runs with `warn`
  levels for the `pedantic`/`nursery` groups and escalates targeted lints
  (`too_many_arguments`, `type_complexity`). The root `clippy.toml` currently
  only bans `std::thread::sleep` so we do not block inside async code.
- CI (`.github/workflows/ci.yml`) mirrors the local workflow: formatting,
  clippy, `cargo nextest --profile default --workspace` against TimescaleDB,
  SQLx offline metadata checks (`cargo sqlx prepare --check`), and optional
  coverage via `just coverage` (Tarpaulin).
- Coverage generation uses `cargo tarpaulin` (`just coverage`). Run it during
  larger refactors or before landing riskier changes to confirm we are
  exercising new execution paths.

## Reliability & Observability

- Structured error handling (`sinex_core::types::error::SinexError`) keeps
  failure modes explicit. When adding new domains, extend the error enums rather
  than falling back to opaque strings.
- `IntegrityTester` and related utilities (`doc/database_pool.md`) provide
  repeatable data checks—use them whenever schema or ingestion logic changes.
- Tracing/metrics emit through `sinex-telemetry`; enable `RUST_LOG` and Grafana
  dashboards when diagnosing timing or ordering issues in tests.

## Developer Workflow at a Glance

- `just dev` – fast loop (fmt, clippy, `nextest --profile fast`).
- `just test` / `just test-reliable` – full suites tuned for speed vs. retries.
- `just pre-commit` – gate equivalent to CI (`fmt`, `clippy`, `nextest`,
  `sqlx-prepare`).
- `nix develop` – reproducible environment with TimescaleDB, NATS, and compiler
  toolchains pinned.

## Current Focus Areas

- Continue migrating legacy tests away from ad-hoc SQL toward the shared query
  builders exposed by `sinex-core`. This removes duplication and keeps SQLx
  metadata accurate.
- Consolidate older helper macros and event builders into the modern
  `TestContext` & fixture APIs so new contributors have a single obvious path.
- Monitor flaky suites with the `reliable` profile; promote fixes upstream once
  the repeated retries stop catching regressions.
