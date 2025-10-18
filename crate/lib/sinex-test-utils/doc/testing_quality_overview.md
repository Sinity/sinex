# Testing, Quality Assurance, and Reliability Overview

This note complements the day-to-day guides by highlighting the cross-cutting
quality controls that keep the Sinex test ecosystem healthy. For detailed
instructions see:

- `docs/documentation-and-testing-playbook.md` – canonical testing strategy,
  suite layout, linting rules, and CI stages.
- `TESTING.md` – quick-start guide for writing and running tests with
  `TestContext`.
- `doc/overview.md` – API-level documentation for `sinex-test-utils`.

## Testing Architecture Snapshot

- Test categories (`unit`, `integration`, `system`, `property`, `adversarial`,
  `performance`, `security`, `concurrency`) are documented in the playbook
  (§5.1). Align new suites with that structure so ownership and runtime
  expectations stay clear.
- Nextest profiles (`default`, `fast`, `reliable`, `parallel`) live in
  `nextest.toml` and are summarised in the playbook (§5.2). Use `cargo nextest
  run --profile <name>` or the `just test-*` helpers to pick the right balance of
  speed vs. determinism.
- `TestContext` remains the single entry point for DB access, fixtures, timing
  utilities, and assertion helpers. See `doc/overview.md` for the full feature
  set and examples.

## Quality Controls

- `clippy.toml` enforces architectural invariants: no raw SQL (`sqlx::query`),
  structured errors instead of `anyhow`, async hygiene (`await_holding_lock`),
  and bounded complexity. Treat any new lint as a design conversation rather
  than an annoyance.
- CI (`.github/workflows/ci.yml`) mirrors the local workflow: formatting,
  clippy, `cargo nextest` against TimescaleDB, SQLx offline metadata checks, and
  optional coverage via `cargo llvm-cov`. Keep local runs (`just dev`, `just
  test`) green before pushing so CI stays boring.
- Coverage helpers (`just coverage-*`) rely on `cargo llvm-cov`. Use them during
  larger refactors to confirm we are exercising new execution paths.

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
