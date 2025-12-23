# Test Harness: DB Pooling (Template + Per-test Databases)

This document captures the agreed direction and execution plan for improving Sinex’s test harness,
specifically around:

- PostgreSQL template + pooled per-test databases (`sinex-test-utils`)

It keeps historical context (including earlier “Tetris scheduling” discussions), but the current
design goal is simplicity: run nextest with high global parallelism and rely on the DB pool +
connection budgeting to keep PostgreSQL stable. If we ever see real infra collapse under load,
we can reintroduce test-group caps later.

It is intentionally detailed so the rationale doesn’t get lost in chat history.

## Sources (historical discussion)

Primary session log (Codex CLI JSONL):

- `~/.codex/sessions/2025/12/12/rollout-2025-12-12T01-53-33-019b100c-7f40-73e3-9e95-7bf96e3cb7f3.jsonl`
  - See especially around `:1780` (pool + parallelism discussion and the “clean-after-use” direction).

Follow-up sessions (implementation + debugging):

- `~/.codex/sessions/2025/12/18/rollout-2025-12-18T21-56-24-019b333f-e4f5-7501-9d8b-94eba7df30bf.jsonl`

## Status (what’s implemented)

- Phase 0 (template/pool provisioning correctness): implemented.
- Phase 1 (nextest test groups / “Tetris scheduling”): previously experimented with, but intentionally removed/disabled (simplicity first).
- Phase 2 (clean-after-use): implemented behind `SINEX_TESTUTILS_CLEAN_AFTER_USE=1` (benchmark-only knob for now).
- Follow-ups (2025-12-19): implemented (see “Recent fixes” below).

## Current reality (as of now)

### How template DB works

- There is a single shared template database: `sinex_test_template_shared`.
- `sinex-test-utils` attempts to keep the template:
  - migrated to current schema
  - “clone-safe” (no lingering connections / `ALLOW_CONNECTIONS false`)
  - tagged with metadata (fingerprint + extension versions) in `COMMENT ON DATABASE …`.
- A *migrations fingerprint* is computed from `crate/lib/sinex-schema/src/migrations/*` contents
  (deterministic ordering), and stored in template metadata. When it changes, the template is rebuilt.
- Cross-process coordination is handled with PostgreSQL advisory locks:
  - shared lock held while cloning from the template
  - exclusive lock held while recreating the template

### How pool DBs work

- The pool consists of databases `sinex_test_pool_0..N-1` (size controlled by `SINEX_TESTUTILS_POOL_SIZE`,
  clamped by an overall connection budget).
- Under nextest, pool databases are *lazily provisioned*:
  - a test process will create a missing pool DB by cloning from the template on first acquire.
- Each test obtains exclusive use of a pool DB via an advisory lock held for the duration of the test.
- Default: the pool DB is cleaned **on acquisition** (truncate/reset) rather than on release.
- Optional (benchmark knob): with `SINEX_TESTUTILS_CLEAN_AFTER_USE=1`, the slot is marked dirty on acquire and
  cleaned on release (before unlocking) so the next test can skip cleanup if the DB is known-clean.

### What this implies (and why it feels bad)

- “Pool size > parallel tests” does not help much if we always clean on acquire:
  - the next test does cleanup work on the critical path before it can start assertions.
- Tests that don’t need DB/NATS still frequently pay for DB setup because the default macro path
  constructs a `TestContext` (which acquires a DB).
- Nextest profiles default to `test-threads = "num-cpus"`, so parallelism is mostly gated by
  external resources (DB pool size, Postgres connections, NATS/JetStream behavior, and timeouts).

## Problems observed / motivating failures

1) **Template-related flakes / races**

- A pool clone can fail with: `template database "sinex_test_template_shared" does not exist`.
- This can happen if a process drops/recreates the template while another process is trying to clone,
  or if a previous process died mid-rebuild and left the system in a “template missing” state.
- The fix is to make pool provisioning *robust*: always “ensure template exists + lock it” immediately
  before cloning, and retry if the template is missing.

2) **Parallelism is underutilized**

- Even on high-core machines, reliable profiles often cap threads too low.
- Result: DB-pool machinery exists, but tests do not actually run with meaningful concurrency.

3) **(Historical) “Tetris scheduling”**

We previously discussed a nextest test-group cap for DB+NATS-heavy tests (“cap the heavy lane,
not the whole highway”). We are currently *not* doing this: it added complexity and “shadow”
mechanisms without clear need once the DB pool became robust.

## Recent fixes (2025-12-19)

These were discovered while benchmarking and running the CI harness on a machine where
Postgres extensions are provided via Nix (versioned shared objects).

### A) Stale pool DBs after TimescaleDB upgrade

Symptom:

- Tests fail early with `could not access file "$libdir/timescaledb-<old-version>"`.
- Root cause: old pool databases were cloned when TimescaleDB was at `<old-version>`, and keep
  referencing the old versioned `.so` filename; after upgrading TimescaleDB, that file no longer exists.

Fix (implemented):

- Add a fast liveness check in `DatabasePool::acquire` (`SELECT 1`) and treat the missing-library
  error as recoverable: drop + recreate the pool DB from the current template.
- Add the same recovery path for connect failures, session preflight failures, and advisory-lock
  query failures.
- Treat the missing-library error as “retryable” in `clean_database` so cleanup can self-heal too.

### B) `xtask ci postgres` lifetime correctness

Symptom:

- Postgres would sometimes shut down immediately after “server started”, causing downstream commands
  to fail.

Fix (implemented):

- Ensure the `PgInstance` guard in `xtask ci postgres` remains alive until after the wrapped command
  exits (avoid early drop under NLL).

### C) Make Postgres selection deterministic in devenv

Fix (implemented):

- `devenv.nix` now exports `SINEX_PG_BIN=${postgresForSqlx}/bin` so `cargo xtask ci postgres` uses the
  pinned Postgres build that includes required extensions (TimescaleDB, pg_jsonschema, pgvector, …),
  regardless of host PATH ordering.

## Target design (agreed direction)

### A) Nextest parallelism (current approach)

We run with high global parallelism (`test-threads = "num-cpus"`) and tune:

- `SINEX_TESTUTILS_POOL_SIZE` / connection budgets
- per-test timeouts (only when justified by real load, not as a band-aid)

If we later observe credible “shared infra collapse” (NATS/JetStream/DB timeouts that go away
with fewer concurrent integration tests), we can reintroduce nextest test groups at that point.

### B) Pool lifecycle: move toward clean-after-use (not clean-on-acquire)

Change semantics so that:

- A DB slot is marked “dirty” immediately on acquisition (crash-safe).
- On test completion, the slot is cleaned **before** the advisory lock is released.
- Only after cleanup succeeds do we mark the slot “clean” and make it available for reuse.

Why this matters:

- Cleanup work moves off the next test’s critical path.
- Pool size > test concurrency becomes beneficial:
  - while one slot is being cleaned, other tests can run on other clean slots.

Crash-safety requirement:

- If a test process crashes, “dirty” must persist so the next acquirer performs cleanup.
- Persist minimal slot state via a Postgres-side metadata store (e.g. `COMMENT ON DATABASE` JSON),
  similar to template meta.

Operational rules:

- If cleanup fails or verification shows residual data, quarantine the slot and recreate it from the template.
- Keep advisory lock held until slot is either clean or quarantined/recreated, to avoid concurrent users.

### C) Make DB/NATS optional for tests that don’t need them

This was considered but intentionally dropped: it adds macro surface area and test “modes” that
we don’t want to maintain. The current direction is to keep the harness simple and instead tune
throughput via pool sizing, connection budgets, and correctness fixes (no hidden scheduling).

### D) Performance / tuning loop

We will not guess; we will tune based on measurement:

- Raise concurrency locally (`NEXTEST_TEST_THREADS`, pool size, connection budgets).
- Observe failure modes:
  - “too many clients” → adjust Postgres `max_connections` and harness connection budgets.
  - timeouts under load → increase per-test timeout for known heavy tests, or lower heavy-group cap.
  - logic races → fix the code (preferred outcome).

## Execution plan (phased)

## Benchmarking (how to measure)

Use `scripts/bench-nextest.sh` to compare throughput settings without editing repo config:

- `scripts/bench-nextest.sh --help`
- Example (ingestd-focused): `RUNS=3 BENCH_TARGET=ingestd BENCH_MODE=refine THREADS_LIST="8 16 24" POOL_SIZES="8 16 24" scripts/bench-nextest.sh`
- Example (workspace, quick smoke): `RUNS=1 BENCH_TARGET=workspace BENCH_MODE=sweeps scripts/bench-nextest.sh`

The script records a dedicated compile log/duration (`--no-run`) and then run-only timings per
combo, plus per-run `summary.txt` for slowest tests.

### Phase 0 — Make template/pool provisioning correct

- Ensure pool provisioning always calls `ensure_template_database` (and holds the shared advisory lock)
  immediately before cloning from the template.
- If cloning fails with “template does not exist”, force a template ensure/rebuild and retry once.

Success criteria:

- `cargo xtask ci postgres -- cargo xtask ci workspace` is green reliably.

### Phase 1 — (Deferred) Nextest test groups (“Tetris scheduling”)

We intentionally removed the test-group cap mechanism for now. If we later observe repeatable
“shared infra collapse” under high parallelism (and it’s not fixable via pool sizing/connection
budgets), we can reintroduce nextest test groups at that point.

### Phase 2 — Switch pool to clean-after-use (gated rollout)

- Implement “dirty/clean” metadata per pool database (persisted in Postgres).
- Change release path to clean-before-unlock.
- Add an env flag for controlled rollout (e.g. `SINEX_TESTUTILS_CLEAN_AFTER_USE=1`).

Success criteria:

- No cross-test contamination.
- Wall time improves for multi-test runs where pool size > heavy concurrency.

## Notes on “how far to go”

- The goal is maximum throughput without hiding real race bugs.
- We currently run without nextest test-group caps; if shared infra becomes the bottleneck under high parallelism, reintroduce caps then.
- If the harness needs to scale beyond a single local Postgres, we can later introduce a dedicated
  test Postgres service (or multiple clusters), but that is not required for the current phase.
