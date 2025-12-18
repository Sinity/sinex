# Test Harness: DB Pooling + Nextest “Tetris” Scheduling

This document captures the agreed direction and execution plan for improving Sinex’s test harness,
specifically around:

- PostgreSQL template + pooled per-test databases (`sinex-test-utils`)
- Nextest parallelism (“Tetris scheduling”: cap DB/NATS-heavy tests while letting everything else fan out)

It is intentionally detailed so the rationale doesn’t get lost in chat history.

## Sources (historical discussion)

Primary session log (Codex CLI JSONL):

- `~/.codex/sessions/2025/12/12/rollout-2025-12-12T01-53-33-019b100c-7f40-73e3-9e95-7bf96e3cb7f3.jsonl`
  - See especially around `:1780` (pool + parallelism discussion and the “clean-after-use” direction).

Follow-up sessions (implementation + debugging):

- `~/.codex/sessions/2025/12/18/rollout-2025-12-18T21-56-24-019b333f-e4f5-7501-9d8b-94eba7df30bf.jsonl`

## Status (what’s implemented)

- Phase 0 (template/pool provisioning correctness): implemented.
- Phase 1 (nextest test groups / “Tetris scheduling”): implemented.
- Phase 2 (clean-after-use): not implemented (still clean-on-acquire).
- Phase 3 (DB-optional tests): not implemented.

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
- Today, the pool DB is cleaned **on acquisition** (truncate/reset) rather than on release.

### What this implies (and why it feels bad)

- “Pool size > parallel tests” does not help much if we always clean on acquire:
  - the next test does cleanup work on the critical path before it can start assertions.
- Tests that don’t need DB/NATS still frequently pay for DB setup because the default macro path
  constructs a `TestContext` (which acquires a DB).
- Nextest parallelism is currently conservative in the default profiles, leaving cores idle.

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

3) **We need “Tetris scheduling”**

We want:

- High global test concurrency to use available CPU.
- A cap for DB+NATS-heavy tests (so shared infra doesn’t collapse into timeouts).
- Non-DB tests should still saturate remaining threads while heavy tests run.

In other words: cap the “heavy lane”, not the whole highway.

## Target design (agreed direction)

### A) Nextest “Tetris” scheduling via test groups

Implement nextest test groups and per-test overrides:

- Define a heavy group, e.g. `db-nats-heavy` with `max-threads = <small number>`.
- Run the overall profile with a high thread count (`test-threads = "num-cpus"`), and assign only
  known heavy tests into the capped group.
- Everything else uses the default group and can fan out.

Initial grouping strategy (start coarse, then refine):

- Put JetStream/NATS integration tests into `db-nats-heavy`.
  - Examples: `sinex-e2e-tests::jetstream_*`, `sinex-ingestd::*jetstream*`, and explicit “pipeline”
    and “recovery” tests.
- Keep config validation, auth parsing, request validation, and pure logic tests out of the heavy group.

Validation:

- Use `cargo nextest show-config test-groups --profile <profile>` to confirm grouping is applied.
- Iterate by moving tests between groups based on observed flake/timeouts and runtime.

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

Add a way to express intent:

- `#[sinex_test(no_db)]` (or a separate macro) for tests that don’t touch Postgres.
- `ctx.with_nats()` remains opt-in; keep it opt-in and make it cheap when unused.

Goal:

- Reduce unnecessary DB contention.
- Make “Tetris scheduling” more effective by shrinking the heavy set.

### D) Performance / tuning loop

We will not guess; we will tune based on measurement:

- Raise concurrency locally (`NEXTEST_TEST_THREADS`, pool size, connection budgets).
- Observe failure modes:
  - “too many clients” → adjust Postgres `max_connections` and harness connection budgets.
  - timeouts under load → increase per-test timeout for known heavy tests, or lower heavy-group cap.
  - logic races → fix the code (preferred outcome).

## Execution plan (phased)

### Phase 0 — Make template/pool provisioning correct

- Ensure pool provisioning always calls `ensure_template_database` (and holds the shared advisory lock)
  immediately before cloning from the template.
- If cloning fails with “template does not exist”, force a template ensure/rebuild and retry once.

Success criteria:

- `cargo xtask ci postgres -- cargo xtask ci workspace` is green reliably.

### Phase 1 — Add nextest test groups (“Tetris scheduling”)

- Update `.config/nextest.toml`:
  - Add `[test-groups]` definitions.
  - Add `[[profile.<name>.overrides]]` assigning known heavy tests into `db-nats-heavy`.
  - Raise `profile.fast.test-threads` to `"num-cpus"` (or a high cap) so non-heavy tests can saturate.
- Validate grouping with `cargo nextest show-config test-groups`.

Success criteria:

- Under `--profile fast`, we see high overall parallelism, but heavy tests stay capped.

### Phase 2 — Switch pool to clean-after-use (gated rollout)

- Implement “dirty/clean” metadata per pool database (persisted in Postgres).
- Change release path to clean-before-unlock.
- Add an env flag for controlled rollout (e.g. `SINEX_TESTUTILS_CLEAN_AFTER_USE=1`).

Success criteria:

- No cross-test contamination.
- Wall time improves for multi-test runs where pool size > heavy concurrency.

### Phase 3 — Make DB optional for non-DB tests

- Add macro/config support to skip DB acquisition entirely for tests that don’t need it.
- Update the obvious e2e config/auth tests to use the lighter mode.

Success criteria:

- E2E config/auth tests no longer trigger DB pool initialization.

## Notes on “how far to go”

- The goal is maximum throughput without hiding real race bugs.
- When heavy tests bottleneck on shared infra, we cap them *in nextest*, not by lowering global threads.
- If the harness needs to scale beyond a single local Postgres, we can later introduce a dedicated
  test Postgres service (or multiple clusters), but that is not required for the current phase.
