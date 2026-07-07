---
created: "2026-05-25T12:42:00Z"
purpose: "Implementation plan for feature/devloop-next-12 — finish #1393"
status: "active"
project: "sinex"
---

# Devloop Next-12 Implementation Plan

## Baseline (complete)

- test -p xtask: PASSED, 24.96s (job 638)
- check -p xtask: PASSED (job 639)
- Freshness: "miss" (no prior proof on clean branch)
- Impact audit: workspace reuse_exact_proof (clean tree)
- Slowest tests: DB pool tests at 28-37s (targets for Slice D)
- Prior fix job 632: FAILED after ~4min

## Slice A: Proof Coverage Report

### Current state
- dry-run JSON already has `reuse.eligible`, `reuse.hit`, `reuse.reason` (test.rs:1396-1417)
- Human dry-run shows `reuse eligibility: <state>` (test.rs:1380-1391)
- No per-package or per-scope proof classification taxonomy

### Changes
1. test.rs: Add `ProofCoverage` enum + reporting in dry-run
2. test.rs: Extend dry-run JSON with per-package coverage array
3. tests: Add coverage classification tests

### States
- covered: exact proof exists and is reusable
- missing: eligible but no proof in DB
- stale: proof exists but fingerprint changed (need DB query for non-matching)
- ineligible: shape cannot reuse (runtime, listing, mutating, heavy)

## Slice B: Default Gate Subtraction

### Current state
- `subtract_reusable_impact_package_proofs` works when effective_filter is None (test.rs:1559-1564)
- Gaps: no subtraction for explicit -p packages; no subtraction when no impact plan

### Changes
1. test.rs: Extend subtraction to explicit -p packages
2. test.rs: Ensure --no-reuse gate works for all paths
3. tests: Verify subtraction edge cases

## Slice C: Fix Wallclock Reduction

### Current state
- fix.rs: Flag set: --packages, --all, --thorough, --smart
- Flow: preflight → fmt → cargo fix → clippy --fix (or thorough iteration)
- Slow: cargo fix + clippy --fix both compile

### Changes
1. fix.rs: Add --fmt-only flag
2. fix.rs: Make default fix narrower (fmt only, clippy-fix opt-in)
3. fix.rs: Better error output for broken clippy-fix
4. docs: Update command guide if surface changes

## Slice D: Speed Up DB Pool Tests

### Targets
- retry_deferred_stale_slots_repairs_schema_drifted_slot: 37.4s
- test_eagerly_recreate_pruned_lazy_slot_databases_repairs_drifted_slot: 37.4s
- test_try_lock_slot_database_for_drop_defers_transiently_unavailable_database: 28.2s
- test_prune_stale_lazy_slot_databases_skips_transiently_unavailable_clean_slot: 26.7s

### Root cause
- Tests create real PostgreSQL databases from template
- Connect with retry, wait for DB absence with polling
- Schema modification + verification requires full DB lifecycle

### Approach
- Reduce lock timeouts via env/config override
- Use existing template fixtures
- Make probe responses injectable
- No fixed sleeps
