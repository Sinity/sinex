# Test Modernization TODO

Scope: align tests with current policy (`#[sinex_test]` default + external `tests/` first).

## A) Migrate viable `#[test]` / `#[tokio::test]` to `#[sinex_test]`

- [x] Inventory raw test attributes across workspace (`rg -n "#\[(test|tokio::test)\]" crate xtask`).
- [x] Convert regular runtime tests to `#[sinex_test]` in small batches.
- [x] Remove ad-hoc runtime/bootstrap code replaced by sandbox helpers.
- [x] Keep allowlisted raw tests unchanged: `trybuild`/compile-fail and proc-macro-internal tests.
- [x] Add a one-line exception note for any remaining raw test outside allowlist.

## B) Move viable inline `#[cfg(test)]` modules to per-crate `tests/`

- [x] Inventory inline test modules (`rg -n "#\[cfg\(test\)\]" crate`).
- [x] Move viable integration/behavior tests to per-crate `tests/*_test.rs`.
- [x] Keep inline tests only as explicit exceptions for small internal behavior where extraction would force undesirable visibility changes.
- [x] Remove duplicated helper setup after extraction to `tests/`.
- [x] Verify test names and module paths remain clear after moves.

## Rollout Notes

- Land changes crate-by-crate; avoid repo-wide mechanical rewrites in one PR.
- Run targeted test commands per crate after each batch.
- Prioritize crates with highest counts of raw attributes and inline modules first.

## Progress (Current Batch)

- [x] Re-checked current baseline (as of 2026-03-05):
  - `#[cfg(test)]`: 163 occurrences across 128 files (113 under `src/`).
  - Raw `#[test]`/`#[tokio::test]`: only allowlisted locations remain.
  - Inline `src` test modules importing `use super::...`: 111/113 files.
- [x] Moved `audit` handler tests from inline module to `crate/core/sinex-gateway/tests/audit_handlers_test.rs`.
- [x] Migrated a first batch of raw `#[test]` unit tests to `#[sinex_test]` in:
  - `xtask/src/commands/verify.rs`
  - `crate/lib/sinex-primitives/src/privacy/envelope.rs`
  - `crate/lib/sinex-primitives/src/privacy/catalog.rs`
  - `crate/lib/sinex-db/src/postgres_copy.rs`
- [x] Migrated remaining non-allowlisted raw tests in:
  - `crate/lib/sinex-primitives/src/privacy/engine.rs`
  - `crate/lib/sinex-primitives/src/privacy/detector.rs`
  - `crate/lib/sinex-primitives/src/privacy/config.rs`
  - `crate/lib/sinex-primitives/tests/subscription_filter_test.rs`
- [x] Raw test attributes now remain only in allowlisted cases:
  - `crate/lib/sinex-macros/tests/compile_fail_test.rs` (`trybuild`)
  - `xtask/macros/src/lib.rs` (proc-macro-generated attribute text/tests)
- [x] Moved additional viable inline handler modules to per-crate tests:
  - `ops.rs` → `crate/core/sinex-gateway/tests/ops_handlers_test.rs`
  - `nodes.rs` → `crate/core/sinex-gateway/tests/nodes_handlers_test.rs`
  - `dlq.rs` → `crate/core/sinex-gateway/tests/dlq_handlers_test.rs` (merged into existing suite)
  - `shadow.rs` → `crate/core/sinex-gateway/tests/shadow_handlers_test.rs`
- [x] Moved `VersionInfo` build-stamp check out of inline `src/lib.rs` to `tests/version.rs` (`sinex-node-sdk`).
- [x] Removed empty placeholder inline test modules in `xtask/src/deps/reports.rs` and `xtask/src/deps/mod.rs`.
- [x] Consolidated extracted gateway handler test setup under shared helper module:
  - `crate/core/sinex-gateway/tests/common/mod.rs` (`NATS + env + auth + stream bootstrap`)
  - removed duplicated per-file auth/NATS bootstrap in moved test files
- [x] Normalized gateway test naming convention to `*_test.rs` (no `*_inline_test.rs` or plural `*_tests.rs` suffixes).
- [x] A.3 runtime/bootstrap cleanup completed in this batch:
  - migrated remaining viable direct `EphemeralNats::start()` callsites to context-managed helpers in:
    - `crate/lib/sinex-node-sdk/tests/material_acquisition.rs`
    - `crate/lib/sinex-node-sdk/tests/integration/checkpoint_performance_test.rs`
    - `crate/lib/sinex-node-sdk/src/coordination.rs`
    - `xtask/src/sandbox/node_runtime.rs`
    - `crate/core/sinex-ingestd/tests/*` and selected node/gateway test files already moved earlier in this track
  - reduced benchmark setup duplication in:
    - `tests/e2e/tests/resource_exhaustion_test.rs` (single shared setup helper)
  - additionally migrated gateway harness + distributed limiter suites to context-managed dedicated NATS:
    - `crate/core/sinex-gateway/tests/common/mod.rs`
    - `crate/core/sinex-gateway/tests/nodes_handlers_test.rs`
    - `crate/core/sinex-gateway/tests/shadow_handlers_test.rs`
    - `crate/core/sinex-gateway/tests/dlq_handlers_test.rs`
    - `crate/core/sinex-gateway/tests/distributed_rate_limit_test.rs`
  - remaining direct start:
    - `tests/e2e/tests/resource_exhaustion_test.rs` (bench macro path; no `ctx` arg support yet)
- [x] Remaining inline `#[cfg(test)]` modules are accepted exception cases under current policy; move only when extraction does not force broad visibility changes.
- [x] Strengthened replay mechanics data-plane assertions in
  `crate/core/sinex-gateway/src/replay_control.rs` (`replay_execution_records_outcome`):
  - preview/execute filter parity for scoped replay targets
  - archive/live movement checks for matched vs non-matched events
  - checkpoint cardinality assertions (`processed_events`, `total_events`)

## Exception Criteria

- Raw `#[test]` / `#[tokio::test]` is allowed only for:
  - `trybuild`/compile-fail harnesses
  - proc-macro-internal tests that cannot use sandbox runtime
- Inline `#[cfg(test)]` is allowed only as an exception for small internal tests when `tests/` extraction would force broader visibility.
- Any exception should include a short comment explaining why policy does not apply.

## C) Critical Path Invariant Coverage (new quality gate)

Goal: eliminate tests that only validate command flow / serialization while skipping
stateful side-effects on behavior-critical paths.

- [x] Replay lifecycle tests assert data-plane effects, not only terminal state strings.
  - required checks: archived/live row movement, cascade behavior, replay payload provenance fields, and fresh replay IDs.
- [x] Replay state-machine tests include persistence-backed transition checks (not enum-only tables).
- [x] Replay preview/execute parity tests verify the same scope filters drive both phases.
- [x] Ops handlers tests assert repository-side state changes (`core.operations_log` rows), not only RPC response shape.
- [x] Token rotation tests assert runtime auth behavior after file mutation (old token rejected, new token accepted) without restart.
- [x] Node registry summary tests assert exact active/inactive partitioning and stale-threshold boundary behavior.
- [x] Lifecycle/watcher state tests assert real task teardown/idempotency under concurrent control operations.

Evidence (key references):
- Replay lifecycle data-plane invariants: `crate/core/sinex-gateway/tests/replay_lifecycle_test.rs`, `crate/core/sinex-gateway/src/replay_control.rs` (`replay_execution_records_outcome`).
- Replay state-machine persistence transitions: `crate/core/sinex-gateway/tests/replay_state_machine_test.rs`.
- Preview/execute filter parity: `ReplayScope::normalized_filters()` in `crate/lib/sinex-db/src/replay/state_machine.rs` and execute path usage in `crate/core/sinex-gateway/src/replay_control.rs`.
- Ops persistence assertions: `crate/core/sinex-gateway/tests/ops_handlers_test.rs`.
- Runtime token reload auth check: `crate/core/sinex-gateway/src/rpc_server.rs` (`gateway_auth_reloads_token_file_without_restart`).
- Node registry active/inactive/stale-threshold checks: `crate/core/sinex-gateway/tests/node_registry_handlers_test.rs`.
- Teardown/idempotency/concurrency lifecycle checks: `crate/lib/sinex-node-sdk/tests/node_shutdown_leak_test.rs`, `crate/lib/sinex-node-sdk/tests/lifecycle_manager_tests.rs`, `crate/lib/sinex-node-sdk/tests/watcher_handle_inline_test.rs`.

### Invariant-first test rubric

For critical workflows, each test should include all three layers:

1. Control-plane: request/response/transition signals.
2. Data-plane: concrete persisted side effects (DB rows, archives, emitted stream data).
3. Safety property: what must never happen (e.g. old-ID reuse, duplicate logical rows, skipped filters).
