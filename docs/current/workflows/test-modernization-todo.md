# Test Modernization TODO

Scope: align tests with current policy (`#[sinex_test]` default + external `tests/` first).

## A) Migrate viable `#[test]` / `#[tokio::test]` to `#[sinex_test]`

- [x] Inventory raw test attributes across workspace (`rg -n "#\[(test|tokio::test)\]" crate xtask`).
- [x] Convert regular runtime tests to `#[sinex_test]` in small batches.
- [ ] Remove ad-hoc runtime/bootstrap code replaced by sandbox helpers.
- [x] Keep allowlisted raw tests unchanged: `trybuild`/compile-fail and proc-macro-internal tests.
- [x] Add a one-line exception note for any remaining raw test outside allowlist.

## B) Move viable inline `#[cfg(test)]` modules to per-crate `tests/`

- [x] Inventory inline test modules (`rg -n "#\[cfg\(test\)\]" crate`).
- [x] Move viable integration/behavior tests to per-crate `tests/*_test.rs`.
- [ ] Keep inline tests only as explicit exceptions for small internal behavior where extraction would force undesirable visibility changes.
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
- [ ] Remaining inline `#[cfg(test)]` modules are mostly internal/private-unit tests; only move when extraction does not force broad visibility changes.

## Exception Criteria

- Raw `#[test]` / `#[tokio::test]` is allowed only for:
  - `trybuild`/compile-fail harnesses
  - proc-macro-internal tests that cannot use sandbox runtime
- Inline `#[cfg(test)]` is allowed only as an exception for small internal tests when `tests/` extraction would force broader visibility.
- Any exception should include a short comment explaining why policy does not apply.
