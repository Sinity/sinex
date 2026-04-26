# Declarative Apply Notes

This crate converges database state through declarative `apply()` execution.

## Current Model

- IDs are native `UUID`.
- `core.events` is partitioned by `id` using `uuid_extract_timestamp(id)`.
- `core.events.ts_coided` is the UUID-derived ingest timestamp.
- `core.events.ts_persisted` is the persisted-at timestamp (`DEFAULT now()`).
- identifier handling and queries stay UUID-native.

## Operational Notes

- current-state convergence is idempotent; legacy migration-chain branching is removed
- telemetry views/materialized views in `sinex_telemetry` are created by schema apply SQL
- validate schema readiness through repository tooling before deploy

## Drift Detection

Two routines, with overlapping but distinct coverage:

- `apply::diff` reports missing tables, columns, named constraints,
  indexes, triggers, views, and continuous aggregates. This is what a
  CI gate or a "did apply do anything?" check uses.
- `strict_diff::check_strict` extends `diff` with categories that
  apply does NOT converge: trigger function bodies (silently
  overwritten on next apply), column DEFAULT expressions on existing
  columns (`ADD COLUMN IF NOT EXISTS` is a no-op for those), and
  reserved slots for FK actions, inline CHECKs, and hypertable
  settings. Issue #556 tracks the remaining categories.

Operator-facing CLI:

```bash
DATABASE_URL=postgres://... schema-strict-diff
```

Outputs JSON; exit code 0 on no drift, 1 on drift detected, 2 on
operator/connection errors.

## Verification

```bash
xtask contracts check-ready
```
