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
  apply does NOT converge:
  - **Trigger function bodies** — silently overwritten on next apply.
  - **Column DEFAULT expressions** on existing columns —
    `ADD COLUMN IF NOT EXISTS` is a no-op for those.
  - **Inline CHECK expressions** — anonymous `CHECK (...)` clauses
    declared via sea-query column statements; convergence has no
    name handle to reconcile against.
  - **Foreign key ON DELETE / ON UPDATE actions** — FK exists by
    name, action change is not applied.
  - **TimescaleDB hypertable settings** — chunk interval, retention
    policy presence/absence.

  Reserved-but-not-yet-implemented: comments / table descriptions
  (issue #556 explicitly lists this as a non-goal).

  Each category is opinionated: detection compares declared
  marker substrings against `pg_get_constraintdef`,
  `pg_get_expr`, `pg_proc.prosrc`, or `_timescaledb_catalog.dimension`
  values rather than full-text equality, since Postgres normalizes
  expression and function-body storage in ways that do not
  round-trip the source SQL verbatim.

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
