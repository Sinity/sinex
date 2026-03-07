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

## Verification

```bash
xtask contracts check-ready
```
