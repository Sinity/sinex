# Migrations Notes

This crate uses a canonical base migration plus incremental migrations.

## Current Model

- IDs are native `UUID`.
- `core.events` is partitioned by `id` using `uuid_extract_timestamp(id)`.
- identifier handling and queries stay UUID-native.

## Operational Notes

- some `down()` paths are destructive for canonical schema migrations
- telemetry views/materialized views in `sinex_telemetry` are created by schema apply SQL
- validate migration readiness through repository tooling before deploy

## Verification

```bash
xtask schema check-ready
```
