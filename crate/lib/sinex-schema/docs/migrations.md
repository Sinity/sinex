# Migrations Analysis

## Executive Summary

- **Total Migrations**: 17
- **Strategy**: Initial canonical schema (v7.0 squash) followed by incremental updates.
- **Safety**: All migrations implement `down()`, but some (001, 008) are destructive.

## Critical Migrations

### `m20241028_000001_create_canonical_schema.rs`s`
- **Purpose**: Creates the entire v7.0 schema (core, raw, audit).
- **Risk**: Destructive rollback (drops all data). Protected by `SINEX_ALLOW_SCHEMA_DOWN`.

##`m20250117_000008_add_retention_policy.rs`.rs`
- **Purpose**: Enforces 90-day retention on `core.events`.
- **Risk**: **Permanent Data Loss**. Data older than 90 days is deleted by background jobs.

`m20250121_000013_fix_partitioning.rs`ng.rs`
- **Purpose**: Fixes partition function volatilit`TimescaleDB`scaleDB`.
- **Change**: `ulid_to_timestamptz` now uses explicit UTC timezone to be IMMUTABLE.

## Known Issues

- **BUG-018**: Embedding dimensions hardcoded t`OpenAI` (`OpenAI`). See `schema/embeddings.rs`.
- **Migration Dates**: Some 2026 migrations likely meant 2025.

## Verification

To verify schema state:
```bash
cargo xtask schema check-ready
```