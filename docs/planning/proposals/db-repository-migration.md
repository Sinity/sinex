# DB Repository / PL/pgSQL Migration Plan

This document tracks the remaining ad-hoc SQL entry points we still need to push under the
repository layer (or into Postgres-side helpers) plus the concrete shape of each change.

## 1. JetStream consumer batch insert (`crate/core/sinex-ingestd/src/jetstream_consumer.rs:430-488`)

### Current state

```rust
let rows = sqlx::query(
    r#"
        INSERT INTO core.events (id, source, event_type, ts_orig, host, payload)
        SELECT CAST(id AS ULID), source, event_type, ts_orig, host, payload
        FROM UNNEST($1::uuid[], $2::text[], $3::text[], $4::timestamptz[], $5::text[], $6::jsonb[])
        AS t(id, source, event_type, ts_orig, host, payload)
        ON CONFLICT (id) DO NOTHING
        RETURNING id as "id: Ulid"
    "#
)
    .bind(&ids)
    .bind(&sources)
    .bind(&event_types)
    .bind(&ts_origs)
    .bind(&hosts)
    .bind(&payloads)
    .fetch_all(&self.pool)
    .await?;
```

The consumer re-implements batching, conversion to UUID/JSON, and conflict handling even though the
rest of the codebase goes through `EventRepository`.

### Status / changes

- Added `scope_window tstzrange` column to `core.operations_log` plus optional parameter on `core.start_operation`.
- `StateRepository::start_replay_operation` now converts `ReplayScope::time_window` into a `tstzrange`, so every replay operation records its window with native Postgres range semantics.
- Existing readers continue to work (column is nullable) while we plumb richer accessors later.

### Proposed change (remaining)

- Add `EventRepository::insert_stream_batch(batch: &[PreparedEventRow]) -> DbResult<Vec<Ulid>>`.
  - The repository will own the UNNEST SQL (or switch to `INSERT ... SELECT FROM UNNEST`) and be the
    only place aware of the `core.events` column list.
  - `PreparedEventRow` should be a lightweight DTO (ids, source, event_type, ts_orig, host, payload)
    to avoid pulling in the entire `PreparedEvent` type tree into sinex-core.
  - The repository handles `ON CONFLICT DO NOTHING` and returns the persisted ULIDs.
- `persist_batch_optimized` becomes:

```rust
let persisted = self.pool
    .events()
    .insert_stream_batch(batch)
    .await
    .map_err(|e| SinexError::database(e.to_string()))?;
```

### Testing

- Reuse the existing consumer integration tests; add a focused unit test under `sinex-core` that
  feeds a couple of DTOs into the repository method to ensure UUID ↔ ULID conversion still works.

## 2. Material assembler ledger writes (`crate/core/sinex-ingestd/src/material_assembler.rs:620-650`)

### Current state

`MaterialAssembler::record_ledger_entry` issues raw SQL against `raw.temporal_ledger`:

```rust
sqlx::query!(
    r#"
        INSERT INTO raw.temporal_ledger
            (source_material_id, offset_start, offset_end, offset_kind, ts_capture, precision, clock, source_type)
        VALUES (($1::uuid)::ulid, $2, $3, $4, $5, $6, $7, $8)
    "#,
    ulid_to_uuid(state.material_id),
    0_i64,
    state.expected_offset,
    "byte",
    state.started_at,
    "bounded",
    "wall",
    state.material_kind
)
.execute(&self.pool)
.await?;
```

### Proposed change

- Extend `SourceMaterialRepository` (or introduce a narrow `LedgerRepository`) with
  `append_temporal_ledger(entry: LedgerEntry)`.
  - `LedgerEntry` carries the ULID, offsets, capture metadata, and source type (enforced via enums).
  - The repository handles ULID ↔ UUID conversion and the constant strings (`byte`, `bounded`, `wall`)
    live next to the schema definition.
- `record_ledger_entry` simply builds the DTO and calls the repository.

### Testing

- Move the existing assertions from `material_assembler` tests into a repository test so that we
  exercise the SQL in isolation.

## 3. Cascade analyzer orchestration (`crate/core/sinex-gateway/src/cascade_analyzer.rs`)

### Pain points

- Rust code currently assembles dynamic strings for:
  - Creating/Dropping session temp tables (`CREATE TEMP TABLE ...`, `DROP TABLE ...`).
  - Populating seeds and walking dependencies.
  - Calculating histograms and integrity checks (multiple SELECT statements built via `format!`).
- The module repeats `quote_ident` gymnastics everywhere and re-runs the same SQL across both the
  transactional and non-transactional paths.

### Status / changes

- Introduced PL/pgSQL helpers: `core.prepare_cascade_session`, `core.cascade_populate_roots`, `core.cascade_depth_histogram`, `core.cascade_count_nodes`, `core.cascade_find_integrity_violations`, and `core.cleanup_cascade_session`.
- Added `CascadeRepository` (+ tx variant) in `sinex-core`, and rewired `StreamingCascadeAnalyzer` to call those functions instead of emitting raw SQL / temp-table DDL.
- Temp-table creation, population, histogram/count queries, integrity checks, and cleanup now live fully inside Postgres.

### Remaining follow-up

- Move the session management + graph walk entirely into PL/pgSQL living in `core` schema. The
  working theory is one or two cohesive functions rather than four micro helpers:
  - Option A (preferred pending spike): a single `core.cascade_analyze(session_id text, seed_ids ulid[], max_depth integer)` that prepares the temp table, runs the traversal, emits depth histogram + cycle info, and returns the derived table name for any follow-up inspection.
  - Option B: two functions (`prepare_session` and `analyze_session`) if we prove that splitting seed preparation from heavy analysis materially simplifies error handling.
- Whatever interface we choose will be exposed via methods on `EventRepository` (e.g.,
  `cascade_prepare`, `cascade_run`, `cascade_cleanup`) rather than a brand new repository. This keeps
  graph analysis co-located with other event-topology queries.
- Rust no longer formats identifiers or SQL snippets; it calls repository methods that in turn invoke
  the PL/pgSQL function with simple bind parameters.

### Testing

- Unit-test the PL/pgSQL functions with fixtures (see existing gateway tests) and add repository
  tests under `sinex-core` that exercise a full session using temp schemas.

## 4. Replay helpers (`crate/core/sinex-gateway/src/replay_control.rs:518-529`)

### Current state

`wait_for_operation` polls `core.operations_log` directly:

```rust
let exists = sqlx::query_scalar!(
    "SELECT 1 FROM core.operations_log WHERE id::uuid = $1::uuid",
    uuid
)
.fetch_optional(pool)
.await?;
```

### Status / changes

- `StateRepository` now stores replay windows via `scope_window`, so downstream consumers can query range coverage.
- Cascade helper functions expose integrity/census queries via repositories; alias-heavy ad-hoc SQL was replaced with stored procedures and strongly-typed repository methods.

### Proposed change (next)

- Extend `StateRepository` with `operation_exists(id: Ulid) -> DbResult<bool>` and optionally
  `load_operation(id: Ulid) -> DbResult<Option<OperationRecord>>`.
- Replay tests (`drive_to_state`, `wait_for_operation`) call the repository instead of issuing raw
  SQL; production code can reuse the same helpers when we add telemetry.

### Testing

- `state.rs` already has coverage for recent operations; add another case that verifies
  `operation_exists` toggles the right value as we insert/delete records.

## Follow-up list (not tackled in this pass)

1. **Schema registry cache** – `ingestd` and `types::events::schema` each maintain their own SQL to
   read from `core.payload_schemas`. We should funnel both through a dedicated
   `SchemaCacheRepository` so eviction/refresh logic is centralized.
2. **Analytics/content pagination** – after the pagination helper propagates, sweep analytics
   services + CLI tooling to ensure they consume the shared types instead of custom limit/offset
   math.
3. **Checkpoint and replay tests** – migrate the remaining inline SQL in tests (checkpoint
   consistency, replay helpers) to repository calls once the new helpers are in place, so tests cover
   the same API surface as production.

Please treat this document as the authoritative checklist until each item lands; update it (or link a
tracking issue) as we knock down the remaining ad-hoc SQL call sites.

## Replay overlap policy

- Recording `scope_window` in `core.operations_log` adds observability but does **not** block users from launching overlapping replays.
- Any enforcement (exclusion constraints, conflict detection) must be explicitly requested by product/ops. Until then, the repository simply persists the range for analytics/debugging.
