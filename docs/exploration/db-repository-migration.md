# DB Repository / PL/pgSQL Migration Plan

This document tracks the remaining ad-hoc SQL entry points we still need to push under the
repository layer (or into Postgres-side helpers) plus the concrete shape of each change.

## Cascade analyzer orchestration (`crate/core/sinex-gateway/src/cascade_analyzer.rs`)

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

## Follow-up list

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
