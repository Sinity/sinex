SeaQuery usage vs SQLx QueryBuilder
==================================

Current usage in repo
----------------------
- `crate/lib/sinex-core/src/db/repositories/events.rs::time_series_aggregate` (SeaQuery Query::select + PostgresQueryBuilder).
- `crate/lib/sinex-core/src/db/query_helpers.rs` (generic helpers used by migrations).
- `crate/lib/sinex-core/tests/seaquery_helpers.rs` + `tests/repositories_common.rs` (test scaffolding).
- `crate/lib/sinex-core/src/db/seaquery_helpers.rs` (ULID extension trait for SeaQuery expressions).

Pros noted so far
-----------------
- Schema-driven query assembly with named tables/columns reduces typo risk when joining many tables.
- Useful in migrations where we refer to `sinex_schema::schema::*` table defs and want compile-time table names.
- Built-in JSONB operators or custom functions via `Func::cust` make expressing some queries ergonomic.

Cons / issues encountered
-------------------------
- `Query::build(PostgresQueryBuilder)` returns `(String, Values)`; SQLx cannot consume `Values` directly, so we hand-bind parameters again in Rust, duplicating bind-order logic (source of past bugs in dynamic search queries).
- For JSON payload filters we ended up interpolating text because SeaQuery lacked a straightforward `jsonb` bind path at adoption time.
- Adds another dependency tree; SQLx's QueryBuilder is already available.
- Debugging requires inspecting generated SQL strings; less transparent during failures.

QueryBuilder evaluation
-----------------------
- SQLx `QueryBuilder<Postgres>` keeps SQL fragments and binds together (`push_bind`, `push_values`), eliminating placeholder math.
- `build_query_as` works directly with row structs and integrates with SQLx's executor.
- Supports arrays/slices naturally when using `ANY($1)` patterns, matching our primary use cases.
- Still stringy wrt identifiers, but we can centralize identifiers via helper constants or use `sinex_schema::schema::*` to produce fully-qualified names.

Recommendation
--------------
1. Standardize on SQLx QueryBuilder for runtime query composition in repositories/services.
2. Keep SeaQuery only where schema metadata is needed ahead of time (migrations, helpers that operate on `sinex_schema::schema::*`).
3. Audit remaining SeaQuery callsites (`time_series_aggregate`, `db::query_helpers`, associated tests) and plan migrations unless they truly need SeaQuery's schema coupling.
4. Once production code stops using SeaQuery, retire `seaquery_helpers.rs` and related tests unless they remain useful for migrations.

Open questions
--------------
- Does SeaQuery still offer unique value for schema-generation tooling (migration feature)? If so, scope it to that feature flag.
- Should we create a thin helper layer for identifier constants when using QueryBuilder so column/table names stay centralized without SeaQuery?
