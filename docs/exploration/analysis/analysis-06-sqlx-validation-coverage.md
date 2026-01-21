# SQLx Validation Coverage

Scope
- Quantify macro-validated SQLx usage vs runtime queries, and identify dynamic SQL construction.

Method
- rg counts for sqlx::query!/query_as!/query_scalar! vs sqlx::query(…); manual inspection of dynamic SQL.

Macro vs runtime mix (rg counts, approximate)
- sqlx::query! ~250
- sqlx::query_as! ~64
- sqlx::query_scalar! ~96
- sqlx::query(…) (runtime, non-macro) ~253

Dynamic SQL hotspots
- Statement timeout uses runtime sqlx::query with format!, which is safe but not compile-time validated (crate/lib/sinex-core/src/db/mod.rs:251-268).
- Repository helper builds COUNT/EXISTS SQL with format!; comments document why this is safe (compile-time table names) but still bypasses macro validation (crate/lib/sinex-core/src/db/repositories/common.rs:120-176).
- Schema KV storage uses sqlx::query_scalar (runtime) to avoid ulid/uuid casting issues, so it is not compile-time validated (crate/core/sinex-ingestd/src/service.rs:592-603).

Observations
- The codebase makes heavy use of compile-time macros, but runtime queries are still substantial. Most runtime queries appear justified (dynamic table names, session-level settings).

Follow-ups
- Where possible, migrate runtime queries to sqlx macros or query builder patterns to regain compile-time checking.
- Track runtime query count in CI to prevent drift (simple rg count or lint rule).
