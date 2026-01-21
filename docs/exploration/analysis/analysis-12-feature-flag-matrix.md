# Feature Flag Matrix (Core Modules)

Scope
- Summarize compile-time feature gates and identify which subsystems disappear under flags.

Method
- rg "cfg(feature" across crate/lib, crate/core, crate/nodes.

Core flags
- sinex-core: sqlx gates the entire db module and most persistence APIs; nats gates coordination; macros gates db_query/db_transaction re-exports (crate/lib/sinex-core/src/lib.rs:15-46).
- sinex-node-sdk: preflight module and exports are gated on feature "preflight" (crate/lib/sinex-node-sdk/src/lib.rs:39-99).
- sinex-schema: sqlx impls for Ulid and arbitrary support are feature-gated (crate/lib/sinex-schema/src/ulid.rs:412-455).
- sinex-macros: metrics feature gates macro output hooks (crate/lib/sinex-macros/src/error_context.rs:327-344).

Test/bench flags
- slow-tests, bench, external-tests, and rstest-preview gate various test suites and macros.

Observations
- Disabling sqlx in sinex-core removes database types and most repository logic; any downstream crate expecting db::* must keep sqlx enabled.
- Some features are only surfaced in tests; ensure CI profiles cover feature combinations used in production.

Follow-ups
- Create a small table in docs listing which binaries require which features (core vs node SDK).
- Add CI jobs for non-default feature sets if they are expected to be supported.
