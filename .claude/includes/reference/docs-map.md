## Documentation Map

| Topic | Primary Location | Also See |
|-------|------------------|----------|
| **Architecture overview** | `docs/current/architecture/Core_Architecture.md` | `docs/current/architecture/` |
| **Security** | `docs/current/security.md` | `security-architecture.md` |
| **Type system patterns** | `docs/current/architecture/type-system-patterns.md` | `newtypes.md` in sinex-primitives |
| **Distributed patterns** | `docs/current/architecture/distributed-patterns.md` | |
| **Observability** | `docs/current/architecture/observability.md` | |
| **Current state tracking** | `docs/current/architecture/current-state-tracking.md` | `timescaledb-ulid-continuous-aggregates.md` |
| **Environment variables** | `docs/current/configuration/environment-variables.md` | |
| **Getting started** | `docs/current/getting-started.md` | `docs/README.md` |
| **Testing guide** | `docs/current/testing/` | `xtask/docs/` |
| **Test patterns** | `xtask/docs/sandbox/` | Via `#[sinex_test]` macro |
| **Pipeline testing** | `xtask/docs/sandbox/pipeline_testing.md` | Database testing |
| **Error handling** | `crate/lib/sinex-primitives/docs/error.md` | `#[with_context]` macro |
| **Database pools** | `crate/lib/sinex-db/docs/pool.md` | `query_helpers.md` |
| **Repository pattern** | `crate/lib/sinex-db/docs/db_repositories.md` | |
| **Domain types** | `crate/lib/sinex-primitives/docs/newtypes.md` | `types_overview.md` |
| **DB schema design** | `crate/lib/sinex-schema/docs/schema_design.md` | `migrations.md` |
| **Event taxonomy** | `crate/lib/sinex-schema/docs/event-taxonomy.md` | |
| **Event payloads** | `crate/lib/sinex-primitives/src/types/events/payloads/` | `EventPayload` derive macro |
| **Node development** | `crate/lib/sinex-node-sdk/docs/overview.md` | `patterns.md` |
| **Checkpoint/replay** | `crate/lib/sinex-node-sdk/docs/stream_processor.md` | `coordination.md` |
| **Provenance** | `crate/lib/sinex-node-sdk/docs/provenance.md` | |
| **ingestd architecture** | `crate/core/sinex-ingestd/docs/architecture.md` | `pipeline-design.md` |
| **Gateway architecture** | `crate/core/sinex-gateway/docs/architecture.md` | `native_messaging.md` |
| **CLI usage** | `crate/cli/README.md` | `crate/cli/DESIGN.md` |
