## Documentation Map

Documentation layout:

- `docs/` is for global architecture, policy, and operational docs.
- `crate/**/docs/` is for crate-local implementation details and API behavior.

| Topic | Primary Location | Also See |
|-------|------------------|----------|
| **Documentation index** | `docs/README.md` | `docs/documentation-guidelines.md` |
| **Architecture overview** | `docs/current/architecture/Core_Architecture.md` | `docs/current/architecture/` |
| **Security** | `docs/current/security.md` | `docs/current/architecture/SystemOperations_And_Integrity_Architecture.md` |
| **Type system patterns** | `docs/current/architecture/type-system-patterns.md` | `crate/lib/sinex-primitives/docs/newtypes.md` |
| **Distributed patterns** | `docs/current/architecture/distributed-patterns.md` | |
| **Observability** | `docs/current/architecture/observability.md` | |
| **Current state tracking** | `docs/current/architecture/current-state-tracking.md` | `crate/lib/sinex-schema/docs/schema_design.md` |
| **Environment variables** | `docs/current/configuration/environment-variables.md` | |
| **Getting started** | `README.md#contributing` | `docs/README.md` |
| **Testing guide** | `xtask/docs/sandbox/README.md` | `.claude/includes/patterns/testing.md` |
| **Perf contracts** | `config/verify/perf-contracts.toml` | `xtask test bench --contracts` |
| **Verification workflow** | `xtask/docs/verification.md` | `xtask/docs/README.md` |
| **Schema GitOps** | `crate/core/sinex-ingestd/docs/schema_gitops.md` | `crate/lib/sinex-schema/docs/gitops-schema-sources-status.md` |
| **Test patterns** | `xtask/docs/sandbox/README.md` | `xtask/docs/sandbox/property_testing.md` |
| **Pipeline testing** | `xtask/docs/sandbox/pipeline_testing.md` | `xtask/docs/sandbox/database_testing.md` |
| **Error handling** | `crate/lib/sinex-primitives/docs/error.md` | `SinexError::with_context(...)` patterns |
| **Database pools** | `crate/lib/sinex-db/docs/pool.md` | `crate/lib/sinex-db/docs/query_helpers.md` |
| **Repository pattern** | `crate/lib/sinex-db/docs/db_repositories.md` | |
| **Domain types** | `crate/lib/sinex-primitives/docs/newtypes.md` | `crate/lib/sinex-primitives/docs/types_overview.md` |
| **DB schema design** | `crate/lib/sinex-schema/docs/schema_design.md` | `crate/lib/sinex-schema/docs/apply.md` |
| **Event taxonomy** | `crate/lib/sinex-schema/docs/event-taxonomy.md` | |
| **Event payloads** | `crate/lib/sinex-primitives/src/events/payloads/` | `crate/lib/sinex-macros/docs/usage_audit.md` |
| **Node development** | `crate/lib/sinex-node-sdk/docs/overview.md` | `crate/lib/sinex-node-sdk/docs/patterns.md` |
| **Checkpoint/replay** | `crate/lib/sinex-node-sdk/docs/stream_node.md` | `crate/lib/sinex-node-sdk/docs/coordination.md` |
| **Provenance** | `crate/lib/sinex-node-sdk/docs/provenance.md` | |
| **ingestd architecture** | `crate/core/sinex-ingestd/docs/architecture.md` | `crate/core/sinex-ingestd/docs/pipeline-design.md` |
| **Gateway architecture** | `crate/core/sinex-gateway/docs/architecture.md` | `crate/core/sinex-gateway/docs/native_messaging.md` |
| **Privacy engine** | `crate/lib/sinex-primitives/src/privacy/mod.rs` | `docs/planning/features/unified-privacy-engine.md` |
| **Domain enums** | `crate/lib/sinex-primitives/src/domain.rs` | `crate/lib/sinex-primitives/docs/domain_types.md` |
| **Event field enums** | `crate/lib/sinex-primitives/src/events/enums.rs` | `crate/lib/sinex-primitives/docs/event_taxonomy_and_enums.md` |
| **COPY batch inserts** | `crate/lib/sinex-db/src/postgres_copy.rs` | Staging table → `INSERT SELECT` pattern |
| **CLI usage** | `crate/cli/README.md` | `crate/cli/DESIGN.md` |
| **Data lifecycle** | `docs/current/architecture/data-lifecycle.md` | 3-tier: live → archive → tombstone |
| **NATS subjects** | `docs/current/architecture/nats-subjects.md` | Subject naming conventions |
| **Feature flags** | `docs/current/configuration/feature-flags.md` | |
| **TLS / NixOS** | `docs/current/configuration/tls-nixos-integration.md` | `docs/current/configuration/tls-setup.md` |
| **xtask guide** | `xtask/docs/README.md` | |
| **Runtime metrics** | `xtask/src/runtime_metrics.rs` | Postgres queries for ingestd health |
