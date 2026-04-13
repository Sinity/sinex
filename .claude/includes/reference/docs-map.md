## Documentation Map

- `README.md` — project overview, architecture, security, deployment
- `crate/**/docs/` — crate-local implementation details
- `/realm/project/sinex-target-vision/` — vision, roadmap, gap analysis

### By Topic

| Topic | Location |
|-------|----------|
| Architecture | `README.md#architecture`, `crate/core/*/docs/architecture.md` |
| Type system | `crate/lib/sinex-primitives/docs/type_system_patterns.md` |
| Error handling | `crate/lib/sinex-primitives/docs/error.md` |
| Domain types/enums | `crate/lib/sinex-primitives/docs/newtypes.md`, `src/domain.rs`, `src/events/enums.rs` |
| Event payloads | `crate/lib/sinex-primitives/src/events/payloads/` |
| Privacy engine | `crate/lib/sinex-primitives/src/privacy/mod.rs` |
| DB schema | `crate/lib/sinex-schema/docs/schema_design.md` |
| DB repositories | `crate/lib/sinex-db/docs/db_repositories.md` |
| COPY inserts | `crate/lib/sinex-db/src/postgres_copy.rs` |
| Data lifecycle | `crate/lib/sinex-db/docs/data_lifecycle.md` (live -> archive -> tombstone) |
| Node SDK | `crate/lib/sinex-node-sdk/docs/overview.md` |
| Checkpoints/replay | `crate/lib/sinex-node-sdk/docs/stream_node.md` |
| Provenance | `crate/lib/sinex-node-sdk/docs/provenance.md` |
| Distributed patterns | `crate/lib/sinex-node-sdk/docs/distributed_patterns.md` |
| ingestd pipeline | `crate/core/sinex-ingestd/docs/architecture.md` |
| Gateway API | `crate/core/sinex-gateway/docs/architecture.md` |
| CLI | `crate/cli/README.md`, `crate/cli/DESIGN.md` |
| Contributing | `CONTRIBUTING.md` |
| Issue / PR workflow | `CONTRIBUTING.md`, `.github/ISSUE_TEMPLATE/*`, `.github/pull_request_template.md` |
| Testing | `TESTING.md`, `xtask/docs/sandbox/README.md` |
| Perf contracts | `xtask/config/perf-contracts.toml` |
| NixOS/TLS/env vars | `nixos/modules/README.md` |
| xtask guide | `xtask/docs/README.md`, `xtask/docs/command-guide.md`, `xtask/docs/command-reference.md` |
| Vision/roadmap | `/realm/project/sinex-target-vision/AGENTS.md` |
