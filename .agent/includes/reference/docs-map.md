## Documentation Map

- `README.md` — project overview, architecture, security, deployment
- `crate/**/docs/` — crate-local implementation details
- `/realm/project/sinex-target-vision/` — vision, roadmap, gap analysis

### By Topic

| Topic | Location |
|-------|----------|
| Architecture | `README.md#architecture`, `crate/sinexd/docs/` |
| Type system | `crate/sinex-primitives/docs/type_system_patterns.md` |
| Error handling | `crate/sinex-primitives/docs/error.md` |
| Domain types/enums | `crate/sinex-primitives/docs/newtypes.md`, `src/domain.rs`, `src/events/enums.rs` |
| Event payloads | `crate/sinex-primitives/src/events/payloads/` |
| Privacy engine | `crate/sinex-primitives/src/privacy/mod.rs` |
| DB schema | `crate/sinex-db/docs/schema/` |
| DB repositories | `crate/sinex-db/docs/db_repositories.md` |
| COPY inserts | `crate/sinex-db/src/postgres_copy.rs` |
| Data lifecycle | `crate/sinex-db/docs/data_lifecycle.md` (live -> archive -> tombstone) |
| Node SDK | `crate/sinex-node-sdk/docs/overview.md` |
| Checkpoints/replay | `crate/sinex-node-sdk/docs/stream_node.md` |
| Provenance | `crate/sinex-node-sdk/docs/provenance.md` |
| Distributed patterns | `crate/sinex-node-sdk/docs/distributed_patterns.md` |
| Event engine pipeline | `crate/sinexd/docs/event_engine/` |
| API gateway | `crate/sinexd/docs/api/` |
| Sources | `crate/sinexd/docs/sources/` |
| CLI | `crate/sinexctl/README.md`, `crate/sinexctl/DESIGN.md` |
| Contributing | `CONTRIBUTING.md` |
| Issue / PR workflow | `CONTRIBUTING.md`, `.github/ISSUE_TEMPLATE/*`, `.github/pull_request_template.md` |
| Testing | `TESTING.md`, `xtask/docs/sandbox/README.md` |
| Perf contracts | `xtask/config/perf-contracts.toml` |
| NixOS/TLS/env vars | `nixos/modules/README.md` |
| xtask guide | `xtask/docs/README.md`, `xtask/docs/command-guide.md`, `xtask/docs/command-reference.md` |
| Vision/roadmap | `/realm/project/sinex-target-vision/AGENTS.md` |
