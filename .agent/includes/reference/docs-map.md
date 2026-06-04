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
| Domain reducers | `crate/sinex-primitives/docs/domain_reducers.md` |
| Knowledge boundaries | `crate/sinex-primitives/docs/knowledge_boundaries.md` |
| Curation authority | `crate/sinex-primitives/docs/curation_authority.md` |
| Event payloads | `crate/sinex-primitives/src/events/payloads/` |
| Privacy engine | `crate/sinex-primitives/src/privacy/mod.rs` |
| DB schema | `crate/sinex-db/docs/schema/` |
| DB repositories | `crate/sinex-db/docs/db_repositories.md` |
| COPY inserts | `crate/sinex-db/src/postgres_copy.rs` |
| Data lifecycle | `crate/sinex-db/docs/data_lifecycle.md` (live -> archive -> tombstone) |
| Document layer | `crate/sinex-schema/docs/document_layer.md` |
| PostgreSQL backup/restore | `crate/sinex-db/docs/backup_restore.md` |
| Inline node SDK / source contracts | `crate/sinexd/docs/sources/`, `crate/sinexd/src/node_sdk/` |
| Source-material evidence lanes | `crate/sinexd/docs/sources/evidence_lanes.md`, `crate/sinexd/docs/sources/sqlite_evidence_lane.md` |
| Integration authority | `crate/sinexd/docs/sources/integration_authority.md` |
| Automata / derived-node guidance | `crate/sinexd/docs/automata/` |
| Checkpoints/replay | `crate/sinexd/docs/sources/historical_backfill_runtime_plane.md`, `crate/sinexd/docs/api/replay_control.md` |
| Staged export parsers | `crate/sinexd/docs/sources/adding_staged_export_parser.md` |
| Provenance | `README.md#the-provenance-model-read-this-first` |
| Distributed patterns | `crate/sinexd/docs/api/coordination.md`, `crate/sinex-primitives/docs/distributed_coordination.md` |
| Event engine pipeline | `crate/sinexd/docs/event_engine/` |
| API gateway | `crate/sinexd/docs/api/` |
| Sources | `crate/sinexd/docs/sources/` |
| CLI | `crate/sinexctl/README.md`, `crate/sinexctl/DESIGN.md`, `crate/sinexctl/docs/` |
| Runtime state snapshot/restore | `crate/sinexctl/docs/state_snapshot.md` |
| Operator privacy/data lifecycle | `crate/sinexctl/docs/operator_data_lifecycle.md` |
| Runtime private mode | `crate/sinexctl/docs/private_mode.md` |
| MCP read-only surface | `crate/sinexctl/docs/mcp_readonly_server.md` |
| Glossary | `.agent/includes/reference/glossary.md` |
| Authority-surface review rule | `.github/authority-surfaces.md` |
| Issue operating model | `.github/issue-operating-model.md` |
| CI policy | `.github/ci-policy.md` |
| Target-vision claim ledger | `.github/target-vision-claim-ledger.md` |
| Threat model / at-rest encryption | `nixos/modules/security-threat-model.md`, `nixos/modules/at-rest-encryption.md` |
| Contributing | `CONTRIBUTING.md` |
| Issue / PR workflow | `CONTRIBUTING.md`, `.github/ISSUE_TEMPLATE/*`, `.github/pull_request_template.md` |
| Testing | `TESTING.md`, `xtask/docs/sandbox/README.md` |
| Perf contracts | `xtask/config/perf-contracts.toml` |
| NixOS/TLS/env vars | `nixos/modules/README.md` |
| NixOS resource scoping | `nixos/modules/resource-scoping.md` |
| xtask guide | `xtask/docs/README.md`, `xtask/docs/command-guide.md`, `xtask/docs/command-reference.md` |
| Dependency hygiene | `xtask/docs/dependency-hygiene.md` |
| Runtime target boundaries | `xtask/docs/runtime-target-boundaries.md` |
| Cloud agent lane | `xtask/docs/cloud-agent-lane.md` |
| Vision/roadmap | `/realm/project/sinex-target-vision/AGENTS.md` |
