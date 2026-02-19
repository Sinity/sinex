# Gitops Schema Sources: Status

The `gitops_schema_sources` table and related types are currently used to define the data model for the upcoming "Auto-Schema Sync" feature.

## Purpose

The goal is to allow `ingestd` or a dedicated service to automatically poll Git repositories for JSON schema updates. This will enable a direct pipeline from data contract definitions (in repo) to runtime enforcement (in `event_payload_schemas`).

## Current Status

- **Schema Defined:** The database table `sinex_schemas.gitops_schema_sources` is defined and migrated.
- **Rust Types:** The `GitopsSchemaSources` enum and `TableDef` trait are implemented in `sinex-schema`.
- **Implementation:** Partial. The syncing logic (polling git, parsing JSON, updating DB) is **not yet implemented**.

## Implementation Status

- [x] Schema definition (`sinex_schemas.gitops_schema_sources`)
- [x] `ingestd` background worker
- [x] `sinex-node-sdk` Git adapter
- [x] `sinexctl` / `xtask` commands

See [Schema GitOps Workflow](../../../../docs/current/workflows/schema-gitops.md) for usage instructions.
