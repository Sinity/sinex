# GitOps Schema Sources: Status

The `gitops_schema_sources` table is live and backs the current repo-driven
schema sync flow.

## What This Crate Owns

- The `sinex_schemas.gitops_schema_sources` table definition.
- The typed schema metadata used by repository and RPC layers.
- The persistence model that ingestd polls against.

## Current Status

- **Schema defined:** yes.
- **Repository support:** yes.
- **Gateway + CLI control plane:** yes.
- **Ingestd background sync worker:** yes.

This crate owns the data model, not the operational workflow. For usage and
runtime behavior, see `crate/core/sinex-ingestd/docs/schema_gitops.md`.
