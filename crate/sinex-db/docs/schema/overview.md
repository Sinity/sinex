# Sinex Database Schema Overview

This crate contains the complete schema definition for Sinex and the declarative convergence
engine that applies it. It uses `sea-query` to define the schema in a type-safe, programmatic way.

## Architecture

- **`src/schema/`** – canonical, state-of-the-art definitions of every table in the database. This
  is the single source of truth for the schema.
- **`src/apply.rs`** – declarative convergence engine that creates and reconciles schema objects
  idempotently (`apply()` + `diff()`).
- **`src/schema_registry.rs`** – canonical schema registry and schema-name metadata.

## Quick Schema Reference

| Schema | Purpose & Key Tables |
| --- | --- |
| `core` | Primary event store and domain tables: `core.events`, `core.event_tombstones`, `core.operations_log`, `core.node_manifests`, `core.entities`, `core.entity_relations`, `core.blobs`, tagging/annotation/embedding tables. |
| `raw` | Provenance staging and source registries: `raw.source_material_registry`, `raw.temporal_ledger`. |
| `audit` | Archive tier table: `audit.archived_events`. |
| `sinex_schemas` | Payload schema and contract management: `event_payload_schemas`, `validation_cache`, `gitops_schema_sources`, `dlq_events`. |
| `metrics` | Reserved schema namespace (created for compatibility with grants/registry; currently no canonical table definitions in `src/schema/`). |
| `sinex_telemetry` | Hourly operator views, activity/status views, and one materialized current-device view created by `src/apply.rs` SQL blocks. |

The design trade-offs and indexing strategies are documented in `docs/schema_design.md`.
