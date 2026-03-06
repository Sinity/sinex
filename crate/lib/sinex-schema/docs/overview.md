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

| Schema          | Purpose & Key Tables                                                                         |
| --------------- | -------------------------------------------------------------------------------------------- |
| `core`          | Primary event store (`core.events`), entity graph (`core.entities`, `core.entity_relations`), operations log (`core.operations_log`), node manifests, automaton checkpoints. |
| `raw`           | Source material registry (`raw.source_material_registry`) with checksums, provenance anchors, and staging metadata. |
| `sinex_schemas` | JSON Schema registry (`sinex_schemas.event_payload_schemas`), schema metadata, validation cache. |
| `metrics`       | Operational telemetry (`metrics.sinex_metrics`) plus materialized views for event throughput and heartbeats. |
| `km`            | Knowledge management entities (`km.concepts`, `km.relations`, `km.embeddings`, `km.event_annotations`). |
| `synthesis`     | Configuration scaffolding for derived event generation.                                      |

The design trade-offs and indexing strategies are documented in `docs/schema_design.md`.
