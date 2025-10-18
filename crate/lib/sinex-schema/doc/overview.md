# Sinex Database Migrations

This crate contains the complete schema definition and the evolutionary history of the Sinex
database. It uses the `sea-orm-migration` framework with `sea-query` to define the schema in a
type-safe, programmatic way.

## Architecture

- **`src/schema/`** – canonical, state-of-the-art definitions of every table in the database. This
  is the single source of truth for the schema.
- **`src/migrations/`** – a squashed initial migration that creates the entire canonical schema from
  scratch. Future schema changes appear as timestamped migration files that apply incremental
  `ALTER` statements.
- **`src/main.rs`** – CLI entry point for managing migrations.

## Quick Schema Reference

| Schema          | Purpose & Key Tables                                                                         |
| --------------- | -------------------------------------------------------------------------------------------- |
| `core`          | Primary event store (`core.events`), entity graph (`core.entities`, `core.entity_relations`), operations log (`core.operations_log`), processor manifests, automaton checkpoints. |
| `raw`           | Source material registry (`raw.source_material_registry`) with checksums, provenance anchors, and staging metadata. |
| `sinex_schemas` | JSON Schema registry (`sinex_schemas.event_payload_schemas`), compatibility metadata, validation cache. |
| `metrics`       | Operational telemetry (`metrics.sinex_metrics`) plus materialized views for event throughput and heartbeats. |
| `km`            | Knowledge management entities (`km.concepts`, `km.relations`, `km.embeddings`, `km.event_annotations`). |
| `synthesis`     | Configuration scaffolding for derived event generation.                                      |
| `sinex`         | Legacy compatibility surface retained for historical migrations.                             |

The design trade‑offs, indexing strategies, and migration history are documented in
`doc/schema_design.md`. That file is included in rustdoc for discoverability alongside this quick
reference.
