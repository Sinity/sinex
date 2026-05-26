# PKM Module

The Personal Knowledge Management (PKM) module coordinates the creation and curation of the Sinex knowledge graph from the database ownership layer. It bridges the gap between raw source materials and structured entities and relationships without routing through a separate services crate.

## API Surface

| Method | Description |
|--------|-------------|
| `create_note` | Attaches a curated annotation to an event, optionally linked to source material. |
| `create_entities_from_source_material` | Batch-creates entities extracted from a specific source artifact. |
| `link_entities` | Establishes a directed, typed relationship between two graph nodes. |
| `register_source_material` | Canonical entry point for tracking external artifacts (files, streams). |
| `register_in_flight_material` | Support for the "Stage-as-you-go" pattern (pre-registration). |
| `finalize_in_flight_material` | Completes an in-flight registration with full content and checksums. |

## Knowledge Graph Patterns

### Stage-as-you-go
To support streaming ingestion, the module allows materials to be registered in a `sensing` state. This provides a stable `material_id` that ingestors can use for event provenance *before* the full artifact has been captured. The material is transitioned to `completed` once the content hash is verified.

### Metadata Segregation
The system automatically manages `_system_metadata` (checksums, sizes, timestamps) while preserving `caller_metadata` in its original form. This ensures system invariants are never corrupted by user-provided data.

### Provenance XOR Invariant
The module enforces the core architectural principle that an event or entity must have exactly one type of provenance:
- **Material Provenance**: Direct link to raw source material.
- **Synthesis Provenance**: Derived from other system events.

## Safety & Integrity

- **Deterministic Deduplication**: Material registration uses BLAKE3 hashes to prevent duplicate entries for identical content.
- **Unicode Safety**: Content previews are generated with UTF-8 character boundary awareness to prevent splitting multi-byte sequences.
- **Type Mapping**: String-based entity types are validated against a canonical allowlist (`person`, `project`, `topic`, etc.) before creation.
