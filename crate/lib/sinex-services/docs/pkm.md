# PKM Service

`PkmService` offers helper methods for creating annotations and knowledge-graph
entities while preserving provenance. It sits directly on top of the
`sinex-core` repositories and keeps gateway handlers small.

## API Surface

| Method | Description |
|--------|-------------|
| `create_note(event_id, content, tags, created_by, source_material_id?)` | Adds a note annotation to an event with metadata (tags, timestamps, optional source material). |
| `create_entities_from_source_material(source_material_id, entities, created_by)` | Creates entities linked to the source material; accepts `(name, type)` tuples. |
| `link_entities(from_id, to_id, relationship_type, properties, source_material_id?)` | Establishes a relationship between two entities with optional provenance metadata. |

The service relies on `MetadataBuilder` helpers to keep annotation payloads
uniform, and returns ULIDs for any newly created records.

For broader UX flows see `docs/current/architecture/UserInteraction_And_Query_Architecture.md`.
