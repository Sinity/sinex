# Knowledge Graph Repository

The Knowledge Graph repository manages the storage and retrieval of entities and their relationships. it supports complex operations like entity merging, bidirectional relationship queries, and recursive path finding.

## Core Concepts

- **Entities**: Strongly typed domain objects (e.g., Person, Project, Tool) with a unique `canonical_name` and a bag of flexible JSON `properties`.
- **Relations**: Directed edges between entities with a `relation_type` and associated metadata.
- **Merge Logic**: The process of consolidating duplicate entities into a single canonical record while preserving all associated provenance and relationships.

## Entity Merging & Concurrency

Merging two entities is a complex operation that involves updating multiple tables and re-wiring relationships. To ensure data integrity and prevent deadlocks, the system implements **Ordered Locking**:

1. **ID Ordering**: UUIDs of the source and target entities are sorted lexicographically.
2. **Sequential Locking**: Row-level locks (`SELECT ... FOR UPDATE`) are acquired in the sorted order. This ensures that concurrent merge operations on the same set of entities always acquire locks in the same sequence, preventing cyclic wait conditions (AB-BA deadlocks).
3. **Isolation**: Operations are performed within a `REPEATABLE READ` transaction to ensure a consistent snapshot of the graph during the merge.

## Recursive Path Finding

The graph supports path finding between entities using a recursive Common Table Expression (CTE).

- **Cycle Detection**: To prevent infinite loops in cyclic graphs, the search tracks visited edges in a PostgreSQL array (`relation_ids`) and terminates if a duplicate edge is encountered.
- **Depth Limits**: All recursive searches require a `max_depth` parameter to bound resource consumption.

## Performance Considerations

- **Indexing**: High-performance lookups rely on functional indexes for case-insensitive matching (`LOWER(name)`).
- **Batching**: Path details are retrieved using `ANY($1)` array matching to avoid N+1 query patterns.
- **JSONB Containment**: Metadata searches utilize the `@>` operator. GIN indexes are recommended for production environments with large entity sets.
