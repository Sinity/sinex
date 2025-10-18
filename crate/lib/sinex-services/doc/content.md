# Content Service

`ContentService` orchestrates ingest, storage, and retrieval of binary payloads.
It wraps blob repositories exposed by `sinex-core`, enforces metadata
expectations, and triggers downstream enrichment workflows.

Key responsibilities:

- Store large artifacts and return durable identifiers for later retrieval.
- Maintain metadata records that make content searchable via the search service.
- Coordinate with background workers for checksum validation and lifecycle
  management.

For the storage topology and replication guarantees, consult
`docs/architecture/Core_Architecture.md`.
