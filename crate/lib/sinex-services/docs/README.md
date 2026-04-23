# Sinex Services Layer

The `sinex-services` crate now holds the remaining PKM-oriented orchestration that has not yet
been dissolved into directionally correct owners. Content/blob orchestration moved into
`sinex-gateway`, so this crate is no longer the home of binary storage APIs.

## Service Architecture

Services are intentionally thin, stateless facades around database pools and specialized managers. They follow a consistent pattern:

1.  **Constructor**: `new(pool: DbPool)` or `new(pool, specialized_manager)`.
2.  **State**: Minimal, typically just pool/manager references.
3.  **Methods**: Async orchestration logic wrapping repository calls.
4.  **Errors**: Return `Result<T>`, a unified error type re-exported from `sinex-primitives`.

### Current Service

| Service | Responsibility | Key Workflows |
|---------|----------------|---------------|
| [`PkmService`](./pkm.md) | Knowledge Graph & Provenance | Entity/Relation creation, source material registry, stage-as-you-go workflows. |

### Additional References

- [`current_state_tracking.md`](./current_state_tracking.md) – continuous aggregates, materialized views, and current-state read models.

## Design Principles

### Thin Orchestration
Services are not responsible for transaction management (delegated to repositories) or query
optimization (handled at the DB/repo layer). They primarily focus on translating gateway requests
into coherent repository sequences.

### Metadata Segregation
Particularly in the `PkmService`, we maintain a strict separation between caller-provided metadata and system-generated metadata (`_system_metadata`). This ensures system invariants (checksums, sizes, timestamps) are preserved without polluting user data.

### Unicode-Aware Safety
String operations (truncation, snippet extraction) are performed at UTF-8 character boundaries to prevent invalid substring bugs or corrupted multi-byte sequences.

## Development Guidelines

When extending the remaining PKM service surface:

1.  **Document Invariants**: Update the corresponding `.md` file in `docs/` detailing the API surface and any internal invariants.
2.  **Use DTOs**: Never return raw database records directly; transform results into specialized DTOs (e.g., `MaterialSummary`, `SearchResult`) to decouple the API from the schema.
3.  **Audit Operations**: Use `operations_log` for high-value mutations to provide a durable
    audit trail.
4.  **Clamp Bounds**: Always validate and clamp pagination limits and time horizons to prevent unbounded query risks.
