# Sinex Services Layer

The `sinex-services` crate provides a high-level business logic layer that orchestrates operations between `sinex-db` repositories, `sinex-primitives` types, and the `sinex-gateway` handlers. It serves as a thin facade designed to coordinate multi-step workflows, transform database records into API-ready DTOs, and enforce business rules without duplicating SQL or complex orchestration logic.

## Service Architecture

Services are intentionally thin, stateless facades around database pools and specialized managers. They follow a consistent pattern:

1.  **Constructor**: `new(pool: DbPool)` or `new(pool, specialized_manager)`.
2.  **State**: Minimal, typically just pool/manager references.
3.  **Methods**: Async orchestration logic wrapping repository calls.
4.  **Errors**: Return `Result<T>`, a unified error type re-exported from `sinex-primitives`.

### Core Services

| Service | Responsibility | Key Workflows |
|---------|----------------|---------------|
| [`AnalyticsService`](./analytics.md) | Read-only rollups | Event counts, time-series bucketing, heatmaps, source statistics. |
| [`ContentService`](./content.md) | Binary blob orchestration | Content-addressed storage (git-annex), operations logging, integrity checks. |
| [`PkmService`](./pkm.md) | Knowledge Graph & Provenance | Entity/Relation creation, source material registry, stage-as-you-go workflows. |
| [`SearchService`](./search.md) | Multi-dimensional search | Full-text event search, multi-field filtering, snippet extraction. |

### Additional References

- [`current_state_tracking.md`](./current_state_tracking.md) – continuous aggregates, materialized views, and current-state read models.

## Design Principles

### Thin Orchestration
Services are not responsible for transaction management (delegated to repositories) or query optimization (handled at the DB/repo layer). They primarily focus on translating gateway requests into coherent repository sequences.

### Metadata Segregation
Particularly in the `PkmService`, we maintain a strict separation between caller-provided metadata and system-generated metadata (`_system_metadata`). This ensures system invariants (checksums, sizes, timestamps) are preserved without polluting user data.

### Fail-Fast Resource Management
Services like `AnalyticsService` employ aggressive connection acquisition timeouts (40ms) to ensure that slow analytical queries do not saturate the pool or block high-priority ingestion hot-paths.

### Unicode-Aware Safety
String operations (truncation, snippet extraction) are performed at UTF-8 character boundaries to prevent invalid substring bugs or corrupted multi-byte sequences.

## Development Guidelines

When adding a new service or method:

1.  **Document Invariants**: Update the corresponding `.md` file in `docs/` detailing the API surface and any internal invariants.
2.  **Use DTOs**: Never return raw database records directly; transform results into specialized DTOs (e.g., `MaterialSummary`, `SearchResult`) to decouple the API from the schema.
3.  **Audit Operations**: Use the `operations_log` (via `ContentService` patterns) for any high-value mutations to provide a durable audit trail.
4.  **Clamp Bounds**: Always validate and clamp pagination limits and time horizons to prevent unbounded query risks.
