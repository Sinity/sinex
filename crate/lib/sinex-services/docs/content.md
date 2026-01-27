# Content Service

`ContentService` is the primary interface for managing binary payloads within Sinex. It orchestrates content-addressed storage via `git-annex` and provides an auditable trail of all content-related mutations.

## API Surface

| Method | Description |
|--------|-------------|
| `store_content` | Persists bytes through `BlobManager`, calculates BLAKE3/SHA256 checksums, and returns the unique annex key. |
| `retrieve_content` | Retrieves raw bytes for a given annex key. |
| `get_content_metadata` | Fetches metadata (size, mime, checksums) for a blob. |
| `verify_content` | Forces an integrity check by re-calculating hashes on disk. |

## Auditing and Observability

Every write operation (`store_content`) is logged to the `core.operations_log` table. This audit trail captures:
- **Scope**: Original filename, content type, and byte size.
- **Outcome**: Success or failure (with detailed error chain on failure).
- **Performance**: Operation duration in milliseconds.
- **Summary**: Resulting annex key and content hashes.

Read operations are generally not logged to avoid saturating the audit trail, but failures are bubbled up with full structured context.

## Error Handling

The service uses a "structured context" pattern for errors. Any failure includes:
- The specific `BlobManager` operation that failed.
- Relevant identifiers like the `annex_key` or `filename`.
- The raw source error from the underlying filesystem or git-annex process.

## Design Patterns

### Content Deduplication
Deduplication is handled at the `BlobManager` layer using the annex key natural key (backend + hash + size). The service layer coordinates the registration so that multiple `SourceMaterial` entries can point to the same underlying binary blob.

### Timing Instrumentation
All high-value operations are instrumented with `std::time::Instant` to provide precise metrics for system dashboards.