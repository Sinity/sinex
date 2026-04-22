# Content Service

`ContentService` is the primary interface for managing binary payloads within Sinex. It orchestrates content-addressed storage through `ContentStoreManager` and provides an auditable trail of all content-related mutations.

## API Surface

| Method | Description |
|--------|-------------|
| `store_content` | Persists bytes through `ContentStoreManager`, calculates BLAKE3 and backend checksums, and returns the unique content-store key. |
| `retrieve_content` | Retrieves raw bytes for a given content-store key. |
| `get_content_metadata` | Fetches metadata (size, mime, checksums) for a blob. |
| `verify_content` | Forces an integrity check by re-calculating hashes on disk. |

## Auditing and Observability

Every write operation (`store_content`) is logged to the `core.operations_log` table. This audit trail captures:
- **Scope**: Original filename, content type, and byte size.
- **Outcome**: Success or failure (with detailed error chain on failure).
- **Performance**: Operation duration in milliseconds.
- **Summary**: Resulting content-store key and content hashes.

Read operations are generally not logged to avoid saturating the audit trail, but failures are bubbled up with full structured context.

## Error Handling

The service uses a "structured context" pattern for errors. Any failure includes:
- The specific `ContentStoreManager` operation that failed.
- Relevant identifiers like the `content_key` or `filename`.
- The raw source error from the underlying filesystem or storage backend.

## Design Patterns

### Content Deduplication
Deduplication is handled at the `ContentStoreManager` layer using the content-store key natural identity (backend + digest + size) plus BLAKE3. The service layer coordinates the registration so that multiple `SourceMaterial` entries can point to the same underlying binary blob.

### Timing Instrumentation
All high-value operations are instrumented with `std::time::Instant` to provide precise metrics for system dashboards.
