# Content Service

`ContentService` wraps the annex blob manager so callers can stage, fetch, and
verify binary payloads without touching annex internals.

## API Surface

| Method | Description |
|--------|-------------|
| `store_content(bytes, filename, content_type, source)` | Persists payload bytes through `BlobManager::ingest_from_bytes` and returns the annex key. |
| `retrieve_content(annex_key)` | Reads the stored bytes for the given key. |
| `get_content_metadata(annex_key)` | Fetches `BlobMetadata` for inspection or provenance. |
| `verify_content(annex_key)` | Runs checksum verification via the annex backend. |

All methods emit `SinexError::service(...)` with the failing annex operation in
their context, enabling the gateway to forward helpful errors.

For storage topology, replication, and annex configuration details, see
`docs/architecture/Core_Architecture.md` and `crate/lib/sinex-satellite-sdk/doc/annex.md`.
