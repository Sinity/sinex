# Search Service

`SearchService` executes structured searches against `core.events`. It does not
maintain a separate index; instead it generates parameterised SQL on the fly and
returns lightweight DTOs for the gateway.

## API Surface

- `search_events(query: SearchQuery) -> Vec<SearchResult>`  
  - Filters by sources, event types, time ranges, and free-text payload matches
    (`ILIKE`).  
  - Orders results by `ts_ingest DESC`, applying limit/offset pagination.  
  - Produces an inline snippet for convenience (first match or first 150 chars).

`SearchQuery` and `SearchResult` are serializable structs designed for RPC.

For discovery UX guidance see
`docs/current/architecture/UserInteraction_And_Query_Architecture.md`.
