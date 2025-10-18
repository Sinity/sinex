# Search Service

`SearchService` fronts the indexing pipeline and query execution path. It
normalises user intent, executes queries against the backing search index, and
returns hydrated results ready for gateways or satellites.

Responsibilities:

- Translate high-level `SearchQuery` structs into engine-specific requests.
- Post-process matches with context snippets and relevance metadata.
- Coordinate with ingestion jobs to keep the search index up to date.

The broader discovery workflow is covered in
`docs/architecture/UserInteraction_And_Query_Architecture.md`.
