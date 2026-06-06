# Document Layer

Status: implemented substrate. The v1 document parser and schema projection
landed under #332/#733; remaining work belongs to consumer surfaces such as
embedding and search UX, not to the old global design record.

The document layer turns source events into queryable document projections:

- `document.ingested` is a material-provenance source event.
- `document.parsed` is a derived event emitted by the document parser.
- `document.chunked` is a derived event emitted for each extracted text chunk.
- `core.documents` and `core.document_chunks` are rebuildable projections of
  those events, maintained by the schema trigger in
  `crate/sinex-schema/src/defs/documents.rs`.

## Current Scope

The parser currently handles two corpora:

| Corpus | Input event type | Chunking |
| --- | --- | --- |
| Dendron markdown | `document.ingested` | Paragraph chunks after frontmatter stripping |
| Terminal output | `command.canonical` | Line groups split on blank lines |

`DocumentParsedPayload` carries the deterministic `document_id`, corpus kind,
natural key, extraction version, chunk count, text byte length, and corpus
specific side data. `DocumentChunkedPayload` carries chunk text plus byte
offsets into the extracted text and optional source-material anchors.

The projection tables are not canonical truth. Events remain canonical; the
tables exist for retrieval, full-text search, fuzzy matching, and downstream
consumers that need relational document/chunk lookup.

## Ownership

- Event payload contract: `crate/sinex-primitives/src/events/payloads/document.rs`
- Parser automaton: `crate/sinexd/src/automata/document_parser.rs`
- Projection schema and trigger: `crate/sinex-schema/src/defs/documents.rs`
- Retrieval tests: `crate/sinex-db/tests/document_search_test.rs`
