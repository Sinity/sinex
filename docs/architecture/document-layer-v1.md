# Document Layer v1 — Minimum Honest Contract

This document closes the design question tracked in [`#692`][issue-692]: what
is the smallest document layer sinex can commit to that unblocks at least one
downstream consumer without prejudging the rest of the document/PKM/embedding
program?

`#692` exists because [`#332`][issue-332] has gated entity extraction
(`#331`/`#399`), embeddings (`#477`), semantic search (`#478`), media/document
(`#358`), comms ingestors (`#461`/`#466`), and the PKM vault (`#479`) for
months. None of those consumers can land until "what is a document in sinex"
has a bounded answer that respects the provenance model.

This is a v1 contract. It is opinionated, narrow, and explicit about what it
does not do. Anything outside the listed scope is a v2 question, not a
near-term TODO.

[issue-692]: https://github.com/Sinity/sinex/issues/692
[issue-332]: https://github.com/Sinity/sinex/issues/332

## Problem

### What exists today

There is already an event payload, an ingestor, and a stage-as-you-go path,
but they only describe **a file landing as source material**, not **a
document being a queryable text unit**.

- `crate/lib/sinex-primitives/src/events/payloads/document.rs:7-15` defines a
  single payload — `DocumentIngestedPayload { file_path, source_material_id,
  size_bytes, mime_type, encoding }`. There is no chunk, no extraction
  version, no body content.
- `crate/nodes/sinex-document-ingestor/src/lib.rs:567-623` stages the file as
  source material via `stage_material_from_file` and emits exactly one
  `document.ingested` event per file with `from_material(material_id)`. The
  event records that bytes exist; it does not interpret them.
- `crate/nodes/sinex-document-ingestor/src/lib.rs:756-765` explicitly disables
  continuous ingestion. The ingestor is a managed snapshot scanner.
- `crate/lib/sinex-primitives/src/events/payloads/` contains no chunk
  payload, no parsed-document payload, no extraction-output payload. A grep
  for `DocumentParsed`/`DocumentExtracted` returns only `document.rs` itself.
- `crate/lib/sinex-schema/src/schema/` has no `documents` or
  `document_chunks` table. `entities.rs`, `embeddings.rs`,
  `source_materials.rs`, etc. are present, but documents have no relational
  surface at all.

### What is broken

The current shape is "document = source material with a MIME type." That is
not a document layer. Specifically:

1. **No text-addressable unit.** Embeddings (`#477`), entity extraction
   (`#399`), and tags want to operate on chunks of *text*, not on a 25 MiB
   PDF blob. Today there is nowhere to put a chunk and nothing to point at.
2. **No deterministic identity.** Every consumer needs a stable
   `document_id` so that re-running extraction does not multiply rows. The
   current `material_id` (UUIDv7) is unique per stage, not per logical
   document, and a logical document may span multiple ingestions of the same
   file (e.g., re-scan after edit).
3. **No extraction version.** When the chunker changes, downstream
   embeddings and entities have no signal to invalidate. `#477` cannot land
   safely without one.
4. **No corpus to feed `#399`.** Entity extraction needs non-empty input on
   real text, with provenance back to source material. Today every other
   ingestor produces structured small-payload events (file events, shell
   commands, window focus) — none is a "block of human text."

### What this design fixes

The minimum honest contract is: **two corpora, one document table, one
chunk table, two synthesis events, one downstream consumer wired**. That is
sufficient to unblock `#399` and to keep the embedding/search work
(`#477`/`#478`) on a tractable foundation.

## Decision

Sinex adopts a synthesis-driven document layer. Source bytes remain pinned
to material provenance (the existing `document.ingested` event is kept
unchanged). A new automaton — the **document parser** — emits
synthesis-provenance events that materialize `core.documents` and
`core.document_chunks` rows. Re-extraction is replay: archive the synthesis
events, re-run the parser, get the same `document_id`.

There is no `documents` projection populated by application code outside the
event pipeline. The relational tables are projections of the synthesis
events, in the same way `core.entities` is a projection of entity events
(`crate/lib/sinex-schema/src/schema/entities.rs:1-9`).

## Scope (what v1 ships)

v1 supports four operations, no more:

1. **Parse**: turn a registered `source_material_id` (or, for terminal
   output, a tuple of source events) into one `document.parsed` event plus
   `N` `document.chunked` events.
2. **Persist**: project those events into `core.documents` and
   `core.document_chunks`.
3. **Re-extract**: replaying a `document.parsed` event archives the prior
   document + chunks and regenerates them with the same `document_id`. New
   `extraction_version` invalidates downstream.
4. **Read**: existing gateway `events.query` paths plus a single
   `documents.get(document_id)` SQL helper. No search, no full-text, no
   embedding lookup.

### Two corpora, no more

| Corpus | Trigger | Chunking | Document scope |
|--------|---------|----------|----------------|
| Dendron markdown | `document.ingested` for files under a configured Dendron vault root with `.md` extension | Paragraph (split on `\n\n+`, drop empties) | One document per `.md` file |
| Terminal command output | `command.canonical` event from `sinex-terminal-command-canonicalizer` carrying captured stdout/stderr | Line-group (split on blank line, falling back to whole-output if no blanks) | One document per command invocation |

These are the *only* two paths in v1. Browser pages, AI chat sessions,
emails, OCR'd images, PDFs, DOCX, and anything else stay out (see
**Non-goals**).

### Concrete event types

| Event type | Source | Provenance | Purpose |
|------------|--------|------------|---------|
| `document.parsed` | `document-parser` | Synthesis (`from_parents([source_event_id])`) | One per document; carries `document_id`, kind, extraction_version, and the structured side data (frontmatter, wikilinks for Dendron; exit code, command for terminal) |
| `document.chunked` | `document-parser` | Synthesis (`from_parents([document_parsed_event_id])`) | One per chunk; carries `document_id`, chunk index, byte offsets into source material, and chunk text |

Both events are emitted by a single new automaton crate
(`crate/nodes/sinex-document-parser`) modelled on
`sinex-terminal-command-canonicalizer`. It is a `TransducerNode`: 1:1 input
→ document, with chunks emitted in the same dispatch.

## Non-goals (explicit)

These are out of v1. Listing them here is the contract — re-opening any of
them requires a new design document, not a "while we're here" PR.

- **Full-text search.** No `pg_trgm` / `tsvector` index on chunks in v1.
  `documents.search()` is not part of the gateway surface.
- **OCR / image / audio / video.** v1 is text-only by construction.
- **PDF, DOCX, HTML, EPUB.** The MIME allowlist in
  `DocumentIngestorConfig::default()` keeps mentioning these; v1 does not
  extract text from any of them. The ingestor will continue to stage them
  as source material (no behavior change), but the parser will skip them.
- **Embeddings.** No `event_embeddings` row written from the parser. `#477`
  consumes `document.chunked` events — that is its problem, not v1's.
- **Semantic chunking.** Paragraph and line-group splitters only. No token
  budgeting, no sliding windows, no overlap, no LLM summarization. `#358`
  may revisit this.
- **Cross-document linking beyond declared wikilinks.** Wikilinks in
  Dendron frontmatter/body are stored as structured side data on
  `document.parsed`. They are *not* resolved to entity references in v1.
- **Markdown rendering / citations / footnotes / heading hierarchy.** The
  parser emits text spans with byte offsets. It does not produce an AST,
  HTML, or any "rendered" view.
- **LLM-based extraction.** No model calls anywhere in the parser.
- **Bidirectional vault sync.** Read-only.
- **Privacy policy beyond inheriting source-unit privacy tier.** The
  parser runs the chunker output through `privacy::engine()` exactly once
  (see **Privacy boundary**). It does not introduce new policy primitives.
- **Version diffing.** Re-extraction archives and replaces. No
  per-chunk diff is computed or stored.
- **A `documents` query in the gateway.** v1 ships a single SQL helper for
  consumers; the gateway-level RPC surface is a v2 question.

## Event taxonomy

### `document.parsed`

```text
DocumentParsedPayload {
    document_id: Uuid,           // UUIDv5(NS_DOCUMENTS, source_unit || natural_key)
    kind: DocumentKind,          // DendronMarkdown | TerminalOutput
    natural_key: String,         // vault-relative path, or `command_event_id` string
    extraction_version: u32,     // bump invalidates downstream; v1 ships at 1
    chunk_count: u32,            // for sanity-checking projection completeness
    text_byte_len: u64,          // total bytes of extracted text (post-redaction)
    side_data: serde_json::Value,// kind-specific structured fields, see below
}
```

Provenance: synthesis. `from_parents([parent_event_id])`.

- For Dendron: `parent_event_id` is the `document.ingested` event ID;
  `side_data` is `{ "frontmatter": {...}, "wikilinks": ["[[name]]", ...],
  "title": "..." }`.
- For terminal output: `parent_event_id` is the `command.canonical` event
  ID; `side_data` is `{ "command": "...", "exit_code": 0, "shell": "zsh" }`.

### `document.chunked`

```text
DocumentChunkedPayload {
    document_id: Uuid,           // matches the parent document.parsed
    chunk_index: u32,            // 0-based, monotonic, dense
    text: String,                // post-privacy-redaction chunk content
    byte_offset_start: u64,      // anchor into the *extracted text*, not source
    byte_offset_end: u64,
    source_anchor_start: Option<u64>, // raw source-material offset (Dendron only)
    source_anchor_end: Option<u64>,
}
```

Provenance: synthesis. `from_parents([document_parsed_event_id])` — the
chunk is derived from the document, not directly from source bytes.

`source_anchor_*` are populated for Dendron (the parser walks byte offsets
through paragraphs of the underlying file). They are `None` for terminal
output (see **Replay & idempotency** for why).

## Schema impact

Two new tables in the `core` schema. Both are projections of the synthesis
events above; both can be rebuilt by replaying the parser against archived
synthesis. No CAs, no materialized views in v1.

### `core.documents`

| Column | Type | Notes |
|--------|------|-------|
| `id` | `uuid` PRIMARY KEY | The deterministic `document_id` (UUIDv5) |
| `kind` | `text` NOT NULL | CHECK in `('dendron_markdown', 'terminal_output')` |
| `natural_key` | `text` NOT NULL | Vault-relative path or command event id |
| `parsed_event_id` | `uuid` NOT NULL | Most recent `document.parsed` event |
| `extraction_version` | `int4` NOT NULL | Tracks chunker schema version |
| `chunk_count` | `int4` NOT NULL | Denormalized cardinality |
| `text_byte_len` | `int8` NOT NULL | Sum of chunk lengths post-redaction |
| `side_data` | `jsonb` NOT NULL | Per-kind structured fields |
| `created_at` | `timestamptz` NOT NULL DEFAULT `now()` | First-seen by projection |
| `updated_at` | `timestamptz` NOT NULL DEFAULT `now()` | Touched on re-extraction |

Indexes:
- `(kind, natural_key)` UNIQUE — at most one live document per natural key.
- `(parsed_event_id)` — replay/audit lookup.

CHECK constraints:
- `extraction_version >= 1`
- `chunk_count >= 0`

### `core.document_chunks`

| Column | Type | Notes |
|--------|------|-------|
| `document_id` | `uuid` NOT NULL | FK → `core.documents(id)` ON DELETE CASCADE |
| `chunk_index` | `int4` NOT NULL | 0-based |
| `text` | `text` NOT NULL | Redacted chunk content |
| `byte_offset_start` | `int8` NOT NULL | Into extracted text |
| `byte_offset_end` | `int8` NOT NULL | Exclusive |
| `source_anchor_start` | `int8` | Nullable (terminal corpus does not have it) |
| `source_anchor_end` | `int8` | Nullable |
| `chunked_event_id` | `uuid` NOT NULL | The `document.chunked` event row |

Primary key: `(document_id, chunk_index)`.

Indexes:
- `(chunked_event_id)` — replay/audit lookup.

CHECK constraints:
- `byte_offset_end >= byte_offset_start`
- `(source_anchor_start IS NULL) = (source_anchor_end IS NULL)`
- `source_anchor_end >= source_anchor_start` when present.

### Strict-diff coverage

Both tables, indexes, and named CHECKs are added through
`crate/lib/sinex-schema/src/schema/documents.rs` and registered in
`apply.rs`. They become part of the strict-diff scope so drift between the
declarative schema and the live DB surfaces in
`schema-strict-diff` exactly as the table in `schema_design.md` describes.

This satisfies the **Strict_diff detects schema drift** AC item directly.

## Storage strategy

The document layer reuses the existing material/blob infrastructure. It does
not introduce a new storage tier.

| Source | Where bytes live | Why |
|--------|------------------|-----|
| Dendron markdown | Already in `raw.source_material_registry` via `sinex-document-ingestor` (today). The parser does not duplicate the bytes. | Source bytes remain authoritative; chunks store the *redacted text view* in `core.document_chunks.text`. |
| Terminal command output | Already in the captured `command.canonical` event payload. The parser does not stage a new material. | Treating each command output as its own source material would amplify material-frame traffic for tiny records — exactly the pattern that #338-class deployment work pushed back on. |

**Inline-vs-blob threshold for chunks:** chunk text lives inline in the
`text` column. Maximum chunk size is 64 KiB before the parser splits a
paragraph at the next sentence boundary (or hard 64 KiB if no boundary).
Any chunk that would exceed 64 KiB after splitting is dropped with a
warning event — v1 does not handle pathological single-paragraph megabyte
text. This is an explicit budget; not a TODO.

**Document-level ceiling:** the parser refuses to produce a document where
total extracted text exceeds 4 MiB. Such files are skipped with a warning;
they remain in `raw.source_material_registry` so a v2 parser can revisit
them. This caps `core.document_chunks` row width at a predictable shape
during v1 operation.

## Privacy boundary

Privacy redaction happens **once**, in the parser, **before chunking**.

Both corpora go through `privacy::engine().process(text, context)` with the
appropriate `ProcessingContext`:

- Dendron markdown → `ProcessingContext::Document`
- Terminal output → `ProcessingContext::Command`

Implications:

1. The chunk text stored in `core.document_chunks.text` is post-redaction.
   This is the canonical text downstream consumers see.
2. The byte offsets in `document.chunked.byte_offset_*` are into the
   **post-redaction** text, not the raw source material. This matters for
   replay (see below).
3. Source bytes in `raw.source_material_registry` remain unredacted by
   construction — the privacy policy applies at the synthesis boundary, not
   at the material boundary, consistent with the model in the existing
   document ingestor (`crate/nodes/sinex-document-ingestor/src/lib.rs:592-605`).
4. The parser does not introduce new privacy strategies. It uses the
   existing engine surface. v1 explicitly inherits source-unit privacy
   tier; the AC item *"inheriting source-unit privacy tier"* maps directly
   to "we run the engine with the source's normal context."
5. If `privacy::engine()` errors, the parser fails the synthesis emit (no
   document or chunks land). A redaction-broken document is not honest
   output. This matches `redact_metadata` in
   `crate/nodes/sinex-document-ingestor/src/lib.rs:665-674`.

The Dendron source-anchor offsets (`source_anchor_start/end`) are
pre-redaction byte offsets into the source file. They are stored so a v2
consumer can correlate a redacted chunk back to its original location for
audit. They are never used to read the unredacted bytes through the document
layer surface.

## Ingestion path

### Dendron path

```
filesystem watch (existing sinex-fs-ingestor)
   → file.created/modified events for *.md under vault root
   → sinex-document-ingestor scan (existing)
   → document.ingested (material provenance, existing payload)
   → sinex-document-parser (NEW)
     • subscribes to document.ingested
     • filters: kind detection (Dendron vault root prefix + .md extension)
     • reads source material bytes via SourceMaterialRepository
     • runs frontmatter + wikilink + paragraph extraction
     • emits document.parsed (synthesis)
     • emits document.chunked × N (synthesis)
   → ingestd projection writer (NEW)
     • on document.parsed: upsert core.documents
     • on document.chunked: insert core.document_chunks
```

The Dendron vault root is a config field on the parser node, not on the
ingestor. The ingestor stays kind-agnostic.

### Terminal path

```
sinex-terminal-ingestor → shell.command (existing)
sinex-terminal-command-canonicalizer → command.canonical (existing,
   carries the captured stdout/stderr text)
   → sinex-document-parser (NEW)
     • subscribes to command.canonical
     • filters: presence of non-empty captured output
     • runs line-group chunking on the canonicalized output
     • emits document.parsed + document.chunked (synthesis)
   → ingestd projection writer (same as Dendron)
```

### Where v1 expands the existing ingestor

Nothing changes in `sinex-document-ingestor`. The v1 expansion is the new
crate `sinex-document-parser`, modelled on
`sinex-terminal-command-canonicalizer`. Adding parsing logic to the
ingestor would conflate two concerns: bringing bytes into source-material
registry (material provenance, ingestor) versus interpreting bytes as a
queryable text unit (synthesis provenance, automaton). The existing crate
boundary (`crate/nodes/sinex-document-ingestor/src/lib.rs:174-770`) is
correct; v1 builds alongside, not inside.

The projection writer (the side that turns `document.parsed` /
`document.chunked` into rows in `core.documents` / `core.document_chunks`)
lives in `sinex-ingestd`, next to the existing batch insert routing
described in the architecture event-lifecycle map. It uses the QueryBuilder
path, not COPY: chunk volume per document is small (typical Dendron note <
20 chunks; typical command output < 5), and the synthesis provenance forces
the REPEATABLE READ + QueryBuilder path anyway per the routing rule.

## Replay & idempotency

### Deterministic `document_id`

`document_id = UUIDv5(NS_DOCUMENTS, source_unit || "/" || natural_key)`
where:
- `NS_DOCUMENTS` is a fixed UUID constant added in
  `sinex_primitives::ids` (a new constant; not a runtime-derived value).
- `source_unit` is `"dendron"` or `"terminal"`.
- `natural_key` is the vault-relative path for Dendron, the
  `command.canonical` event ID stringified for terminal.

Replaying parser logic against the same source produces the same
`document_id`. This satisfies the **Replay-safe: re-running extraction on
same source material produces same `document_id`** AC item directly.

### Replay flow

Re-extraction follows the existing replay model
(`docs/architecture/historical-backfill-runtime-plane.md` patterns + the
provenance overview in `crate/lib/sinex-node-sdk/docs/provenance.md`):

1. Operator triggers replay scoped to `document.parsed` /
   `document.chunked` events for a given `document_id` (or a kind, or all).
2. Archive cascade moves the matching events into `audit.archived_events`.
3. The projection writer in ingestd reacts to the archive event by
   deleting the `core.documents` / `core.document_chunks` rows pointed at
   by `parsed_event_id` / `chunked_event_id`. (FK ON DELETE CASCADE
   handles chunks if the document row is removed first.)
4. The parser's NATS scan command re-runs against the same parents.
5. New synthesis events arrive with the same `document_id` (UUIDv5 is
   deterministic) but new event IDs.
6. The projection writer upserts `core.documents` (PK = `document_id`),
   inserts new `core.document_chunks` rows.

Bumping `extraction_version` triggers the same flow: a parser at version
`N+1` overwrites the live document. Downstream consumers (entities,
embeddings) key off `(document_id, extraction_version)` and invalidate
themselves.

### Anchor-byte semantics for non-byte-stream documents

The Dendron parser produces `source_anchor_*` because Dendron documents
*are* byte streams (a single `.md` file). The chunker walks paragraphs and
preserves source offsets.

Terminal output is **not** a byte stream document for v1's purposes:

- The "source" is a sequence of events (`shell.command` →
  `command.canonical`), not a registered material file. The
  `command.canonical` payload carries the canonicalized text inline.
- Re-extraction needs to be a function of the parent event payload, not of
  bytes-in-a-file. UUIDv5 over the parent event ID gives the deterministic
  `document_id`; no byte anchor into source material is meaningful.
- `source_anchor_*` is therefore `NULL` for terminal chunks. The
  `byte_offset_*` columns continue to point into the redacted document
  text, which is sufficient for in-document chunk navigation.

This is not a deferred feature. It is the chosen semantics: terminal
documents anchor to a parent event, not a byte range. If a future corpus
arrives that *is* a multi-event byte stream (e.g., a streamed log), it
gets a v2 design — not a leaky generalization here.

### Idempotency guard

The projection writer uses `INSERT ... ON CONFLICT (id) DO UPDATE` for
`core.documents` and a delete-then-insert for chunks (scoped to
`document_id`). Duplicate delivery of the same `document.parsed` is
absorbed; duplicate `document.chunked` for the same
`(document_id, chunk_index)` is absorbed by replacing the row.

### Downstream consumer wired in v1

The AC requires "one downstream consumer wired." v1 picks the **tag
automaton** path: a small `sinex-document-tagger` automaton subscribes to
`document.parsed` for the Dendron kind, reads `side_data.frontmatter.tags`,
and emits `tagged_items` rows via the existing tag pipeline
(`crate/lib/sinex-schema/src/schema/...` covers `tags` /
`tagged_items`). This is concrete proof that the layer can drive a
non-trivial consumer.

The first-pass entity extractor (`#399`) is *not* wired in v1; it is the
first v2 consumer and uses the same `document.parsed` /
`document.chunked` events as input. Wiring it now would re-couple v1 to
the entity-extraction design and defeat the "minimum honest" framing.

## Open questions

The following are intentionally not settled by this design. Future work
must answer them — they are not implementation details.

1. **Dendron vault discovery.** v1 takes the vault root as a single
   parser config field. Multi-vault setups, nested vaults, and
   `dendron.yml` introspection are not handled. Closed by: a v2 parser
   config issue.
2. **Frontmatter schema.** `side_data.frontmatter` is whatever YAML the
   Dendron file declares, parsed as `serde_json::Value`. There is no
   schema. Some users will write malformed YAML; the parser captures the
   error message in a `parse_warnings` array on `side_data` and proceeds
   with empty frontmatter. Whether to promote a strict frontmatter schema
   in v2 is open.
3. **Wikilink resolution.** Wikilinks are stored as raw strings
   (`["[[note-a]]", "[[note-b#heading]]"]`). Whether they resolve to
   entity IDs, to other `document_id`s, or stay opaque is a v2 question
   gated by `#399`.
4. **Terminal output truncation.** The parser inherits whatever truncation
   the canonicalizer already applies. If `command.canonical` truncates at
   N KiB, the document does too. Defining a separate per-document
   truncation policy is open.
5. **Re-chunking on `extraction_version` bump.** v1 archives and
   reinserts the whole document. Whether the projection writer should
   detect "no change in chunks" and avoid downstream invalidation
   ("stable extraction") is deferred to whichever consumer first
   demonstrates a real cost.
6. **Operations-log entries.** Whether parser replays show up in
   `core.operations_log` alongside other replay/restore actions is a
   follow-up: v1 logs through standard tracing only.
7. **Privacy context for terminal output documents.** v1 uses
   `ProcessingContext::Command`. Whether long captured output (e.g.,
   `git log -p`) should use `Document` instead (different rule set) is
   open and tracked as a follow-up only if v1 redaction proves wrong on
   real traffic.

## Acceptance-criteria mapping

Tracking the AC list in `#692` against this design:

| AC item | Where addressed |
|---------|-----------------|
| Schema for `core.documents` + `core.document_chunks` | "Schema impact" — full column lists, indexes, CHECKs |
| Two ingestion paths: Dendron + terminal-output | "Scope" → corpora table; "Ingestion path" — concrete dataflows |
| Replay-safe: same source material → same `document_id` | "Replay & idempotency" — UUIDv5 derivation, replay flow |
| One downstream consumer wired (tag automaton or entity extractor) | "Replay & idempotency" → "Downstream consumer wired in v1" — tag automaton |
| Strict_diff detects schema drift | "Schema impact" → "Strict-diff coverage" |
