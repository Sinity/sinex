# Knowledge Boundaries

Plaintext notes, documents, typed records, living documents, entities,
relations, artifacts, and context packs are adjacent but not interchangeable.
Sinex should not recreate markdown-folder PKM chores, and it should not replace
that with vague graph-first ontology.

This record defines the layer model and decision rules.

## Layer Model

| Layer | Authority | Examples | Not for |
| --- | --- | --- | --- |
| Source material | Original bytes/files/logs/databases. | Markdown notes, Taskwarrior exports, hledger journals, raw logs, chat archives. | Query-optimized semantic truth. |
| Material interpretation | Parser outputs anchored to source material. | `document.parsed`, chunks, task events from authoritative declarations, health intake rows. | Hidden schema in plaintext. |
| Domain projections | Rebuildable current/read models over typed events. | task state, health timelines, finance balances, project state. | Canonical replacement for event stream. |
| Derived graph | Evidence-backed entities, relations, claims. | person/project/entity links, candidate merges, extracted relations. | Owning task/finance/health lifecycle. |
| Workspace artifacts | Saved work surfaces and projections. | context packs, reports, semantic diff reports, living document snapshots. | Source material unless explicitly exported and re-ingested. |

The default flow is:

```text
source material -> material interpretation -> typed events/documents/chunks
typed events -> domain projection
typed events/documents/chunks -> derived graph
events/documents/graph/query results -> workspace artifact
```

Reverse flow is explicit import/export, not implicit authority.

## Decision Rules

### Plaintext Template vs Typed Events

Use plaintext templates when:

- humans need a low-friction input surface;
- the content is still exploratory prose;
- the parser can preserve the original source material;
- ambiguous extraction can route through proposals.

Use typed events when:

- the shape recurs;
- queries need stable fields;
- lifecycle or corrections matter;
- exports/imports need parity;
- privacy policy needs field-level treatment.

Do not let a template become hidden schema. If consumers depend on a field, make
that field part of a typed event or projection contract.

### Document Chunking vs Domain Parser

Use document/chunk records when:

- prose search/retrieval is the main need;
- the source is mostly narrative or mixed content;
- domain extraction is uncertain or partial;
- later reinterpretation should remain possible.

Use a domain parser when:

- rows/entries have stable occurrence identity;
- the domain has typed payloads;
- repeated queries need structured fields;
- direct canonical interpretation is justified by source authority.

The same source material can produce both document chunks and typed events, but
their authority must be named.

### Entity/Relation Graph

Use graph relations when:

- relation evidence can be traced;
- a consumer needs cross-domain traversal;
- merge/supersession policy exists;
- confidence/judgment is represented for uncertain links.

Do not use the graph to own domain lifecycle. A task is not "done" because a KG
edge says so; a task reducer owns task status.

### Workspace Artifacts

Artifacts are saved work products or projections:

| Artifact | Canonicality |
| --- | --- |
| context pack | Saved evidence bundle, not canonical event by default. |
| semantic diff report | Experiment artifact; may justify promotion. |
| report | Derived view; source events remain canonical. |
| living document snapshot | Working surface over events/materials; not universal knowledge substrate. |

Artifacts can become source material only through explicit export/import or a
dedicated lifecycle event that records the new material.

## Domain Classifications

| Domain | Classification |
| --- | --- |
| Drug/health logs | Typed health/self-observation events with protected raw material. Plaintext is an input surface; ambiguous extraction becomes proposals. KG can link substances/effects later, but does not own the domain. |
| Finance/hledger | Either hledger remains authoritative staged material, or Sinex finance events become canonical and hledger is a projection export. Do not keep both as silent competing ledgers. |
| Knowledgebase/raw logs | Preserve notes/logs as source material and document chunks. Extract stable tasks/health/decisions/declarations into typed events or proposals. Keep weak entities/relations as graph candidates until evidence and consumers justify promotion. |
| Tasks | Typed lifecycle events plus reducer projection. Notes and Taskwarrior exports are input/adapters, not lifecycle owners. |
| AI session archives | Structured session/message/tool-call events plus document chunks for long text. Context packs are projections over these events. |
| Living documents | Event-backed work surfaces/snapshots. They do not replace source material, typed domain records, or graph evidence. |

## Cross-References

| Issue | Boundary |
| --- | --- |
| #356 | Living documents need this layer boundary before becoming a primitive. |
| #1075 | Raw-log parsing should classify bullets into document chunks, typed records, or proposals by these rules. |
| #1087 | Entity/relation activation should stay behind evidence-backed graph semantics. |
| #1095 | Context packs are workspace artifacts, not canonical facts. |
| #1100 | Parser/source details should choose document versus typed-domain interpretation explicitly. |
| #1107 | Tasks are typed lifecycle records and reducer projections. |
| #1108 | Health logs are privacy-heavy typed observations/interventions. |
| #1113 | Declarations and conceptual time turn selected prose assertions into typed events. |

## Boundaries

- Do not make markdown folders the primary database.
- Do not make the knowledge graph the primary product by assertion.
- Do not make context packs or living documents canonical by default.
- Do not require plaintext templates for structured entry.
- Do not discard original source material when creating typed interpretations.
- Do not open implementation issues from this doctrine unless the domain has
  concrete source shape, payloads, privacy policy, and fixtures.
