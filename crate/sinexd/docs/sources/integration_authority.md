# Integration Authority

Status: design record for #1119.

Sinex integrates with sibling tools and external ecosystems through an explicit
authority category. The category decides whether Sinex owns the canonical fact,
interprets raw source material, mirrors another system for context, exports a
projection, or uses an external project only as a transitional parity oracle.

## Categories

| Category | Meaning | Sinex responsibility |
|---|---|---|
| `SourceMaterialOnly` | External data is raw evidence. | Stage material, apply privacy/admission policy, and interpret it through source parsers. |
| `EventNativeCanonical` | Sinex owns the domain facts. | Persist events and projections as the canonical personal record. |
| `FederatedCanonicalMirror` | Another system owns the canonical domain. | Mirror metadata or source-backed signals for search, joins, context packs, and traceability. |
| `ProjectionExport` | Sinex emits an external-compatible view. | Generate exports from Sinex-native projections without making the external format the ontology. |
| `TransitionalReference` | External project is a migration or parity oracle. | Stage generated artefacts, compare representative windows, and retire the bridge once Sinex-native output exists. |
| `BidirectionalAdapter` | Import and export both matter. | Require explicit conflict policy, proposal/judgment boundaries, and privacy policy before enabling writes. |

## Adapter Contract

Every durable adapter should record:

- `adapter_id` and `external_system`;
- authority category;
- import/source bindings and emitted event types;
- projection exports, if any;
- conflict policy for any write-back or bidirectional path;
- privacy policy reference;
- parity checks;
- retirement conditions for transitional or mirrored surfaces.

Adapters should use existing source-material, parser-job, event-intent,
projection, and parity surfaces before introducing a new runtime topology. NATS
is the useful external boundary only when an independently useful producer can
publish admitted event intents or material-staged signals without linking the
Rust node SDK.

## Examples

### Polylogue

Authority: `FederatedCanonicalMirror` for archive metadata, with an optional
`SourceMaterialOnly` path for raw provider exports.

Polylogue remains authoritative for normalized AI conversation archives,
message hashes, provider detection, renderers, and its MCP surface. Sinex may
mirror metadata-only conversation/session/work-event signals for joins with
terminal, filesystem, issue, project, and context-pack data. Raw conversation
text is not duplicated into event payloads by default; if Sinex needs native
AI-session interpretation, raw exports or rendered artefacts are staged as
source material and parsed by a dedicated source parser.

First implementation slice: #1122. A Polylogue-style producer should publish a
metadata-only event intent or material-staged signal through the admitted
envelope, preserve Polylogue IDs and content hashes, receive event-engine
confirmation/rejection, and avoid linking `sinexd` internals.

### Lynchpin

Authority: `TransitionalReference`.

Lynchpin is a personal dataset modelling prototype and parity oracle, not a
runtime dependency or canonical ontology for Sinex. Useful artefacts include
generated context packs, source-readiness reports, parity reports, and artefact
catalog snapshots. Sinex should stage those artefacts as evidence material or
parse them into readiness/caveat/parity records only while native source
bindings, parsers, context packs, readiness checks, and parity checks catch up.

Retirement condition: each covered source family has a Sinex-native source
binding/parser/projection or is explicitly out of scope; native context/readiness
outputs exist; parity checks pass for representative windows; no production
workflow has needed Lynchpin-generated reports for 30 days.

### hledger

Authority: `ProjectionExport` unless a specific finance source is staged as raw
evidence.

Sinex should keep event-native finance facts and generate hledger-compatible
serializations as exports. hledger syntax is an output format and parity target,
not the foundational finance ontology.

### Task Adapters

Authority: unresolved until task facts have a native proposal/judgment boundary.

Taskwarrior, Obsidian task blocks, or other task systems may become mirrors,
source-material imports, projections, or bidirectional adapters. Bidirectional
sync requires a conflict policy and must route changes through proposal/finalizer
semantics rather than silently overwriting external task state.

## Guardrails

- Do not absorb an external ecosystem wholesale because it is useful.
- Do not treat flat imports as authority-free data; record who owns the fact.
- Do not publish raw private text into durable NATS by default.
- Do not make transitional projects permanent runtime dependencies.
- Do not let an external serialization format become the canonical ontology by
  convenience.
- Do not enable bidirectional writes without conflict policy, privacy policy, and
  proposal/judgment semantics.
