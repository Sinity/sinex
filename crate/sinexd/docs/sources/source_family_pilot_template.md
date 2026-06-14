# Source-family pilot template

Every new Sinex source should be a small, testable unit that fits the existing
**SourceDefinition → InputShapeAdapter → MaterialParser → admission** model. A
source is *mostly a source definition plus an adapter, a parser, and fixtures* —
not changes scattered across unrelated registries, and not a bespoke private
pipeline. This template keeps each source issue uniform so that reviewing one
source teaches you how to review the next, and so each later source diff stays
small.

Use it together with:

- [`staged_source_parser_substrate.md`](staged_source_parser_substrate.md) — the
  adapter/parser substrate a pilot plugs into.
- [`integration_authority.md`](integration_authority.md) — which adapter
  authority category a source belongs to.
- [`evidence_lanes.md`](evidence_lanes.md) — occurrence vs snapshot material
  roles.

> **Authoring surface.** Prefer the Rust-native derives over hand-wiring the
> registration sites:
>
> - **`#[derive(SourceDefinition)]`** — fully declarative sources. One annotated
>   struct (`#[source_definition(...)]`) collapses all four sites:
>   `SourceContract`, `SourceRuntimeBinding`, the `register_source!`
>   adapter+parser factory, and a generated `MaterialParser` via the declarative
>   parser path (field attributes `#[source(...)]`, `#[privacy(...)]`,
>   `#[timestamp(...)]`, `#[occurrence_key]`, `#[event_dispatch(...)]`).
> - **`#[derive(SourceMeta)]`** — the imperative sibling. Collapses the three
>   registration sites but keeps your hand-written `impl MaterialParser`. Use it
>   when the parser needs logic the declarative DSL can't express (stateful
>   dedup, multi-line state machines, multi-event fan-out, custom timestamps).
> - **Manual `register_source_contract!` + `register_source_runtime_binding!`** —
>   the pre-derive form; still valid and used by many existing sources, but a new
>   source should reach for a derive first.
>
> Per-field privacy *tier* (distinct from the field-level `ProcessingContext` /
> `SensitivityHint` hints that exist today) and migration of the remaining manual
> registrations are tracked in **#1727 (SNX-41)**. The required-field list below
> is stable regardless of which authoring path you choose.

## Required fields (every source issue)

Fill in each field. Add *one* source definition and no extra architectural
nouns:

```text
Source family:            stable SourceId, e.g. terminal.atuin-history
External occurrence:      what real-world datapoint this observes
Input materials:          what source material is registered (file, SQLite DB,
                          IPC stream, API page) and its raw-material privacy class
Adapter shape:            which InputShapeKind / InputShapeAdapter + cursor type
Parser:                   the MaterialParser that interprets records
Emitted event types:      (EventSource, EventType) pairs, one payload per type
Timestamp policy:         how ts_orig is chosen (RealtimeCapture / IntrinsicContent
                          / InferredMtime / …) — see the temporal-ledger precedence
OccurrenceKey policy:     the (source_material_id, anchor_byte) coordinates, and
                          any object-level dedup/equivalence key (never the event id)
Privacy tier:             per-field privacy (NOT a single source-level label):
                          payload fields, source-material class, parser/admission
                          policy, and view/export rules
Source coverage:          expected behavior for stale / gap / no-new-data cases
Fixture data:             raw sample → adapter output → parsed candidate →
                          admitted event/material → privacy/export view → a
                          negative validation case
Admission tests:          property/contract tests over the above fixtures
View/query examples:      how the emitted events are queried back
Sinnix binding example:   register_source_runtime_binding! + NixOS binding, only
                          if the source is host-specific
```

## Per-pilot contract (definition of done)

A pilot is complete only when every field below is satisfied — not when the code
merely compiles:

```text
source id                 declared and registered (inventory lookup, no match arms)
binding config            typed config wired through RuntimeConfig
adapter                   InputShapeAdapter impl with an explicit cursor contract
parser                    MaterialParser producing ParsedEventIntent
emitted event types       EventPayload structs with registered schemas
occurrence key            object-level coordinates documented; not the event id
privacy tier              per-field annotations exercised by admission
source coverage behavior  stale/gap/no-data paths covered
fixtures                  the full raw→admitted→view chain plus a negative case
property tests            adapter cursor monotonicity + parser determinism
readiness view row        the source surfaces in a runtime readiness/coverage view
```

## Pilot order

The `SourceDefinition` / `SourceMeta` derives are landed; the per-field privacy
tier and the coherence / identity work (**#1727**, #1682, #1685) are still in
progress. Implement pilots in dependency order so each one exercises a new
capability against known data, deferring a field's policy detail to its tracking
issue rather than blocking the pilot:

1. `terminal.atuin-history` — proves the source-definition path with a simple,
   well-understood SQLite source.
2. `polylogue.session.external` — external AI-session metadata ingestion.
3. `browser.history` — SQLite snapshot + privacy-sensitive source coverage.
4. `notification.dbus` — a live-stream source with runtime-module state.
5. `email.mailbox` — staged-mailbox parser before any live Gmail/IMAP sync
   (umbrella #1469 carries the secrets/network/publication risk; decompose it,
   do not treat it as one patch).

## Anti-patterns

- A source-specific private ingestion pipeline when a SourceDefinition can
  express it.
- Two authoritative abstractions for the same observation domain (e.g. a
  separate "live browser" taxonomy beside historical browser history).
- A single source-level privacy label standing in for per-field privacy.
- Encoding host-specific `/realm` or Sinnix paths in core crates or parser
  semantics — those belong in the Sinnix binding.
