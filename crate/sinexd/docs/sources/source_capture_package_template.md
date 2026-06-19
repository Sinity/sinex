# Source/Capture package template

Every new Sinex source/capture mode should be a complete, testable unit that
fits the existing **SourceDefinition → InputShapeAdapter → MaterialParser →
admission** model. A runnable mode is a source definition plus an adapter,
parser, event/admission contracts, disclosure policy, resource behavior,
coverage, operations, and fixtures. It is not changes scattered across
unrelated registries, and not a bespoke private pipeline. This template keeps
source/capture issues uniform so reviewing one package mode teaches you how to
review the next.

Use it together with:

- [`staged_source_parser_substrate.md`](staged_source_parser_substrate.md) — the
  adapter/parser substrate a package mode plugs into.
- [`integration_authority.md`](integration_authority.md) — which adapter
  authority category a source belongs to.
- [`evidence_lanes.md`](evidence_lanes.md) — occurrence vs snapshot material
  roles.
- [`package_completeness_gate.md`](package_completeness_gate.md) — the #1792
  executable report and strict-gate surface.

Generate the first reviewed Rust draft for a package/mode directly from the
compiled completeness report:

```bash
sinexd export-source-skeleton --package-id terminal.atuin-history --mode-id terminal.atuin-history
```

The emitted file is a review starting point. It deliberately contains a
package-specific `compile_error!` until parser, disclosure, fixtures,
coverage/debt, operations, and deployment fields are completed.

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
> Per-field disclosure policy (distinct from the field-level
> `ProcessingContext` / `SensitivityHint` hints that exist today) and migration
> of the remaining manual registrations are tracked in **#1727 (SNX-41)**. The
> required-field list below is stable regardless of which authoring path you
> choose.

## Required fields (every source/capture package issue)

Fill in each field for every runnable mode. Proposed modes may be present when
their caveats and owners are explicit.

```text
Package identity:         stable package id/family, e.g. terminal.activity
Mode identity:            stable mode id, e.g. terminal.atuin-history
External occurrence:      what real-world datapoint this observes
Input materials:          what source material is registered (file, SQLite DB,
                          IPC stream, API page) and its material policy owner
Adapter shape:            which InputShapeKind / InputShapeAdapter + cursor type
Parser:                   the MaterialParser that interprets records
Event contracts:          EventContract / payload schema refs for emitted events
Admission policy:         AdmissionPolicy/AdmissionOutcome behavior for malformed,
                          duplicate, rejected, quarantined, and admitted records
Timestamp policy:         how ts_orig is chosen (RealtimeCapture / IntrinsicContent
                          / InferredMtime / …)
Occurrence policy:        material anchors plus object-level occurrence,
                          dedup/equivalence, and scope keys (never event id alone)
Disclosure policy:        field/material/view/export/log/completion/DLQ behavior
                          under operator-owned policy; no hidden censorship
Resource behavior:        queue/batch/pressure/degraded behavior and operator
                          actions; no silent semantic changes under pressure
Coverage/debt views:      expected stale/gap/no-new-data/deferred/rejected states
Operations:               install/check/pause/resume/drain/replay/rebuild/redact
Fixtures/tests:           raw sample → adapter output → parsed candidate →
                          admitted event/material → disclosure/export view → a
                          negative validation case
View/query examples:      how emitted events and coverage/debt rows are read back
Sinnix binding example:   register_source_runtime_binding! + NixOS binding, only
                          if the mode is host-specific
```

## Per-mode contract (definition of done)

A runnable mode is complete only when every field below is satisfied — not when
the code merely compiles:

```text
source id                 declared and registered (inventory lookup, no match arms)
binding config            typed config wired through RuntimeConfig
adapter                   InputShapeAdapter impl with an explicit cursor contract
parser                    MaterialParser producing ParsedEventIntent
emitted event types       EventPayload structs with registered schemas
occurrence key            object-level coordinates documented; not the event id
disclosure policy         per-field policy exercised by admission and views
source coverage behavior  stale/gap/no-data paths covered
fixtures                  the full raw→admitted→view chain plus a negative case
property tests            adapter cursor monotonicity + parser determinism
readiness view row        the source surfaces in a runtime readiness/coverage view
package completeness row  the #1792 report names remaining blocking fields or
                          reports the mode complete
```

## Package mode order

The `SourceDefinition` / `SourceMeta` derives are landed; per-field disclosure
coverage and the coherence / identity work (**#1727**, #1682, #1685) are still
in progress. Implement package modes in dependency order so each one exercises a
new capability against known data, with missing policy-owned fields surfaced by
the package-completeness row rather than hidden in prose:

1. `terminal.atuin-history` — proves the source-definition path with a simple,
   well-understood SQLite source.
2. `polylogue.session.external` — external AI-session metadata ingestion.
3. `browser.history` — SQLite snapshot + privacy-sensitive source coverage.
4. `notification.dbus` — a live-stream source with runtime-module state.
5. `email.mailbox` — staged-mailbox parser before any live Gmail/IMAP sync
   (umbrella #1469 carries the secrets/network/publication risk; implement one
   complete runnable mode at a time).

## Anti-patterns

- A source-specific private ingestion pipeline when a SourceDefinition can
  express it.
- Two authoritative abstractions for the same observation domain (e.g. a
  separate "live browser" taxonomy beside historical browser history).
- A single source-level privacy label standing in for per-field privacy.
- Encoding host-specific `/realm` or Sinnix paths in core crates or parser
  semantics — those belong in the Sinnix binding.
