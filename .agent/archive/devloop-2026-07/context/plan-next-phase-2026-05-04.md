---
created: "2026-05-04T00:00:00"
purpose: "Next-phase implementation plan targeting #733 document-layer v1 + P0 correctness hardening"
status: "active"
project: "sinex"
---

# Sinex Next-Phase Plan — Document Layer + Correctness Hardening

## Context

Session III's compiled-wigderson plan targeted ~31 issues in 4 phases. That plan deferred #733 because the design wasn't settled. Now #733's design is resolved (May 3 derived comment), foundation is shipped in #742, and it's the P0 that unblocks everything downstream (#331 entity extraction, #332 knowledge graph).

This plan is a tighter focus: ship the document layer (#733), then knock out the 4 P0 correctness hardening issues (#743/#755/#759/#762) in parallel. P1 items (#846 CLI, #1007 verification, #847 browser) follow after.

## Architecture of the Plan

**Principle:** Sequential for the SDK-breaking change (#733), then parallel for correctness issues. The document layer's `MultiOutputTransducerNode` SDK extension is a prerequisite that all subsequent work can use but doesn't block the existing correctness issues.

**4 phases, ~1-2 PRs each, focused on the issues user specified.**

---

## Phase 1 — #733 Document Layer v1 (sequential PR train)

**PR 1: SDK extension — MultiOutputTransducerNode** (~140 lines)

Files: `crate/lib/sinex-node-sdk/src/derived_node/output.rs`, `traits.rs`, `adapter/output.rs`, `lib.rs`

1. Add `event_type: Option<&'static str>` field to `DerivedOutput<T>` + builder
2. Add `MultiOutputTransducerNode` trait (process returns Vec<DerivedOutput<Output>>)
3. Add `MultiOutputTransducerWrapper` bridging to `Automaton`
4. Add `output_event_types()` to `Automaton` (default: `&[self.output_event_type()]`)
5. Update `build_event()` in adapter/output.rs to use per-output event_type when present
6. Re-export from `crate::derived_node`

**PR 2: Parser crate — sinex-document-parser**

New crate at `crate/nodes/sinex-document-parser/`:
- MultiOutputTransducerNode implementation
- Input: `document.ingested` → Output: `document.parsed` + N× `document.chunked`
- pulldown-cmark for markdown, identity for plaintext
- Dendron paragraph-split + terminal line-group chunking
- Frontmatter/wikilinks extraction
- Privacy redaction at chunker boundary
- 64 KiB/chunk + 4 MiB/document caps

**PR 3: Projection writer + wiring**

- ingestd projection writer: `document.parsed` → `core.documents` upsert, chunks → `core.document_chunks` insert
- Source-unit descriptor for `document-parser`
- Wire into sinex-process runner-pack (if it becomes an automaton) or as standalone service

**PR 4: Tag automaton + NixOS module**

- `sinex-document-tagger` automaton consuming `document.parsed` Dendron events
- NixOS module units for document-parser + document-tagger

**Verify:** Full event flow: document.ingested → document.parsed + document.chunked events → core.documents + core.document_chunks rows

---

## Phase 2 — P0 Correctness Hardening (parallel where possible)

### #755 F12 — SourceIdentifier newtype (Agent A)

Files: `crate/lib/sinex-primitives/src/domain.rs`, `acquisition_manager.rs`, `unified_node.rs`, `collect.rs`

1. Add `SourceIdentifier` newtype with `FromStr`/`Display`
2. Replace `#material=` string encoding/parsing at ~5 sites
3. Verify: `rg -n '#material=' crate/ --type rust` → 0

### #759 PR3 — Gateway color_eyre → SinexError (Agent B)

Files: `crate/core/sinex-gateway/src/handlers/`, `rpc_server.rs`, `replay_control/`

1. Replace ~32 `color_eyre::eyre` / `eyre!()` sites with `SinexError`
2. Convert `format!()` error messages to `.with_context()` chains
3. Verify: `rg 'use color_eyre' crate/core/sinex-gateway/` → 0

### #762 D1 — ReplayRepository extraction (Agent C)

Files: `crate/lib/sinex-db/src/replay/state_machine.rs`, `repositories/replay.rs`

1. Extract `ReplayRepository` from state_machine.rs (1734 lines)
2. Expose via `DbPoolExt::replay()`
3. Move SQL queries to repository struct, keep FSM logic in state_machine

### #762 D4 — Convert free functions to repository methods (Agent D)

Files: `crate/lib/sinex-db/src/integrity.rs`, `telemetry.rs`, `lifecycle.rs`, `audit.rs`

1. Convert ~37 free functions taking `&PgPool` to repo methods
2. `IntegrityRepository`, `TelemetryRepository`, `LifecycleRepository`
3. Use `pool.state()` / `pool.integrity()` pattern

---

## Phase 3 — P1 Items

### #846 CLI IA (2-3 days)
- Compact sinexctl IA checklist
- Renames + shared DTO

### #1007 Verification Surface
- Consolidate #806, #807, #809, #849
- Unified verification and demo surface

### #847 Browser Live Capture
- Full plan exists, medium effort
- WebExtension native messaging

---

## Phase 4 — Gated (need design before code)

### #331, #332 Entity/Search
- Research phase first
- Concrete design needed before implementation

---

## Dependency Graph

```
Phase 1 PR1 (SDK extension) [NO DEPS]
  └── Phase 1 PR2 (parser crate)
       └── Phase 1 PR3 (projection writer)
            └── Phase 1 PR4 (tag automaton + NixOS)

Phase 2 (correctness hardening) [NO DEPS — can run in parallel with Phase 1]
  ├── #755 F12 (SourceIdentifier)
  ├── #759 PR3 (gateway color_eyre)
  ├── #762 D1 (ReplayRepository)
  └── #762 D4 (free function conversion)

Phase 3 (P1 items) [after Phase 1-2 completion]
  ├── #846 CLI IA
  ├── #1007 verification
  └── #847 browser capture

Phase 4 (gated) [after Phase 1 for entity extraction foundation]
  └── #331, #332 entity/search
```

## Issues Closed

| Phase | Issues |
|-------|--------|
| 1 | #733 (document-layer v1) |
| 2 | #755 (F12), #759 (PR3), #762 (D1, D4) |
| 3 | #846, #1007, #847 |

## Verification Strategy

Per-PR: `xtask check -p <pkg>`, `xtask test -p <pkg>`
Post-phase: `xtask check --full`, `xtask test --workspace`
Before merge: `xtask ci postgres -- xtask ci workspace`
