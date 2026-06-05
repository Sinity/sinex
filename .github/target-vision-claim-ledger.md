# Target-Vision Claim Ledger

Target-vision material is input evidence, not implementation truth. This
ledger process records which ideas have been promoted, which are still raw,
and which have been superseded so future sessions do not re-import stale
greenfield assumptions as current architecture.

This is deliberately lightweight. GitHub remains the implementation tracker;
the ledger only records claim state, evidence, and promotion gates.

## When To Add A Claim

Add a ledger row when a target-vision-derived idea is:

- likely to influence architecture, issue scope, naming, or proof obligations;
- attractive enough that future agents may rediscover and over-promote it;
- referenced by more than one issue, report, or source document;
- superseded by current implementation but still present in raw/reference prose.

Do not add every raw note bullet. Raw source files can stay raw unless they
start steering work.

## Claim Statuses

| Status | Meaning |
|---|---|
| `raw_idea` | Captured in raw/report material; not yet synthesized into a design claim. |
| `design_candidate` | Plausible current design idea, but not issue-ready. |
| `semantic_debt` | Valuable idea, but its concepts are not yet cleanly layered or proven. |
| `issue_backed` | GitHub issue exists and is the implementation/planning authority. |
| `implementation_ready` | Issue has enough scope, invariants, and verification to start. |
| `implemented` | Code/docs landed, but verification or closure evidence is incomplete. |
| `verified` | Implementation evidence exists and closure/proof obligations are recorded. |
| `superseded` | Replaced by a newer design, implementation, or issue family. |
| `rejected` | Deliberately not pursuing; rationale is recorded. |

## Ledger Row Format

Use this compact table shape in a target-vision claim ledger file, issue
comment, or design doc:

```markdown
| Claim ID | Claim | Status | Evidence source | Authority | Proof / promotion gate | Notes |
|---|---|---|---|---|---|---|
| TV-### | Short claim text | issue_backed | target-vision/reference/foo.md | #123 | xtask test ... | Supersedes raw/bar.md wording |
```

Fields:

- `Claim ID`: stable local ID, usually `TV-###`.
- `Claim`: one sentence; no full issue body duplication.
- `Status`: one of the statuses above.
- `Evidence source`: raw/report/reference file or issue comment where the idea came from.
- `Authority`: current source of truth: issue, PR, generated catalog, code path, or doc.
- `Proof / promotion gate`: what must be true before promotion to the next status.
- `Notes`: supersession, non-goals, or semantic-debt warning.

If a claim becomes implementation work, create or update a GitHub issue and
link the ledger row to it. Do not paste the full target-vision prose into the
issue; summarize the claim and link the evidence source.

## Promotion Gates

Promotion is a state change with evidence:

| From | To | Gate |
|---|---|---|
| `raw_idea` | `design_candidate` | A maintainer or agent synthesizes the claim into one sentence and identifies non-goals. |
| `design_candidate` | `issue_backed` | A GitHub issue records scope, authority, acceptance criteria or decision question, and source links. |
| `issue_backed` | `implementation_ready` | Blocking design questions are answered; verification plan is concrete. |
| `implementation_ready` | `implemented` | PR/commit lands the scoped change. |
| `implemented` | `verified` | Verification commands or generated proof artifacts are recorded in the issue/PR closure trail. |
| any active status | `semantic_debt` | The idea is useful but mixes layers or depends on unproven substrate. |
| any active status | `superseded` | Newer code, docs, or issues replace the claim. |
| any active status | `rejected` | Rationale says why not. |

## Seed Claims

These rows are the initial authority for recurring target-vision claims.

| Claim ID | Claim | Status | Evidence source | Authority | Proof / promotion gate | Notes |
|---|---|---|---|---|---|---|
| TV-001 | Bus-first/source-unit staged interpretation is the current ingestion spine; old per-source ingestor prose is historical unless explicitly re-promoted. | verified | `/realm/project/sinex-target-vision/reference/historical-architecture.md`; source-unit execution issues | #1054, #1126, #1223, #1225; `crate/sinexd/src/sources/source_units/` | Production-path source-unit tests and source-unit descriptors stay green. | Supersedes raw/reference wording that assumes one long-lived ingestor crate per source family. |
| TV-002 | Event source/type naming must be treated as semantic taxonomy, not ad hoc strings. | issue_backed | `/realm/project/sinex-target-vision/reference/design-intent.md`; event taxonomy work | #744 | Promote only through schema registry updates and call-site verification. | The old event-taxonomy/source-unit catalog docs were pruned as stale generated-catalog authority. |
| TV-003 | Staged export parsers are a family under the staged-source parser substrate, not isolated import scripts. | issue_backed | `/realm/project/sinex-target-vision/report/work-ahead.md`; parser backlog | #1070, #1054, `crate/sinexd/docs/sources/staged_source_parser_substrate.md` | Each parser needs occurrence identity, privacy handling, and parser/runtime tests. | Parser children should cite #1070 but close on their own evidence. |
| TV-004 | Thick knowledge graph ambitions must stay behind a thinner semantic assertion and provenance substrate. | semantic_debt | `/realm/project/sinex-target-vision/reference/knowledge-graph.md`; `/realm/project/sinex-target-vision/report/intelligence.md` | #1087, #1339 | Activate entity/relation automata with observed events and proof before expanding graph ownership. | Prevents horizon KG prose from overriding current event-native authority boundaries. |
| TV-005 | Broad embeddings are not authoritative until document/chunk evaluation and recorded model-effect replay policy exist. | semantic_debt | `/realm/project/sinex-target-vision/reference/embedding-pipeline.md` | #1076, #1063 | Require recorded model effects, cache/replay policy, and retrieval evaluation before promotion. | Keeps embeddings as derived evidence, not source truth. |
| TV-006 | MCP/agent loops are downstream of read-only/query surfaces and authority boundaries. | semantic_debt | `/realm/project/sinex-target-vision/reference/prescriptive-ideas.md` | #1105, #1086 | Read-only context/query server and proposal/finalizer authority must exist before actuator-like loops. | Do not treat agent-loop prose as runtime control-plane design. |
| TV-007 | Active inference and actuators require explicit instruction/observation event design first. | semantic_debt | `/realm/project/sinex-target-vision/raw/main-spine.md` | #1104 | Instruction events, observation events, safety boundaries, and replay semantics documented. | Horizon material only. |
| TV-008 | Event-native domains and KG ownership are separate concerns until reducers and authority surfaces are defined. | semantic_debt | `/realm/project/sinex-target-vision/reference/design-rationale.md` | #1120, #1206 | Current-state reducers and one-authority-per-concern audit complete. | Avoid duplicate "truth" between domain reducers and graph entities. |
| TV-009 | Living documents are not yet a primitive; artifact/workspace/substrate boundaries must be clarified first. | semantic_debt | `/realm/project/sinex-target-vision/report/vision.md` | #356, #1102 | Define notes/artifacts/typed records and graph boundaries before implementation. | Prevents old living-document prose from becoming implicit product scope. |

## Target-Vision Audit (2026-05-24)

Audit pass over `/realm/project/sinex-target-vision/` (report/ + reference/ except raw/) against
the live sinex repository and open/closed GitHub issues. Each row classifies a vision claim as
outdated (a), issue-expressed (b), straightforward not-yet-tracked (c), ill-defined (d), or
catch-all (e). Vision files were not modified — they are input evidence.

### Outdated (already implemented, verified, or superseded)

| Claim ID | Claim | Status | Evidence source | Authority | Proof / promotion gate | Notes |
|---|---|---|---|---|---|---|
| TV-010 | "Document retrieval / `search_docs_v1` derived table is the missing piece before embeddings" — intelligence.md and DR-12 recommended path. | implemented | `report/intelligence.md` (Semantic Search §); `reference/intelligence-designs.md` | #332 (closed 2026-05-15); `crate/sinex-schema/docs/document_layer.md` | Document layer v1 parser/chunker landed; remaining work is consumer surfaces, not the missing substrate. | Vision still describes #332 as open work. |
| TV-011 | SDK input adapters (append-only file, IPC stream, one-time dump, incremental dump, API-backed, file-drop, watcher) missing. | implemented | `report/architecture.md` (Source Material Input Shapes table); `reference/sdk-assessment.md` (What's Missing) | #1011 (closed); shipped in input-shape substrate | Reuse the shipped adapter pattern; per-source reimplementation is no longer the default cost. | Vision tables still mark them "Not built". |
| TV-012 | Settlement/ErrorClass adoption, ingestor health, `pub mod testing`, COPY routing, cascade trigger, DB credential redaction are open architectural fragilities. | verified | `report/architecture.md` (Architectural Fragilities table); `reference/sdk-assessment.md` (Related Issues) | #1009/#1010/#754/#988/#951/#986/#995 all closed | Maintain regression coverage in source-unit production matrix (#1367). | Fragility table already annotated "closed" but historical prose persists across vision files. |
| TV-013 | Local BLAKE3 CAS not yet first-class; git-annex still in play. | superseded | `report/data-landscape.md`; `report/architecture.md` (Fragilities) | #848/#987 (closed); current CAS in `crate/.../blob_storage` | None. | Historical references to git-annex are obsolete. |
| TV-014 | Bus-First / source-unit host architecture is target but legacy domain ingestors remain. | implementation_ready | `report/architecture.md` (System Shape); `report/work-ahead.md` (Cluster 1-2) | #1054 (open spine) + #1081, #1097, #1098, #1057, #1067, #1100, #1064 all closed | Active migration tracked in #1126 execution plan; per-source-unit production smoke matrix already lands via #1367. | Treat #1054 as the live execution authority. |
| TV-015 | Per-row SQLite staging is the only staging shape; lacks epistemic snapshot/WAL lane. | issue_backed | `report/architecture.md` (Source Material as a Role §); `reference/historical-architecture.md` | #1285 (closed — design decided); #1207 (open — evidence lanes / snapshot + WAL backing) | Complete #1207 evidence-lane implementation. | Design dispute is closed; remaining work is the snapshot/WAL companion lane. |
| TV-016 | Anchor-uniqueness fix needed at DB level. | superseded | `report/architecture.md` (Invariant Enforcement Status) | TimescaleDB hypertable limitation documented; application-level `ON CONFLICT (id) DO NOTHING` is current design | None — accepted limitation. | Vision text reads as live gap; architecture has formally accepted it. |
| TV-017 | DLQ stream name mismatch between gateway/runtime and event_engine. | verified | `report/architecture.md` (NATS Topology) | Fixed 2026-04-16 across 5 files. | None. | Already annotated in vision but worth ledgering. |
| TV-018 | "Honesty sweep" — `Timestamp::now()` fabrication still present. | verified | `report/architecture.md` (Three Clocks §; Invariant Enforcement Status) | Resolved 2026-03-27 in event_engine ts_orig DLQ routing. | None. | Vision text annotates but readers occasionally reintroduce the assumption. |
| TV-019 | qutebrowser WAL/recovery: needs write ACL, sinex only has read. | superseded | `reference/communication-social.md` discussion; data-landscape implication | #1325 (closed) — adapter handles qutebrowser DB read path. | None. | Closed via adapter changes; "completely uncaptured" remains true at the live-capture level but the WAL-write blocker no longer applies. |
| TV-020 | Native-messaging browser ingress not yet built. | issue_backed | `report/data-landscape.md` (Browser History §); vision.md | #847 open (live webhistory capture via WebExtension native messaging); #808 closed (server-side ingress). | Ship the extension and admission path under #847. | Vision treats this as design-only; #847 is the live implementation issue. |

### Issue-expressed (vision claim → open issue)

| Claim ID | Claim | Status | Evidence source | Authority | Proof / promotion gate | Notes |
|---|---|---|---|---|---|---|
| TV-021 | Read-only MCP context/query server is downstream of authority boundaries. | issue_backed | `reference/prescriptive-ideas.md` Cross-Cutting; `report/vision.md` (refusals + layer stack) | #1105 | Ship read-only context/query MCP scope before any actuator MCPs. | Already in seed TV-006; this row pins the concrete open issue. |
| TV-022 | Source-worker drain / in-flight material shutdown protocol. | issue_backed | `report/architecture.md` (Three-Phase Ingestor Startup §); `reference/prescriptive-ideas.md` | #1125 | Land drain semantics + recovered_partial markers + continuity surface. | — |
| TV-023 | Domain adapters / federated truth boundary (Polylogue, Lynchpin, hledger). | issue_backed | `reference/lynchpin-subsumption.md`; `report/data-landscape.md` (Sibling-Tool Integration) | #1119 (design), #1122 (Polylogue pilot), #1074 (finance) | Polylogue bridge slice + parity check landing in #1050. | — |
| TV-024 | Recorded LLM model effects + cache/replay policy before any embeddings work. | issue_backed | `reference/embedding-pipeline.md`; `report/intelligence.md` (Eventify Non-Determinism) | #1063 (model-effect cache), #1076 (embeddings through recorded model effects) | Ship #1063 → enable #1076 with replay regression test. | Already in seed TV-005; this row pins specific open issues. |
| TV-025 | Privacy audit/export/delete/redact CLI workflows + private-mode runtime state. | issue_backed | `reference/privacy-and-operations.md` (status_note) | #1071 (closed — runtime suppression), #1072 (audit/export/delete/redact), #1042 (admission/field policy), #1065 (raw-material policy) | Land #1072 workflows; admission policy via #1042. | Live spine — multiple of these closed in May 2026; #1072 remains the active operator UX surface. |
| TV-026 | Source readiness/continuity/drift cockpit. | issue_backed | `reference/cli-tui-design.md` (verify subsumes coverage) | #1441 (Source Readiness Cockpit), #1438-#1443 UX mk3 program | Land DTO spine #1438 → cockpit #1441 → fixture suite #1443. | — |
| TV-027 | Operations Room authority grammar (replay/DLQ/snapshot/lifecycle/privacy). | issue_backed | `reference/cli-tui-design.md`; replay state machine | #1442 | Authority grammar landing under ux-mk3 program. | — |
| TV-028 | Living document primitive remains horizon — gated on artifact/workspace boundaries. | issue_backed | `report/intelligence.md` (Living Document §) | #356, #1102 (notes/artifacts/typed records boundary design) | #1102 boundary design must precede any implementation. | Already in seed TV-009; this row pins #1102. |
| TV-029 | Active inference / instruction-event substrate before any actuator loop. | issue_backed | `reference/prescriptive-ideas.md`; `raw/main-spine.md` | #1104 (instruction events + actuator loops) | Stay on instruction/observation event design; no runtime actuators until proof obligations defined. | Already in seed TV-007; this row pins #1104. |
| TV-030 | Entity/relation automata are code-complete but not yet activated as consumers. | issue_backed | `report/intelligence.md` (Implemented, Not Deployed); `reference/sdk-assessment.md` (What's Unwired) | #1087 (activate entity and relation automata as consumer substrate); #1339 (closed obs that schema/automata produce 0 events) | Wire entity-resolver consumer + ship semantic shadow lane via #1346. | — |
| TV-031 | Explore command / human-in-the-loop disambiguation workbench. | issue_backed | `report/intelligence.md` (Explore Command §); `reference/cli-tui-design.md` | #1062 (staged-material interpretation workbench); #1440 (Event Inspector and copy/action system) | Land staged interpretation workbench + event inspector. | "Dedicated duplicate-resolution workflow doesn't exist" line in vision is captured by #1062 scope. |
| TV-032 | High-fan-in derivation provenance (day summary → 500+ parents → TOAST). | issue_backed | `report/architecture.md` (Provenance Scaling Concern); `report/work-ahead.md` (Open Design Problems) | #1112 (high-fan-in derivation lineage) | Land Provenance::Query / scope-based replay or pragmatic hybrid per #1112. | — |
| TV-033 | Late-arrival settlement semantics (windowed canonicalizer late events). | issue_backed | `report/architecture.md` (Micro-Replay); work-ahead Open Design Problems | #1111 (late-arrival settlement), #1110 (moments and evidence windows) | Define settlement semantics + moment query API. | — |
| TV-034 | Declarative SQL derivation engine (SQL-as-Automaton). | issue_backed | `report/intelligence.md` (SQL-as-Automaton); `reference/design-rationale.md` | #1117 (declarative SQL derivation engine) | Ship SQL executor node type under existing AutomatonRuntime. | Former global doc dissolved into #1117 comment on 2026-06-04. |
| TV-035 | Audited semantic renames without parser replay. | issue_backed | `report/architecture.md` (Event taxonomy concerns); `reference/event-taxonomy/` | #1101 (audited semantic renames) | Land renaming workflow with audit events. | Refines TV-002; former global doc dissolved into #1101 comment on 2026-06-04. |
| TV-036 | Event QoS / load-shedding policy. | issue_backed | `report/architecture.md` (Architectural Fragilities — NatsPublisher semaphore); design-intent QoS rings | #1093 (event QoS and load-shedding policy) | Define per-priority drop semantics. | NatsPublisher semaphore concern feeds into this issue. |
| TV-037 | Semantic teardown and multiperspective provenance. | issue_backed | `reference/design-intent.md`; `reference/knowledge-graph.md` | #1114 | Land teardown semantics for derived chains. | Former global doc dissolved into #1114 comment on 2026-06-04. |
| TV-038 | Domain reducers / one authority per concern. | issue_backed | `reference/design-rationale.md` (One authority surface per concern); `report/architecture.md` (Design Doctrine) | #1120 (current-state reducers), #1206 (consolidate to one authority surface), `crate/sinex-primitives/docs/domain_reducers.md`, `.github/authority-surfaces.md` | Reducer spec vocabulary landed; authority map remains review guidance, not an implementation authority. | Refines TV-008. |
| TV-039 | Tasks as event-native workflow objects (not KG primitives). | issue_backed | `reference/design-rationale.md` (Event-native domains §) | #1107 | — | — |
| TV-040 | Medication / self-observation logs as event-native domain. | issue_backed | `report/data-landscape.md` (Knowledgebase Vault §); substance log discussion | #1108 (medication and self-observation logs) + #1348 (closed — structured intake) | Land event-native model post #1348. | — |
| TV-041 | Inference confidence seeds and decision metadata for automata. | issue_backed | `reference/intelligence-designs.md`; LLM integration discussion | #1118 | — | — |
| TV-042 | Shadow lanes / semantic epochs for derived schema evolution. | issue_backed | `reference/embedding-pipeline.md`; `reference/intelligence-designs.md` | #1109 + #1346 (entity/relation shadow lane registry, closed) | — | Former global doc dissolved into #1109 comment on 2026-06-04. |
| TV-043 | Inspectable context packs from events + resources. | issue_backed | `reference/cli-tui-design.md` Session 1-2 narrative reports | #1095 (context packs) | Build context-pack DTO consumed by `sinexctl context`. | — |
| TV-044 | Read-only SinexFS projection mount. | issue_backed | `reference/cli-tui-design.md`; `reference/desktop-integration.md` | #1121 | — | — |
| TV-045 | Upstream record-shape drift detection per source. | issue_backed | `reference/capture-infrastructure.md`; `reference/communication-social.md` parser specs | #1103 | — | — |
| TV-046 | Wayland/Hyprland bridge for desktop sources. | issue_backed | `reference/desktop-integration.md` | #1234 | — | — |
| TV-047 | Audio transcripts + screen OCR capture pipelines. | issue_backed | `reference/media-document-processing.md`; `report/intelligence.md` (Voice Capture) | #1043 | — | — |
| TV-048 | Kitty / asciinema / notification capture slices. | issue_backed | `reference/event-taxonomy/`; `reference/desktop-integration.md` | #1033 | — | — |
| TV-049 | sinexctl timeline interactive temporal browser. | issue_backed | `reference/cli-tui-design.md` Sessions 1-3 | #1025 | — | — |
| TV-050 | Replay completion + parser operation lifecycle restored. | issue_backed | `report/architecture.md` (Replay §) | #1115 | — | — |
| TV-051 | Privacy-relevant DLQ / confirmation / capture gaps surfaced in readiness. | issue_backed | `reference/privacy-and-operations.md` | #1364 (closed) | None — verify next readiness regression. | — |
| TV-052 | Declarations / omissions / conceptual time modelling. | issue_backed | `reference/design-intent.md`; design-rationale ledger §10 | #1113 | — | Former global doc dissolved into #1113 comment on 2026-06-04. |

### Filed as new issues (2026-05-24 audit)

The following claims were not yet tracked. Issue numbers are filled in after `gh issue create`:

| Claim ID | Claim | Status | Evidence source | Authority | Proof / promotion gate | Notes |
|---|---|---|---|---|---|---|
| TV-053 | Add `supersedes_event_id: Option<Id<Event>>` so replay-replacements know which event they replace; tighten micro-replay scope_key/equivalence_key contract. | issue_backed | `reference/prescriptive-ideas.md` (Provenance & Identity); `report/architecture.md` (Micro-Replay) | #1446 | Land field + propagation through AutomatonRuntime micro-replay path. | — |
| TV-054 | Source material integrity proof: BLAKE3 hash of byte range stored on event row; verified on replay. | issue_backed | `reference/prescriptive-ideas.md` (Provenance & Identity) | #1447 | Field + verification step in replay preview metrics. | Distinct from local CAS BLAKE3 — this is per-event anchor verification. |
| TV-055 | Cross-material duplicate detection workflow (operator-driven, judgment events). | issue_backed | `report/architecture.md` (Cross-material duplicate detection is human-in-the-loop); `reference/intelligence-designs.md` (explore §) | #1448 | Surface candidates in explore workbench + record merge/prefer/ignore judgments. | Complements #1062 workbench. |
| TV-056 | Capture qutebrowser primary browser history (file-drop or live adapter; current ingestor only ingests Chrome export + qutebrowser snapshot). | issue_backed | `report/data-landscape.md` (qutebrowser blind spot, no export script exists) | #1449 | Decide adapter shape + admission/privacy policy + ship parser. | qutebrowser is the user's primary browser. |
| TV-057 | Replay preview metric set: anchor churn %, time-quality flip %, cascade depth warn + schema-mismatch force gate (TARGET_CANONICAL gates not yet enforced). | issue_backed | `report/architecture.md` (Replay Discipline §; target-state gates table) | #1450 | Add metrics to CascadeAnalyzer preview output + gate defaults. | Distinct from existing CascadeAnalyzer integrity checks. |

### Ill-defined (needs design pass, not yet actionable)

These claims appear across multiple vision documents but are too vague or contradictory to file
as issues without a real design decision first:

- **"Self-improving prompts"** (intelligence.md, Voice Capture §): "system observes which prompt
  patterns work and adapts" — no proof model, no observation event design, no replay story.
- **"Tiered LLM architecture"** as a Sinex concern (intelligence.md): fast-local + strong-remote
  routing — but Sinex has zero LLM client; this conflates capability with substrate.
- **"LLM-generated UI: the document itself becomes an interface"** (intelligence.md): aesthetic
  goal, not architecture; no clear coupling to event-native discipline.
- **"Hypothesis notes are behavioral contracts ... regression test contracts awaiting data"**
  (data-landscape.md vault §): suggestive framing but no concrete substrate maps hypotheses to
  query/automaton/proof obligations.
- **"Best-of-n with evaluator model, A/B testing of prompts, auto-tuning"** (intelligence.md
  LLM Integration §): listed as user-proposed advanced controls, but their interaction with
  recorded model effects / replay determinism is unspecified.
- **"AI chatlog import gap — 20+ raw-log entries reference AI conversations by URL"** (data-
  landscape.md vault §): a real observation, but the corrective action could be Polylogue
  bridge (#1122), staged AI-session parser (#1068), or a cross-source link automaton; needs a
  scoping decision before becoming an issue.
- **"Behavioral modeling system: what should I do now?"** (data-landscape.md vault §): the
  framing is interesting but explicitly out of scope for Sinex (refuses productivity
  prescription); should not become an issue at all unless framing changes.
- **Provenance scaling: 125-UUID TOAST threshold vs ~thousands-parent threshold (work-ahead)
  contradicts architecture.md ("not yet known whether this is a real problem")** — pick one
  story before reopening design beyond #1112.

### Catch-all (raw notes, meta, durable principles)

- **`raw/` directory** stays raw. Do not synthesize into ledger entries; treat as evidence.
- **Hygiene rules in INDEX.md** are durable process content — already governed by issue #354
  history; no ledger action.
- **`prescriptive-ideas.md` Quick Wins** (ProtectHome=read-only, per-service NATS identities,
  per-service PG roles) are good operational hygiene items but each is small enough to land
  via direct PR; filing dedicated issues would add overhead.
- **Vision principles + refusals (vision.md §"What We Refuse")** are durable identity
  statements, not implementation claims.
- **`historical-architecture.md`** is intentionally historical; do not promote.
- **CLI/TUI simulated sessions** (cli-tui-design.md Section 1) are illustrative UX north-star
  material — refactor through the ux-mk3 program (#1438-#1443) rather than direct issue per
  session.

## Target-Vision Audit (2026-05-30)

Dissolution batch 1 (`report/` + `reference/`, first pass). Nine fictions/stale claims corrected in target-vision prose (each verified against code), three Phase-1 files drained, two claims promoted to issues. Target-vision prose edits are committed to its own repo; this section records the sinex-side status. Root cause: target-vision was a source of the event-identity idempotency fiction — see #1570 and `.agent/includes/architecture/provenance.md`.

### Fictions corrected (prose fixed in target-vision)
| Claim ID | Claim | Status | Evidence |
|----------|-------|--------|----------|
| TV-058 | Three Clocks: `ts_coided` diverges only on historical imports / "when first observed" | superseded | `ts_coided = uuid_extract_timestamp(id)`; replay mints a new UUIDv7 → new `ts_coided`. Canon: `provenance.md`, `.agent/includes/reference/glossary.md`, #1570. |
| TV-059 | `(material_id, anchor_byte)` UUIDv5 "accidental idempotency via ON CONFLICT"; replay/occurrence idempotency | rejected | Non-goal. Event ids = interpretation identity (random, new on replay); `ON CONFLICT (id)` is NATS at-least-once redelivery only. #1570. |
| TV-060 | `_1h` telemetry surfaces are ordinary views, "no continuous aggregates" | superseded | `apply.rs:66-76` `TELEMETRY_CONTINUOUS_AGGREGATES` = 9 CAs + `current_system_state` (5m CA). #952. |
| TV-061 | 9 automata in `sinex-process`; analytics window 1000-event | superseded | sinex-process dissolved into sinexd (Wave-B #1054/#1223/#1225); 13 specs in `nixos/modules/lib/automata.nix`; `analytics.rs:27` = 250. |
| TV-062 | Per-domain ingestor binaries (`sinex-fs-ingestor`, …) are ACTIVE services | superseded | `Cargo.toml`: no per-domain ingestor crates; source units hosted by sinexd post-Wave-B. |
| TV-063 | pgsodium as exploratory encryption option | rejected | Non-goal per #367; supported path is `Strategy::Encrypt` (XChaCha20-Poly1305). |
| TV-064 | Replay ordering invariant cites "id (ULID) order" | superseded | System uses UUIDv7, not ULID. Semantic invariant (`ts_orig` ordering) is correct. |

### Promoted to new issues
| Claim ID | Claim | Status | Issue |
|----------|-------|--------|-------|
| TV-065 | Typed `OccurrenceKey` for `(source_material_id, anchor_byte)` occurrence identity | issue_backed | #1588 |
| TV-066 | Roll out `#[derive(SinexConfig)]` to remaining ~10 manual `from_env()` impls | issue_backed | #1589 |

### Drained files (Phase 1)
- `reference/embedding-pipeline.md` — DELETED (was a pure authority-pointer; authority: `crate/sinexd/docs/automata/embedding_runtime.md`, `crate/sinex-schema/docs/document_layer.md`, #1076/#1021/#1063).
- `reference/design-intent.md` §29 (17 superseded ideas) — DELETED (each superseded by named code).
- `reference/historical-architecture.md` — KEPT (conservative; sensd section still feeds the #1207 evidence-lane design).

> Note: the 2026-05-30 audit claims were renumbered to TV-058+ on 2026-05-30 to remove collisions with the pre-existing 2026-05-24 audit, which already occupied TV-010..057.

### Batch 2–3 (2026-05-30) — `reference/` drain continued

Nine more fictions/stale claims corrected in target-vision prose (each verified against code). Same root cause as TV-058…064: target-vision was a source of the event-identity / Wave-B-topology drift. Prose edits committed to the target-vision repo (`ef82ba0`, `b0a2b6f`, `edd6b06`, `a159c26`); this records the sinex-side status.

| Claim ID | Claim | Status | Evidence |
|----------|-------|--------|----------|
| TV-067 | `UNIQUE (source_material_id, anchor_byte)` enforces ingestor idempotency | rejected | TimescaleDB hypertable cannot enforce UNIQUE without the partition key (`id`); `ix_events_material_anchor` is non-unique by design (`defs/events.rs:348`). Idempotency is checkpoint/`ON CONFLICT (id)`-based. #1570. |
| TV-068 | Generated column is named `ts_ingest` | superseded | Column is `ts_coided` (`defs/events.rs`). |
| TV-069 | Compute deterministic event IDs via `UUIDv5(material_id, anchor_byte)` in NatsPublisher | rejected | Event IDs are interpretations (random UUIDv7); deterministic IDs collide with archived events on replay. UUIDv5 domain identity is legit only for named domain objects (entities, documents), not event PKs. #1570. |
| TV-070 | 6 automata in `sinex-process` binary (in-body deployment tables) | superseded | `sinex-process` dissolved into sinexd (Wave-B #1054/#1223/#1225); 13 automata in `nixos/modules/lib/automata.nix`. Mirrors TV-061; was missed in two in-body tables. |
| TV-071 | Runner-pack table names `sinex-process` / `sinex-{desktop,terminal,system}-ingestor` as live deployment vehicles | superseded | Wave-B dissolved all per-domain binaries into `sinexd`; `flake.nix` lists only `sinexd`/`sinexctl`/`xtask` as runtime packages. |
| TV-072 | "8 existing ingestors/automatons" baseline count | superseded | Post-Wave-B: 13 automata specs + 20+ source-unit modules in `crate/sinexd/src/sources/source_units/`. |
| TV-073 | Hourly/daily summarizers live "in sinex-process" | superseded | Dissolved into `sinexd::automata` (`crate/sinexd/src/automata/{hourly,daily}.rs`). |
| TV-074 | `sinex-fs-ingestor` watches the vault directory | superseded | Binary dissolved; `fs` source unit hosted in sinexd (`crate/sinexd/src/sources/source_units/fs/mod.rs`). |
| TV-075 | Event-taxonomy files name `**RuntimeActor:** sinex-*-ingestor` per domain | superseded | All per-domain binaries dissolved by Wave-B; README supersession callout added rather than 13 individual edits (proportionate). |

### Batch 4 (2026-05-30) — topology drain (`report/architecture.md`, `reference/design-rationale.md`, `reference/desktop-integration.md`)

Wave-B (#1054/#1223/#1225) dissolved the old split admission/API/process binaries into `sinexd`. Prose corrected in target-vision commit `4014afa`.

| Claim ID | Claim | Status | Evidence |
|----------|-------|--------|----------|
| TV-076 | The admission/event-writer role is a distinct binary | superseded | `crate/sinexd/Cargo.toml` (`name = "sinexd"`); event engine is `sinexd::event_engine`. Wave-B #1054/#1223/#1225. |
| TV-077 | Merging the old admission and API binaries was a rejected design alternative | superseded | Wave-B reversed it: `sinexd` hosts both behind internal module boundaries; `nixos/modules/default.nix` `runner_binary = "sinexd"`. |
| TV-078 | `AutomatonAdapter<N>` / `IngestorNodeAdapter<T>` are the adapter type names | superseded | `AutomatonRuntime<N>` (`sinexd/src/runtime/automaton/adapter/mod.rs:84`); `SourceUnitRuntime<I>` (`sinexd/src/runtime/source_driver.rs:144`). |
| TV-079 | `sinex-desktop-ingestor` is a deployed binary | superseded | Desktop capture is source units hosted in sinexd; no such crate in `crate/`. |

### Batch 5 (2026-05-30) — binary-name drain (event-taxonomy, cli-tui, intelligence-designs)

Remaining split-binary drift across taxonomy + CLI/intelligence reference files. Prose corrected in target-vision commits `1ea30e4`, `9380fd8`. After this batch `reference/` is ~80% dissolved (binary-name drift resolved across 13 of ~16 files; remainder is claim-verification passes).

| Claim ID | Claim | Status | Evidence |
|----------|-------|--------|----------|
| TV-080 | `capture-infrastructure.md` runner-pack table lists `sinex-{desktop,terminal,system}-ingestor` / `sinex-process` as live deployment vehicles | superseded | Post-Wave-B sinexd is the single runtime host (`crate/sinexd/Cargo.toml`); row strikethrough-annotated in file. |
| TV-081 | `cli-tui-design.md` file-hotspot path points at the old API crate | superseded | API code lives in `crate/sinexd/src/api/`. Fixed in `1ea30e4`. |
| TV-082 | `cli-tui-design.md` "sinex-terminal-ingestor captures the execution event" | superseded | Terminal source unit hosted in sinexd. Fixed in `1ea30e4`. |
| TV-083 | `cli-tui-design.md` stack-line routes `sinexctl` through the old API binary | superseded | Stack is `sinexctl → sinexd (API) → Postgres`. Fixed in `1ea30e4`. |
| TV-084 | `event-taxonomy/L-intelligence.md` "Deployed (sinex-process)" automata table | superseded | Automata run as sinexd instances (`crate/sinexd/src/automata/`). Fixed in `1ea30e4`. |
| TV-085 | `event-taxonomy/M-self-telemetry.md` automata rows labelled "(sinex-process)" | superseded | sinex-process dissolved (Wave-B #944/#1223). Fixed in `1ea30e4`. |
| TV-086 | `intelligence-designs.md` §9 "session detector (part of sinex-process)" | superseded | `crate/sinexd/src/automata/session.rs`. Fixed in `9380fd8`. |
| TV-087 | `intelligence-designs.md` §2.1 daily summarizer "deployed in sinex-process" | superseded | `crate/sinexd/src/automata/daily.rs`. Fixed in `9380fd8`. |
| TV-088 | `intelligence-designs.md` §11 "summarizers shipped in sinex-process" | superseded | All shipped automata are in sinexd. Fixed in `9380fd8`. |
| TV-089 | `intelligence-designs.md` living-doc "automata within sinex-process" | superseded | Would be `sinexd::automata`. Fixed in `9380fd8`. |
| TV-090 | `sdk-assessment.md` entity-automata path `sinex-process/src/automata/` | superseded | Authority is `crate/sinexd/src/automata/`; struck through in batch-2 prose. |
| TV-091 | `communication-social.md` `sinex-comms-ingestor` / `sinex-document-ingestor` as deployment targets | design_candidate | Design-sketch crate names for PLANNED sources under #1070; not deployed binaries. Annotated "design sketch only" — legitimately planned, not fiction. |

### Batch 6 (2026-05-30) — claim-verification pass (work-ahead, media-document, privacy-and-operations, prescriptive)

Lower-risk remainder: stale automata counts, dissolved crate paths, and the TOML privacy-config model. Prose corrected in target-vision commit `a8f105a`. `reference/` is ~92% dissolved after this batch; only `reference/event-taxonomy/` A–M domain files remain (batch 7).

| Claim ID | Claim | Status | Evidence |
|----------|-------|--------|----------|
| TV-092 | `work-ahead.md` old automata count plus split admission/API runtime | superseded | Post-Wave-B: 14 automata in `sinexd::automata`; the old split runtime dissolved into `sinexd` (#944/#1559/#1054). |
| TV-093 | `work-ahead.md` "source-unit host (#1081) will eventually consolidate the 5 ingestors" | superseded | #1081 landed — `sinexd` is the unified host; per-ingestor crates dissolved (Wave-B). |
| TV-094 | `media-document-processing.md` `BlobManager` path points at the old SDK crate | superseded | BlobManager-related code lives under `crate/sinexd/src/sources/`. |
| TV-095 | `media-document-processing.md` `DocumentIngestorNode (sinex-document-ingestor)` separate crate | superseded | Dissolved; logic at `crate/sinexd/src/sources/source_units/document/node.rs`. |
| TV-096 | `media-document-processing.md` component paths `crate/nodes/sinex-{image-processor,audio-transcriber}/` | superseded | `crate/nodes/` dissolved (Wave-B Tier-2, #1225); future automata under `crate/sinexd/src/automata/`. |
| TV-097 | `media-document-processing.md` tag path points at the old SDK crate | superseded | Tag logic at `crate/sinexd/src/automata/tag_applier.rs`. |
| TV-098 | `privacy-and-operations.md` §7.1–7.2 `services.sinex.privacy` TOML/NixOS rendering as authoritative privacy-config model | superseded | #1042 (consolidated 2026-05-30) redesigns policy as DB tables via `sinexctl privacy`, not static TOML. Interim TOML remains in code pending #1042. |
| TV-099 | `prescriptive-ideas.md` day/hourly summarizer sinex-process parenthetical | verified | `sinexd::automata::{daily,hourly}` confirmed; prose sharpened. |

### Batch 7 (2026-05-31) — `event-taxonomy/` A–M + `sdk-assessment.md` final pass

Closes the dissolution: `reference/` is now ~100% drained. Prose corrected in target-vision commit `6ec6d05`. All 13 taxonomy files carry `status_note` frontmatter; surviving `**RuntimeActor:** sinex-*-ingestor` table labels are covered by the `event-taxonomy/README.md` blanket Wave-B SUPERSEDED notice (historical labels, not live claims). Planned source types (`sinex-audio-transcriber` H3, `sinex-web-archiver` D5) are aspirational `[PLANNED]` names, not dissolved crates.

| Claim ID | Claim | Status | Evidence |
|----------|-------|--------|----------|
| TV-100 | `event-taxonomy/C-terminal-shell.md` `**RuntimeActor:** command canonicalizer (sinex-process)` | superseded | sinex-process dissolved (Wave-B); canonicalizer at `crate/sinexd/src/automata/canonicalizer.rs`. Fixed in `6ec6d05`. |
| TV-101 | `C-terminal-shell.md` prose "Command canonicalizer (sinex-process) produces `command.canonical`" | superseded | Same; prose updated to the sinexd automaton. Fixed in `6ec6d05`. |
| TV-102 | `A-system-hardware.md` `**RuntimeActor:** sinex-fs-ingestor` / `sinex-system-ingestor` | superseded | Per-domain binaries dissolved; source units at `crate/sinexd/src/sources/source_units/`. status_note added (`6ec6d05`). |
| TV-103 | `B-desktop-interaction.md` `**RuntimeActor:** sinex-desktop-ingestor` / `sinex-system-ingestor` | superseded | Source units at `…/source_units/desktop/` + `system/`. status_note added (`6ec6d05`). |
| TV-104 | `D-web-browser.md` `**RuntimeActor:** sinex-browser-ingestor` | superseded | Browser source unit at `…/source_units/browser/`. status_note added (`6ec6d05`). |
| TV-105 | `H-media.md` `**RuntimeActor:** sinex-desktop-ingestor` (H2 MPRIS2) | superseded | Desktop source unit in sinexd. status_note added (`6ec6d05`). |
| TV-106 | `sdk-assessment.md` entity/relation automata registration | verified | `entityResolver`/`relationExtractor`/`entityEnricher` in `nixos/modules/lib/automata.nix:22-24`; implementations substantive (222/274/277 LoC); activation tracked by open #1087 + #1346. |
| TV-107 | `knowledge-graph.md` "#1346 closed" (batch-6 follow-up) | rejected | No false claim found — the file already treats #1346 as active work (lines 150, 343). #1346 is OPEN. Follow-up resolved. |

### `report/` dissolution (2026-05-31) — authority migrated to the sinex repo

`reference/` was drained to ~100% (TV-013..107); this pass dissolves the `report/` *current-state* chapters by redirecting their authority into the repo (dissolving-headers, not deletion — the synthesis is kept as orientation). target-vision CLAUDE.md updated with a dissolution-status section. Target-vision commit `6e814f9`.

| Claim ID | Claim | Status | Evidence |
|----------|-------|--------|----------|
| TV-108 | `report/work-ahead.md` is the authoritative work backlog | superseded | Authority = the GitHub issue-set (`gh issue list`); target-vision's own charter says "active work is in GitHub issues." Dissolving-header added (`6e814f9`). |
| TV-109 | `report/architecture.md` is the authoritative current-state architecture | superseded | Authority = `.agent/includes/architecture/*`, owning crate docs, and code. Derived narrative; dissolving-header added (`6e814f9`). |
| TV-110 | `report/deployment.md` is the authoritative deployment state | superseded | Authority = `nixos/modules/deployment-topology.md` + `nixos/modules/README.md`. Dissolving-header added (`6e814f9`). |
| TV-111 | `report/intelligence.md` is the authoritative automata/intelligence state | superseded | Authority = `nixos/modules/lib/automata.nix` + `crate/sinexd/src/automata/*`; frontier #1087/#1346. Dissolving-header added (`6e814f9`). |
| TV-112 | `report/vision.md` + `report/data-landscape.md` | design_candidate | **Retained, not dissolved** — north-star "why" and cross-project (lynchpin/captures) estate have no in-repo authority surface. Marked retained in `6e814f9`. |

With this, target-vision is dissolved except: `raw/` (source material, stays), `report/vision.md` + `report/data-landscape.md` (deliberately retained), and `reference/` specs kept for design rationale (status-noted, not current-state). Authority for everything current lives in the sinex repo + issue-set.

## Where New Claims Go

Use this order:

1. If the claim is only useful inside one issue, add a compact ledger row in
   that issue comment.
2. If the claim crosses issues or repeats across sessions, add it to this
   document.
3. If target-vision itself needs a richer local ledger later, create a
   `claim-ledger.md` there and link it from this document. Keep GitHub issues
   as the implementation authority.

## Avoiding Duplication

Issue bodies should contain scope, acceptance criteria, and verification.
Ledger rows should contain state and evidence pointers. If both need the same
paragraph, the paragraph belongs in a design doc and both should link to it.

When closing or superseding a claim-backed issue, update the ledger row or
leave an issue comment with:

- final status;
- commit/PR evidence;
- verification command;
- superseded/rejected rationale when applicable.
