---
created: "2026-06-28T17:55:00Z"
purpose: "Algebra-vs-silos audit findings (answers operator's 'do we already have a mess' worry). Drives the consolidation roadmap = the real enabler for demo lenses."
status: active
project: sinex
---

# Sinex algebra-vs-silos audit (read-only, opus)

## Verdict
Fear is HALF right. Core read algebra is REAL and most CLI commands are genuine lenses over it. BUT the algebra STOPS exactly where the valuable composite lenses begin → the one high-value composite (`context.rs`, recall/agent-brief) was forced into a 1871-line SILO that even re-rolls a primitive that already ships.

## A) Algebra that EXISTS (the spine — build on these)
- `EventQuery`+`PayloadFilter`+`AggregationMode` (sinex-primitives/src/query.rs:167-531) → ONE executor (sinex-db .../events/composable_query.rs:43). Replaced "22+ hardcoded query methods". STRONG.
- `LineageQuery`→lineage() (query.rs:551; composable_query.rs:480) provenance CTE. STRONG but DUPLICATED.
- `EventRelationExpr`+`EvidenceWindow` (sinex-primitives/src/relations.rs:366,464; RPC events.relation_evidence; CLI commands/relations.rs) — relation-anchored evidence window + ExpansionTrace. CLEANEST layering. STRONG but NARROW.
- `SubscriptionFilter` (query.rs:678) one live-stream algebra (= sse_bus). STRONG.
- `SinexObjectRef`/`SinexObjectKind` + `show` resolver (views/common.rs:11-41; commands/show.rs:62) — 30+ kinds but resolver handles only 5. PARTIAL.
- `SinexQuery` DSL + `QueryUnitDescriptor` (query_units.rs) — unified grammar but CLIENT-ONLY, 6 hardcoded lowerings, events can't fully lower. PARTIAL.
- `ViewEnvelope`/`CaveatView` (fmt/envelope.rs) render algebra. STRONG.
- `EvidenceBundle` (evidence_bundle.rs) — client-side, operator/debt-anchored. PARTIAL.
- 15 telemetry continuous-aggregates exposed as 15 one-off RPCs.

## B) SILOS (ranked)
1. **`commands/context.rs` (1871 lines) — FLAGSHIP SILO.** recall/agent-brief/desktop-context, bespoke top to bottom. Hand-rolls multi-source fusion (grouped_context_sources:189 = AggregationMode::CountBy{Source}), hardcoded source-family string taxonomy duplicated ~7 fns (is_*_evidence 963-987, display_source:1016), 4 copy-pasted projection builders w/ ad-hoc confidence literals, and CLIENT-SIDE hand-builds `EvidenceWindow` (desktop_context_evidence_window:433) — bypassing the server events.relation_evidence primitive that commands/relations.rs:93 uses correctly.
2. **~30 pre-algebra event-repo methods** (repositories/events/queries.rs + events_extensions.rs "missing query methods"): get_by_source/_type/_time_range, count_by_*, estimate_* — all expressible as EventQuery, hand-written sqlx beside the composable executor. FOUNDATIONAL dup. HIGH.
3. **3 provenance walks**: composable_query.lineage() CTE; replay cascade temp-tables+topo-sort (api/cascade_analyzer.rs:248); repositories/replay.rs own build_filter_query:59. One walk, 3 impls. HIGH.
4. **knowledge_graph.rs**: get_entity_relations:698 = 4-arm match dup'ing one SELECT; find_paths:852 bespoke CTE+N+1. NO expand_neighbors(entity,hops) primitive. MEDIUM-HIGH. Blocks entity/project lenses.
5. telemetry.* 15 one-relation RPCs (rpc_registry.rs:767-826). MEDIUM.
6. query-unit DSL fan-out to 6 bespoke RPCs (commands/query_units.rs:69). MEDIUM.
7. report.rs builds same 4 EventQuery twice. LOW.
8. continuity.rs coverage (source coverage/gap) is a SILO — "coverage" not a callable primitive; family axis = split_part(source,'.',1) heuristic. LOW-MED.

## C) GAPS that FORCE the target lenses into silos today
- Recall-around-T fused multi-source window w/ coverage: **MISSING** (relation_evidence is relation-anchored not time-anchored/source-fused/coverage-bearing). context.rs proves the gap by hand-building it.
- Incident reconstruction: PARTIAL (lineage dup'd; deliberately no incident model; no orchestrator around fault/time anchor).
- Agent brief / context pack: **MISSING AT OBJECT LEVEL** — `SinexObjectKind::ContextPack`+`MomentCandidate` declared (views/common.rs:35) but ZERO impl in sinexd/sinex-db. No persisted artifact, no assembler.
- Commit/project context: MISSING (no entity-anchored evidence primitive; knowledge_graph lacks N-hop; "project" = hardcoded payload-family string in context.rs:930).
- Cross-source fused timeline: MISSING (timeline.rs is single-stream EventQuery list; only fusion in codebase = bespoke context.rs).

## D) CONSOLIDATION ROADMAP (root-cause, highest leverage first) — THE DEMO ENABLER
1. **KEYSTONE: server-side "evidence window around an anchor" primitive.** Generalize events.relation_evidence to accept TIME / ENTITY / relation anchor, fan in across sources, attach per-source COVERAGE (lift continuity's computation into callable), return EvidenceWindow + ExpansionTrace. Then COLLAPSE context.rs onto it (delete ~1500/1871 lines). This single change turns recall-T / fused-timeline / incident / agent-brief from silos into lenses.
2. **Make ContextPack/MomentCandidate REAL** — persisted artifact (artifact provenance) + assembler composing #1. Every brief/pack/digest = assemble window→save artifact→`show contextpack:<id>`.
3. **Retire ~30 pre-algebra event-repo methods** → lower get_by_*/count_by_*/estimate_* to EventQuery (one executor, one semantics). Pre-release, zero-prod-data, no-compat → exactly the mandated cleanup.
4. **Unify provenance traversal** into ONE primitive used by events.lineage + replay cascade + replay scope.
5. **N-hop entity-relation expansion** in knowledge_graph; collapse 4-arm get_entity_relations. Prereq for entity/project/commit lenses.
6. **Finish the 2 half-built grammars**: complete `show` resolver for all kinds; lower SinexQuery DSL→EventQuery server-side so query-units is THE single read grammar (CLI/TUI/MCP/gateway); replace telemetry.*'s 15 endpoints w/ one generic projection query unit.
7. **Shared source-family classifier** primitive (replace copy-pasted string taxonomy) as canonical grouping axis for windows/timelines/coverage.

## How this composes (updates 016 demo lane)
- The demo-value lenses (S1 recall, S2 incident, S3 agent-brief, S4 bridge) should NOT be built as artifacts first — build D1 (evidence-window primitive) + D2 (ContextPack) + D7 (family classifier), then the demos are thin lenses + the artifact PROVES the algebra. context.rs collapse = the proof + the cleanup.
- D3/D4/D5 are foundational dedup (architectural correctness, pays dividends). D4 overlaps AGENT-CORRECT's replay work — coordinate (lineage unification).
- This is the GOVERNING-PRINCIPLE work made concrete: capabilities as lenses over D1-D7, not silos.

Cross-ref: [[015-confirmed-delivery-redesign]], [[016-demo-value-plan-assimilation]].
