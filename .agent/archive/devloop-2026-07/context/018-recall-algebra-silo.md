---
created: "2026-06-28T22:50:00Z"
purpose: "Dogfood-recall thread: assess the 'what was I doing' recall lens vs the general evidence algebra; collapse the desktop silo; produce a real recall artifact over my own dev activity."
status: active
project: sinex
branch: feature/dev/automaton-confirmed-delivery
---

# Recall algebra vs the desktop silo (dogfood-recall thread)

## Goal (operator)
Reconstruct prior-session state THROUGH Sinex — "what was I doing around T" / agent-brief over my OWN dev activity — so work survives context resets. MUST be a thin LENS over general algebra, not a one-off silo. AND: find & COLLAPSE existing silos.

## Finding: the recall capability already exists but is a DESKTOP SILO
`sinexctl events context` (crate/sinexctl/src/commands/context.rs, 1871 lines) is documented "Show activity context for session resumption ('what was I doing?')". BUT:
- The generic part (execute → EventQuery last-N-hours → cutoff) is family-agnostic and fine.
- The BULK (lines ~335–1017+) is desktop-source-specific: `DesktopContextView`, `DesktopNotificationPressureView`, `DesktopFocusSessionListView`, `DesktopProjectContextListView`, all built with HARDCODED `match card.source.raw.as_str()` / `match card.event_type.as_str()` arms.
- => Recall is rich ONLY for desktop sources (window-manager, notifications, activitywatch). My REAL dev data — `shell.atuin` (30K commands), and soon fs/git — gets nothing but the flat event-card list. The "what was I doing" lens is blind to terminal/fs/git work.
This is precisely the FAILURE MODE 2 the operator flagged: a capability accreted as a source-specific silo instead of a general projection.

## The algebra is GOOD (build on it, don't replace it)
`sinex-primitives/src/relations.rs` (the #1729/#1789 design) is a solid general evidence algebra:
- `ObservedRange { range, basis: TimeBasis, quality: TimeQuality }` — provenance-aware time; `ObservedRange::from_event` already maps material→MaterialAnchor / derived→DerivedInterval / temporal-ledger source-type→basis+quality. Honors the provenance model.
- `EventRelationExpr` — flat relation enum (Sequence{within_secs}, SameField, ...). Deliberate non-goal: NOT a graph query engine.
- `EvidenceWindow { seeds, supporting/contradiction EvidenceRef[], observed_range, expansion_trace, query }` — a finite assembled view with `with_contradiction` / `with_caveat`.
- `ExpansionTrace`/`ExpansionStep{kind}` — records HOW the window was built: RelationIncluded, CoverageGapCaveat, etc. (coverage honesty baked in.)
`sinex-primitives/src/evidence_bundle.rs` — `EvidenceBundle` framed as "a finite view over existing observability surfaces, NOT a new source of truth" — exactly the operator's principle.

## Collapse plan (make recall a thin lens; desktop becomes one projector among many)
1. Define a GENERAL `ActivityProjection` over an `EvidenceWindow`: family-agnostic grouping of events in [T-Δ, T] into (a) sessions/episodes by temporal proximity (the Windowed/session-boundary notion already exists), (b) per-family rollups via a REGISTRY of family projectors keyed by source-family, NOT match arms. Desktop focus-session / notification-pressure / project-context become registered projectors. Terminal/shell/git/fs register their own (or get a default rollup) through the SAME interface.
2. The recall lens (`events context`) = build EvidenceWindow(seed=now or T, range=Δ) → run registered projectors → render ViewEnvelope. No source-specific match arms in the command; dispatch by registry.
3. `events timeline` (143 lines) + `events relations` (315) + `ops evidence` (667) likely overlap — audit whether they should share the same EvidenceWindow assembly. Unify the assembly path; keep distinct RENDERINGS as the only difference.
4. Coverage honesty: every family with no data in the window emits a CoverageGapCaveat (already supported by ExpansionTrace) so recall never silently omits a family.

## Empirical artifact plan (real data)
- Get sinexd up on the dev loop (manifest ingestion) + gateway.
- Run `sinexctl events context` / `events timeline` over my dev DB (30K shell.atuin + self-telemetry). Capture output = artifact #1 (expected: near-empty for my real work → proves the silo empirically).
- Implement collapse slice 1: a general per-family activity rollup that includes terminal/shell. Re-run = artifact #2 (recall now reconstructs my command session). This is the dogfood proof.

## Dev DB state (main checkout, 2026-06-28 ~22:48)
core.events families: sinex 87027 (self-telem), entity-extractor 31123 (derived), shell 30000 (atuin REAL), derived 14810, sinexd 13629 (self-telem), health-aggregator 26. Only real external family = shell.atuin. Need fs + git for multi-family recall.
PG: postgresql:///sinex_dev?host=/var/cache/sinex/sinity/566e9bf8d5e8/dev-state/run

## PROGRESS (2026-06-28 ~23:2x)
- Dev gateway connection (durable): `SINEX_API_URL=https://127.0.0.1:19086`, `SINEX_API_TOKEN=dev-token-sinnix-prime:admin`, mTLS `--ca-cert .sinex/tls/ca.pem --client-cert .sinex/tls/client.pem --client-key .sinex/tls/client-key.pem`. Run sinexd persistently with `xtask run core --bg` (foreground `xtask run core` self-times-out at 300s).
- git binding FAILED: `git-commit-history` uses StaticFileAdapter which opens path as a FILE → `Is a directory (os error 21)` for repo root. Needs a directory-capable adapter or a file target. Dropped from active use; finding recorded. (terminal/atuin = enough real activity for the demo.)
- ARTIFACT #1 (silo, captured): `.agent/dev/recall-artifacts/before-context-default.txt` (3 self-telemetry sources, 0 of my 30K shell commands) + `before-context-desktop.txt` (all families "missing"). DEMO writeup at `.agent/dev/recall-artifacts/DEMO-recall-silo-collapse.md`.
- COLLAPSE IMPLEMENTED in context.rs: default path → general family-aware RecallPack (CountBy(source) coverage pass + activity-only detail fetch + self-observation separated + coverage honesty). Deleted `render_context_machine_output` + its 2 dead-in-prod tests; added RecallPack/source_family/is_self_observation tests. `--desktop` kept as separate specialized contract. `xtask check -p sinexctl` CLEAN (exit0). Building binary (job 2000147) for AFTER artifact.
- xtask-check exit-code FIX (operator-demanded, done): `parse_cargo_json_output` now ANDs success with cargo `build-finished` verdict + `errors==0` — "success with errors" structurally impossible. 2 regression tests added. (Already proved itself: caught my own 2 sinexctl errors with correct exit 1.)
- KEYSTONE agent (abb78a3c53a8a5f1d): landed WIP commit 60253373d (Option C consumer/handler/bridge + buffer delete), compiling.

## ✅ SLICE 1 DONE + COMMITTED (2026-06-28 ~23:30)
- `927f31055` refactor(sinexctl): general family-aware recall lens (collapse). 3 recall tests pass.
- `436bda0cf` fix(xtask): check success cross-checked vs build-finished + errors==0. 2 tests pass.
- BEFORE/AFTER demo + evidence in `.agent/dev/recall-artifacts/` (committed). AFTER: 66 shell activity events surfaced, 48,235 self-obs separated.
- Memory: dogfood-dev-loop-recipe saved.

## NEXT SLICES (dogfood-recall vector)
- **"around T" anchor**: context only does `--since/last N`. Goal literally says "what was I doing around T". Add `--at <ts>`/window-centered (time_range both bounds) → reconstruct a PRIOR session. HIGH value.
- **Sample enrichment**: samples show generic "command.executed with 11 payload field(s)". Need a GENERAL payload-preview (not source-specific) so recall shows actual command text. Check EventCardView for a headline/preview field.
- **git source fix**: StaticFileAdapter rejects dir → git history can't ingest. Directory-capable adapter or file target. Gets git into multi-family recall.
- **fs family**: wire fs watcher binding (config shape unknown — buried; the fs source driver is generic, not in fs/mod.rs which is parser-only).

## ✅ SLICE 2 DONE + COMMITTED (2026-06-28 ~23:50)
- `7758c168d` feat(sinexctl): `--at <T>` anchored "around T" recall (±since window, RFC3339 or relative-ago) + generic payload-preview samples (EventCardView.payload_preview, no per-source knowledge). 5 sinexctl tests pass. Live: `events context --at 3h -s 2h` → 34 shell activity events around T, 2294 self-obs separated.
- DEMO writeup updated with enriched + anchored AFTER + follow-ups.
- Recall thread CLI slices complete. Remaining recall work needs SINEXD changes (git-source adapter, fs wiring) → do AFTER keystone agent finishes (avoid conflict in its domain).

## KEYSTONE INTEGRATION (pending)
Agent abb78a3c53a8a5f1d worktree-branch commits (to cherry-pick onto feature/dev/automaton-confirmed-delivery after it finishes):
- 60253373d wip(runtime): Option C consumer/handler/bridge + delete buffer
- d2e6b2cbf wip(api): SSE bus → confirmed-events (drop DB refetch)
- e5cf394f9 docs(event_engine): confirmed-delivery flow
INTEGRATE: cherry-pick in order, then full `xtask check` + run runtime/event_engine/sse tests, grep for `error[`. Then producer cleanup (drop watermark stream) if agent didn't finish it.
DIFF VALIDATED (read-only, agent still running): net **−3808 lines** (416+/4224-). confirmation_handler.rs −1779, jetstream_consumer.rs −1348, sse_bus.rs −454, provisional.rs gutted, automaton_runtime/service_container rewired. = correct Option C collapse. Gut-in-place (files kept, bodies removed) not file-delete — fine. MUST verify it COMPILES after cherry-pick before trusting (agent commits were WIP; its final check status unknown until it reports).
RECALL CLI THREAD COMPLETE: commits 927f31055, 7758c168d, c93149bf0 (+ xtask fix 436bda0cf). 13 sinexctl recall tests pass total. Demo durable. Remaining recall = sinexd-side (git adapter, fs) AFTER keystone integration.

## ✅ KEYSTONE INTEGRATED (2026-06-28 ~late) + xtask tooling fix
- Agent abb78a3c FINISHED: Option C complete + **7/7 tests pass incl e2e `test_jetstream_e2e_event_flow` (13.4s)** = behavioral keystone proof.
- Cherry-picked onto branch (clean, disjoint files): b8bccc09d (consumer/buffer-delete), 1e6665919 (SSE), 61612e58a (docs), 4294e1242 (durability gate). Net ~−3700 lines.
- `c2fc8cdd3` fix(xtask): print error diagnostics in non-interactive mode (was is_human-gated → bg checks undebuggable; the friction that forced blind error-reasoning). 
- ✅ INTEGRATED TREE VERIFIED: `xtask check -p sinexd` clean (job 2000156, 0 errors) + 6/6 keystone tests pass on MY branch (job 2000157). Keystone #2187/#2202 integrated + verified.
- DEFERRED by agent (honest, scoped #2187 follow-up): Phase 3b = rip out now-unused watermark confirmations stream + retry queue topology (~10 files + primitives). Still published, NO consumer reads it. Harmless leftover. + clippy dead_code warnings expected.

## ✅ KEYSTONE LIVE-DOGFOODED (2026-06-29 ~00:05)
Rebuilt sinexd bin, restarted dev loop on keystone binary. LIVE PROOF:
- `DEV_SINEX_RAW_EVENTS_CONFIRMED` stream: 308,416 msgs / 290 MiB (producer publishing full confirmed events).
- 14 automata each have a `*-confirmed-events` durable consumer on it (analytics/canonicalizer/daily/document-parser/embedding-producer/entity-enricher/entity-extractor/entity-resolver/health/hourly/instruction-reconciler/relation-extractor/session/tag-applier). Several actively delivering (AckPending 36, LastDelivery 474ms, Unprocessed 0).
- **#2202 provisional-fallback jank = 0 occurrences** (was firing en masse pre-keystone). The deleted path is gone.
- 5382 events persisted in 3 min (ingestion healthy).
KEYSTONE = compile + unit tests + e2e test + LIVE dogfood all green. DONE.

## NEXT (after integration verify)
- Run integrated keystone tests on MY branch (7 tests) to confirm cherry-pick didn't break (disjoint, expected pass).
- Optional: restart live dev sinexd on new binary → dogfood confirmed-delivery path live (e2e already proves it).
- Phase 3b watermark teardown (scoped follow-up).
- Recall sinexd-side: git-source adapter fix (dir), fs family wiring.

## Status / next
- [x] Recovered session state; cherry-picked salvage commit 01c6e84a1 (confirmed-event filter helper).
- [x] Ground-truth: HEAD compiles clean (job 2000143 exit0 + zero `error[`).
- [x] Launched keystone Option-C consumer-migration bg agent (abb78a3c53a8a5f1d, opus, worktree).
- [x] Assessed recall lens = desktop silo; algebra is good; collapse plan above.
- [ ] Expand dev manifest (fs + git) — config archaeology pending (fs watcher config key unknown; git = StaticFileAdapter, source_id "git-commit-history", config.path = repo root).
- [ ] Bring sinexd up; produce recall artifact #1 (proves silo).
- [ ] Implement collapse slice 1 (general family rollup incl terminal/shell); artifact #2.
- [ ] FIX xtask check exit-code unreliability (cross-check: errors>0 must never report success; detect false-pass fast return). Operator explicitly demanded this be fixed, not recorded as a "lesson".
