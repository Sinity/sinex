---
created: "2026-06-30T03:30:00+02:00"
purpose: "Current explicit Sinex conductor loop state"
status: "active"
project: "sinex"
---

# Active Loop

## Current Objective

Conduct the Sinex dogfood/demo devloop indefinitely: continuously choose the
highest-value live-data capability slice, produce inspectable artifacts proving
that Sinex makes agents and the operator better at reconstructing real work and
machine/personal context, collapse silos into general acquisition/query/evidence
projection/rendering substrate, verify on the active store or live local
captures, maintain timestamped operating logs and handoffs, adversarially
review the loop scaffold and resource state, and use each loop's evidence to
reprioritize the next slice while maximizing devloop velocity.

## Current Slice

sinex-60r: attention-source quality and closure proof

## Slice Contract

Finish the attention-source repair needed for real recall demos. Raw
ActivityWatch ingestion now preserves nanosecond timestamps and populated
app/title/url fields; Hyprland focus events now have live per-transition
timestamps and failed snapshot materials have been archived. The current
sub-slice follows Lynchpin's ActivityWatch model: keep raw upstream oddities as
truth, but repair zero-duration heartbeat spam in the derived interval layer so
attention/recall consumers see honest intervals instead of one marker per poll.

## Current Focus

Focus: Proof -> Construction

Trigger: adversarial #2197 closure review found the source fixes real but the
ActivityWatch replay/derived-quality and terminal/journald cross-source proof
criteria still under-proved; the terminal/journald cross-source gap is now
fresh-proven in the 2026-07-05 10:06..10:22+02 live window.

Decision: keep #2197 open for now, land the xtask opt-in source-binding launch
repair, then finish or consciously narrow the ActivityWatch replay/archive
fresh-vs-archived proof before assured-close.

## Accepted Warnings

- ActivityWatch fresh ingestion is proven; replay/archive counts for an old
  bad ActivityWatch material are not yet proven.

## Next Action

1. Land the xtask `all-sources --include-default-excluded` repair so journald
   proof runs are no longer routed around broken tooling.
2. Produce ActivityWatch replay/archive fresh-vs-archived counts for old bad
   material, or record a durable AC correction with stronger fresh-ingestion
   proof.
3. After that gap is handled, rerun an assured-close audit before
   closing `sinex-60r` / GitHub #2197.

## Focus Change 2026-07-02T19:00:20+02:00

Focus: Construction -> Proof
Trigger: remediation-plan is merged; side-agent backlog enrichment restarted
after stale completed agents were closed; focused debt projection tests are in
flight.
Decision: finish capture-debt/query promotion first, then reconcile side
research and choose the next slice from query algebra, runtime catch-up,
inline-test automation, or devloop sidecar scaffolding.

## Do Not Drift

- Do not begin Sinex runtime/build work before the scaffold review is clean or
  warnings are explicitly accepted here.
- Do not add more process prose without an executable or reviewable consequence.
- Do not let ignored scratch be the only copy of important conductor state; sync
  the inbox packet.
- Stay on the current long-lived branch for ordinary loop work; commit
  logical/proven chunks by path and avoid worktrees unless isolation is actually
  needed.
- Use compile/test wait time for ahead work in this checkout. A failed proof can
  be retried after batched fixes; it should not freeze the rest of the loop.

## Focus Change 2026-07-02T15:23:30+02:00

Focus: Integration -> Direction
Trigger: xtask stream-selector fix merged as #2230; runtime health is healthy and DLQ is empty after purging 19 cleanup-plan-approved orphaned sensing-material messages.
Decision: next object slice is root-cause analysis/fix for recurring self-observation orphaned sensing-material DLQ bursts; parallel cleanup lane is branch-backlog audit after the recent squash-merged integration train.

## Focus Change 2026-06-30T04:00:54+02:00

Focus: Artifact -> Velocity
Trigger: context recall ambient tier has focused tests plus a live dev-runtime artifact and runtime baseline
Decision: commit the proven code slice, keep dev sinexd running, then choose the next demo/query slice

## Focus Change 2026-06-30T04:18:43+02:00

Focus: Velocity -> Direction
Trigger: context slice committed; live dev artifact still contains only self-observation because dogfood source bindings are not active
Decision: start live dogfood source-bindings runtime slice and prove whether dev-local Sinex can capture real terminal/git/fs/system activity

## Focus Change 2026-06-30T04:37:21+02:00

Focus: Proof -> Direction
Trigger: query since slice committed and live proof passed
Decision: inspect runtime list/source visibility mismatch as next candidate unless stronger evidence appears

## Focus Change 2026-06-30T05:17:08+02:00

Focus: Artifact -> Construction
Trigger: DLQ live artifact showed 20/20 sampled messages are equivalence-key duplicates routed to DLQ
Decision: Patch event-engine prepare_events so suppressed occurrence duplicates skip DLQ and ACK when no admitted events remain

## Focus Change 2026-06-30T06:19:00+02:00

Focus: Proof -> Artifact
Trigger: tail DLQ proof exposed empty logical source material and descriptor-materialization proof passed
Decision: finish the shared logical adapter descriptor fix, keep seq-197 live evidence caveated as pre-fix in-flight finalization, then commit product/process slices separately

## Focus Change 2026-07-01T04:57:57+02:00

Focus: Proof -> Meta
Trigger: query slice committed; user requested next work shift to meta/devloop
Decision: Harden devloop review/status conventions first, then resume object-level slice selection from demo value

## Focus Change 2026-07-01T05:24:28+02:00

Focus: Artifact -> Direction
Trigger: source_family query demo captured and runtime health restored
Decision: Commit query slice, then choose next object-level slice from test cleanup, terminal acquisition gap, or query grammar cleanup.

## Focus Change 2026-07-01T05:40:37+02:00

Focus: Direction -> Meta
Trigger: source coverage family slice committed; operator requested meta/devloop hardening next
Decision: Harden active-state freshness and shared convention discoverability before resuming object-level Sinex capability slices

## Focus Change 2026-07-01T05:41:48+02:00

Focus: Meta -> Direction
Trigger: active-state freshness guard verified
Decision: Commit the tracked meta slice, then select the next object-level Sinex capability slice from live evidence

## Focus Change 2026-07-01T05:53:38+02:00

Focus: Artifact -> Direction
Trigger: annotated source coverage demo captured and live runtime proof passed
Decision: Commit coverage role metadata, then investigate terminal acquisition freshness as the next live-data gap

## Focus Change 2026-07-01T05:58:42+02:00

Focus: Meta -> Direction
Trigger: devloop contract guard committed and review passes
Decision: resume terminal acquisition freshness investigation as the next object-level Sinex capability slice

## Focus Change 2026-07-01T06:06:21+02:00

Focus: Artifact -> Direction
Trigger: terminal acquisition truth proof complete
Decision: inspect source-status binding terminology and active-vs-registered semantics next

## Focus Change 2026-07-01T06:15:11+02:00

Focus: Direction -> Meta
Trigger: source-status terminology proof committed
Decision: shift to devloop/process improvement per Sinex/Polylogue convention spec

## Focus Change 2026-07-01T06:22:20+02:00

Focus: Direction -> Construction
Trigger: source-status/meta/temporal slices committed; per-file test proof is compile-dominated
Decision: batch small sinex-primitives inline test splits into sibling *_test.rs files, then verify once

## Focus Change 2026-07-01T07:04:40+02:00

Focus: Proof -> Direction
Trigger: RPC/source inline-test split committed with fmt and focused tests passing
Decision: inspect the two remaining primitive inline bodies, split kv_client first if clean, and avoid forcing testing.rs until nested path semantics are proven

## Focus Change 2026-07-01T07:05:43+02:00

Focus: Direction -> Meta
Trigger: completed current test cleanup batch; operator requested meta/devloop work next
Decision: harden devloop conventions and active-loop ergonomics before resuming object-level cleanup

## Focus Change 2026-07-01T07:07:42+02:00

Focus: Meta -> Direction
Trigger: conductor context indexing hardening committed and review passed
Decision: return to object-level slice selection, with remaining primitive inline-test cleanup as the default low-risk lane unless runtime/demo evidence points to a higher-value live-data slice

## Focus Change 2026-07-01T07:10:28+02:00

Focus: Direction -> Direction
Trigger: focus freshness parser hardening committed
Decision: use timestamp-max freshness checks as the shared projection rule, then continue object-level slice selection with runtime/DLQ checks first

## Focus Change 2026-07-01T07:19:05+02:00

Focus: Proof -> Direction
Trigger: coordination kv, error retryability, and source-status inline-test splits committed with focused proofs
Decision: inspect the final nested testing.rs proptest module before deciding whether to extract or explicitly leave it inline

## Focus Change 2026-07-01T07:27:00+02:00

Focus: Proof -> Direction
Trigger: final primitive strategy test body moved out and runtime health is green
Decision: primitive cleanup reached a phase boundary; choose next between broader test-attribution cleanup and live runtime/query/demo capability work

## Focus Change 2026-07-01T07:10:09+02:00

Focus: Direction -> Direction
Trigger: focus freshness parser fixed
Decision: commit the tracked status/review timestamp robustness change, then continue object-level slice selection

## Focus Change 2026-07-01T07:31:34+02:00

Focus: Direction -> Evidence
Trigger: primitive cleanup phase boundary reached; live recent-event query is dominated by self-observation
Decision: capture the query/source-status affordance gap as a demo artifact, then shift to requested meta/devloop work

## Focus Change 2026-07-01T07:32:18+02:00

Focus: Artifact -> Meta
Trigger: live query signal artifact captured and demo indexes refreshed
Decision: audit the devloop scaffold for context-reset resilience, queue handling, and convention enforcement

## Focus Change 2026-07-01T07:35:07+02:00

Focus: Meta -> Direction
Trigger: queue-channel scaffold verified and queued meta directive completed
Decision: commit the tracked meta slice, then inspect query/projection code for recent non-self signal affordance

## Focus Change 2026-07-01T07:43:47+02:00

Focus: Proof -> Direction
Trigger: signal selector code, focused tests, build, fmt, live proof, and commit caf3e3312 are complete
Decision: choose next slice from source-role taxonomy, query/demo rendering, or test-attribution cleanup

## Focus Change 2026-07-01T07:47:25+02:00

Focus: Artifact -> Direction
Trigger: signal selector and table rendering slices committed with focused and live proof
Decision: choose next slice from source-role taxonomy, grouped/source summaries, or test-attribution cleanup

## Focus Change 2026-07-01T07:54:09+02:00

Focus: Meta -> Direction
Trigger: proof-lane guard committed and reviewed
Decision: Resume object-level loop from the live query/source taxonomy frontier; check foreground proof lanes before any new verification.

## Focus Change 2026-07-01T08:04:51+02:00

Focus: Artifact -> Direction
Trigger: tiered signal query algebra committed
Decision: Choose next between grouped source summary projection, an activity-context demo using signal=activity, or returning to test-attribution cleanup.

## Focus Change 2026-07-01T08:11:14+02:00

Focus: Artifact -> Direction
Trigger: source summary query projection committed
Decision: Choose next between an activity-context demo using the improved query table, test-attribution cleanup, or a deeper source-status projection.

## Focus Change 2026-07-01T08:17:11+02:00

Focus: Meta -> Direction
Trigger: devloop scaffold boundary commit cb54f35e0 is verified
Decision: resume object-level slice selection from activity-context demo, deeper source-status projection, or test-attribution cleanup

## Focus Change 2026-07-01T08:24:19+02:00

Focus: Artifact -> Direction
Trigger: activity-context-current-2026-07-01 captured and indexed
Decision: choose next between salience/timeline ranking, DLQ cleanup, terminal freshness, or test-attribution cleanup

## Focus Change 2026-07-01T08:35:52+02:00

Focus: Artifact -> Direction
Trigger: salience timeline code committed as dfe27ebe8
Decision: choose next between DLQ cleanup, terminal freshness, semantic rollup, or test-attribution cleanup

## Focus Change 2026-07-01T08:41:48+02:00

Focus: Direction -> Meta
Trigger: DLQ pressure artifact closed and operator queued Sinex/Polylogue devloop convergence work
Decision: Harden devloop-review so executable checks derive more policy from the shared contract, then commit the tracked scaffold change.

## Focus Change 2026-07-01T08:44:27+02:00

Focus: Meta -> Evidence
Trigger: contract-driven meta slice committed and review is clean; terminal source is hot but output quiet
Decision: Investigate terminal acquisition freshness using live source-status, events/context queries, and source material evidence; fix or artifact the result.

## Focus Change 2026-07-01T08:55:51+02:00

Focus: Evidence -> Artifact
Trigger: terminal source classified as healthy-but-quiet and context now surfaces quiet_sources
Decision: Commit the shared context view improvement, then pick the next demo/product slice.

## Focus Change 2026-07-01T08:56:53+02:00

Focus: Artifact -> Direction
Trigger: quiet-source context slice committed as 94f05cd5b and live artifact indexed
Decision: Choose next between semantic activity rollup and inline-test attribution cleanup; keep runtime healthy and demo-driven.

## Focus Change 2026-07-01T08:59:26+02:00

Focus: Direction -> Meta
Trigger: post-slice convention hardening after Sinex/Polylogue comparison
Decision: make generated conductor manifest expose primitive contract compliance and reduce context-cleared rediscovery

## Focus Change 2026-07-01T09:02:10+02:00

Focus: Meta -> Direction
Trigger: manifest primitive-contract projection and stale-manifest review guard committed and verified
Decision: return to object-level slice selection; semantic activity rollup is the leading demo-value candidate, with inline-test cleanup still queued as cleanup work

## Focus Change 2026-07-01T09:14:55+02:00

Focus: Artifact -> Direction
Trigger: activity digest recall-pack slice committed as e3232dda4
Decision: choose next between inline-test attribution cleanup, phase/event sidecar parsing cleanup, and deeper digest/source freshness refinement

## Focus Change 2026-07-01T09:03:22+02:00

Focus: Direction -> Evidence
Trigger: semantic activity rollup selected from post-meta active loop
Decision: start evidence pass over live context output and existing recall pack projection before deciding implementation shape

## Focus Change 2026-07-01T09:14:39+02:00

Focus: Evidence -> Artifact
Trigger: activity digest context proof captured
Decision: commit the shared view/CLI digest slice after focused tests, build, schema check, and live before/after artifact

## Focus Change 2026-07-01T09:15:12+02:00

Focus: Artifact -> Direction
Trigger: activity digest recall-pack slice committed as e3232dda4
Decision: choose next between inline-test attribution cleanup, phase/event sidecar parsing cleanup, and deeper digest/source freshness refinement

## Focus Change 2026-07-01T09:17:42+02:00

Focus: Direction -> Construction
Trigger: inline-test cleanup selected
Decision: extract low-risk sinexctl formatter inline tests into sibling *_test.rs files without changing visibility

## Focus Change 2026-07-01T09:24:08+02:00

Focus: Proof -> Direction
Trigger: formatter inline-test extraction committed as d98e9dac7
Decision: choose next cleanup target from remaining true inline test bodies; prefer another low-risk sinexctl/xtask module before broadening

## Focus Change 2026-07-01T09:30:20+02:00

Focus: Direction -> Meta
Trigger: operator requested devloop/process improvement after current work; event sidecar parsing was the concrete process bottleneck
Decision: harden devloop-refresh-events so the operating log becomes reliable machine-analyzable loop telemetry

## Focus Change 2026-07-01T09:30:21+02:00

Focus: Meta -> Direction
Trigger: event sidecar parser hardening committed as a62b72207
Decision: return to slice selection with devloop-velocity/PHASES now trustworthy enough for timing review

## Focus Change 2026-07-01T09:31:30+02:00

Focus: Direction -> Meta
Trigger: testing focus primitive current-block rewrite
Decision: verify devloop-focus updates both history and the active Current Focus block

## Focus Change 2026-07-01T09:31:30+02:00

Focus: Meta -> Direction
Trigger: focus primitive current-block rewrite verified
Decision: commit devloop-focus self-maintaining active-state update, then continue meta/object slice selection

## Focus Change 2026-07-01T09:55:22+02:00

Focus: Direction -> Meta
Trigger: post-test-cleanup commit 90fdca0a6 and operator-requested meta shift
Decision: harden the meta primitive and active process scaffold before resuming object-level Sinex work

## Focus Change 2026-07-01T10:06:01+02:00

Focus: Meta -> Artifact
Trigger: memory PSI high, runtime healthy, no broad proof lane
Decision: capture a read-only live activity query artifact instead of starting compile-heavy cleanup

## Focus Change 2026-07-01T10:06:42+02:00

Focus: Artifact -> Evidence
Trigger: activity query signal artifact captured and indexed
Decision: map the grouped/diversified activity projection boundary before editing or compiling

## Focus Change 2026-07-01T10:09:36+02:00

Focus: Evidence -> Meta
Trigger: operator requested post-artifact meta/devloop convention hardening
Decision: make convention enforcement executable before returning to object-level grouped activity projection

## Focus Change 2026-07-01T10:10:36+02:00

Focus: Meta -> Evidence
Trigger: meta scaffold hardening committed and verified
Decision: return to grouped/diversified activity projection boundary with source inspection first

## Focus Change 2026-07-01T10:17:33+02:00

Focus: Artifact -> Direction
Trigger: grouped query projection committed and live artifact captured
Decision: choose next slice: backend aggregate/grouping, source-status projection, or resume inline-test cleanup under current host pressure

## Focus Change 2026-07-01T10:23:05+02:00

Focus: Artifact -> Direction
Trigger: source-driver summary projection committed and live artifact captured
Decision: choose next slice: backend aggregates for query projections, continue inline-test cleanup, or improve source-driver active-first ordering

## Focus Change 2026-07-01T10:34:08+02:00

Focus: Direction -> Meta
Trigger: post-query slice meta shift and xtask overlap tripwire added
Decision: harden devloop resource serialization and then return to object work only after review state is explicit

## Focus Change 2026-07-01T10:36:01+02:00

Focus: Meta -> Direction
Trigger: meta proof-overlap status/review hardening committed
Decision: next object slice should be chosen with high memory PSI in mind: prefer lightweight inline-test cleanup or query descriptor cleanup over broad proof work

## Focus Change 2026-07-01T10:41:12+02:00

Focus: Proof -> Direction
Trigger: readiness descriptor fix committed with live artifact
Decision: next choose lightweight query/source cleanup or inline-test attribution; avoid broad proof while memory PSI remains high

## Focus Change 2026-07-01T10:44:20+02:00

Focus: Proof -> Direction
Trigger: command_catalog inline-test split committed
Decision: continue test-suite attribution cleanup or choose another lightweight query/source cleanup while memory PSI is high

## Focus Change 2026-07-01T10:55:42+02:00

Focus: Direction -> Evidence
Trigger: runtime restored and devloop review has only high-memory warning
Decision: Investigate the activity family latest-vs-timeline mismatch with live runtime evidence; avoid broad compile while memory PSI is high

## Focus Change 2026-07-01T11:12:14+02:00

Focus: Meta -> Direction
Trigger: event sidecar completeness hardening committed
Decision: Meta slice is complete; next choose between further process hardening and object-level Sinex work from live evidence

## Focus Change 2026-07-01T11:18:27+02:00

Focus: Evidence -> Direction
Trigger: context tier projection suspicion retired
Decision: No code change needed for the stale latest/ambient suspicion; choose the next slice from inline-test cleanup, query/source projection, or live demo evidence

## Focus Change 2026-07-01T11:20:08+02:00

Focus: Direction -> Direction
Trigger: devloop-focus next-action projection patched
Decision: Choose the next low-compile slice from live evidence; avoid broad compile while memory PSI remains high

## Focus Change 2026-07-01T11:22:06+02:00

Focus: Meta -> Direction
Trigger: blocked-task pressure diagnostic committed
Decision: Avoid broad compile while borg/btrbk D-state tasks keep memory/IO PSI high; choose source review, live artifact, or a very narrow shell-only scaffold slice next

## Focus Change 2026-07-01T11:23:38+02:00

Focus: Meta -> Direction
Trigger: blocked-task pressure review committed
Decision: Broad compile remains inappropriate while borg/btrbk D-state blockers persist; next work should be source review, live evidence artifact, or very narrow shell-only tooling

## Focus Change 2026-07-01T11:27:38+02:00

Focus: Direction -> Meta
Trigger: devloop contract boundary committed under pressure
Decision: Meta hardening landed in b946f9ae6; while borg D-state pressure persists, continue low-load scaffold/source review or live evidence artifacts, then resume test-suite cleanup once broad proof is reasonable.

## Focus Change 2026-07-01T11:32:44+02:00

Focus: Meta -> Velocity
Trigger: fast status modes committed
Decision: devloop-status --focus/--quick landed in 8082d3759; under current borg/btrbk pressure, continue low-load work or live evidence artifacts until broad proof is reasonable, then resume inline-test cleanup.

## Focus Change 2026-07-01T11:35:25+02:00

Focus: Velocity -> Direction
Trigger: focus status contract fixed
Decision: devloop-status --focus now matches its contract in 470c00a41 and the velocity demo packet was refreshed; next continue low-load source review while pressure persists, or resume inline-test cleanup once host pressure clears.

## Focus Change 2026-07-01T11:38:32+02:00

Focus: Direction -> Construction
Trigger: small inline-test split committed
Decision: Commit 872f641cd moved two small inline test modules into sibling *_test.rs files; continue low-load source cleanup while pressure persists, and run focused xtask tests/checks once host pressure clears.

## Focus Change 2026-07-01T11:40:24+02:00

Focus: Construction -> Construction
Trigger: second small inline-test split committed
Decision: Commit dd6472435 moved watcher and control-protocol inline tests into sibling *_test.rs files; keep doing small source-only attribution cleanup until pressure clears, then run focused xtask proof for the accumulated splits.

## Focus Change 2026-07-01T11:41:35+02:00

Focus: Construction -> Evidence
Trigger: DLQ timeout residue purged
Decision: Runtime is healthy and DLQ-empty after purging sequence 85; continue low-load inline-test/source cleanup while pressure persists, with focused xtask proof queued for when host pressure clears.

## Focus Change 2026-07-01T11:43:15+02:00

Focus: Evidence -> Construction
Trigger: third small inline-test split committed
Decision: Commit d665e190d moved sandbox preflight and runtime tag helper inline tests into sibling *_test.rs files; continue source-only cleanup under pressure, with focused xtask proof queued for accumulated splits when host pressure clears.

## Focus Change 2026-07-01T11:44:55+02:00

Focus: Construction -> Construction
Trigger: db error inline-test split committed
Decision: Commit 901dd27cb moved sinex-db error helper tests into a sibling *_test.rs file; continue source-only cleanup while pressure persists, with focused xtask proof queued for accumulated splits when host pressure clears.

## Focus Change 2026-07-01T11:46:23+02:00

Focus: Construction -> Construction
Trigger: runtime target inline-test split committed
Decision: Commit 3f7c97b3c moved xtask runtime-target tests into a sibling *_test.rs file; continue source-only cleanup while pressure persists, with focused xtask proof queued for accumulated splits when host pressure clears.

## Focus Change 2026-07-01T11:49:46+02:00

Focus: Construction -> Evidence
Trigger: empty-material sidecar write fixed
Decision: Generated devloop files now use temp+rename writes; runtime health is healthy and raw-ingest DLQ is empty, so next proof can return to focused xtask checks when host pressure allows.

## Focus Change 2026-07-01T11:52:51+02:00

Focus: Meta -> Direction
Trigger: active-loop compactness guardrail complete
Decision: Guardrail committed next; then choose between more process hardening, low-risk inline-test attribution cleanup, or returning to query/context demo work when host pressure allows.

## Focus Change 2026-07-01T11:57:31+02:00

Focus: Construction -> Proof
Trigger: runtime socket test split ready
Decision: Commit the sibling-test split, then continue low-load cleanup while memory pressure is high or run focused sinexd proof when pressure clears.

## Focus Change 2026-07-01T11:59:46+02:00

Focus: Construction -> Proof
Trigger: runtime error-helper test split ready
Decision: Commit the error-helper sibling-test split, then continue low-load attribution cleanup or wait for lower pressure before focused sinexd test execution.

## Focus Change 2026-07-01T12:00:11+02:00

Focus: Proof -> Direction
Trigger: low-load runtime test splits committed
Decision: Next: continue low-load inline-test attribution cleanup while host pressure stays high; run focused sinexd test proof when pressure clears enough to justify compilation.

## Focus Change 2026-07-01T12:03:26+02:00

Focus: Construction -> Proof
Trigger: content-store manager test split ready
Decision: Commit the content-store sibling-test split, then continue low-load inline-test attribution cleanup while host pressure remains high.

## Focus Change 2026-07-01T12:04:30+02:00

Focus: Proof -> Direction
Trigger: content-store split committed and DLQ cleared
Decision: Next: continue low-load inline-test attribution cleanup while host pressure stays high; defer focused sinexd test execution until pressure clears or a narrow proof is worth the contention.

## Focus Change 2026-07-01T12:06:18+02:00

Focus: Construction -> Proof
Trigger: checkpoint test split ready
Decision: Commit the checkpoint sibling-test split, then continue low-load inline-test attribution cleanup while host pressure remains high.

## Focus Change 2026-07-01T12:06:28+02:00

Focus: Proof -> Direction
Trigger: checkpoint split committed
Decision: Next: continue low-load inline-test attribution cleanup while host pressure stays high; defer broad compile/test proof until pressure clears or a narrow proof is worth the contention.

## Focus Change 2026-07-01T12:09:17+02:00

Focus: Construction -> Proof
Trigger: preflight service test split ready
Decision: Commit the preflight service sibling-test split, then continue low-load inline-test attribution cleanup while host pressure remains high.

## Focus Change 2026-07-01T12:09:27+02:00

Focus: Proof -> Direction
Trigger: preflight service split committed
Decision: Next: continue low-load inline-test attribution cleanup while host pressure stays high; defer broad compile/test proof until pressure clears or a narrow proof is worth the contention.

## Focus Change 2026-07-01T12:13:02+02:00

Focus: Construction -> Proof
Trigger: db pool test split ready
Decision: Commit the DB pool sibling-test split and formatter cleanup, then continue low-load attribution cleanup while host pressure remains high.

## Focus Change 2026-07-01T12:13:15+02:00

Focus: Proof -> Direction
Trigger: db pool split committed
Decision: Next: continue low-load inline-test attribution cleanup while host pressure stays high; defer broad compile/test proof until pressure clears or a narrow proof is worth the contention.

## Focus Change 2026-07-01T12:19:20+02:00

Focus: Direction -> Proof
Trigger: source-driver inline-test split proof complete
Decision: Commit the low-load cleanup slice, then shift next slice to devloop/meta convention improvements per attached cross-pollination spec.

## Focus Change 2026-07-01T12:23:41+02:00

Focus: Proof -> Meta
Trigger: implementation slice committed and operator requested meta/process work next
Decision: Finish and commit executable queue lifecycle support, then use review/status evidence to pick the next object-level Sinex capability slice.

## Focus Change 2026-07-01T12:24:20+02:00

Focus: Meta -> Direction
Trigger: queue lifecycle scaffold committed
Decision: Choose the next object-level Sinex capability/demo slice; under current host pressure prefer low-load source review, query/demo planning, or narrow proof only.

## Focus Change 2026-07-01T12:27:12+02:00

Focus: Artifact -> Direction
Trigger: demo hygiene catalog projection verified
Decision: Commit the demo refresh generator improvement, then choose between fixing the worst activity-query-signal summaries or returning to object-level query/context capability work.

## Focus Change 2026-07-01T12:27:47+02:00

Focus: Direction -> Direction
Trigger: demo hygiene generator committed
Decision: Choose between fixing the worst activity-query-signal summary packets surfaced by Demo Hygiene, or returning to object-level query/context capability work when host pressure allows.

## Focus Change 2026-07-01T12:31:03+02:00

Focus: Artifact -> Direction
Trigger: activity-query-signal hygiene repaired and sync hardening committed
Decision: Choose the next slice: repair the next weak demo packet if host IO remains high, or return to grouped activity/query projection when broad proof becomes acceptable.

## Focus Change 2026-07-01T12:33:02+02:00

Focus: Artifact -> Direction
Trigger: activity-group-samples hygiene repaired
Decision: Host pressure still discourages broad proof; next choose between continuing demo hygiene on the remaining top weak packets or returning to grouped activity/query projection once pressure clears.

## Focus Change 2026-07-01T12:36:49+02:00

Focus: Direction -> Meta
Trigger: DLQ demo metadata repair closed the current artifact slice
Decision: Implement devloop/process convergence hardening from the Sinex/Polylogue convention spec: keep conductor-devloop as active source of truth, reduce stale ACTIVE-LOOP risk, and make shared primitives enforce the contract locally.

## Focus Change 2026-07-01T12:38:24+02:00

Focus: Meta -> Meta
Trigger: meta hardening verified
Decision: Commit the tracked devloop script changes, then continue with low-load Sinex work while host pressure remains high.

## Focus Change 2026-07-01T12:41:49+02:00

Focus: Direction -> Artifact
Trigger: source/query demo hygiene repaired
Selected/improved demo: source-driver-* and query-signal metadata summaries
Artifact action: Added machine-readable claim boundaries to five ignored demo summary JSON files and refreshed SUMMARY_INDEX/CURATED_CATALOG.
Proof/caveat: Proves existing captured live demo packets are now inspectable by catalog tooling; does not add new runtime capability in this slice.
Decision: Next choose between remaining timeline/source-id demo hygiene or returning to object-level query/source algebra once host pressure clears.

## Focus Change 2026-07-01T12:43:05+02:00

Focus: Direction -> Artifact
Trigger: source-id query bridge demo hygiene repaired
Selected/improved demo: source-id-query-bridge-20260630T181822Z summaries
Artifact action: Added claim/non-claim/proof/caveats to fs, terminal, and xtask-status summary JSON files; refreshed demo catalog.
Proof/caveat: Proves the existing source-contract-ID normalization demo is now machine-indexable; does not add new query behavior in this slice.
Decision: Next low-load option is timeline/context demo metadata; object-level query/source work should wait for lower host pressure.

## Focus Change 2026-07-01T12:48:08+02:00

Focus: Direction -> Artifact
Trigger: demo summary hygiene completed
Selected/improved demo: complete .agent/demos/sinex summary metadata coverage
Artifact action: Added claim/non-claim/proof/caveats metadata to remaining summary JSON records and refreshed the generated catalog/index.
Proof/caveat: Generated SUMMARY_INDEX coverage is now zero missing claim/non-claim/proof/caveat fields across 42 summary records; this is demo-shelf metadata proof, not new runtime functionality.
Decision: Next return to object-level Sinex capability work when pressure permits; current best candidates are query/source algebra, source status projection, or test-suite attribution cleanup.

## Focus Change 2026-07-01T12:51:19+02:00

Focus: Direction -> Artifact
Trigger: live source posture packet captured
Selected/improved demo: live-source-posture-20260701T105000Z
Artifact action: Captured source status, source-driver ready/eventful JSON+table outputs, activity signal JSON+table output, README, and summary metadata.
Proof/caveat: Proves current checkout-local source/query posture and keeps SUMMARY_INDEX zero-gap across 43 summary records; does not remediate missing-material sources.
Decision: Next object-level candidates remain source-status projection cleanup, query/source algebra, or inline-test attribution once host pressure clears.

## Focus Change 2026-07-01T12:57:51+02:00

Focus: Artifact -> Meta
Trigger: source-status summary slice verified
Decision: Shift next work to devloop/meta scaffold improvement using attached Sinex/Polylogue convention analysis; specifically harden proof serialization and reduce context clutter.

## Focus Change 2026-07-01T13:00:46+02:00

Focus: Meta -> Meta
Trigger: meta guardrails committed
Decision: Remain in Meta long enough to continue scaffold/process cleanup; next candidates are reducing conductor context noise and improving proof-serialization prompts.

## Focus Change 2026-07-01T13:07:37+02:00

Focus: Artifact -> Direction
Trigger: live dogfood capture packet indexed
Decision: Choose the next object-level slice from the packet caveats: deploy/restart summary projection, reduce terminal source identity noise, or improve query affordances.

## Focus Change 2026-07-01T13:07:49+02:00

Focus: Direction -> Artifact
Trigger: live dogfood capture packet
Selected/improved demo: live-dogfood-capture-20260701T110451Z
Artifact action: created and indexed .agent/demos/sinex/live-dogfood-capture-20260701T110451Z
Proof/caveat: Proof: runtime healthy, DLQ empty, ready active fs/git/system/terminal sources, terminal 24h query returns 12 shell.atuin rows via normalization, activity 30m query returns fs/git/shell rows. Caveat: live runtime lacks payload.summary until rebuilt/restarted with da55f0e89.
Decision: Which packet caveat should become the next object-level slice: deploy/restart summary projection, reduce terminal source identity noise, or improve query affordances?

## Focus Change 2026-07-01T13:10:26+02:00

Focus: Proof -> Artifact
Trigger: query source normalization bridge verified
Decision: Commit the query table projection fix, then decide whether to rebuild/restart for live artifact refresh or continue with another low-pressure object slice.

## Focus Change 2026-07-01T13:15:16+02:00

Focus: Artifact -> Direction
Trigger: post-normalization query demo refreshed
Decision: Next choose between daemon rebuild/restart for SourceCoverageSummaryView live proof when pressure allows, or another low-pressure source/query affordance slice.

## Focus Change 2026-07-01T13:25:01+02:00

Focus: Direction -> Meta
Trigger: live dogfood proof closed; operator requested meta shift
Decision: Harden projection freshness and convention enforcement before returning to object-level Sinex slices

## Focus Change 2026-07-01T13:33:54+02:00

Focus: Direction -> Artifact
Trigger: current context recall now includes shell activity
Selected/improved demo: activity-context-fs-git-shell-20260701T113145Z
Artifact action: Created ignored demo packet with context JSON/table, terminal source status, terminal-family query proof, README, and summary; refreshed demo manifests.
Proof/caveat: Live packet proves 3724 operator activity events across fs-watcher, git, and shell; terminal-family query returns shell.atuin rows through normalization; caveat: high host PSI, filesystem-dominated window, no browser coverage.
Decision: Next: improve source/query projection so logical terminal family and stored shell.atuin identity are easier to understand without caveat prose.

## Focus Change 2026-07-01T13:52:58+02:00

Focus: Direction -> Artifact
Trigger: browser acquisition gap surfaced
Selected/improved demo: browser-source-posture-20260701T115042Z
Artifact action: added live packet and refreshed demo index to 46 summaries
Proof/caveat: proof shows inspectable missing-material/gapped acquisition sources, not working browser ingestion
Decision: should the next object slice implement browser material acquisition or first improve the devloop/conductor scaffold per operator directive?

## Focus Change 2026-07-01T13:56:34+02:00

Focus: Meta -> Direction
Trigger: meta compact-sidecar pass closed
Decision: resume object-level demo/capability slice selection

## Focus Change 2026-07-01T14:05:52+02:00

Focus: Direction -> Artifact
Trigger: browser family query/status silo collapsed
Selected/improved demo: browser-family-query-aliases-20260701T120251Z
Artifact action: added demo packet and refreshed catalog to 47 summaries
Proof/caveat: proof shows unified selector semantics and continued zero-event acquisition gap; it does not claim ingestion works
Decision: next high-value slice: browser material acquisition vs query/status alias explanation UX

## Focus Change 2026-07-01T14:05:52+02:00

Focus: Artifact -> Direction
Trigger: browser family alias artifact committed
Decision: select next demo-producing slice from remaining browser acquisition/query gaps

## Focus Change 2026-07-01T14:15:43+02:00

Focus: Direction -> Artifact
Trigger: browser acquisition gap converted to live material
Selected/improved demo: browser-history-dev-binding-live-20260701T121335Z
Artifact action: added live acquisition demo and refreshed catalog to 48 summaries
Proof/caveat: proof shows browser.history ready/active with 23921 events; it does not claim raindrop or recent browser activity
Decision: next: recent/live browser capture freshness or raindrop staged export

## Focus Change 2026-07-01T14:15:43+02:00

Focus: Artifact -> Direction
Trigger: browser history acquisition artifact committed
Decision: choose next demo slice: live browser freshness or raindrop staged export

## Focus Change 2026-07-01T15:11:21+02:00

Focus: Direction -> Artifact
Trigger: raindrop dev binding proof
Selected/improved demo: raindrop-bookmarks-dev-binding-live-20260701T1307Z
Artifact action: Captured DB/NATS/source-status/query packet and refreshed demo manifests
Proof/caveat: Proof used isolated Raindrop binding to avoid unrelated browser-history backlog; full dogfood manifest still needs backlog/resource tuning
Decision: Should the next slice attack browser-history backlog/material assembly throughput or promote source checkpoint reset/observability further?

## Focus Change 2026-07-01T15:17:50+02:00

Focus: Artifact -> Velocity
Trigger: focused dev-bindings control verified
Decision: Use xtask-supported source filters for high-velocity live-data proof loops; next object slice can attack browser-history/material backlog with less demo coupling.

## Focus Change 2026-07-01T15:26:33+02:00

Focus: Velocity -> Artifact
Trigger: stream pressure status proof captured
Decision: Commit the xtask status stream-pressure surface and demo packet; then choose between retained-stream cleanup and the next live-data capability slice.

## Focus Change 2026-07-01T15:28:09+02:00

Focus: Artifact -> Direction
Trigger: runtime evidence stale after stream-pressure cleanup
Decision: Start a runtime-presence/freshness slice: bring checkout-local sinexd up explicitly, verify gateway/runtime metrics/source activity, and capture a demo packet proving live store readiness.

## Focus Change 2026-07-01T15:38:19+02:00

Focus: Direction -> Velocity
Trigger: focused runtime baseline proved
Decision: Next improve launch ergonomics so focused runtime is a first-class xtask path, or inspect material assembly/source registry ordering if returning to full-manifest throughput.

## Focus Change 2026-07-01T15:39:15+02:00

Focus: Velocity -> Evidence
Trigger: operator rejected focused-runtime workaround as insufficient
Decision: Investigate and fix the full-manifest material backlog root cause: source-material frame publication, registry ordering, material assembler throughput, and event-engine staleness under browser/terminal backfill.

## Focus Change 2026-07-01T18:06:47+02:00

Focus: Evidence -> Artifact
Trigger: full-manifest pressure fix committed and live daemon proof passed
Decision: Package the full-runtime pressure proof as a demo artifact, refresh demo manifests, then choose the next object slice from stale source-material lifecycle reconciliation or query/demo capability.

## Focus Change 2026-07-01T18:41:04+02:00

Focus: Artifact -> Direction
Trigger: resource-control proof packet captured
Decision: Choose next object slice from live evidence: pressure observability gap, stale infra-lock reporting, or query/demo source-material lifecycle.

## Focus Change 2026-07-01T20:40:19+02:00

Focus: Direction -> Construction
Trigger: DLQ cleanup-plan left transient timeout group 156..163 but requeue lacks a sequence-range selector
Decision: Implement bounded DLQ sequence-range requeue instead of using --all against unrelated retained messages

## Focus Change 2026-07-01T20:46:03+02:00

Focus: Construction -> Proof
Trigger: bounded DLQ requeue implementation passed package tests
Decision: Restart dev-local sinexd and prove sequence-range requeue on live DLQ group 156..163

## Focus Change 2026-07-01T21:08:42+02:00

Focus: Direction -> Direction
Trigger: operator clarified integration means PR flow and set 16-24 PR target
Decision: Use LLM-reasoned 20-phase integration plan in local INTEGRATION.md; do not publish mechanical micro-slices

## Focus Change 2026-07-01T21:13:05+02:00

Focus: Direction -> Direction
Trigger: integration is a lane, not a focus mode
Decision: Keep focus mode Direction while the active slice is branch integration planning

## Focus Change 2026-07-02T10:26:22+02:00

Focus: Construction -> Evidence
Trigger: source-status/default latency fixed and devloop review now only warns about stale active state plus fs/git critical source liveness
Decision: Investigate fs and git-commit-history liveness/status semantics, then patch the generic runtime/projection layer or source binding path with live proof.

## Focus Change 2026-07-02T23:59:33+02:00

Focus: Direction -> Artifact
Trigger: catch-up material remediation proof
Selected/improved demo: catchup-material-remediation
Artifact action: created .agent/demos/sinex/catchup-material-remediation-20260702T215524Z and refreshed demo manifests
Proof/caveat: live proof shows remediation_candidates=759 remediation_events=2229660 and top browser.history 206899-event recover_timeout_partial candidate; caveat: runtime remains blocked because this is read-only visibility, not debt mutation
Decision: Next: either implement bounded remediation action policy for recover_timeout_partial rows or shift to event-query lowering if visibility is sufficient for Recall v2.

## Focus Change 2026-07-03T00:17:33+02:00

Focus: Direction -> Artifact
Trigger: queued Recall v2 baseline-arm directive
Selected/improved demo: sinex-recall-v2-audit
Artifact action: created .agent/demos/sinex/sinex-recall-v2-audit-20260703T0013Z and refreshed demo manifests
Proof/caveat: proof: context has 201 events across 9 sources including shell.atuin and fs-watcher plus raw git/Atuin baselines; caveat: browser is absent so this is not terminal
Decision: browser-history dev config repaired; current configured browser materials are stale, so next is live Chrome/browser acquisition or narrower Recall v2 proof claim

## Focus Change 2026-07-03T00:42:19+02:00

Focus: Artifact -> Evidence
Trigger: Recall v2 terminal proof still lacks browser/git signal; source pulse shows browser.history and git-commit-history runtimes hot but output-quiet
Decision: investigate source participation before producing another Recall v2 packet

## Focus Change 2026-07-03T12:03:38+02:00

Focus: Direction -> Construction
Trigger: research writebacks complete; no raw analysis beads remain
Decision: claim sinex-17w and implement narrow material-assembler timeout classification

## Focus Change 2026-07-03T15:59:05+02:00

Focus: Direction -> Evidence
Trigger: sinex-vhm remaining scope narrowed after live recall proof
Decision: Run a bounded checkout-local dev-runtime recall/session proof: start dev sinexd if absent, verify source bindings, prove recall parity and session-detector degradation without broad production event-type scans.
