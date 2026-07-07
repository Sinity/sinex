---
created: "2026-06-29T13:41:00+02:00"
purpose: "Synthesize parallel devloop/codebase audits into a prioritized implementation queue"
status: "active"
project: "sinex"
---

# Devloop Audit Swarm

## Context

The current devloop goal is rapid Sinex capability growth guided by useful,
impressive demos, with dev-loop velocity treated as a first-class product
constraint. Three read-only explorer agents audited:

- compositional/demo view algebra
- divergent runtime/API/CLI handling
- compile/test/generated-surface tax

This note preserves the high-signal findings so the next slices can be chosen
from evidence rather than chat memory.

## Highest Priority Findings

### 1. Source status likely undercounts material

`sinexd::api::handlers::source_status` groups
`raw.source_material_registry.source_identifier` by exact string and compares it
to source contract IDs. Runtime material registration stores a material-scoped
wire identifier using `SourceIdentifier::new(logical_source_identifier,
material_id).to_wire()`. That means `sources status` can claim
`MissingMaterial` for a logical source whose material exists under a
material-scoped identifier.

Next slice: add a shared parser/helper that extracts the logical source ID from
wire source identifiers, then have `sources status` group by logical ID.

Why first: this is both correctness and demo quality. Source readiness is a
front-door Sinex demo surface; false missing-material status undermines trust.

### 2. Recall/context intelligence is CLI-private

`sinexctl events context` now emits useful context-pack artifacts, but
`RecallPack`, `FamilyActivity`, family tiers, samples, and self-observation
accounting are private CLI structs. `sinex-primitives::views` already has thinner
context view types, but not the richer recall lens.

Next slice: promote a versioned `ContextRecallPackView` into shared views, then
make CLI/API/TUI render from the same object.

Why: this is one of the strongest “Sinex is useful on real work” demos.

### 3. Reports/demo/source cockpit are not compositional enough

Daily report JSON/table logic, demo seeding, and source status/TUI source
cockpit all have local projection/state/rendering rules. The repeated pattern is
shared DTOs without shared algebra.

Next slices:

- `ActivityReportView` / `ActivityCalendarView`
- typed `DemoScenario` / `DemoEventIntent`
- `SourceCockpitView` helpers for readiness, continuity, labels, summaries, and
  primary action grouping

### 4. Error/status models still rely on string classification

JSON-RPC server maps `SinexError` into structured public payloads, but some API
paths special-case unknown methods by message prefix, and the CLI classifies
errors with HTTP status plus lowercase string contains.

Next slice: shared `RpcErrorClass` with kind/status/rpc_code/retryable/help key,
emitted by server and consumed by client.

### 5. Compile/test tax frontiers moved but remain real

The committed content-store refactor removed the immediate `sinexctl -> sinexd`
compile dependency. Remaining large hotspots:

- `xtask` is monolithic and exports sandbox/runtime-heavy dependencies through
  the everyday tooling crate.
- `sinexctl` still pays DB/CAS admin tax by default through direct local blob
  maintenance.
- `changed-strict` serializes per-package child `xtask check` invocations.
- generated docs/schema checks appear broader than local CI policy implies.

Next slices should be measurement-first:

- rank xtask compile invalidations and prototype `xtask-core`/sandbox split or
  feature boundary without dissolving the shared sandbox semantics
- batch `changed-strict` package checks into one cargo/check invocation while
  preserving per-package reporting
- add changed-file-aware docs checks instead of removing drift gates

## Working Conclusion

The audit does not argue against refactoring. It argues for refactors that make
the demo/control surfaces more truthful and more reusable while reducing the
compile/test tax paid by every next slice. The best immediate move is a small
correctness fix on source material identity, followed by promoting context/report
views into shared algebra.

## 2026-06-29 Loop Outcomes

- Source material identity: fixed in `60385f8dc` by grouping source coverage on
  normalized logical `SourceIdentifier` instead of material-scoped wire strings.
- Context recall algebra: fixed in `f6c120844` by promoting the recall-pack DTO
  into `sinex_primitives::views::events::ContextRecallPackView`; the CLI now
  renders a shared view object instead of owning the data shape.
- Dev-loop velocity: the next measured tax was `xtask --bg` launch latency and
  schema contention. The shell wrapper was still applying SQLx schema before a
  launcher-only background request could return, and the Rust preflight follower
  path waited on a readiness bit instead of the schema-apply lock holder. The
  queued fix skips SQLx bootstrap for launcher-only `--bg` requests and makes
  schema followers wait for the lock critical section before proceeding.
- Activity report algebra: daily/calendar report JSON shapes were promoted out
  of `sinexctl` ad hoc `json!` assembly into shared activity view DTOs. The CLI
  now maps query aggregation transport rows into view-native count/value/bucket
  rows so report demos can be reused by API/TUI/MCP surfaces later without
  depending on query internals.
- Demo seed algebra: Polylogue's DSL/projection lesson maps cleanly to Sinex
  demos as "shared intent/plan, edge-local rendering/materialization." The
  first v1 slice is `DemoSeedPlanView`: scenario id, source/material URI, count,
  batch size, and weighted event families live in `sinex-primitives::views`;
  `sinexctl ops demo` now materializes DB rows from that shared plan instead of
  owning the scenario vocabulary privately.
- Dev-loop wrapper reload boundary: launching three `xtask --bg` commands from
  the current Codex shell reproduced the old checkout-local schema apply race
  (`duplicate key value ... ix_archived_events_ts_orig`) because PATH still
  resolved the stale Nix-store wrapper without the launcher-only predicate.
  `nix develop --command ...` uses the regenerated wrapper, returns a background
  job id without SQLx bootstrap output, and the smoke job passed. Source is
  fixed; long-lived agent shells need reload/`nix develop --command` to see it.
- Source material list algebra: `sinexctl sources list` no longer owns a private
  JSON envelope payload for source-material inventory. `SourceMaterialListView`
  and `SOURCE_MATERIAL_LIST_SCHEMA_VERSION` now live in
  `sinex-primitives::views`, backed by `SourceMaterialSummary` with schema
  generation. The CLI still owns table rendering, but the JSON/API/TUI contract
  is now shared.
- Runtime module list algebra: `sinexctl runtime list` no longer owns its
  envelope payload either. `RuntimeModuleListView` now lives in shared views,
  backed by `InstanceInfo` with schema generation, so the runtime inventory demo
  has the same reusable API/TUI contract shape as source material inventory.
- Source material coverage algebra: `sinexctl sources coverage` no longer owns
  a private raw coverage envelope. The raw registry coverage rows remain distinct
  from the richer cockpit `SourceCoverageView`; the shared
  `SourceMaterialCoverageListView` makes that distinction explicit and reusable
  without inventing readiness/binding data that the raw RPC surface does not
  carry.
- Source readiness algebra: `sinexctl status` no longer owns the count/status
  classifier for `SourceReadiness` rows. `SourceReadinessSummary` and
  `summarize_source_readiness` now live in shared source views, keeping CLI
  prose/rendering local while making the health rollup reusable by API/TUI/demo
  surfaces.
- Source action registry: inspired by Polylogue's query-action registry shape,
  source operation affordance construction moved from the `sinexd` source-status
  handler into shared source views. The API handler now assembles source facts
  and asks the shared registry for labels, command hints, RPC methods, effects,
  and confirmation posture, so API/TUI/demo surfaces can reuse one action
  vocabulary.
- RPC error algebra: the Polylogue DSL lesson here is typed contract/registry
  first, syntax second. Unknown-method handling moved from server/client display
  string checks into a shared `RpcErrorClass` envelope in primitives, with a
  safe public context marker for `method_not_found`. Server protocol mapping,
  API registration, and CLI help classification now share that vocabulary, so
  later demos/API/TUI surfaces can classify gateway failures structurally
  instead of rediscovering JSON-RPC code/status/message heuristics.
- Command-center algebra: the `sinexctl` first screen now uses a shared
  `CommandCenterView` instead of private CLI structs. This keeps the demo
  launchpad vocabulary — runtime target, primary actions, root groups, and
  default format — reusable by future API/TUI surfaces while leaving terminal
  rendering local to `sinexctl`. Dev-loop note: new source files must be staged
  before Nix-backed `xtask` rebuilds, because the flake source snapshot cannot
  see untracked module files; otherwise the launcher reports a misleading
  `file not found for module` error before normal local compilation.
- Completion endpoint algebra: `_complete` now returns shared
  `CompletionResponseView` / `CompletionCandidateView` DTOs from primitives
  instead of CLI-private structs. The completion algorithm remains in
  `sinexctl`, but shell/picker/API/TUI consumers can now share a stable
  schema-versioned candidate contract with replacement spans, grouping,
  privacy/danger posture, staleness, previews, and scores.
- Baseline verification algebra: `ops verify baseline` now emits shared
  `BaselineReportView` / `BaselineCheckView` DTOs from primitives instead of
  CLI-private structs. The check runners and terminal rendering stay in
  `sinexctl`, while score weights, statuses, serialization, and schema coverage
  become reusable by demo, CI, API, and TUI readiness surfaces.
- Operator surface catalog algebra: `sinexctl --list-formats --format
  json|yaml` now projects the CLI/RPC/MCP registry into a shared
  `OperatorSurfaceCatalogView` rather than a CLI-private numeric-versioned
  struct. The underlying CLI registries stay where their dependencies live, but
  the external contract is now a schema-versioned, string-normalized view that
  future demos, API docs, TUI command palettes, and MCP surfaces can consume
  without re-parsing `sinexctl` internals.
- Privacy audit algebra: `sinexctl privacy audit` now emits a shared
  `PrivacyAuditReportView` with private-mode, DLQ, source-readiness, and finding
  rows. Runtime RPC collection and table rendering stay local to `sinexctl`, but
  the privacy posture artifact is now schema-versioned and reusable by demos,
  API/TUI status panels, and operator evidence bundles.
- Verification summary algebra: generic `sinexctl ops verify --format
  json|yaml` now emits a shared `VerificationSummaryView` instead of a private
  ad hoc JSON object, and source-contract inventory uses
  `SourceContractsReportView`. The live checks and terminal glyphs stay local,
  but CI, demos, API docs, and TUI evidence panels can now consume one
  schema-versioned pass/skip/warn/fail record contract.
- Privacy export algebra: `sinexctl privacy export` now emits shared
  `PrivacyExportReportView` / `PrivacyExportEventView` / provenance/scope/receipt
  DTOs instead of CLI-private numeric-versioned structs. The redaction/query/file
  writing behavior stays local, while the metadata-only export artifact becomes a
  schema-versioned evidence primitive for demos, operator bundles, and future API
  or TUI privacy workflows.
- Query-unit catalog algebra: borrowing the useful part of Polylogue's DSL
  posture, `sinexctl query --catalog` now exposes the Sinex query-unit registry
  as a shared `QueryUnitCatalogView`. The parser/executor stays intentionally
  small, but units, fields, operators, sort keys, backing RPC methods, limits,
  and command hints become inspectable by CLI, docs, TUI, API, and demo surfaces
  from one registry-derived contract.
- Blob maintenance algebra: `ops blob sweep-orphans`, `fsck`, `migrate`, and
  `verify-integrity` now build shared `Blob*View` DTOs in primitives instead of
  CLI-private summary structs. The direct DB/CAS maintenance behavior and table
  rendering stay local to `sinexctl`, while storage health and integrity
  evidence become schema-versioned artifacts suitable for demos, TUI panels,
  API wrappers, and operator evidence bundles.
- Runtime presence algebra: `sinexctl runtime modules` now builds a shared
  `RuntimePresenceListView` instead of CLI-local enriched rows and ad hoc
  JSON/YAML. Raw runtime inventory remains `RuntimeModuleListView`; the
  presence view owns the operator-facing health/stale/unknown classification,
  counts, heartbeat display fields, and leader/host/module labels for future
  TUI, API, and demo reuse.
