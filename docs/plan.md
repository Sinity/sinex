Status: working document (consolidated, prescriptive)

# Sinex Refactor Plan — Consolidated Blueprint to End‑State

This plan consolidates the evolving blueprints and plan_v3 into a single, actionable end‑state specification and a sequence of concrete steps. We optimize for a clean refactor that compiles at each phase. Backward compatibility is not required during refactor; correctness will follow compilation.

## 0. Scope, Goals, Non‑Goals

- Goals:
  - Move ingestion to NATS JetStream: satellites publish; ingestd archives.
  - Standardize provenance + invariants at DB with strong validation.
  - Provide SDK primitives for acquisition (source material) and event publishing.
  - Keep the repo compiling at each step (rebase-friendly, small surface changes).
- Non‑Goals (for this refactor):
  - Feature completeness for all satellites/automata.
  - Full UI or gateway authentication redesign.

## 1. End‑State Architecture (Authoritative)

- Data primitives
  - Evidence: Source Material stored in git‑annex; registry `raw.source_material_registry`.
  - Information: `core.events` with XOR provenance:
    - Material: `{ source_material_id, anchor_byte, offset_start?, offset_end?, offset_kind }`
    - Synthesis: `{ source_event_ids: NonEmpty, operation_id? }`
  - Invariants (DB‑enforced):
    - XOR provenance CHECK constraint.
    - Idempotency UNIQUE: `(source_material_id, anchor_byte, id)` (include partition key for hypertable uniqueness semantics).
    - Append‑only `raw.temporal_ledger` trigger (capture window → ts mapping).
    - JSON Schema validation via `sinex_schemas`.

- Streams (JetStream; env‑namespaced subjects)
  - `source_material.begin` / `source_material.slices.<material_id>` / `source_material.end`
  - `events.raw` (provisional events)
  - `events.confirmations` (compacted confirmation set; `{ event_id, ts_ingest }`)
  - `events.dlq` (persistent failures)
  - Dedupe: `Nats-Msg-Id` on all publishes; DB uniqueness ensures idempotency after persist.

- ingestd (Universal Archiver)
  - Consumers:
    - Materials: assemble slices → content hashes → move to annex → finalize registry; reaper for abandoned.
    - Events: validate → batch insert via `UNNEST` → publish confirmations → ack.
  - Transitional (during refactor): may keep gRPC server disabled; not required.

- SDK (`sinex-satellite-sdk`)
  - Acquisition: `AcquisitionManager` + `SourceMaterialHandle` (begin/append/finalize).
  - Publishing: `NatsPublisher` for events/slices; env‑namespaced subjects; timers/retries/backoff.
  - Runner: `StreamProcessorRunner` that buffers provisional and delivers confirmed by default; optional provisional fast‑path; KV (NATS) leases for leader/standby.
  - Testkit: in‑memory mocks for unit tests.

- Satellites/Automata
  - No DB writes; publish only.
  - Phases: snapshot → gap‑fill → continuous; checkpointed.
  - Automata process confirmed events by default; may opt‑in provisional.

- Control Plane
  - KV buckets: `sinex_manifests` (manifests), `leadership_leases` (TTL lease).
  - Subjects: `sinex.control.*` (replay, config, coordination).

## 2. Repo Changes (Top‑Level)

- `docs/plan.md` (this file) is the authoritative refactor plan.
- Keep workspace compiling by limiting workspace members while we land core building blocks.
- sensd decommission plan:
  - Salvage reusable acquisition code into SDK.
  - Remove crate after migration paths compile.

## 3. Phases & Deliverables (Compilation First)

Phase 1 — Event Path via JetStream (Minimal Viable Backbone)
- ingestd: add `events.raw` consumer → batch insert → publish to `events.confirmations`.
- ingestd: bootstrap streams `events.raw`, `events.confirmations` on startup (env‑namespaced).
- SDK: `NatsPublisher::publish_event` with `Nats-Msg-Id` from `event_id`.
- Satellite (one): behind feature flag `ingest=nats`, publish to `events.raw` using SDK.
- Tests: unit for insert + confirm; integration publishes → persisted → confirmation.
- Deliverable: cargo check/build passes; one E2E integration test green (optional if infra unavailable locally).

Phase 2 — Confirmation‑Aware Runner (Automata‑Ready)
- SDK: `StreamProcessorRunner` buffering provisional until confirmed; configurable low‑latency provisional handler.
- ingestd: dedupe semantics verified; DLQ stubbed (enqueue on unrecoverable errors).
- Satellite (more): add `ingest=nats` path; default to NATS in dev.
- Deliverable: compiles; basic runner unit tests.

Phase 3 — Source Material (Slices)
- SDK: `AcquisitionManager` + `SourceMaterialHandle` (begin/append/finalize) publishing to `source_material.*`.
- ingestd: material assembler consumer; annex write + registry finalize; reaper.
- Migrate one path (e.g., terminal recordings) to slice ingestion.
- Deliverable: compiles; happy‑path unit tests.

Phase 4 — Coordination & Control Plane
- KV: `sinex_manifests`, `leadership_leases` (TTL) wiring in SDK.
- Subjects: `sinex.control.*` initial handlers (e.g., ping, basic replay stub).
- Deliverable: compiles; smoke tests.

Phase 5 — Cleanup
- Remove gRPC ingestion from ingestd and SDK client paths.
- Remove sensd crate after salvage.
- Expand tests, integrate more satellites/automata.

## 4. Task Breakdown (Initial Landing)

- ingestd
  - [ ] Add `events.raw` consumer module.
  - [ ] Batch insert with `UNNEST`; leverage `sinex-core` types.
  - [ ] Publish `events.confirmations` (subject env‑namespaced) post‑commit.
  - [ ] Stream bootstrap on startup (create/get `events.raw`, `events.confirmations`).
  - [ ] Config: enable_nats_events_consumer (default true during refactor).

- SDK (`sinex-satellite-sdk`)
  - [ ] `NatsPublisher` with minimal config (url, env namespacing, retries).
  - [ ] `publish_event(Event<JsonValue>)` → subject `events.raw`.
  - [ ] Feature gate legacy gRPC ingestion (temporary or disabled).

- Satellite (pick one: fs‑watcher)
  - [ ] Add `--ingest nats|grpc` CLI flag (default nats after Phase 2).
  - [ ] Use SDK `publish_event` in scanner/sensor paths.

- Tests
  - [ ] Unit: insert + confirm path in ingestd.
  - [ ] Integration: provisional publish → persisted → confirmation emitted.

## 5. Compilation Cadence & CI Guardrails

- For each patch:
  - `cargo check --workspace` before/after edits.
  - Keep workspace members minimal; enable crates as we refactor them.
  - Avoid API churn touching unrelated crates in the same patch.
  - Prefer feature flags over branching code paths.

- CI (later):
  - Add a fast `check` job; optional `integration` matrix running NATS locally.

## 6. Risks & Mitigations

- Risk: Large compile break when enabling multiple crates.
  - Mitigation: phase enabling; compile after each file/module addition.
- Risk: Schema drift (SQLx offline cache).
  - Mitigation: run `just sqlx-prepare` after schema changes; keep `.sqlx/` current.
- Risk: NATS dependencies & runtime.
  - Mitigation: feature flag code; no runtime dependency for `cargo check`.

---

This plan is the working source of truth for the refactor. Update as modules land.

