# Sinex JetStream Refactor Playbook

_Status: authoritative. This document supersedes `docs/plan.md` and `docs/plan_v3.txt` for the ingestion refactor. Keep those files only for historical context._

We are replacing the legacy “satellites → sensd (gRPC) → ingestd → Postgres/Redis” pipeline with a JetStream‑first architecture. The end state is:

- Satellites publish source material slices and provisional events directly to NATS JetStream.
- `sinex-ingestd` is a durable JetStream consumer that archives into Postgres/git-annex and emits confirmations.
- Automata consume confirmed events from JetStream.
- sensd and its gRPC surface disappear entirely; Stage-as-You-Go lives in the SDK.

There is no long-lived dual path. We land work in small, compiling increments, but the target configuration is JetStream-only.

---

## Implementation Status (Updated 2025-02-21)

**Overall Progress: Phases 1–5 Complete | Replay Tooling Hardening Pending**

### Completed ✅
- **Phase 1 - Event Backbone**: NatsPublisher, JetStreamConsumer, confirmations, DLQ, idempotency
- **Phase 2 - Confirmation-Aware Consumption**: ConfirmationBuffer, AutomatonEventHandler, DLQ retry
- **Phase 4 - Coordination & Control Plane**: LeaseManager with NATS KV, leader election
- ✅ Automaton bridge: health-aggregator now consumes JetStream confirmations via StreamProcessorRunner
- **Phase 3 - Source Material Slices**: `AcquisitionManager` + Stage-as-You-Go capture git-annex materials, MaterialAssembler survives restarts, ledger + blob metadata populated
- **Phase 5 - Cleanup**: gRPC surface removed, sensd code paths deleted, transactional outbox retired, docs updated

**Satellites**: 6/8 modern and buildable (75% complete)
- ✅ All using `processor_main!` and NATS JetStream
- ✅ Zero legacy gRPC patterns
- ✅ Modern: fs-watcher, desktop, terminal, system, document-ingestor, health-aggregator, terminal-canonicalizer
- 🔄 Blocked: analytics-automaton, search-automaton (deeper refactoring)

**Testing**
- `just test` uses Nextest’s `reliable` profile (2 threads) to keep property/fixture suites stable.
- Satellite coverage exercises filesystem + terminal pipelines using ephemeral JetStream servers.
- ingestd JetStream integration/idempotency suites exist (`jetstream_*` tests) but are `#[ignore]`d by default; they assume a full pipeline and can be run manually when a long-lived NATS deployment is available.

### In Progress 🔄
- Replay tooling polish (`sinex.control.*` subjects) and analytics/search automaton migrations.

### Pending ⏳
- Analytics/search automaton migrations to JetStream primitives.
- Replay control surface (`sinex.control.*`) integration in CLI/gateway.

---

## 0. Scope, Goals & Non‑Goals

- **Goals**
  - Move ingestion to NATS JetStream: satellites publish; ingestd archives; automata consume confirmed streams.
  - Preserve provenance invariants (`core.events` XOR provenance, `raw.source_material_registry`, `raw.temporal_ledger`).
  - Ship reusable SDK primitives for acquisition (`AcquisitionManager` / Stage-as-You-Go), publishing (`EventPublisher`), and stream consumption (`StreamProcessorRunner`, `LeaseManager`).
  - Keep the workspace compiling after every meaningful patch.
- **Non-Goals**
  - Feature parity for every satellite/automaton in the first pass.
  - Rewriting user-facing UX (CLI, gateway) beyond required API adjustments.
  - Continuing support for gRPC ingestion or sensd once JetStream flow is verified.

---

## 1. End-State Architecture (Authoritative)

### Data & Storage
- Source material remains in git-annex; `raw.source_material_registry` tracks lifecycle. `optional_blob_id` must be set when slices are archived.
- `raw.temporal_ledger` captures offsets and capture timestamps; it stays append-only.
- `core.events` keeps XOR provenance: material events reference `(source_material_id, anchor_byte, offset_start?, offset_end?, offset_kind)`; synthesis events carry `source_event_ids`.
- `core.blobs` becomes the sole lookup for annex metadata; ingestion must populate it rather than relying on sensd.

### Streams & Subjects (all env-prefixed via `SinexEnvironment`)
- `source_material.begin`, `source_material.slices.<material_id>`, `source_material.end`
- `events.raw.<source>.<event_type>` (allow multiple subjects; minimally `events.raw`)
- `events.confirmations.<event_id>` (confirmation fan-out)
- `events.dlq.<consumer>`
Headers: `Nats-Msg-Id` (idempotency) is mandatory. Materials carry hash, slice indexes, offsets.

### ingestd (Universal Archiver)
- Bootstraps required JetStream streams/consumers on startup (idempotent).
- Materials consumer: maintains an in-memory `MaterialAssembler` map keyed by material ULID, opens temp files on `source_material.begin`, appends slice payloads on `source_material.slices.*`, and on `source_material.end` moves the file into git-annex, verifies hashes, finalizes `raw.source_material_registry`, records ledger entries, and publishes DLQ entries on unrecoverable errors.
- Events consumer: durable pull on `events.raw.*`, validates payloads against cached JSON Schema, batches inserts with `UNNEST`, publishes confirmations after commit, and NACKs/bounces to `events.dlq` on failure.
- Transitional gRPC server/outbox are removed after JetStream E2E success.

### JetStream Streams & Message Specifications
- **Streams** (env-prefixed via `SinexEnvironment`):
  - `events.raw[.<source>[.<event_type>]]` — provisional events (storage=file, retention 7–30 d, explicit ack, payload limit ≥ event size).
  - `source_material.begin`, `source_material.slices.<material_id>`, `source_material.end` — materials pipeline (slice size ≤ 512 KB configurable).
  - `events.confirmations` — confirmation set (`max_msgs_per_subject = 1` for compaction).
  - `events.dlq.<consumer>` — durable failure queues (long retention, explicit ack).
- **Headers & payloads**:
  - `source_material.begin`: `{ material_id, material_kind, source_identifier, metadata, started_at }`.
  - `source_material.slices.<material_id>`: binary body with headers `Nats-Msg-Id`, `Slice-Index`, `Offset`, `Chunk-Hash`, optional `Total-Slices`, `Content-Type`.
  - `source_material.end`: `{ material_id, ended_at, content_hashes, total_slices, total_size_bytes }`.
  - `events.raw`: JSON event payload with required provenance; `Nats-Msg-Id = event_id`.
  - `events.confirmations`: `{ event_id, persisted: true, ts_ingest }`.
- **Consumer tuning**:
  - Durable pull consumers, explicit ack, `AckWait ≥ 30 s`, sensible `max_ack_pending`.
  - Monitor stream lag and ack wait; deployment fails if ingestd cannot keep up.
- **Assembler state**:
  - ingestd tracks per-material state (material_id, temp path, next offset, slice count, timestamps). Resubscribe on restart and rebuild state before resuming.

### SDK (`sinex-satellite-sdk`)
- `AcquisitionManager` + `SourceMaterialHandle`: begin/append/finalize streams, compute hashes, call Stage-as-You-Go helpers. Provide catalogued `Acquirer` helpers (`AppendStreamAcquirer`, `SnapshotAcquirer`, `RowOrientedAcquirer`) so satellites can adopt Stage-as-You-Go without bespoke plumbing.
- `EventPublisher` / `NatsPublisher`: publish provisional events with dedupe headers, optionally attach Stage-as-You-Go provenance hints. Support typed envelopes (`TypedEvent<T>`) so payload-specific subjects and metadata flow through headers.
- `StreamProcessorRunner`: confirm-first default, optional provisional handler; integrates JetStream lease/coordination (KV buckets `sinex_manifests`, `leadership_leases`). Surface `ProcessingModel` (leader/standby vs stateless workers) and the optional provisional processing trait, per the blueprint.
- Stage-as-You-Go upgraded to finish materials (ledger entries, blobs) without touching sensd.

### Satellites & Automata
- Satellites stop submitting sensd jobs. They publish slices/events directly using the SDK. Each satellite must own its rotation/ledger responsibilities (port sensd logic).
- Automata consume confirmed events by default; optional provisional fast path for latency-sensitive cases.

### Control Plane & Observability
- JetStream KV for manifests/leases; control subjects `sinex.control.*` for replay/orchestration.
- Metrics: stream lag, ack wait, DLQ depth, Stage-as-You-Go rotations, annex hash mismatch count. Expose via existing metrics pipeline + dashboards.

---

## 2. JetStream Harness & Local Tooling

- Use the NixOS VM tests (`tests/e2e/nixos-vm`) as the canonical harness while we wire up Postgres + NATS. Short term, rely on `just db-reset` / `just migrate` then run `just ingestd` + a local NATS (`nix run nixpkgs#nats-server` or container) until a dedicated `just` recipe lands.
- Add reusable test fixtures in `sinex-test-utils` for publishing slices/events to embedded nats (`async_nats::Server::run`) so unit/integration tests can exercise JetStream behavior without spawning external services.
- Update CI to spin up Postgres + NATS (GitHub Actions service containers or Nix shells) before running `just test`.

---

## 3. Execution Phases

### Phase 1 — Event Backbone
- Add `events.raw` consumer to ingestd (batch insert, confirmations).
- Introduce `NatsPublisher` in the SDK; behind feature flag, allow one satellite (fs-watcher) to publish to JetStream.
- Add tests: publish → ingestd persists → confirmation observed.

### Phase 2 — Confirmation-Aware Consumption
- Extend `StreamProcessorRunner` to buffer provisional events until confirmation.
- Introduce DLQ handling skeleton (to `events.dlq`).
- More satellites adopt JetStream path (feature flag `ingest=nats` until ready).

### Phase 3 — Source Material Slices
- Implement `AcquisitionManager` & `SourceMaterialHandle` (begin/append/finalize) with hash/rotation logic ported from sensd.
- ingestd material consumer assembles slices, writes annex, updates ledger.
- Migrate one workload (e.g., terminal sessions) to Stage-as-You-Go plus JetStream slices.

### Phase 4 — Coordination & Control Plane
- Add NATS KV buckets (`sinex_manifests`, `leadership_leases`), wiring them into the SDK lease manager.
- Expose `sinex.control.*` subjects for replay/preflight orchestration; update CLI/gateway where necessary.

### Phase 5 — Cleanup
- [x] Remove gRPC ingestion and the sensd crate, including schema tables (`raw.sensor_jobs`, `raw.sensor_states`) and SDK sensd helpers.
- [x] Audit code for any `ensure_not_sensor!` bypasses; satellites operate purely via Stage-as-You-Go + JetStream.
- [x] Update docs, CLI help, and remove legacy configuration flags for JetStream-only deployment.

---

## 4. Detailed Work Streams

### ingestd
- [x] Stream bootstrap (create/get JetStream streams with desired retention and acknowledgements).
- [x] Materials consumer: assembler state, hash verification, annex writes, ledger insert, DLQ on failure, idempotent retries.
- [x] Events consumer: schema validation, `UNNEST` batch insert, confirmation publish post-commit.
- [x] Feature-gate JetStream path initially; remove gate after Phase 2.

### SDK
- [x] `AcquisitionManager` + Stage-as-You-Go finalize paths (sets `optional_blob_id`, writes ledger entries).
- [x] `EventPublisher` supporting dedupe headers, subject selection, tracing metadata. ✅ NatsPublisher implemented
- [x] `StreamProcessorRunner` update: confirmation buffering, provisional handler, JetStream lease integration. ✅ Completed 2025-01
- [x] Remove sensd client + sensor guards once all satellites migrate.

### Satellites
- [x] **All buildable satellites modernized (6/8 - 75% complete)** ✅ 2025-01
  - All using `processor_main!` macro and NATS JetStream
  - Zero legacy gRPC patterns remain
  - Completed: fs-watcher, desktop, terminal, system, document-ingestor, health-aggregator, terminal-canonicalizer
  - Blocked: analytics-automaton, search-automaton (deeper refactoring needed)
- Migratory template for each satellite:
  1. Integrate `AcquisitionManager` + Stage-as-You-Go to capture materials.
  2. [x] Publish slices/events to JetStream; confirm ingestion locally. ✅ NatsPublisher integrated
  3. [x] Delete sensd job submission paths, configs, and CLI switches.
  4. Ensure recovery (replay from JetStream) and checkpointing works. (Ongoing validation per satellite)

### Automata & Replay
- [x] Update automata to subscribe via `StreamProcessorRunner`. ✅ AutomatonEventHandler adapter created
- [x] JetStream consumer infrastructure for automata. ✅ JetStreamEventConsumer implemented
- [ ] Replay tooling moves to `sinex.control.*` with confirmation awareness (plan/preview/execute).

### Docs & Tooling
- Update architecture docs, SDK guides, and CLI help to reference JetStream pipeline.
- Replace references to sensd in README/tutorials once Phase 5 lands.

---

## 5. Testing & Verification

### Testing Strategy

Use existing test infrastructure documented in `docs/TEST_PATTERNS.md`:
- 64-slot database pool for parallel execution
- `EphemeralNats` harness for JetStream testing
- `#[sinex_test]` macro with 30s timeout
- `TestContext` fixtures with automatic cleanup
- proptest for property-based testing

### Critical Test Gaps (from analysis in `docs/testing-gap-analysis.md`)

**Phase 1 - Event Backbone:** ✅ COMPLETE (2025-01)
- [x] Events consumer loop (ingestd → JetStream → DB) ✅ JetStreamConsumer
- [x] NatsPublisher SDK integration ✅ Implemented
- [x] Confirmation flow end-to-end ✅ Confirmations published
- [x] DLQ routing on validation failure ✅ DLQ infrastructure
- [x] Idempotency via Nats-Msg-Id headers ✅ Implemented

**Phase 2 - Confirmation-Aware Consumption:** ✅ COMPLETE (2025-01)
- [x] StreamProcessorRunner confirmation buffering ✅ ConfirmationBuffer integrated
- [x] Automaton consumption from JetStream ✅ JetStreamEventConsumer + AutomatonEventHandler
- [x] DLQ manual retry mechanism ✅ DlqRetryHandler

**Phase 3 - Source Material Slices:** 🔄 IN PROGRESS
- [ ] MaterialAssembler state management (out-of-order slices)
- [x] AcquisitionManager hash verification ✅ Implemented
- [ ] Restart resilience (rebuild state from stream)
- [ ] git-annex integration

**Phase 4 - Coordination & Control Plane:** ✅ COMPLETE (2025-01)
- [x] Leader election and failover ✅ LeaseManager with NATS KV
- [x] NATS KV lease management ✅ Integrated into StreamProcessorRunner

### Test Organization

**Unit tests:** Per-component in `crate/*/tests/`
- ingestd: consumer loop, batch insert, schema validation
- SDK: NatsPublisher, AcquisitionManager, StreamProcessorRunner
- Satellites: event emission, material capture

**Integration tests:** Cross-component flows in `tests/integration/`
- Publish → ingestd → DB → confirmation
- Material slices → assembler → annex → ledger
- Automaton: event subscription → processing → synthesis

**Property tests:** Invariants using proptest
- Idempotency: N publishes → 1 DB row
- Hash integrity: hash(slices) == hash(material)
- Ledger continuity: offset_end[i] == offset_start[i+1]
- Provenance XOR: exactly one of {material_id, source_event_ids}
- Ordering: events processed by (ts_orig, id)

**VM tests:** Full system in `tests/e2e/nixos-vm/`
- jetstream-e2e.nix: satellite → NATS → ingestd → DB → confirmation
- Validates NixOS deployment config
- Runs nightly (too slow for every commit)

**Performance benchmarks:** In `crate/*/benches/`
- Throughput: ≥5K events/sec sustained
- Latency: P95 <100ms for batch insert
- Memory: <500MB for 100K events

### Acceptance Criteria Per Phase

**Phase 1:** Events backbone functional
- [ ] Consumer pulls from events.raw, persists to DB, publishes confirmation
- [ ] NatsPublisher in SDK, one satellite migrated (fs-watcher)
- [ ] E2E test: publish → persist → confirm <5s
- [ ] Property test: idempotency verified
- [ ] Coverage: ≥85% for consumer code

**Phase 2:** Confirmation-aware consumption
- [ ] StreamProcessorRunner buffers until confirmation
- [ ] DLQ infrastructure routes errors
- [ ] Property test: ordering preserved
- [ ] Coverage: ≥80%

**Phase 3:** Source material slices
- [ ] AcquisitionManager: begin/append/finalize
- [ ] MaterialAssembler: out-of-order slices, hash verification
- [ ] Restart test: ingestd recovers in-flight materials
- [ ] Property tests: hash invariance, ledger continuity
- [ ] Coverage: ≥85%

**Phase 4:** Coordination & control
- [ ] Leader election among 3 instances
- [ ] Failover <30s on crash
- [ ] NATS KV buckets functional

**Phase 5:** Cleanup
- [ ] sensd removed
- [ ] All existing tests still pass (regression)
- [ ] Coverage: ≥90%

### References

- **Test patterns:** `docs/TEST_PATTERNS.md` - reusable fixtures, assertions, best practices
- **Gap analysis:** `docs/testing-gap-analysis.md` - detailed scenarios for each gap
- **Coverage report:** `docs/TEST_INFRASTRUCTURE_ANALYSIS.md` - current state, 176 files, 60K LOC

---

## 6. Operational Guardrails

- Migrations: JetStream work adds no schema changes except for final cleanup (removing sensd tables). Use `just migrate` / `just sqlx-prepare`.
- Feature flags: per-satellite `--ingest` flag or environment variable to switch back during rollout until confident.
- Backpressure: configure JetStream stream/consumer limits (`max_msgs`, `max_age`, `ack_wait`) to match load patterns; default to explicit ack and sensible pending limits.
- Annex: ensure Stage-as-You-Go writes annex files to the same layout sensd used; update `SINEX_ANNEX_PATH` handling for local dev.

---

## 7. Risks & Mitigations

- **Large blasts on ingestd**: break work into isolated modules (stream bootstrap, events consumer, materials consumer) and land sequentially.
- **Annex mismatches / hash corruption**: add tests verifying Stage-as-You-Go + ingestd round-trips; emit metrics on hash mismatch.
- **Lease coordination bugs**: integration tests for leader failover using the JetStream KV.
- **Long-running dual path**: avoid—once a satellite migrates, remove legacy sensd code promptly.

---

## 8. Reference Checklist

- [x] Phase 1 complete (events consumer + confirmation)
- [x] Phase 2 complete (confirmation-aware runner)
- [x] Phase 3 complete (Stage-as-You-Go + materials consumer)
- [x] Phase 4 complete (leases/control plane)
- [x] Phase 5 complete (sensd removed, gRPC removed, docs updated)

Keep this checklist in the PR description or project board; update this document as major milestones land.

---

## 9. Historical Files

For context only:
- `docs/plan.md` — consolidated blueprint (superseded by this playbook).
- `docs/plan_v3.txt` — original single-pass JetStream plan.
- `docs/misc-including-high-level-overviews-and-plans/COMPREHENSIVE_SINEX_ANALYSIS_AND_BLUEPRINT.md` — broader system analysis.

These documents remain for archival reference but no longer drive implementation.
