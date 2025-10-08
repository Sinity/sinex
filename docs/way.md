# Sinex JetStream Refactor Playbook

_Status: authoritative. This document supersedes `docs/plan.md` and `docs/plan_v3.txt` for the ingestion refactor. Keep those files only for historical context._

We are replacing the legacy “satellites → sensd (gRPC) → ingestd → Postgres/Redis” pipeline with a JetStream‑first architecture. The end state is:

- Satellites publish source material slices and provisional events directly to NATS JetStream.
- `sinex-ingestd` is a durable JetStream consumer that archives into Postgres/git-annex and emits confirmations.
- Automata consume confirmed events from JetStream.
- sensd and its gRPC surface disappear entirely; Stage-as-You-Go lives in the SDK.

There is no long-lived dual path. We land work in small, compiling increments, but the target configuration is JetStream-only.

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
- Materials consumer: assembles slices, verifies hashes, writes to annex/blob store, finalizes `raw.source_material_registry`, records ledger entries, publishes DLQ on unrecoverable errors.
- Events consumer: validates payloads against schemas, batches inserts with `UNNEST`, emits `events.confirmations` only after commit.
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
- `AcquisitionManager` + `SourceMaterialHandle`: begin/append/finalize streams, compute hashes, call Stage-as-You-Go helpers.
- `EventPublisher` / `NatsPublisher`: publish provisional events with dedupe headers, optionally attach Stage-as-You-Go provenance hints.
- `StreamProcessorRunner`: confirm-first default, optional provisional handler; integrates JetStream lease/coordination (KV buckets `sinex_manifests`, `leadership_leases`).
- Stage-as-You-Go upgraded to finish materials (ledger entries, blobs) without touching sensd.

### Satellites & Automata
- Satellites stop submitting sensd jobs. They publish slices/events directly using the SDK. Each satellite must own its rotation/ledger responsibilities (port sensd logic).
- Automata consume confirmed events by default; optional provisional fast path for latency-sensitive cases.

### Control Plane & Observability
- JetStream KV for manifests/leases; control subjects `sinex.control.*` for replay/orchestration.
- Metrics: stream lag, ack wait, DLQ depth, Stage-as-You-Go rotations, annex hash mismatch count. Expose via existing metrics pipeline + dashboards.

---

## 2. JetStream Harness & Local Tooling

- Use the NixOS VM tests (`tests/nixos-vm`) as the canonical harness while we wire up Postgres + NATS. Short term, rely on `just db-reset` / `just migrate` then run `just ingestd` + a local NATS (`nix run nixpkgs#nats-server` or container) until a dedicated `just` recipe lands.
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
- Remove gRPC ingestion and the sensd crate, including schema tables (`raw.sensor_jobs`, `raw.sensor_states`) and SDK sensd helpers.
- Audit code for any `ensure_not_sensor!` bypasses; satellites should operate purely via Stage-as-You-Go + JetStream.
- Update docs, CLI help, and remove legacy configuration flags.

---

## 4. Detailed Work Streams

### ingestd
- [ ] Stream bootstrap (create/get JetStream streams with desired retention and acknowledgements).
- [ ] Materials consumer: assembler state, hash verification, annex writes, ledger insert, DLQ on failure, idempotent retries.
- [ ] Events consumer: schema validation, `UNNEST` batch insert, confirmation publish post-commit.
- [ ] Feature-gate JetStream path initially; remove gate after Phase 2.

### SDK
- [ ] `AcquisitionManager` + Stage-as-You-Go finalize paths (sets `optional_blob_id`, writes ledger entries).
- [ ] `EventPublisher` supporting dedupe headers, subject selection, tracing metadata.
- [ ] `StreamProcessorRunner` update: confirmation buffering, provisional handler, JetStream lease integration.
- [ ] Remove sensd client + sensor guards once all satellites migrate.

### Satellites
- Migratory template for each satellite:
  1. Integrate `AcquisitionManager` + Stage-as-You-Go to capture materials.
  2. Publish slices/events to JetStream; confirm ingestion locally.
  3. Delete sensd job submission paths, configs, and CLI switches.
  4. Ensure recovery (replay from JetStream) and checkpointing works.

### Automata & Replay
- Update automata to subscribe via `StreamProcessorRunner`.
- Replay tooling moves to `sinex.control.*` with confirmation awareness (plan/preview/execute).

### Docs & Tooling
- Update architecture docs, SDK guides, and CLI help to reference JetStream pipeline.
- Replace references to sensd in README/tutorials once Phase 5 lands.

---

## 5. Testing & Verification

- Unit tests for new SDK primitives (`sinex-satellite-sdk/tests/...`).
- ingestd integration tests: fixture publishes slices/events → ingestd archives → confirmation seen.
- VM tests updated to start NATS and verify event flow via JetStream.
- Property tests for idempotent replays, slice replays after ingestd restarts, DLQ handling, lease failover.
- Observability checks: stream lag metrics, ack wait, Stage-as-You-Go rotation stats.
- Acceptance criteria before deleting sensd/gRPC:
  - ingestd archives from JetStream and survives restart (materials + events).
  - Satellites publish exclusively via JetStream, with replay/regression coverage.
  - Automata consume confirmed streams via `StreamProcessorRunner`.
  - DLQ coverage: injected unrecoverable error arrives in `events.dlq`.

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

- [ ] Phase 1 complete (events consumer + confirmation)
- [ ] Phase 2 complete (confirmation-aware runner)
- [ ] Phase 3 complete (Stage-as-You-Go + materials consumer)
- [ ] Phase 4 complete (leases/control plane)
- [ ] Phase 5 complete (sensd removed, gRPC removed, docs updated)

Keep this checklist in the PR description or project board; update this document as major milestones land.

---

## 9. Historical Files

For context only:
- `docs/plan.md` — consolidated blueprint (superseded by this playbook).
- `docs/plan_v3.txt` — original single-pass JetStream plan.
- `docs/misc-including-high-level-overviews-and-plans/COMPREHENSIVE_SINEX_ANALYSIS_AND_BLUEPRINT.md` — broader system analysis.

These documents remain for archival reference but no longer drive implementation.
