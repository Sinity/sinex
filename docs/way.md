# Sinex JetStream Refactor Playbook

This document captures the one-shot plan for replacing the legacy gRPC+sensd ingestion stack with the JetStream-first architecture. There are no transitional fallbacks or compatibility layers: when the work lands, the new pipeline is the only pipeline.

## 1. JetStream Harness & Interfaces
- Add repo tooling to spin up Postgres and NATS locally (`just dev-env`, reusable test fixtures that publish/consume slices + events).
- Specify Rust interfaces for the new SDK components (AcquisitionManager, EventPublisher, ConfirmationSubscriber, LeaseManager) and the metrics they must expose.
- Define subject conventions (`events.raw.<source>.<type>`, `events.confirmations.<event_id>`, `events.dlq.<consumer>` and `source_material.*`) plus required headers (`Nats-Msg-Id`, offsets, hashes).

## 2. Rebuild ingestd on JetStream
- Implement a durable JetStream consumer for `events.raw` that validates payloads, batch-inserts into Postgres, emits confirmation messages, and pushes hard failures to `events.dlq`.
- Implement a second consumer for `source_material.begin/slices/end`, reusing the salvaged rotation/ledger logic to assemble blobs, write ledger entries, and finalize `raw.source_material_registry`.
- Remove the tonic gRPC server, transactional outbox, and associated configuration once integration tests pass on the JetStream path.

## 3. SDK v2
- Port sensd’s rotation manager, temporal ledger helpers, and blob loading into SDK modules for direct satellite use.
- Implement the new primitives:
  - `AcquisitionManager`: begin/append/finalize material streams with rotation, hashing, and ULID-based provenance.
  - `EventPublisher`: serialize events + metadata into `events.raw` subjects with idempotent headers.
  - `StreamProcessorRunner`: subscribe to JetStream (confirmed by default, optional provisional) and feed processors.
  - `LeaseManager`: acquire/refresh JetStream KV leases for leader/standby coordination.
- Rewrite Stage-as-You-Go on top of these primitives so it never touches ingestd or Postgres directly.

## 4. Satellite Conversion (sensd Removal)
- For each satellite (filesystem → terminal → document → others):
  - Replace sensd job submission with direct calls to `AcquisitionManager` and `EventPublisher`.
  - Ensure the satellite writes ledger entries, publishes slices/events, handles confirmations, and recovers from failures.
  - Delete sensd integration code, configs, and dependencies once the satellite compiles and passes tests on the new stack.
- Remove sensd-specific utilities from the SDK (`sensd_client`, job helpers, etc.).

## 5. Automata & Replay
- Update automata to consume confirmed JetStream events via the new runner and publish synthesis events back through `EventPublisher`.
- Reimplement replay tooling on top of NATS control subjects (plan/preview/execute) with confirmation awareness and DLQ handling.

## 6. Observability & Tests
- Expand unit/integration/property tests to cover acquisition, ingestion, confirmations, DLQ behavior, and lease failover using the new harness.
- Emit metrics and structured logs for stream lag, ack wait, acquisition rotations, and lease status; provide dashboards/notebooks for these signals.

## 7. Final Cleanup & Docs
- Delete the `sinex-sensd` crate, gRPC protos/clients, sensd schema modules, and `raw.sensor_jobs` / `raw.sensor_states` tables.
- Normalize configuration (environment-prefixed JetStream/KV settings) and update documentation: architecture overview, SDK guides, satellite manuals, testing instructions, and the roadmap (DuckDB QueryRouter remains future work).

## Operational Note
Commit after completing each meaningful chunk (e.g., ingestd consumer, SDK module, satellite migration) and run the relevant tests before pushing, so the refactor stays reviewable end-to-end.
