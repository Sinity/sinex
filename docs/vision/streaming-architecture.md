# Streaming Architecture & Backpressure

> **Operational note (2025-10-23)**  
> JetStream ingestion is canonical. Any retired pipeline references here are historical context.


This document consolidates our streaming/backpressure guidance and replaces ad‑hoc channel sizing approaches with a principled, durable pipeline built on NATS JetStream with confirmations. (The transactional outbox that once bridged Postgres→NATS was retired during the JetStream refactor; see the historical note below.)

> **Accuracy Notice (2025-07-24, refreshed 2025-02-24)**  
> Legacy references to `docs/TARGET_final.md` were replaced with pointers to the JetStream-first architecture docs under `docs/current/architecture/`. If you still find links to removed files, treat them as historical context only and update them to match the JetStream plan.

## Goals
- Natural backpressure without arbitrary channel sizes.
- Preserve Postgres as the single source of truth for events.
- Keep publish semantics reliable via JetStream confirmations and durable consumers.
- Maintain event ordering and durability (ULIDs + persisted streams).

## Data Flow (Current)
```
nodes → NATS (staging) → sinex-ingestd → Postgres (core.events)
                                         ↓
                               JetStream confirmations/DLQ → Automata & Replay
```

Notes:
- The staging stream is optional but recommended for bursty producers.
- Postgres (core.events) remains authoritative; JetStream transports provisional events, confirmations, and DLQ messages.

## Key Components

### NATS JetStream (Staging)
Use a short‑lived “staging” stream to buffer high‑throughput inputs (e.g., large shell histories) and provide natural backpressure.

Example (illustrative):
```yaml
streams:
  staging:
    subjects: ["staging.>"]
    max_age: 3600s      # short retention
    max_msgs: 10000000  # absorb bursts
    storage: file
    discard: old        # drop oldest if truly saturated
```

Implementation pointers:
- node producers use the Stage-as-You-Go staging path (`crate/lib/sinex-node-sdk/src/stage_as_you_go.rs::process_with_staging`) to publish immediately to JetStream instead of buffering in local channels.
- Ingestd consumes staging subjects via the JetStream consumer (`crate/core/sinex-ingestd/src/jetstream_consumer.rs`) and persists slices before emitting confirmations/DLQ.
- Channel drain helpers live behind the `channel-testing` feature in `sinex-test-utils`; production code should prefer streaming publishes over draining in-memory queues.
- Default Nix bootstrap does **not** create a staging stream; add one via `services.sinex.nats.bootstrapStreams.streams` when you need explicit burst buffering.

Purpose:
- Replace brittle in‑process channels (e.g., fixed capacity mpsc).
- Absorb bursts while preserving order.
- Let consumer pace apply backpressure naturally.

### JetStream Confirmation Flow
In the JetStream‑first architecture, ingestd persists events inside a single database transaction and, once committed, publishes confirmations back to JetStream (e.g., `events.confirmations.<event_id>`) plus any DLQ entries. Automata and replay tooling consume those confirmation streams via durable consumers, which gives the same ordering and reliability guarantees the old transactional outbox provided—without a second delivery mechanism. For default stream/subject bootstrapping (and environment namespacing), see `nixos/modules/nats.nix`.

**Historical note:** older revisions described a Postgres transactional outbox that relayed events to NATS. That component was removed when Phase 5 of the JetStream refactor completed; the section above captures the current behaviour.

### NATS JetStream (Events)
Authoritative notifications for persisted events (consumers replay as needed).

Example (illustrative, aligned with the default Nix bootstrap):
```yaml
streams:
  events_raw:
    subjects: ["events.raw.>"]
    max_age: 604800s   # 7 days
    storage: file
    discard: none
  events_confirmations:
    subjects: ["events.confirmations.>"]
    max_age: 2592000s  # 30 days
    max_msgs_per_subject: 1
    storage: file
    discard: none
  source_material_begin:
    subjects: ["source_material.begin"]
    max_age: 604800s   # 7 days
  source_material_slices:
    subjects: ["source_material.slices.>"]
    max_age: 604800s
  source_material_end:
    subjects: ["source_material.end"]
    max_age: 604800s
```

## node Guidelines
- Do not collect unbounded data into memory (avoid building huge Vecs before send).
- Stream transformations; chunk large sources; prefer backpressure‑aware pipelines.
- Use bounded concurrency (e.g., buffer_unordered(n)) to limit parallel work.
- Treat NATS publish as I/O that can apply pressure; handle retries with jitter.

## Ingestd Guidelines
- Validate and persist first; only then publish via outbox.
- Preserve ULID ordering where meaningful; don’t reorder batches arbitrarily.
- Instrument with tracing spans and structured fields (source, event_type, counts).

## Operational Notes
- Backpressure is expected: when consumers lag, staging grows and producers slow.
- Tune staging retention/capacity to your environment; keep Postgres the source of truth.
- For incident response, prefer replay from DB queries and/or events stream.

## See Also
- nixos/modules/nats.nix (default JetStream streams/subjects + env namespacing)
- docs/current/architecture/Core_Architecture.md (system overview)
- docs/vision/project-target-state.md (historical snapshot; see banner)
- docs/current/architecture/security-architecture.md (reliability/attack surface)
