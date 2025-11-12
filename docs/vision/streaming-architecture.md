# Streaming Architecture & Backpressure

> **Operational note (2025-10-23)**  
> JetStream ingestion is canonical (`docs/way.md`). Any sensd/gRPC references here are historical context.


This document consolidates our streaming/backpressure guidance and replaces ad‑hoc channel sizing approaches with a principled, durable pipeline built on NATS JetStream with confirmations. (The transactional outbox that once bridged Postgres→NATS was retired in Phase 5 of `docs/way.md`; see the historical note below.)

> **Accuracy Notice (2025-07-24)**  
> References to `docs/TARGET_final.md` and other legacy plans point to files that were removed during documentation consolidation. Treat those links as historical context only. The authoritative ingestion/JetStream plan now lives in `docs/way.md`. Update any downstream documents or tooling that still expect the old path.

## Goals
- Natural backpressure without arbitrary channel sizes.
- Preserve Postgres as the single source of truth for events.
- Keep publish semantics reliable via JetStream confirmations and durable consumers.
- Maintain event ordering and durability (ULIDs + persisted streams).

## Data Flow (Current)
```
Satellites → NATS (staging) → sinex-ingestd → Postgres (core.events)
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

Purpose:
- Replace brittle in‑process channels (e.g., fixed capacity mpsc).
- Absorb bursts while preserving order.
- Let consumer pace apply backpressure naturally.

### JetStream Confirmation Flow
In the JetStream‑first architecture, ingestd persists events inside a single database transaction and, once committed, publishes confirmations back to JetStream (e.g., `events.confirmations.<event_id>`) plus any DLQ entries. Automata and replay tooling consume those confirmation streams via durable consumers, which gives the same ordering and reliability guarantees the old transactional outbox provided—without a second delivery mechanism. See `docs/way.md` for the authoritative subject list.

**Historical note:** older revisions described a Postgres transactional outbox that relayed events to NATS. That component was removed when Phase 5 of the JetStream refactor completed; the section above captures the current behaviour.

### NATS JetStream (Events)
Authoritative notifications for persisted events (consumers replay as needed).

Example (illustrative):
```yaml
streams:
  events:
    subjects: ["events.>"]
    max_age: 604800s   # 7 days
    storage: file
    discard: none
```

## Satellite Guidelines
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
- docs/TARGET_final.md (authoritative blueprint)
- docs/architecture/security-architecture.md (reliability/attack surface)
