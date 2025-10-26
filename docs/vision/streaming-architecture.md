# Streaming Architecture & Backpressure

> **Operational note (2025-10-23)**  
> JetStream ingestion is canonical (`docs/way.md`). Any sensd/gRPC references here are historical context.


This document consolidates our streaming/backpressure guidance and replaces ad‑hoc channel sizing approaches with a principled, durable pipeline built on NATS JetStream and the transactional outbox pattern.

> **Accuracy Notice (2025-07-24)**  
> References to `docs/TARGET_final.md` and other legacy plans point to files that were removed during documentation consolidation. Treat those links as historical context only. The authoritative ingestion/JetStream plan now lives in `docs/way.md`. Update any downstream documents or tooling that still expect the old path.

## Goals
- Natural backpressure without arbitrary channel sizes.
- Preserve Postgres as the single source of truth for events.
- Keep publish semantics reliable via a transactional outbox.
- Maintain event ordering and durability (ULIDs + persisted streams).

## Data Flow (Current)
```
Satellites → NATS (staging) → sinex-ingestd → Postgres → Outbox → NATS (events) → Consumers
                     ↓                         ↓                        ↓
                Buffer/transport         Source of truth           Notifications
```

Notes:
- The staging stream is optional but recommended for bursty producers.
- Postgres (core.events) remains authoritative; NATS is for transport/notifications.

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

### Transactional Outbox (Postgres)
Persist events and enqueue an outbox record in the same transaction; a background publisher drains the outbox to the “events” stream.

Pseudo‑flow:
```
BEGIN;
  INSERT INTO core.events (...);
  INSERT INTO core.outbox (...);
COMMIT;
# Publisher processes outbox → NATS events
```

Benefits:
- Atomic DB write + publish.
- Crash‑resilient delivery (replay outbox on restart).

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
