# Streaming Architecture & Backpressure

This document consolidates current streaming/backpressure guidance around a durable JetStream-first ingestion pipeline.

## Goals

- natural backpressure without arbitrary channel sizing
- preserve Postgres as the single source of truth for persisted events
- keep publish semantics reliable via JetStream confirmations and durable consumers
- maintain deterministic processing order through explicit event metadata and query ordering

## Data Flow (Current)

```
nodes -> NATS (staging) -> sinex-ingestd -> Postgres (core.events)
                                      ↓
                            JetStream confirmations/DLQ -> Automata & Replay
```

Notes:

- the staging stream is optional but recommended for bursty producers
- Postgres (`core.events`) remains authoritative; JetStream transports staging, confirmations, and DLQ messages

## Key Components

### NATS JetStream (Staging)

Use a short-lived staging stream to buffer high-throughput inputs and provide backpressure.

Example (illustrative):

```yaml
streams:
  staging:
    subjects: ["staging.>"]
    max_age: 3600s
    max_msgs: 10000000
    storage: file
    discard: old
```

Implementation pointers:

- node producers use stage-as-you-go publish paths (`sinex-node-sdk`)
- ingestd consumes staging subjects via JetStream consumers and persists slices before emitting confirmations/DLQ
- channel-drain helpers remain test-only; production paths stream directly
- default Nix bootstrap does not create staging by default; add it via `services.sinex.nats.bootstrapStreams.streams` when required

### JetStream Confirmation Flow

Ingestd persists events in a DB transaction and, after commit, publishes confirmation messages (`events.confirmations.<event_id>`) plus DLQ entries. Automata and replay tooling consume these via durable consumers.

### NATS JetStream (Events)

Authoritative notifications for persisted events.

Example (illustrative, aligned with default Nix bootstrap):

```yaml
streams:
  events_raw:
    subjects: ["events.raw.>"]
    max_age: 604800s
    storage: file
    discard: none
  events_confirmations:
    subjects: ["events.confirmations.>"]
    max_age: 2592000s
    max_msgs_per_subject: 1
    storage: file
    discard: none
  source_material_begin:
    subjects: ["source_material.begin"]
    max_age: 604800s
  source_material_slices:
    subjects: ["source_material.slices.>"]
    max_age: 604800s
  source_material_end:
    subjects: ["source_material.end"]
    max_age: 604800s
```

## Node Guidelines

- do not build unbounded in-memory batches
- stream transformations and chunk large inputs
- use bounded concurrency
- treat publish as pressure-aware I/O and retry with jitter/backoff where needed

## Ingestd Guidelines

- validate and persist first, then publish confirmations/DLQ
- keep ordering explicit and avoid arbitrary batch reordering
- instrument with tracing spans and structured fields (`source`, `event_type`, counts)

## Operational Notes

- backpressure is expected; staging growth and producer slowdown are normal under load
- tune staging retention/capacity per environment
- replay/incident workflows should prefer DB truth and confirmation streams

## See Also

- `nixos/modules/nats.nix` (default JetStream streams/subjects + env namespacing)
- `docs/current/architecture/Core_Architecture.md` (system overview)
- `docs/current/security.md` (reliability and attack surface)
