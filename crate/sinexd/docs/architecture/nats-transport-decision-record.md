# NATS Role and Transport Decision Record

> Status: **decision record (documentation slice of #1731).** This page defines
> when Sinex uses a direct in-process call, Core NATS, or JetStream, and
> enumerates every current NATS subject, JetStream stream, and NATS-KV bucket
> with its traffic class. The Direct transport path now exists as
> `EventTransport::Direct`; the code-backed route matrix lives in
> `sinex_primitives::transport::CURRENT_ROUTE_DECISIONS` and is the current
> executable catalog for #1732.

Companion documents:

- `crate/sinex-primitives/docs/transport.md` — the publish-**class** catalog
  (`Class::Critical` … `Class::Telemetry`), the route matrix, the DLQ /
  processing-failure / local-recovery-spool boundary, and the per-class drain
  protocol. This decision record is the **per-subject / per-stream** view and
  references that catalog rather than repeating it.
- `crate/sinex-primitives/src/transport.rs` — the class enum as code.
- `crate/sinex-primitives/src/nats.rs` — `JetStreamTopology`, the authoritative
  subject/stream wiring.

---

## 1. Traffic-class framework

Admission is the conceptual boundary, not a transport. **Admission** is
`AdmissionService` (`crate/sinexd/src/event_engine/admission.rs:270`): it owns
schema validation, the privacy chokepoint (#1042), and the DB write. Every event
that becomes durable fact passes through it. NATS is only one way to *reach*
admission — not the boundary itself.

Three transport classes feed (or bypass) that boundary:

### Direct in-process

Use when there is no process or network boundary between producer and admission:

- local staged-material parsing inside one `sinexd` process;
- deterministic replay inside one process;
- tests and fixture worlds;
- adapter → parser → `AdmissionService` paths.

A direct call is synchronous, lossless, and needs no ack/redelivery machinery
because the caller holds the result. This path exists as
`EventTransport::Direct`, while JetStream remains the correct route for
cross-process/external intent and material-frame streams.

### Core NATS

Use for transient signalling where loss is acceptable and consumers can resync
from an authoritative store (the DB or a JetStream read model):

- ephemeral notifications;
- live fan-out where loss is acceptable;
- status / progress / coordination signals where a missed message is recovered
  on the next interval or by re-reading state.

At-most-once, no stream, no replay. Do **not** put a durable event intent that
must survive process loss on Core NATS.

### JetStream

Use for durable cross-process intent:

- provenance-bearing event delivery that must survive process loss;
- external producers needing server ack and redelivery;
- multi-consumer replay boundaries;
- backpressure boundaries where a durable queue is wanted.

At-least-once with `Nats-Msg-Id` idempotency, durable consumers, and
server-side retention.

### Non-goals

- **No uniform transport façade** that hides these semantics behind one
  interface. The `Class` catalog deliberately keeps the differences visible
  (`Sinex-Transport-Class` header preserves intent on the wire).
- **Do not route staged local material through NATS merely because NATS
  exists.** Co-located producer/admission should go direct (#1732).

---

## 2. Subject / stream namespace facts (read from code)

The issue requires enumeration *from code, not docs*. Two facts that the prose
docs (`transport.md`, `docs/architecture.md`) currently
state incorrectly:

1. **The event plane has no `sinex.` infix.** `JetStreamTopology::new`
   (`crate/sinex-primitives/src/nats.rs:476-496`) builds the event-family
   subjects from the bases `events.raw.>`, `events.confirmed.>`,
   `events.dlq.>`, `events.processing_failures.>`, then prepends only the
   environment name via `nats_subject`
   (`crate/sinex-primitives/src/environment.rs:241`). The live subjects are
   therefore `{env}.events.raw.>` etc. — **not** `{env}.sinex.events.raw.>` as
   the prose docs claim. Only the invalidation and control planes carry the
   `sinex.` infix (their base strings include it):
   `{env}.sinex.derived.invalidation` (`nats.rs:496`) and
   `{env}.sinex.control.*` (`api/replay_control/server.rs:41`,
   `sources/parse_listener.rs:4`).
2. **Stream names are env-prefixed and upcased:** `nats_stream_name`
   (`environment.rs:257`) turns `SINEX_RAW_EVENTS` into `DEV_SINEX_RAW_EVENTS`
   etc. The bootstrap definitions live in
   `nixos/modules/nats.nix:447-493`.

Throughout this document `{env}` is the lowercase environment subject prefix
(`dev`, `staging`, `prod`) and `{ENV}` the upcased stream prefix.

---

## 3. Decision table — every current subject / stream / KV bucket

Columns: **Class (now → rec)** = current traffic class → recommended;
**Dur** = durability requirement; **Delivery** = delivery semantics;
**Ack/redeliv** = ack & redelivery policy; **Idemp key** = idempotency key;
**DLQ** = dead-letter behavior; **Replay**; **Admission dest** = where it lands
relative to admission; **Test transport**.

### 3.1 Direct in-process

| Path | Class (now → rec) | Dur | Delivery | Ack/redeliv | Idemp key | DLQ | Replay | Admission dest | Test transport |
|---|---|---|---|---|---|---|---|---|---|
| adapter → parser → `AdmissionService` (local staged material) | Direct → **Direct** | n/a (synchronous) | exactly-once (call returns result) | none needed | none (caller holds result) | caller-handled `AdmissionRejection` | re-run parser on same staged material | `AdmissionService` directly (`admission.rs:270`) | direct call in-process |

*Evidence:* the executable route catalog row is
`local.staged_parser.admission` in
`sinex_primitives::transport::CURRENT_ROUTE_DECISIONS`.

### 3.2 JetStream streams (durable)

| Stream / subject | Class (now → rec) | Dur | Delivery | Ack/redeliv | Idemp key | DLQ | Replay | Admission dest | Test transport |
|---|---|---|---|---|---|---|---|---|---|
| `{ENV}_SINEX_RAW_EVENTS` / `{env}.events.raw.{src}.{type}` — **Critical** raw event intents | Critical → **Critical** | must survive process loss | at-least-once | server ack; redeliver on NAK/timeout; spool on hard fail | `Nats-Msg-Id` = event id (`nats_publisher.rs:337-360`) | raw-ingest DLQ after retry budget | source material/archive re-read (not the stream; 7d cap) | via JetStream | JetStream (sandbox NATS) |
| same stream — **Derived** automaton outputs | Derived → **Derived** | recoverable via replay | at-least-once | server ack; redeliver | `Nats-Msg-Id` = event id | processing-failure stream | re-run automaton on parents | via JetStream | JetStream |
| `{ENV}_SINEX_REFLECTION_EVENTS` / `{env}.events.reflection.raw.{src}.{type}` — **Telemetry/reflection** | **Telemetry (JetStream reflection lane)** | loss acceptable | at-least-once today | server ack; **no caller retry**, drop+warn (`transport.rs`) | `Nats-Msg-Id` | reflection DLQ/processing-failure streams | n/a | via JetStream (persists as self-observation events, feeds CAs) | JetStream |
| `{ENV}_SOURCE_MATERIAL` / `{env}.source_material.frames.>` (`begin`/`slices.*`/`end`) | SourceMaterial → **SourceMaterial** | must survive (anchors depend on it) | at-least-once, ordered, `work` retention | server ack required before anchors publish | slice idempotency headers | none — acquisition op fails first | re-acquire material | material assembler → admission | JetStream |
| `{ENV}_SINEX_RAW_EVENTS_CONFIRMED` / `{env}.events.confirmed.{provenance}.{source}.{event_type}` | **Confirmation (JetStream full-payload bus) → Confirmation** | delivery bus only; DB is archive | at-least-once to durable consumers, bounded Limits+discard:Old | publish after commit; raw ACK only after confirmed publish succeeds | `Nats-Msg-Id` = event id | fatal durability-gap leaves raw message unacked for redelivery | consumers catch up from DB when stream tail is gone | n/a (signal *from* admission to automata/SSE) | JetStream |
| `{ENV}_SINEX_RAW_EVENTS_DLQ` / `{env}.events.dlq.>` (publish: `events.dlq.event_engine`) | Critical (DLQ) → **Critical (DLQ)** | operator-durable, 7d, `dupe_window=1h` | at-least-once | operator-reviewed; `sinexctl dlq retry` re-submits | original event id + dupe window | terminal surface (this *is* the DLQ) | `sinexctl dlq retry` → normal pipeline | re-enters admission on requeue | JetStream |
| `{ENV}_SINEX_RAW_EVENTS_PROCESSING_FAILURES` / `{env}.events.processing_failures.{module}.{event_id}` | Derived (failure) → **Derived (failure)** | operator-durable, 7d | at-least-once | not auto-retried (retry = replay) | `{module}.{event_id}` | terminal surface | re-run automaton via replay | n/a (failure capture) | JetStream |
| `{ENV}_SINEX_RAW_EVENTS_DERIVED_INVALIDATIONS` / `{env}.sinex.derived.invalidation` | Invalidation → **Invalidation (keep JetStream)** | must reach active consumers, 24h | at-least-once, durable consumers | JetStream holds for offline consumers; publish error → caller | none (fan-out signal) | none (error to caller) | replayed by re-publishing on next scope change | triggers automaton scope recompute (post-admission) | JetStream |

### 3.3 Core NATS subjects (no stream; request-reply / fire-and-forget)

| Subject | Class (now → rec) | Dur | Delivery | Ack/redeliv | Idemp key | DLQ | Replay | Admission dest | Test transport |
|---|---|---|---|---|---|---|---|---|---|
| `{env}.sinex.control.sources.{id}.scan` — `SourceScanCommand` (`runtime/stream/wire_types.rs:173`) | Control → **Control** | none | at-most-once, request-reply + timeout | timeout → `SinexError`; caller retries at app level | operation id in payload | none | command re-issued by operator/API | n/a (triggers source scan) | Core NATS |
| `{env}.sinex.control.sources.{id}.parse` — staged-source parse replay (`sources/parse_listener.rs:4`) | Control → **Control** | none | at-most-once | timeout/error to caller | operation id | none | re-issue parse command | n/a | Core NATS |
| `{env}.sinex.control.sources.{id}.drain_complete` — drain signal | Control → **Control** | none | at-most-once | none | none | none | re-signalled on next drain | n/a | Core NATS |
| `{env}.sinex.control.replay.progress.{op}` — `SourceScanProgress` (`wire_types.rs:205`) | Control → **Control** | none (resyncable) | at-most-once fan-out | none — consumer resyncs from `ops` read model | operation id | none | progress re-emitted; terminal state in DB | n/a | Core NATS |
| `{env}.sinex.control.replay` — replay control request-reply (`api/replay_control/server.rs:41`) | Control → **Control** | none | at-most-once, request-reply + timeout | timeout → error | request id | none | client re-requests | n/a | Core NATS |
| coordination: handoff-ready / handoff-request / heartbeat ready-signals (`runtime/coordination.rs`) | Control → **Control** | none | at-most-once | timeout/error; next heartbeat resends | none | none | next heartbeat interval | n/a | Core NATS |

### 3.4 NATS-KV buckets (durable key-value)

| Bucket | Purpose | Class (now → rec) | Dur | Idemp / consistency | Replay | Notes |
|---|---|---|---|---|---|---|
| `KV_{env}_sinex_checkpoints` (`runtime/checkpoint.rs:264-274`) | runtime-module consumer checkpoints (1000-event interval) | JetStream-KV → **keep** | at-least-once; survives restart | last-writer-wins per `{module}.{group}.{consumer}` key | resume from last checkpoint on restart | local file backup also written |
| `{env}_sinex_schemas` (`event_engine/service.rs:1306,1406`) | event payload JSON-schema registry, hydrated to validator | JetStream-KV → **keep** | durable | keyed by `payload_schema_id` | rebuilt from DB on demand | bucket name has no `KV_` prefix |
| `KV_{env}_sinex_instances` (`coordination/kv_client.rs:184`) | runtime instance registry / health | JetStream-KV → **keep** | durable, TTL'd | keyed by instance id | rebuilt by re-registration | |
| `KV_{env}_sinex_leadership` (`coordination/kv_client.rs:185`) | leader election (CAS, 30s TTL) | JetStream-KV → **keep** | durable | CAS on `leadership.{service}` | re-elected on TTL expiry | |

---

## 4. Per-path recommendations (current ≠ recommended)

### 4.1 Telemetry uses the reflection stream

**Current:** `publish_telemetry` publishes to
`{env}.events.reflection.raw.{source}.{event_type}` on
`{ENV}_SINEX_REFLECTION_EVENTS`. It remains JetStream because Sinex models
self-observation as durable events that feed telemetry read models, but it no
longer consumes the activity raw stream's retention or byte budget.

The remaining migration boundary is event-engine persistence and general query
policy, not ingress routing or physical storage: `reflection.events` is the
managed telemetry hypertable, but the event engine still needs the lane-aware
write path and the general query stack still needs explicit lane defaults.

### 4.2 No direct in-process admission path — add it (#1732)

**Current:** the only route to `AdmissionService` is `JetStreamConsumer`
(`jetstream_consumer.rs:53`). Co-located staged material is published to
JetStream and re-consumed, paying network/serialization/ack cost for a
same-process hop and making deterministic replay and fixture worlds depend on a
live NATS server.

**Recommendation:** implement the direct adapter → parser → `AdmissionService`
call for local staged material and replay, keeping JetStream for genuine
cross-process / external-producer intent. This is the #1732 code AC; it is the
single largest current-vs-recommended gap and the reason for the "do not route
staged local material through NATS merely because NATS exists" non-goal.

### 4.3 Confirmed events are a bounded delivery bus, not an archive

**Current:** the event engine publishes full post-redaction confirmed events on
`events.confirmed.{provenance}.{source}.{event_type}` after the database commit.
The raw message is ACKed only after that publish succeeds. If the publish retry
budget is exhausted, the consumer returns a fatal durability-gap error and leaves
the raw message unacked for JetStream redelivery.

**Assessment:** the compacted `events.confirmations.{event_id}` watermark lane
and durable confirmation-retry stream were retired by the confirmation-plane
consolidation. Keeping the full-payload delivery bus on JetStream is still
correct: automata and SSE need durable ordered delivery of new confirmed events,
while PostgreSQL remains the archive for catch-up if a consumer falls beyond the
bounded stream tail. The stream must use bounded Limits retention with
`discard: Old`; it must not become an unbounded archive or a publish-rejecting
backpressure point.

### 4.4 Subject namespace drift (doc correctness, not reclassification)

Older prose claimed `{env}.sinex.events.raw.>`; the code emits
`{env}.events.raw.>` (§2). The invalidation/control planes do carry `sinex.`
because their base strings include it. The executable route catalog and
`transport.md` now follow the code-authoritative event-plane subject prefix.

---

## 5. Inventory summary

- **1** direct in-process admission path.
- JetStream event-ingress is split into activity (`Critical` + `Derived`) and
  reflection (`Telemetry`) stream families.
- **4** Core NATS control/coordination route families in the executable catalog
  (source commands, replay control/progress, private-mode control, coordination).
- **4** NATS-KV buckets.

`CURRENT_ROUTE_DECISIONS` currently classifies runtime route decisions.
The remaining major placement question is the local admission adapter (§4.2), plus
the conceptual note that confirmations are a Core-NATS-class signal carried on
JetStream as an optimization (§4.3).

---

## 6. Migration notes

- **#1732 (SNX-46)** owns the route matrix and targeted migrations that keep
  Direct, Core NATS, JetStream, and JetStream KV choices explicit. The
  executable route catalog in `sinex_primitives::transport` is the spec tests
  assert against.
- **#1739 (SNX-53)** owns the performance / QoS follow-through (semaphore
  budgets, backpressure, the telemetry-stream split in §4.1, retention tuning).
- **sinnix#188** owns host NATS deployment policy (TLS, authorization, loopback
  binding) — the bootstrap stream definitions in `nixos/modules/nats.nix` are
  the deployment surface this record enumerates.
- **#1725** tracks that the dev NATS currently binds all interfaces; the
  loopback assertion in `nats.nix` (`isLoopbackHost … || (serverTlsEnabled &&
  serverAuthorizationEnabled)`) is the guard. Relevant because every subject in
  §3 is exposed on whatever interface that server binds.
