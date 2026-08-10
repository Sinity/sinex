# Transport Semantics Catalog

This page documents the publish-class taxonomy (`sinex_primitives::transport::Class`),
the route matrix (`sinex_primitives::transport::CURRENT_ROUTE_DECISIONS`),
the DLQ / processing-failure / local-recovery-spool boundary decisions, and the
drain protocol for each class.

Closes: #326, #327, #338, #693.

---

## Publish-class catalog

| Class | Use | Subject pattern | QoS | On local failure | Drain on SIGTERM |
|---|---|---|---|---|---|
| `Critical` | Provenance-bearing raw event payloads from source contracts | `{env}.events.raw.{src}.{type}` | JetStream, idempotency header, semaphore 100 | local recovery spool | wait for in-flight ACKs |
| `Derived` | Derived events from automata | `{env}.events.raw.{src}.{type}` | JetStream, idempotency header, semaphore 100 | processing-failure stream | wait for ACKs + save checkpoint |
| `SourceMaterial` | Ordered material begin/slice/end frames | `{env}.source_material.frames.*` | JetStream, ordered stream, ACK required | material acquisition fails before event publish | wait for ACKs before anchor use |
| `Confirmation` | Full post-redaction events after persistence | `{env}.events.confirmed.{provenance}.{source}.{event_type}` | JetStream, bounded delivery bus | raw message left unacked on publish failure | publish before raw ACK |
| `Invalidation` | Scope fan-out to automatons | `{env}.sinex.derived.invalidation` | JetStream, ephemeral push consumers, Limits retention (24h) | error propagated to caller | no special drain (JetStream holds) |
| `Control` | Lifecycle and coordination traffic | `{env}.sinex.control.>` / request-reply | Core NATS, request-reply + timeout | error returned (`SinexError::network`) | drop pending |
| `Telemetry` | Self-observation metrics and health | `{env}.events.reflection.raw.sinex.*` | JetStream, semaphore 16 | drop with warn log | best-effort flush |

## Route matrix

`CURRENT_ROUTE_DECISIONS` in `sinex_primitives::transport` is the executable
route catalog for #1732. It names the current Direct, Core NATS, JetStream, and
JetStream KV runtime paths with their selected transport, route, semantic class,
reason, degraded behavior, and verification surface. New publish or coordination
paths should add a row there in the same change that introduces the route.

---

## Wire-class mapping

The `Sinex-Traffic-Class` NATS header (`NatsTrafficClass`) is the wire enum.
`Class` adds semantic resolution for the publish contexts above, and is emitted
as `Sinex-Transport-Class`.

| `Class` | `NatsTrafficClass` header value |
|---|---|
| `Critical` | `raw_event` |
| `Derived` | `raw_event` |
| `SourceMaterial` | `source_material` |
| `Confirmation` | `control` |
| `Invalidation` | `control` |
| `Control` | `control` |
| `Telemetry` | `telemetry` |

`Critical` and `Derived` share the `raw_event` wire class because they share
the same subject plane and storage path through the event engine. They are
distinguishable by the `Sinex-Transport-Class` header and by the
`source_event_ids` / `source_material_id` provenance XOR.

---

## Publisher inventory

Every NATS publish site in the workspace is tagged below. The tag appears as a
comment on or near the `publish_with_headers` / `js.publish` / `nats.publish`
call in the source.

| File | Method | Class |
|---|---|---|
| `crate/sinexd/src/runtime/nats_publisher.rs` | `NatsPublisher::publish` | `Critical` |
| `crate/sinexd/src/runtime/nats_publisher.rs` | `NatsPublisher::publish_telemetry` | `Telemetry` |
| `crate/sinexd/src/runtime/nats_publisher.rs` | `NatsPublisher::publish_to_raw_ingest_dlq` | `Critical` (DLQ routing of raw events) |
| `crate/sinexd/src/runtime/nats_publisher.rs` | `NatsPublisher::publish_processing_failure` | `Derived` (failure envelope) |
| `crate/sinexd/src/runtime/acquisition_manager.rs` | material begin/slice/end publishers | `SourceMaterial` |
| `crate/sinexd/src/runtime/dlq_retry.rs` | raw-ingest DLQ retry re-publish | `Critical` |
| `crate/sinexd/src/runtime/coordination.rs` | `send_handoff_ready` / `send_handoff_request` / `publish_failure_signal` | `Control` |
| `crate/sinexd/src/runtime/stream/mod.rs` | scan ack / scan progress / module status | `Control` |
| `crate/sinexd/src/event_engine/jetstream_consumer/confirmation.rs` | `publish_confirmed_event` | `Confirmation` |
| `crate/sinexd/src/event_engine/jetstream_consumer.rs` | DLQ re-publish (`publish_dlq_entry`) | `Critical` |
| `crate/sinexd/src/event_engine/material_assembler/finalize.rs` | material DLQ routing | `SourceMaterial` |
| `crate/sinexd/src/event_engine/service.rs` | active schema broadcast | `Control` |
| `crate/sinexd/src/api/handlers/modules.rs` | drain/resume/horizon command publish | `Control` |
| `crate/sinexd/src/api/replay_control/` | replay control response | `Control` |
| `crate/sinexd/src/api/replay_control/` | `publish_scope_invalidations` | `Invalidation` |

---

## DLQ vs processing-failure vs local-recovery-spool

These three surfaces are distinct. Conflating them is a recurring source of
confusion; the boundaries below are authoritative.

### Raw-ingest DLQ (`events.dlq.*`)

- **What goes here**: raw event batches from source contracts that the event engine cannot
  persist after all retries. The event bytes are still syntactically valid NATS
  messages; the failure is at the DB or schema layer.
- **Who writes**: the event engine's `JetStreamConsumer` after exceeding retry budget.
- **Who reads**: operator tooling (`sinexctl`), human review.
- **Retry tooling**: `sinexctl dlq retry` re-submits messages into the normal
  ingest pipeline.
- **Subject**: `{env}.events.dlq.{component}` (stream: `{BASE}_DLQ`).
- **Traffic class**: `NatsTrafficClass::RawIngestDlq`.

### Processing-failure stream (`events.processing_failures.*`)

- **What goes here**: derived/runtime processing failures — an automaton could
  not transform its input, a windowed automaton emitted an invalid output, a
  transducer panicked.
- **Who writes**: `NatsPublisher::publish_processing_failure` (called from
  automaton adapter).
- **Who reads**: operator tooling; not automatically retried (retry = re-run
  the automaton via replay).
- **Subject**: `{env}.events.processing_failures.{component}.{event_id}` (stream:
  `{BASE}_PROCESSING_FAILURES`).
- **Traffic class**: `NatsTrafficClass::ProcessingFailure`.

### Local recovery spool (`sinex_event_recovery_spool.jsonl`)

- **What goes here**: events that a runtime module batcher could not publish to NATS at
  all — NATS was down, the semaphore was closed, or the connection was lost
  before the ACK arrived.
- **Who writes**: event batching in the inline runtime under
  `crate/sinexd/src/runtime/`.
- **Who reads**: the same runtime module on next startup; it replays the spool into the
  normal publish path before beginning new captures.
- **Subject**: none — file-local until NATS is available.
- **Location**: `{module_work_dir}/sinex_event_recovery_spool.jsonl`.
- **Traffic class**: not applicable (not on NATS yet).

### Decision rule

| Situation | Route |
|---|---|
| Event engine could not persist a raw event | Raw-ingest DLQ |
| Automaton could not process a derived event | Processing-failure stream |
| RuntimeModule could not reach NATS to publish | Local recovery spool |
| Confirmed-event publish failed after DB commit | Raw message remains unacked; event engine stops so JetStream redelivers |

---

## Drain protocol

Drain = stop accepting new work, finish in-flight, save state, exit cleanly.
The protocol per class:

### `Critical` — source event batches

1. Batch accumulator stops accepting new events (controlled by `shutdown_rx`).
2. All accumulated events are flushed to NATS.
3. Each publish awaits a JetStream ACK (bounded by `DEFAULT_PUBLISH_ACK_TIMEOUT`
   = 10 s).
4. On ACK timeout: events go to local recovery spool; the module exits with a warning.
5. On clean flush: checkpoint is saved; sd_notify sends `STOPPING=1`.

### `Derived` — automaton derived outputs

1. NATS consumer stops pulling new messages.
2. In-flight event processing completes.
3. Derived events are published and ACKed.
4. Checkpoint is saved (NATS KV + optional local backup).
5. RuntimeModule exits cleanly.

On crash (no SIGTERM): JetStream NAK timeout causes redelivery; automaton
deduplicates via equivalence key or scope reconciliation.

### `Confirmation` — confirmed-events delivery bus

1. The event engine publishes the full post-redaction `Event<JsonValue>` after
   the database commit.
2. The raw message is ACKed only after the confirmed-event publish succeeds.
3. If publish retries are exhausted, the event engine returns a fatal
   durability-gap error and leaves the raw message unacked for JetStream
   redelivery.
4. The confirmed-events stream is a bounded delivery bus, not an archive;
   PostgreSQL is the historical authority for catch-up.

### `Invalidation` — scope fan-out

No special drain needed. `recv_invalidation` (`crate/sinexd/src/runtime/
automaton/adapter/mod.rs`) acks each invalidation message immediately on
receipt, before debounce/recompute/checkpoint ever run — a crash in that
window (sinex-r6d.7) does not durably lose the invalidation. The
`SINEX_RAW_EVENTS_DERIVED_INVALIDATIONS` stream uses Limits retention (24h
`maxAge`), not WorkQueue: acking a message only advances the acking
consumer's own delivery/ack floor, it does not remove the message from the
stream. `run_continuous` subscribes with a fresh, unnamed (ephemeral) push
consumer every time an automaton starts — no `durable_name`, so a restart
after a crash creates a brand-new consumer with `DeliverPolicy::All` and no
inherited ack state, and therefore redelivers every invalidation still
inside the 24h retention window, including ones already acked by a
now-dead consumer. Setting `deliver_group` (queue-grouping the automaton's
own subscribers) does not change this — a fresh `create_consumer` call
with the same group string still gets its own independent ack floor.
Proven empirically, not just by reading `async-nats`/JetStream semantics,
by the sinex-r6d.9 crash-injection harness
(`r6d9_invalidation_ack_fail_point_fires` and
`r6d9_invalidation_deliver_group_does_not_share_ack_state` in
`adapter_test.rs`): the harness genuinely crashes the process
(`std::process::exit(98)`) at the ack-succeeded/payload-not-yet-returned
boundary and asserts a fresh consumer still receives the same message.

This means the window sinex-r6d.7 investigated (JetStream-level invalidation
ack-before-recompute) is bounded and self-healing, contingent on: (1) the
crashed process actually restarts within the 24h retention window (ordinary
systemd auto-restart), and (2) the crash happens before any DB work — the
fail point sits inside `recv_invalidation` itself, strictly before the ack'd
payload is even returned to `handle_invalidation_message`, so redelivery
always replays from a clean slate for this specific window. A **different**,
still-open risk lives one layer deeper: if a crash instead happens *during*
recompute, after `invalidate.rs`'s deliberate emit-outputs-before-archive-
stale-outputs ordering has already emitted replacement events but before the
stale originals are archived, redelivery of the same invalidation can
recompute and emit a second time before archiving catches up — a
duplicate/stale window the invalidation AC also flags, needing
operation-scoped dedup or a durable pending-operation id to close (tracked
separately; out of scope for the ack-ordering window this section
describes). Malformed invalidation payloads are also out of scope here: a
deserialize failure in `handle_invalidation_message` is logged and dropped
(`Ok(None)`) without creating durable invalidation debt — low-severity today
because only sinexd itself publishes this subject, but not the "durable
debt before ack/term" contract invalidation's own design intent describes.

### `Control` — coordination traffic

Drop pending. Control messages are either:
- Request-reply with timeout: the caller already handles timeout as an error.
- Fire-and-forget heartbeat / ready-signals: loss is non-fatal; the next
  heartbeat interval will resend.

Runtime modules do not need to flush control messages on SIGTERM.

### `Telemetry` — self-observation

1. Best-effort flush of any buffered metric events.
2. No wait; if NATS is unavailable the metrics are dropped.
3. Gaps in telemetry are acceptable; they do not affect event correctness.

---

## NixOS restart behavior

NixOS systemd unit restarts issue SIGTERM followed by SIGKILL (after
`TimeoutStopSec`). The drain protocol above applies on SIGTERM. The local
recovery spool and JetStream's at-least-once delivery provide the durability
guarantee across SIGKILL scenarios.

Components must not set `TimeoutStopSec` below the sum of:
- Max batch accumulation window (1 s)
- Max publish ACK timeout (10 s)
- Checkpoint save time (~100 ms)

A `TimeoutStopSec = 30s` is sufficient for all current components.

---

## Test shutdown behavior

In the sandbox (`xtask::sandbox`), modules receive a controlled shutdown via
`shutdown_rx`. The drain sequence is identical to SIGTERM. Tests that assert on
event counts must call `ctx.timing().wait_for_event_count(N)` before triggering
shutdown; otherwise in-flight events may not yet be confirmed.
