# Transport Semantics Catalog

This page documents the publish-class taxonomy (`sinex_primitives::transport::Class`),
the DLQ / processing-failure / local-recovery-spool boundary decisions, and the
drain protocol for each class.

Closes: #326, #327, #338, #693.

---

## Publish-class catalog

| Class | Use | Subject pattern | QoS | On local failure | Drain on SIGTERM |
|---|---|---|---|---|---|
| `Critical` | Provenance-bearing raw event payloads from ingestors | `{env}.sinex.events.raw.{src}.{type}` | JetStream, idempotency header, semaphore 100 | local recovery spool | wait for in-flight ACKs |
| `Derived` | Synthesis events from automata | `{env}.sinex.events.raw.{src}.{type}` | JetStream, idempotency header, semaphore 100 | processing-failure stream | wait for ACKs + save checkpoint |
| `Confirmation` | Persistence ACK signals from ingestd | `{env}.events.confirmations.{event_id}` | JetStream, best-effort | retry queue → durability-gap warn | best-effort flush |
| `Invalidation` | Scope fan-out to derived nodes | `{env}.sinex.derived.invalidation` | JetStream, durable consumers | error propagated to caller | no special drain (JetStream holds) |
| `Control` | Lifecycle and coordination traffic | `{env}.sinex.control.>` / request-reply | Core NATS, request-reply + timeout | error returned (`SinexError::network`) | drop pending |
| `Telemetry` | Self-observation metrics and health | `{env}.sinex.events.raw.sinex.*` | JetStream, semaphore 16 | drop with warn log | best-effort flush |

---

## Wire-class mapping

The `Sinex-Traffic-Class` NATS header (`NatsTrafficClass`) is a five-value
wire enum. `Class` adds semantic resolution for the six publish contexts above.

| `Class` | `NatsTrafficClass` header value |
|---|---|
| `Critical` | `raw_event` |
| `Derived` | `raw_event` |
| `Confirmation` | `control` |
| `Invalidation` | `control` |
| `Control` | `control` |
| `Telemetry` | `telemetry` |

`Critical` and `Derived` share the `raw_event` wire class because they share
the same subject plane and storage path through ingestd. They are
distinguishable at the application level by the `Sinex-Traffic-Class` header
value and by the `source_event_ids` / `source_material_id` provenance XOR.

---

## Publisher inventory

Every NATS publish site in the workspace is tagged below. The tag appears as a
comment on or near the `publish_with_headers` / `js.publish` / `nats.publish`
call in the source.

| File | Method | Class |
|---|---|---|
| `crate/lib/sinex-node-sdk/src/nats_publisher.rs` | `NatsPublisher::publish` | `Critical` |
| `crate/lib/sinex-node-sdk/src/nats_publisher.rs` | `NatsPublisher::publish_telemetry` | `Telemetry` |
| `crate/lib/sinex-node-sdk/src/nats_publisher.rs` | `NatsPublisher::publish_to_raw_ingest_dlq` | `Critical` (DLQ routing of raw events) |
| `crate/lib/sinex-node-sdk/src/nats_publisher.rs` | `NatsPublisher::publish_processing_failure` | `Derived` (failure envelope) |
| `crate/lib/sinex-node-sdk/src/coordination.rs` | `send_handoff_ready` / `send_handoff_request` / `publish_failure_signal` | `Control` |
| `crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs` | scan ack / scan progress / node status | `Control` |
| `crate/core/sinex-ingestd/src/jetstream_consumer.rs` | `publish_confirmation` | `Confirmation` |
| `crate/core/sinex-ingestd/src/jetstream_consumer.rs` | DLQ re-publish (`publish_dlq_entry`) | `Critical` |
| `crate/core/sinex-gateway/src/replay_control.rs` | replay control response | `Control` |
| `crate/core/sinex-gateway/src/replay_control.rs` | `publish_scope_invalidations` | `Invalidation` |

---

## DLQ vs processing-failure vs local-recovery-spool

These three surfaces are distinct. Conflating them is a recurring source of
confusion; the boundaries below are authoritative.

### Raw-ingest DLQ (`events.dlq.*`)

- **What goes here**: raw event batches from ingestors that ingestd cannot
  persist after all retries. The event bytes are still syntactically valid NATS
  messages; the failure is at the DB or schema layer.
- **Who writes**: ingestd's `JetStreamConsumer` after exceeding retry budget.
- **Who reads**: operator tooling (`sinexctl`, gateway CLI), human review.
- **Retry tooling**: `sinexctl dlq retry` re-submits messages into the normal
  ingest pipeline.
- **Subject**: `{env}.events.dlq.{node}` (stream: `{BASE}_DLQ`).
- **Traffic class**: `NatsTrafficClass::RawIngestDlq`.

### Processing-failure stream (`events.processing_failures.*`)

- **What goes here**: derived/runtime processing failures — an automaton could
  not transform its input, a windowed node emitted an invalid output, a
  transducer panicked.
- **Who writes**: `NatsPublisher::publish_processing_failure` (called from
  derived-node adapter).
- **Who reads**: operator tooling; not automatically retried (retry = re-run
  the automaton via replay).
- **Subject**: `{env}.events.processing_failures.{node}.{event_id}` (stream:
  `{BASE}_PROCESSING_FAILURES`).
- **Traffic class**: `NatsTrafficClass::ProcessingFailure`.

### Local recovery spool (`sinex_event_recovery_spool.jsonl`)

- **What goes here**: events that a node batcher could not publish to NATS at
  all — NATS was down, the semaphore was closed, or the connection was lost
  before the ACK arrived.
- **Who writes**: `EventBatcher` in `sinex-node-sdk/src/event_node.rs`.
- **Who reads**: the same node on next startup; it replays the spool into the
  normal publish path before beginning new captures.
- **Subject**: none — file-local until NATS is available.
- **Location**: `{node_work_dir}/sinex_event_recovery_spool.jsonl`.
- **Traffic class**: not applicable (not on NATS yet).

### Decision rule

| Situation | Route |
|---|---|
| ingestd could not persist a raw event | Raw-ingest DLQ |
| Automaton could not process a derived event | Processing-failure stream |
| Node could not reach NATS to publish | Local recovery spool |
| Confirmation could not be published | Retry queue → durability-gap warn |

---

## Drain protocol

Drain = stop accepting new work, finish in-flight, save state, exit cleanly.
The protocol per class:

### `Critical` — ingestor event batches

1. Batch accumulator stops accepting new events (controlled by `shutdown_rx`).
2. All accumulated events are flushed to NATS.
3. Each publish awaits a JetStream ACK (bounded by `DEFAULT_PUBLISH_ACK_TIMEOUT`
   = 10 s).
4. On ACK timeout: events go to local recovery spool; node exits with a warning.
5. On clean flush: checkpoint is saved; sd_notify sends `STOPPING=1`.

### `Derived` — automaton synthesis outputs

1. NATS consumer stops pulling new messages.
2. In-flight event processing completes.
3. Synthesis events are published and ACKed.
4. Checkpoint is saved (NATS KV + optional local backup).
5. Node exits cleanly.

On crash (no SIGTERM): JetStream NAK timeout causes redelivery; automaton
deduplicates via equivalence key or scope reconciliation.

### `Confirmation` — ingestd ACK signals

1. ingestd flushes the confirmation queue concurrently with the batch ACK.
2. Remaining confirmation failures go to the durable retry consumer
   (`events.confirmation_retries.*`).
3. If the retry stream is also unreachable: durability-gap counter and warn log.
4. Event data is already safe (persisted to DB); confirmations are re-derivable.

### `Invalidation` — scope fan-out

No special drain needed. JetStream holds undelivered invalidations for all
durable consumers. Derived nodes that restart will receive queued
invalidations and recompute affected scopes.

### `Control` — coordination traffic

Drop pending. Control messages are either:
- Request-reply with timeout: the caller already handles timeout as an error.
- Fire-and-forget heartbeat / ready-signals: loss is non-fatal; the next
  heartbeat interval will resend.

Nodes do not need to flush control messages on SIGTERM.

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

In the sandbox (`xtask::sandbox`), nodes receive a controlled shutdown via
`shutdown_rx`. The drain sequence is identical to SIGTERM. Tests that assert on
event counts must call `ctx.timing().wait_for_event_count(N)` before triggering
shutdown; otherwise in-flight events may not yet be confirmed.
