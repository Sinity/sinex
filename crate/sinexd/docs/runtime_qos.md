# Runtime QoS And Load Shedding

Status: design record for #1093.

Sinex is lossless by default for source material and admitted events. Any
bounded, lossy, sampled, or coalesced behavior must be explicit, operator
visible, and tied to the traffic class affected.

## Current Mechanisms

- `NatsPublisher` separates publisher concurrency into raw events, telemetry,
  raw-ingest DLQ, and processing-failure lanes. Defaults are 100 raw-event
  publishes and 16 publishes for each other lane.
- Every JetStream publish waits for an ack with a bounded timeout. Timeout is a
  producer-visible failure, not silent loss.
- `sinexd::event_engine` NAKs retryable persistence/material-readiness failures, routes
  terminal validation or material failures to DLQ, and leaves fatal confirmation
  durability gaps unacked for redelivery instead of pretending the batch landed.
- `insert_stream_batch` routes derived batches through transactional VALUES,
  large material-only batches through COPY, and small material-only batches
  through VALUES. That is throughput routing, not a lossy policy.
- Material assembly has explicit limits for buffered slices and material size;
  terminal failures route to DLQ and mark material failed when the failure mark
  can land durably.

## Policy Table

| Class | Examples | Policy | Current mechanism | Gap |
|---|---|---|---|---|
| Control and privacy state | private-mode toggles, scan/drain commands, replay invalidations | Must not be starved; fail closed if publish/ack fails. | Control messages use explicit NATS subjects and ack/error paths. | No global priority scheduler; keep command/control traffic out of bulk raw-event lanes. |
| Confirmations and durability receipts | event-engine confirmations, retry confirmations | Must persist or make the gap fatal/visible. | Confirmation retry path and fatal confirmation durability-gap classification. | Operator UI should surface confirmation retry backlog and fatal gaps. |
| Durable source material | staged files, SQLite snapshots, material slices | Lossless; may defer/retry; must DLQ terminal corruption. | WAL-backed material assembler, max size/slice limits, DLQ on terminal failures. | Continuity gap records should summarize failed/deferred material windows. |
| Admitted event intents | source events, external event-intent bridge records | Lossless after admission; may backpressure producers; no silent drop. | Raw-event publisher lane, JetStream ack timeout, event-engine retry/DLQ. | External producers need clear retry/confirmation guidance. |
| Derived events | automata outputs, summaries, model-derived records | Lossless relative to parent events; may be replayed from parents; failures route to processing DLQ. | Derived processing failure lane and per-event DLQ fallback. | High-fan-in summaries need compact lineage records, not huge parent arrays. |
| Telemetry/self-observation | runtime metrics, heartbeat, publisher counters | Best effort but accountable; may sample/coalesce under pressure if a gap record is emitted. | Separate telemetry publisher lane. | No explicit sampling/coalescing policy or telemetry gap event yet. |
| Bulk parser output | historical backfills, staged export parsers | May defer and throttle; must not starve control/privacy/confirmation. | Source isolation plus raw-event publisher backpressure. | Need per-source budget accounting and operator-visible deferred-work status. |
| Model and embedding effects | embeddings, LLM summaries, recorded model effects | Deferrable; must record model/effect provenance when emitted; may be skipped under privacy policy. | Embeddings have their own repository/runtime track. `core.model_effects` is registered schema/repository surface, but has no production caller yet. | Wire any live model-effect caller through proposal/judgment/operation or derivation authority, with replay/disclosure policy visible; do not use the table as a hidden cache. |
| Live lossy sensors | future audio/OCR/screen streams, high-rate UI signals | May sample/drop/coalesce only with auditable gap records. | No general mechanism yet. | Follow-up required before adding high-rate lossy sources. |
| Diagnostics and inventories | diagnostic inventories, readiness probes, docs checks | May be stale or advisory if labelled that way; must not masquerade as proof. | Inventory-surface pruning in #1129. | Continue demoting non-gates to inventory/evidence wording. |

## Load-Shedding Rules

1. Source material and admitted events do not silently drop. If they cannot land,
   producers backpressure, retry, or receive a failure/confirmation gap.
2. Privacy/control traffic preempts bulk work by using separate subjects,
   bounded operations, and future scheduler priority if contention proves real.
3. Telemetry may be sampled only when the sample loss is itself observable.
4. Live high-rate sensors may be lossy only after they define a gap/seam event
   that records time window, source, count estimate when known, and reason.
5. Bulk historical/parser work may defer, throttle, or split batches; it may not
   weaken provenance or replay semantics.
6. Any operator-facing “healthy” state must account for DLQ backlog, deferred
   material, confirmation retry backlog, and known lossy gaps.

## Operator Accounting

Load shedding and backpressure should converge into a small set of explainable
operator signals:

- `deferred_work`: source, scope, reason, retry horizon;
- `capture_gap`: source, time window, class, dropped/coalesced/deferred
  count if known, and whether replay can fill it;
- `confirmation_gap`: batch scope and retry/durability status;
- `dlq_backlog`: class, oldest failure, terminal vs retryable count;
- `bulk_throttle`: source, configured budget, current budget pressure.

These signals should feed source continuity/readiness surfaces rather than live
only in logs.

## Follow-Ups

- Add operator-visible confirmation retry and DLQ backlog summaries to the
  status/readiness surface if they are not already visible.
- Define the first `capture_gap` event before adding any high-rate lossy source.
- Add per-source budget/deferred-work accounting for bulk staged parser
  output.
- Extend external-producer documentation so non-SDK producers know when to retry,
  wait for confirmation, or stop on privacy/admission failures.
