## Event Lifecycle (23 Steps, Source to Query)

This is the complete path of an event through the system. Each step is a decision point where things can break.

```
 1. Source data exists (file change, shell command, window focus, systemd event)
 2. Source detects change (inotify/polling/socket/journal API)
 3. Source material registered in DB (raw.source_material_registry) — provenance root
 4. Ingestor parses source bytes into typed payload struct
 5. EventPayload trait provides (SOURCE, EVENT_TYPE) as compile-time constants
 6. .from_material(source_material_id) sets provenance + anchor_byte
 7. .build() creates Event<T> with UUIDv7 id, ts_orig, host, provenance
 8. Privacy engine runs synchronously (per-event, in ingestor process)
 9. EventBatcher accumulates (100 events OR 1 second, whichever first)
10. Batch published to NATS JetStream (Events stream)
11. `sinexd::event_engine` consumer receives batch from NATS
12. JSON parse + event ID presence check (fail -> DLQ)
13. Schema validation against sinex_schemas registry (lenient: unknown types pass)
14. MaterialReadySet pre-check for FK constraint (not ready -> NAK + retry)
15. Batch routing: derived -> REPEATABLE READ TX, material >=50 -> COPY, else -> QueryBuilder
16. COPY path: staging table, tab-delimited SIMD-escaped rows, INSERT SELECT
17. XOR provenance CHECK fires at DB level (redundant with step 6, defense-in-depth)
18. Full post-redaction confirmed events published to NATS confirmed-events stream
19. SSE SubscriptionBus delivers to connected browser/CLI clients
20. Automata consume confirmed events directly through durable JetStream consumers
21. Automaton processes event -> emits derived event with .from_parents()
22. Derived event enters pipeline at step 10 (back to NATS)
23. Event queryable via `sinexd::api` RPC (events.query, sinexctl, telemetry CAs)
```

### Where Things Break (Ordered by Likelihood)

| Step | Failure | Detection | Recovery |
|------|---------|-----------|----------|
| 10 | NatsPublisher semaphores (raw events: 100, other lanes: 16 each) — flood starves other sources | Backpressure on publish | Per-traffic-class semaphores deployed |
| 16 | COPY batch failure — one bad row kills 1000-row batch | `insert_stream_batch()` error | Bisect-retry: the batch is split in half and each sub-batch retried independently (`jetstream_consumer.rs`); isolated poison rows route to DLQ while healthy siblings commit |
| 12 | JSON parse failure | Immediate in `prepare_event()` | Route to DLQ |
| 14 | Material FK not ready | MaterialReadySet pre-check | NAK + retry after delay (safe) |
| 18 | NATS confirmed-event publish failure | Per-event result check | Fatal durability-gap error; raw message remains unacked for redelivery |
| 20 | Checkpoint save failure (NATS KV slow) | Warn log only | RuntimeModule continues with stale checkpoint; crash -> duplicates |

### Batch Insert Routing Decision

```
if has_synthesis  -> REPEATABLE READ transaction + QueryBuilder
                     (derived needs cycle detection in same TX)
elif batch >= 50  -> COPY protocol
                     (staging table, SIMD-escaped tab-delimited, non-pooled connection)
else              -> QueryBuilder (VALUES path)
                     (no staging overhead for small batches)
```

Implication: automaton-heavy workloads never hit the COPY fast path. COPY only benefits material-provenance batches from source contracts.

### Trust Boundaries

| Boundary | Validated | NOT validated |
|----------|-----------|---------------|
| Source -> NATS | Privacy engine (per-event, synchronous) | Payload size (10MB NATS-payload cap, enforced in Rust only at source side — `ingestion_helpers.rs:32`, `file_drop.rs:268`) |
| NATS -> `sinexd::event_engine` | JSON parse, event ID, schema (lenient) | ts_orig plausibility, anchor_byte sign |
| `sinexd::event_engine` -> DB | XOR provenance CHECK, material FK, self-ref cycle | Payload-to-material correspondence |
| DB -> `sinexd::api` | Client message sanitization, role authorization | — |
| `sinexd::api` -> CLI | Token-suffix RBAC (stateless, no revocation) | HTTP request body capped at 2MB (`SINEX_API_MAX_BODY_BYTES`, `api/config.rs`) — separate from the 10MB source NATS-payload limit |

### Key Thresholds

| Parameter | Value | Why |
|-----------|-------|-----|
| Event batch | 100 events or 1s | Latency vs throughput balance |
| COPY threshold | 50 rows | Below: QueryBuilder faster. Above: COPY faster |
| NATS semaphores | Raw events: 100, telemetry: 16, DLQ: 16, processing failures: 16 | Per-traffic-class flood protection (`nats_publisher.rs:21-24`) |
| Confirmation buffer | 10K events | Memory cap for provisional events |
| Payload filter depth | 8 levels | Prevents pathological recursive JSONB queries |
| Pagination max | 1000 rows | Prevents unbounded result sets |
| SSE batch | 20ms / 32 IDs | Reduces DB fetches during burst |
| Leadership TTL | 30s | Failover window if leader dies |
| Cascade depth | 100 levels | Prevents runaway recursive replay expansion (`cascade_analyzer.rs:17`) |
| Checkpoint interval | 1000 events | Durability vs overhead |
