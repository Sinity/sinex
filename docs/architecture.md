# Sinex Architecture Deep-Dive

> Reference companion to the always-loaded `CLAUDE.md` (= `AGENTS.md`). This file
> holds the long-form architecture material: the provenance model, the full event
> lifecycle with failure/threshold tables, the type-enforcement hierarchy, system
> topology, the database schema map, and the privacy/redaction plane.

---

## The Provenance Model (Read This First)

Everything in sinex follows from one idea: **events have provenance, and provenance determines what you can do with them.**

### XOR Provenance: The Core Invariant

```
Material provenance (source_material_id set, source_event_ids NULL):
  "I translated this byte range of this source file into this event."
  - Can be replayed by re-reading the source material
  - The source material is the ground truth; the event is interpretation
  - Created by: source contracts (fs, terminal, desktop, system, document)

Derived provenance (source_material_id NULL, source_event_ids set):
  "I derived this conclusion from these parent events."
  - Can be replayed by re-running the automaton on the parents
  - The parents are the ground truth; the derived is interpretation
  - Created by: automata (canonicalizer, analytics, health)
```

An event MUST have exactly one provenance type. This is enforced at four levels:
1. **DB CHECK constraint** on `core.events` (XOR on nullable columns)
2. **EventBuilder typestate** (`NoProvenance` has no `.build()` method)
3. **Wire-format rejection** — `Provenance`'s serde impl deserializes a flat
   wire shape and rejects both-set and neither-set (`builder.rs` ProvenanceWire)
4. **NonEmptyVec** in `Provenance::Derived` (empty parent arrays impossible in Rust)

### Why This Matters For Every Decision

| When you... | The provenance model says... |
|-------------|------------------------------|
| Create an event from raw data | Use `.from_material(source_material_id)` — you're interpreting source bytes |
| Create an event from other events | Use `.from_parents(event_ids)?` — you're deriving a conclusion |
| Fix a bug in an ingestor | Replay: archive old events, re-read source material, fresh events through normal pipeline |
| Fix a bug in an automaton | Replay: archive derived events, re-run automaton on (unchanged) parent events |
| Query event lineage | Walk `source_event_ids[]` recursively until you hit material-provenance events |
| Import historical data | Register the data file as source material first, THEN create events referencing it |

### The Three Clocks

| Clock | Column | What it means | Across replay |
|-------|--------|---------------|---------------|
| `ts_orig` | `timestamptz` | When the datapoint happened in the real world | **stable** — re-derived from the same material |
| `ts_coided` | `GENERATED ALWAYS AS (uuid_extract_timestamp(id))` | When sinex created *this interpretation* | **differs** — each replay is a new interpretation at a new "now" |
| `ts_persisted` | column `DEFAULT current_timestamp` | When the row was written to disk | differs (batch delay) |

`ts_coided` is **not an independent column** — it is a pure function of the event `id`'s UUIDv7 timestamp prefix. The `id` is a random UUIDv7 minted at creation, so `ts_coided` is the moment sinex produced this interpretation. This is *why* replayed events differ from their predecessors: a replay re-creates the interpretation now, so its `id` — and therefore its `ts_coided` — is new, even though `ts_orig` is unchanged.

`ts_orig` is a **quality-ranked** real-world timestamp, chosen from the best evidence in `raw.temporal_ledger` (precedence: `RealtimeCapture > IntrinsicContent > InferredMtime > InferredCtime > InferredUser > StagedAt`). **Wired** (#1570 Prong B): material-provenance events may arrive with `ts_orig = None` and defer resolution to the persistence stage, where the event engine reads `raw.temporal_ledger` and calls `LedgerReader::derive_ts_orig` to pick the best-evidence timestamp (`event_engine/jetstream_consumer.rs:1399` ← `read_temporal_ledger:1363`; ledger rows are written at material-begin time, `sinex-db/src/repositories/source_materials.rs:489`). A caller that supplies an explicit `ts_orig` is honored as-is.

**Decision rule:** query by `ts_orig` for "when did it happen?", by `ts_coided` for "when did sinex interpret it?".

### Identity: occurrence vs interpretation

Two different relations over an event row. Conflating them is the single most common modeling error in this codebase — guard against it:

- **Interpretation identity = the event `id`** (random UUIDv7, the primary key). It identifies *this interpretation*, not the real-world thing. **Replay creates new ids** — re-reading or re-deriving the same datapoint yields a brand-new row with a new `id` and new `ts_coided`.
- **Occurrence identity = the `(source_material_id, anchor_byte)` columns.** These are the real-world coordinates of the datapoint — *lineage data you query*, **never the primary key**. They are stable across replay (replay re-reads the same material at the same offsets), which is exactly how you relate a fresh event to the archived interpretation it supersedes: match the columns, not the key.

**One live interpretation per occurrence.** `core.events` should hold at most one row per `(source_material_id, anchor_byte)`. Ideally a `UNIQUE` constraint — but TimescaleDB hypertables can't enforce uniqueness without the partition key, so it is upheld by code: replay *archives the current live row before inserting the new one*, so only one is ever live. The archive (`audit.archived_events`) is intentionally **multi-valued**: replaying N times yields N archived interpretations + 1 live. This is a *defensive single-live-interpretation* invariant — **not** idempotency.

### Replay: new interpretations, by design

Replay is not a special mode and is **not idempotent** — an idempotent replay would be a noop, which is pointless. It deliberately produces *new records*:
1. Archive cascade: old events + derived descendants move to `audit.archived_events`
2. Publish scope invalidation signals to downstream automata
3. NATS request-reply to the source: "re-read this source material"
4. Fresh events flow through the NORMAL pipeline — **new `id`, new `ts_coided`**, same `(material_id, anchor_byte)` and `ts_orig`
5. Automata process naturally via JetStream subscription

Bug fixes, privacy-rule changes, and schema validation all apply retroactively because replay re-runs the real pipeline. Replay is NOT bit-for-bit deterministic (current rules, not original) — correct by design.

### Dedup is downstream and object-level — never the PK

Suppressing a duplicate *real-world occurrence* (user re-ingests the same file; ingests a newer version that logically overlaps an old one) is a **downstream, object-level concern** — never on the event `id`. It is not automatic or universal: some source types admit a clean automatic rule, others need heuristics or user involvement. **Partly wired** (#1570 Prong C). The live mechanism is the `equivalence_key` column: emitters stamp it (`EventBuilder::with_equivalence_key` — session/analytics/hourly/health/entity automata), and the event engine suppresses a re-insert when a live row already carries that key (`exists_with_equivalence_key`, `event_engine/admission.rs:410`, fail-open on DB error). The earlier *named* stubs are NOT the mechanism: `IdempotenceKey` does not exist in code at all, and `OccurrenceFilter` / `build_occurrence_filter` (the #1050 offline-import dedup path) exist but have no production callers (tests-only).

> **Tripwire.** If you reach for a deterministic/content-derived event `id`, an `ON CONFLICT (id)` to suppress "the same datapoint," or a `UNIQUE(material_id, anchor_byte)` *for idempotency* — **stop, you've misread the model.** Event ids are interpretations (new on replay); occurrence identity lives in columns; there is no id-based idempotency. (`ON CONFLICT (id) DO NOTHING` on the insert paths exists only to absorb NATS at-least-once *redelivery* of an already-minted message — a transport concern, not occurrence dedup.)

### Negative Space (Load-Bearing Absences)

These are design decisions, not missing features:
- **No id-based idempotency** — replay is not idempotent; it creates new interpretations (new `id`, new `ts_coided`) by design. Re-ingesting identical bytes is a *new material* → new events.
- **No occurrence dedup via the PK** — occurrence dedup, where it exists, is downstream and object-level (above), keyed on `(material_id, anchor_byte)`, never the event `id`.
- **No mutation** — events are immutable after persistence (simplifies queries, enables replay)
- **No federation** — single user, single database (no conflict resolution needed)
- **No real-time guarantee** — 2-5s pipeline latency is acceptable
- **No auto-pruning** — lifecycle management is explicit (live -> archive -> tombstone)
- **No multi-tenant** — one user, one DB, one trust domain

---

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
 8. EventBatcher accumulates (100 events OR 1 second, whichever first)
 9. Batch published to NATS JetStream (Events stream)
10. `sinexd::event_engine` consumer receives batch from NATS
11. JSON parse + event ID presence check (fail -> DLQ)
12. Schema validation against sinex_schemas registry (lenient: unknown types pass)
13. MaterialReadySet pre-check for FK constraint (not ready -> NAK + retry)
14. Privacy policy engine redacts the admitted batch (central `redact_batch` chokepoint in the event engine — NOT in the ingestor process)
15. Batch routing: derived -> REPEATABLE READ TX, material >=50 -> COPY, else -> QueryBuilder
16. COPY path: staging table, tab-delimited SIMD-escaped rows, INSERT SELECT
17. XOR provenance CHECK fires at DB level (redundant with step 6, defense-in-depth)
18. Full post-redaction confirmed events published to NATS confirmed-events stream
19. SSE SubscriptionBus delivers to connected browser/CLI clients
20. Automata consume confirmed events directly through durable JetStream consumers
21. Automaton processes event -> emits derived event with .from_parents()
22. Derived event enters pipeline at step 9 (back to NATS)
23. Event queryable via `sinexd::api` RPC (events.query, sinexctl, telemetry CAs)
```

### Where Things Break (Ordered by Likelihood)

| Step | Failure | Detection | Recovery |
|------|---------|-----------|----------|
| 9 | NatsPublisher semaphores (raw events: 100, other lanes: 16 each) — flood starves other sources | Backpressure on publish | Per-traffic-class semaphores deployed |
| 16 | COPY batch failure — one bad row kills 1000-row batch | `insert_stream_batch()` error | Bisect-retry: the batch is split in half and each sub-batch retried independently (`jetstream_consumer.rs`); isolated poison rows route to DLQ while healthy siblings commit |
| 11 | JSON parse failure | Immediate in `prepare_event()` | Route to DLQ |
| 13 | Material FK not ready | MaterialReadySet pre-check | NAK + retry after delay (safe) |
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
| Automaton consumer ack-pending | 128 | Per-automaton confirmed-stream in-flight bound (`SINEX_AUTOMATON_CONSUMER_MAX_ACK_PENDING`) |
| Payload filter depth | 8 levels | Prevents pathological recursive JSONB queries |
| Pagination max | 1000 rows | Prevents unbounded result sets |
| SSE batch | 20ms / 32 IDs | Reduces DB fetches during burst |
| Leadership TTL | 30s | Failover window if leader dies |
| Cascade depth | 100 levels | Prevents runaway recursive replay expansion (`cascade_analyzer.rs:17`) |
| Checkpoint interval | 1000 events | Durability vs overhead |

---

## Type Enforcement Hierarchy

Six levels of guarantee, from compile-time impossible to convention-only. Know which level you're operating at.

### Level 1: Compile-Time Impossible (Strongest)

The type system makes the wrong thing unrepresentable.

| What it prevents | How |
|-----------------|-----|
| Mixing event IDs with blob IDs | `Id<Event>` vs `Id<Blob>` (phantom type) |
| Building events without provenance | `EventBuilder<T, NoProvenance>` has no `.build()` method |
| Confusing source with event type | `EventSource` vs `EventType` (distinct newtypes) |
| Empty derived parent arrays | `NonEmptyVec<EventId>` in `Provenance::Derived` |
| Invalid source strings in constants | `EventSource::from_static()` validated at compile time |

### Level 2: Lint Enforced / AST-Grep Catalog

Static analysis catches violations before code compiles or merges.

| Rule | Enforcement |
|------|-------------|
| No `unwrap`/`expect` in library code | `deny(unwrap_used, expect_used)` + `allow-unwrap-in-tests` |
| Blocking forbidden patterns | `xtask check --forbidden` (public entrypoint; uses ripgrep-based checks plus ast-grep error-severity rules) |
| Additional structural/style rules | AST-grep catalog in `.config/ast-grep/rules/` (currently advisory warnings/hints unless marked `error`) |

### Level 3: DB Constraint Enforced

PostgreSQL rejects violations at write time.

| Constraint | What it guards |
|------------|----------------|
| XOR provenance CHECK | `source_material_id` XOR `source_event_ids` (exactly one set) |
| Material FK | `source_material_id` references `raw.source_material_registry` |
| Non-empty derived parents | `cardinality(source_event_ids) > 0` |
| Anchor byte non-negative | `CHECK (anchor_byte >= 0)` |
| Audit trigger | DELETE on `core.events` requires `sinex.operation_id` session var |

### Level 4: Runtime Validation

Application code checks at boundaries, but violations can reach the check.

| What's validated | Where | Gap |
|------------------|-------|-----|
| Privacy redaction (policy-driven) | `sinexd::event_engine`, central `redact_batch` chokepoint before persistence (`policy.rs:634` ← `jetstream_consumer.rs:2235`) | DB-backed `privacy.*` policy; **defaults to zero redaction** until the operator opts in. One chokepoint covers material AND derived events |
| Schema validation | `sinexd::event_engine`, before persistence | Lenient: unknown types pass. `payload_schema_id` IS bound and written on every insert path (single, batched VALUES, COPY staging, DLQ replay) — see `crate/sinex-db/src/repositories/events/persistence.rs` |
| Path traversal protection | `validate_path()` at API boundary | Only called where explicitly used |
| JSON depth/size limits | `validate_json()` at API boundary | Only called where explicitly used |
| `ts_orig` plausibility | `sinexd::event_engine`, before persistence | `ts_orig_future_skew_secs` config (`crate/sinexd/src/event_engine/config.rs`) bounds how far in the future ts_orig can be; implausibly-old events route to DLQ |

### Level 5: Convention + Lazy Check

Correctness depends on matching two manual lists, verified lazily on first use.

| Convention | Verification |
|------------|-------------|
| COPY column list matches schema | `verify_event_copy_contract()` lazy via OnceLock on first COPY batch — panics on mismatch |
| EventPayload constants match NATS subjects | Inventory collection at startup |

### Level 6: Convention Only (Weakest)

No automated enforcement. Correctness depends on developer discipline.

| Convention | Risk if violated |
|------------|-----------------|
| `operation_id` honesty | Callers can claim any ID (safety gate, not security) |
| Payload-to-material correspondence | Event can claim any anchor_byte — no cross-check with blob content |
| `privacy_tier` declaration accuracy | A `SourceContract` declares a `privacy_tier` / `ProcessingContext`, but nothing checks the declared tier matches a field's actual sensitivity. Redaction itself is centralized (Level 4 chokepoint), so this is declaration-metadata accuracy, not a per-parser invocation gap. Single-user / zero-prod-data: low impact |
| Health check truthfulness | Defaults to `true` — no verification of actual health |
| `module_run_id` tracking | Wired in heartbeat emitter (`heartbeat.rs:204`), event engine (`service.rs:432`), and stream runner (`initialize.rs:157`); not set in source-contract construction sites (ingestors, automata outputs) |

### Decision: Which Level to Target

When adding a new invariant:
- **Data corruption risk** -> Level 1 (type system) or Level 3 (DB constraint)
- **Code quality rule** -> Level 2 (lint/AST-grep)
- **External input boundary** -> Level 4 (runtime validation)
- **Internal consistency** -> Level 5 (startup check) minimum
- **Never leave at Level 6** if the invariant matters for correctness

---

## System Topology

### Data Flow

```
Sources                    Automata                   Clients
  fs, terminal,              canonicalizer,             CLI (sinexctl),
  desktop, system,           analytics, health          browser extension
  document                        |                         |
       |                          v                         |
       v                   Derived events                 |
  (privacy: event-engine)     (back to NATS)                  |
       |                          |                         |
       v                          v                         |
  +--------------------------------------------+           |
  |           NATS JetStream                    |           |
  |   Events stream (bounded: 2M msgs / 72h)    |           |
  |   Confirmed-events stream (bounded bus)     |           |
  |   DLQ stream (bounded: 72h; Nix decl 168h)  |           |
  +---------------------+----------------------+           |
                        |                                   |
                        v                                   |
              +---------------------+                       |
              |  sinexd             |                       |
              |  ::event_engine     |  Batch writes,        |
              |                     |  validation           |
              +----------+----------+                       |
                         |                                  |
                         v                                  |
              +-----------------+                           |
              |   PostgreSQL    |  TimescaleDB, pgvector    |
              |   + Extensions  |  pg_jsonschema, pg_trgm   |
              +--------+--------+                           |
                       |                                    |
                       v                                    |
              +---------------------+                       |
              |  sinexd             |<----------------------+
              |  ::api              |  Auth, rate limits
              |  JSON-RPC + SSE     |
              +---------------------+
```

### Dependency Hierarchy

```
sinex-primitives         Foundation: types, validation, errors, domain enums, IDs
    |
    +-- sinex-db          Database pools, repositories, query helpers, PKM orchestration
    |   |                 sinex_db::schema: DB schema + declarative convergence
    |   |
    |   +-- sinex-macros      #[derive(EventPayload)]

sinexd                  Unified daemon
    |
    +-- sinexd::runtime     Inline runtime support: lifecycle, checkpoints, replay
    +-- sinexd::sources      Source contracts and adapters
    +-- sinexd::automata     All automata
    +-- sinexd::event_engine Persistence pipeline
    +-- sinexd::api          API layer
    +-- sinexd::supervisor   Orchestration

sinexctl                 Unified CLI (query, trace, telemetry, context, report, import)

xtask                    Build automation, sandbox test infra, dev-loop tooling
```

### NATS Subject Topology

```
Subjects:
  {env}.events.raw.>                    Source event batches
  {env}.events.reflection.raw.>         Self-observation event batches
  {env}.events.confirmed.>              Full persisted confirmed events
  {env}.events.reflection.confirmed.>   Full persisted self-observation confirmations
  {env}.events.dlq.>                    Dead-letter queue
  {env}.sinex.derived.invalidation      Scope invalidation (replay)
  {env}.sinex.control.sources.{id}.scan Replay scan commands
  {env}.sinex.control.replay.progress.{op} Replay progress updates
```

### Telemetry Event-Type Prefixes

| Module | Event-type prefix |
|--------|------------------|
| `sinexd::event_engine` | `sinexd.event_engine.*` |
| `sinexd::api` | `sinexd.api.*` |

### Intelligence Model (Automata)

Three processing models for derived events:

| Model | Trait | State | Emit trigger | Example |
|-------|-------|-------|-------------|---------|
| **Transducer** | `Transducer` | Stateless | 1:1 per input | Command canonicalizer |
| **Windowed** | `Windowed` | Accumulator | `window_complete(&state) -> bool` | Session detector, analytics |
| **ScopeReconciler** | `ScopeReconciler` | Per-scope | Scope reconciled | Health aggregator |

All share `AutomatonRuntime<N>` for: NATS consumer, checkpoint persistence, health reporting, self-observation, shutdown, scope invalidation.

Each derived event carries `automaton_model`, `temporal_policy`, and `semantics_version` — self-documenting provenance metadata.

**Current automata**: the source of truth is the `AutomatonSpec` registry in
`crate/sinexd/src/automata/registry.rs` (16 registered as of 2026-07-06, all
hosted by `sinexd` and selected via `SINEX_AUTOMATA_ENABLED` / the NixOS
`services.sinex.automata` configuration). Rather than a census that drifts,
know the families:
- Rollup/session family — session detector (`activity.session.boundary`),
  hourly/daily summarizers, analytics (Windowed)
- Attention family — interval-lift (declarative transition→`state.interval`
  rules over Hyprland/ActivityWatch/systemd) and attention-stream (the recall
  timeline substrate)
- Entity/relation family — extractor, resolver, enricher, relation-extractor,
  tag-applier (default-registered; consumption is an open decision, bead
  sinex-pq5)
- Mechanical — command canonicalizer (Transducer), health aggregator
  (ScopeReconciler), document-parser, embedding-producer (receipts-only until
  a model client exists), instruction-reconciler

### Windowed Example: Session Detector

```rust
// Groups events by temporal proximity. Gap > 5 minutes = new session boundary.
// Actual implementation: crate/sinexd/src/automata/session.rs
struct SessionDetector;

impl Windowed for SessionDetector {
    type State = SessionState;
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str { "session-detector" }
    fn input_event_type(&self) -> &'static str { "*" }
    fn output_event_type(&self) -> &'static str { "activity.session.boundary" }

    // Accumulate events into the window state.
    async fn accumulate(&mut self, state: &mut Self::State, input: Self::Input,
        ctx: &AutomatonContext) -> Result<(), AutomatonLogicError>
    {
        let ts = ctx.event_timestamp();
        state.events.push(input);
        state.last_ts = Some(ts);
        Ok(())
    }

    // Check if the window should emit (gap > 5 min between events).
    fn window_complete(&self, state: &Self::State) -> bool {
        // Gap detection: if last event was >5 min ago, close the window
        state.last_ts.map_or(false, |last| {
            Timestamp::now() - last > Duration::minutes(5)
        })
    }

    // Emit session boundary event from accumulated state.
    async fn emit(&mut self, state: &mut Self::State)
        -> Result<Option<DerivedOutput>, AutomatonLogicError>
    {
        Ok(Some(DerivedOutput::windowed(json!({
            "start_time": state.start_ts,
            "end_time": state.last_ts,
            "event_count": state.events.len(),
        }))))
    }
}
```

### State Locations

| What | Where | Survives restart? |
|------|-------|------------------|
| Events | PostgreSQL `core.events` | Yes (ACID) |
| Archive | PostgreSQL `audit.archived_events` | Yes (ACID) |
| Source materials | PostgreSQL `raw.source_material_registry` | Yes (ACID) |
| Event schemas | PostgreSQL `sinex_schemas` | Yes (ACID) |
| Checkpoints | NATS KV + local file | Yes (at-least-once) |
| Material readiness | In-memory `MaterialReadySet` | No (rebuilt on startup) |
| Schema cache | In-memory `Arc<RwLock>` | No (rebuilt from DB) |
| xtask history | SQLite `$SINEX_STATE_DIR/xtask-history.db` | Yes |

---

## Database Schema

`core.events` is a TimescaleDB hypertable partitioned by UUIDv7 `id` (`by_range('id')`). `ts_coided` is a generated stored column derived from `id` for query ergonomics.

### Schema Map

| Schema | Key Tables | Purpose |
|--------|------------|---------|
| `core` | `events`, `blobs`, `node_manifests`, `entities`, `entity_relations`, `event_annotations`, `tags`, `tagged_items`, `event_embeddings`, `event_tombstones`, `operations_log` | Primary storage + knowledge graph + embeddings |
| `reflection` | `events` | Self-observation lane: same event model as `core.events` (LIKE shape, own hypertable) but its own stream, retention (30d + 7d compression), and read models — operator life-events never share retention policy with rebuildable telemetry. Events route here by `SourceRole::Reflection` via `EventStorageLane` |
| `raw` | `source_material_registry`, `temporal_ledger` | Provenance roots + observation timestamps |
| `audit` | `archived_events` | Immutable archive of deleted/superseded events (replay target) |
| `sinex_schemas` | `event_payload_schemas`, `validation_cache`, `dlq_events` | JSON schema registry + DLQ |
| `sinex_telemetry` | 9 continuous aggregates + 2 views + 1 materialized view | Self-observation and activity read models (see below) |
| `metrics` | (empty) | Reserved for future operational metrics |

### Telemetry Surface

Per issue #952 (closed), hot-path 1h/5m rollups bucketed on UUIDv7 `id` are
TimescaleDB continuous aggregates with hourly (or 5-minute for 5m buckets)
refresh policies. `current_health` and `recent_activity_summary` remain
ordinary views (one is a point-in-time aggregate over health events, the other
unions across CAs); `current_device_state` remains a regular materialized view
(latest-observation lookup, refreshed explicitly).

| Relation | Type | Bucket | What it tracks |
|----------|------|--------|----------------|
| `event_engine_batch_stats_1h` | Continuous aggregate | 1h | Batch size, latency, deferred/failed counts |
| `gateway_stats_1h` | Continuous aggregate | 1h | Request stats, latency, rate limits |
| `node_stats_1h` | Continuous aggregate | 1h | Events processed, latency, queue depth per runtime module |
| `stream_stats_1h` | Continuous aggregate | 1h | JetStream fill %, message counts |
| `metric_counters_1h` | Continuous aggregate | 1h | Named metric counter totals |
| `assembly_stats_1h` | Continuous aggregate | 1h | Material assembler state-machine activity |
| `command_frequency_hourly` | Continuous aggregate | 1h | Shell command execution frequency by UUIDv7 bucket |
| `file_activity_summary` | Continuous aggregate | 1h | Filesystem event counts by directory |
| `current_window_focus` | Continuous aggregate | 5m | Desktop window focus tracking |
| `current_system_state` | Continuous aggregate | 5m | CPU, memory, disk, systemd units |
| `current_health` | View | now | Latest health-aggregated reports per component |
| `recent_activity_summary` | View | now | Cross-source activity rollup (depends on CAs above) |
| `current_device_state` | Materialized view | now | Latest device state observation |

Source of truth: `TELEMETRY_VIEW_RELATIONS`,
`TELEMETRY_MATERIALIZED_VIEW_RELATIONS`, and `TELEMETRY_CONTINUOUS_AGGREGATES`
in `crate/sinex-schema/src/apply.rs`.

### Schema Convergence

Schema evolution uses declarative convergence (`sinex-schema apply`), not migrations. The apply engine diffs desired state against actual DB state and converges: adding columns, indexes, constraints, functions. Named CHECK constraints are converged; inline column CHECKs are not.

- Schema source: `crate/sinex-schema/src/defs/`
- Apply engine: `crate/sinex-schema/src/apply.rs`
- Strict diff: `crate/sinex-schema/src/strict_diff.rs`
- Design: `crate/sinex-db/docs/schema/schema_design.md`

**Drift detection**: `apply::diff` reports the categories it converges
(missing tables, columns, named constraints, indexes, triggers, views,
continuous aggregates). For categories the convergence engine does NOT
reconcile — trigger function bodies that survived a manual edit,
DEFAULT changes on existing columns, inline CHECKs, FK
ON DELETE / ON UPDATE actions, TimescaleDB hypertable settings (chunk
interval + retention policy presence) — call `strict_diff::check_strict`
(or run the `schema-strict-diff` binary against `DATABASE_URL`).
Comments / table descriptions remain a non-goal per #556. Issues #578
and #579 track real source-vs-live drift the strict diff caught on
fresh-apply state.

---

### Privacy / Redaction

Redaction is **centralized**, not per-source. The event engine applies a single
`redact_batch` chokepoint to every admitted event — material *and* derived —
before persistence (`event_engine/policy.rs:634`, called from
`jetstream_consumer.rs:2235`). Rules come from the DB-backed `privacy.*` policy
tables, edited via `sinexctl privacy`. With no operator-configured rules the
chokepoint is a pass-through: **the default is zero redaction** until the
operator opts in. This is by design for a single-user, zero-production-data
deployment — redaction is an optional convenience feature, not a multi-tenant
security boundary.

The primitive engine is the rule compiler/catalog behind that policy:

```rust
use sinex_primitives::privacy::{self, ProcessingContext};

let result = privacy::engine().process("export TOKEN=ghp_abc123", ProcessingContext::Command);
if result.any_matched() { /* use result.text (Cow<str>) */ }
if result.suppressed { /* drop the field */ }
// Contexts: Command, Clipboard, WindowTitle, Journal, Dbus, Notification, Document, Metadata
// Strategies: Redact, Encrypt (XChaCha20-Poly1305), Hash (BLAKE3 MAC), Suppress
```

Source contracts do **not** invoke `privacy::engine()` directly (there are no
non-test callers in `sinexd`); they declare a `privacy_tier` / `ProcessingContext`
as metadata and rely on the central chokepoint. Because redaction runs once over
all admitted events, derived/automaton output is covered by the same policy as
source events — there is no per-source "leak that persists into derived events"
class once a rule is configured.

---
