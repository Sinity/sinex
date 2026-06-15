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

An event MUST have exactly one provenance type. This is enforced at three levels:
1. **DB CHECK constraint** on `core.events` (XOR on nullable columns)
2. **EventBuilder typestate** (`NoProvenance` has no `.build()` method)
3. **NonEmptyVec** in `Provenance::Derived` (empty parent arrays impossible in Rust)

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
| `ts_persisted` | DB trigger | When the row was written to disk | differs (batch delay) |

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
