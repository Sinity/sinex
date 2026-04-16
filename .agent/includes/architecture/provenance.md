## The Provenance Model (Read This First)

Everything in sinex follows from one idea: **events have provenance, and provenance determines what you can do with them.**

### XOR Provenance: The Core Invariant

```
Material provenance (source_material_id set, source_event_ids NULL):
  "I translated this byte range of this source file into this event."
  - Can be replayed by re-reading the source material
  - The source material is the ground truth; the event is interpretation
  - Created by: ingestors (fs, terminal, desktop, system, document)

Synthesis provenance (source_material_id NULL, source_event_ids set):
  "I derived this conclusion from these parent events."
  - Can be replayed by re-running the automaton on the parents
  - The parents are the ground truth; the synthesis is interpretation
  - Created by: automata (canonicalizer, analytics, health)
```

An event MUST have exactly one provenance type. This is enforced at three levels:
1. **DB CHECK constraint** on `core.events` (XOR on nullable columns)
2. **EventBuilder typestate** (`NoProvenance` has no `.build()` method)
3. **NonEmptyVec** in `Provenance::Synthesis` (empty parent arrays impossible in Rust)

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

| Clock | Column | What it means | When they diverge |
|-------|--------|---------------|-------------------|
| `ts_orig` | `timestamptz` | When it happened in the real world | Historical imports: years before ts_coided |
| `ts_coided` | generated from UUIDv7 `id` | When sinex first observed it | Always "now" at creation time |
| `ts_persisted` | DB trigger | When the row was written to disk | Batch delay (100 events or 1s) |

**Decision rule:** Query by `ts_orig` for "what happened when?", by `ts_coided` for "what did sinex know when?". Continuous aggregates bucket on `ts_coided` (via UUIDv7 `id`), which means historical imports are invisible to CAs until manual refresh.

### The Stable Real-World Identifier

`(source_material_id, anchor_byte)` uniquely identifies an occurrence in the real world.
Event IDs (UUIDv7) identify *interpretations* — replay creates new IDs for the same occurrence.

**Known gap:** TimescaleDB hypertables cannot enforce UNIQUE on this pair (requires partition key). The design describes uniqueness that the architecture cannot physically enforce. UUIDv5 from `(material_id, anchor_byte)` provides accidental idempotency via `ON CONFLICT DO NOTHING`.

### Replay: Where Provenance Proves Itself

Replay is not a special mode. After archiving old events, the system just runs normally:
1. Archive cascade: old events + derived descendants move to `audit.archived_events`
2. Publish scope invalidation signals to downstream automata
3. NATS request-reply to ingestor: "re-read this source material"
4. Fresh events flow through the NORMAL pipeline (NATS -> ingestd -> DB)
5. Automata process naturally via JetStream subscription

This means: bug fixes retroactively apply, privacy rule changes retroactively apply, schema validation runs normally. Replay is NOT bit-for-bit deterministic (current privacy rules, not original ones) — this is correct by design.

### Negative Space (Load-Bearing Absences)

These are design decisions, not missing features:
- **No mutation** — events are immutable after persistence (simplifies queries, enables replay)
- **No federation** — single user, single database (no conflict resolution needed)
- **No real-time guarantee** — 2-5s pipeline latency is acceptable
- **No auto-pruning** — lifecycle management is explicit (live -> archive -> tombstone)
- **No multi-tenant** — one user, one DB, one trust domain
