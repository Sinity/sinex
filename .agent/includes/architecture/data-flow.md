## System Topology

### Data Flow

```
Nodes (Ingestors)          Nodes (Automata)           Clients
  fs, terminal,              canonicalizer,             CLI (sinexctl),
  desktop, system,           analytics, health          browser extension
  document                        |                         |
       |                          v                         |
       v                   Synthesis events                 |
  [privacy engine]          (back to NATS)                  |
       |                          |                         |
       v                          v                         |
  +--------------------------------------------+           |
  |           NATS JetStream                    |           |
  |   Events stream (10M/90d)                   |           |
  |   Confirmations stream (compacted/7d)       |           |
  |   DLQ stream (1M/30d)                       |           |
  +---------------------+----------------------+           |
                        |                                   |
                        v                                   |
              +-----------------+                           |
              |  sinex-ingestd  |  Batch writes, validation |
              +--------+--------+                           |
                       |                                    |
                       v                                    |
              +-----------------+                           |
              |   PostgreSQL    |  TimescaleDB, pgvector    |
              |   + Extensions  |  pg_jsonschema, pg_trgm   |
              +--------+--------+                           |
                       |                                    |
                       v                                    |
              +-----------------+                           |
              | sinex-gateway   |<--------------------------+
              | JSON-RPC + SSE  |  Auth, rate limits
              +-----------------+
```

### Dependency Hierarchy

```
sinex-primitives         Foundation: types, validation, errors, domain enums, IDs
    |
    +-- sinex-schema      DB schema + declarative convergence (library only)
    |
    +-- sinex-db          Database pools, repositories, query helpers, PKM orchestration
            |
            +-- sinex-macros      #[derive(EventPayload)]
            |
            +-- sinex-node-sdk    Node runtime + CLI: lifecycle, checkpoints, replay
                    |
                    +-- All ingestors (fs, terminal, desktop, system, document)
                    +-- All automata (canonicalizer, analytics, health)
            +-- sinex-gateway     API layer: JSON-RPC, SSE, native messaging, content orchestration

sinexctl                 Unified CLI (query, trace, telemetry, context, report, import)

xtask                    Build automation (~115K lines, ~20% of total)
```

### NATS Subject Topology

```
Subjects:
  {env}.sinex.events.raw.>              Ingestor event batches
  sinex.events.confirmed.>              Persistence confirmations
  sinex.events.dlq.>                    Dead-letter queue
  sinex.derived.invalidation            Scope invalidation (replay)
  sinex.telemetry                       Self-observation events
  sinex.control.nodes.{id}.scan         Replay scan commands
  sinex.control.replay.progress.{op}    Replay progress updates
```

### Intelligence Model (Automata)

Three processing models for derived events:

| Model | Trait | State | Emit trigger | Example |
|-------|-------|-------|-------------|---------|
| **Transducer** | `TransducerNode` | Stateless | 1:1 per input | Command canonicalizer |
| **Windowed** | `WindowedNode` | Accumulator | `window_complete(&state) -> bool` | Session detector, analytics |
| **ScopeReconciler** | `ScopeReconcilerNode` | Per-scope | Scope reconciled | Health aggregator |

All share `DerivedNodeAdapter<N>` for: NATS consumer, checkpoint persistence, health reporting, self-observation, shutdown, scope invalidation.

Each synthesis event carries `node_model`, `temporal_policy`, and `semantics_version` — self-documenting provenance metadata.

**Current automata** (consolidated into `sinex-process` per #944, deployed as per-automaton systemd services):
- Command canonicalizer — Transducer, `command.canonical`
- Analytics — Windowed (1000-event sliding window), `analytics.insight`
- Health aggregator — ScopeReconciler, `health.aggregated_report`
- Session detector — Windowed, `activity.session.boundary` (enabled by NixOS module default)
- Hourly summarizer — Windowed, hourly rollups
- Daily summarizer — Windowed, daily rollups

Entity extractor is not yet implemented (#331). Document parsing/chunking is
designed but not wired (#733). Richer derivations are the open frontier.

### WindowedNode Example: Session Detector

```rust
// Groups events by temporal proximity. Gap > 5 minutes = new session boundary.
// Actual implementation: crate/nodes/sinex-process/src/automata/session.rs
struct SessionDetector;

impl WindowedNode for SessionDetector {
    type State = SessionState;
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str { "session-detector" }
    fn input_event_type(&self) -> &'static str { "*" }
    fn output_event_type(&self) -> &'static str { "activity.session.boundary" }

    // Accumulate events into the window state.
    async fn accumulate(&mut self, state: &mut Self::State, input: Self::Input,
        ctx: &DerivedTriggerContext) -> Result<(), NodeLogicError>
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
        -> Result<Option<DerivedOutput>, NodeLogicError>
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
| xtask history | SQLite `.sinex/state/xtask-history.db` | Yes |
