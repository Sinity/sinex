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
    +-- sinex-db          Database pools, repositories, query helpers
            |
            +-- sinex-macros      #[derive(EventPayload)]
            |
            +-- sinex-node-sdk    Node runtime + CLI: lifecycle, checkpoints, replay
                    |
                    +-- All ingestors (fs, terminal, desktop, system, document)
                    +-- All automata (canonicalizer, analytics, health)

sinex-services           Business logic: PKM (entity graph), content (blob storage)
    |
    +-- sinex-gateway     API layer: JSON-RPC, SSE, native messaging

sinexctl                 Unified CLI (query, trace, telemetry, context, report, import)

xtask                    Build automation (63K lines, 40% of project)
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
| **Windowed** | `WindowedNode` | Accumulator | Window complete (count/time/event) | Session detector, analytics summarizer |
| **ScopeReconciler** | `ScopeReconcilerNode` | Per-scope | Scope reconciled | Health aggregator |

All share `DerivedNodeAdapter<N>` for: NATS consumer, checkpoint persistence, health reporting, self-observation, shutdown, scope invalidation.

Each synthesis event carries `node_model`, `temporal_policy`, and `semantics_version` — self-documenting provenance metadata.

**Current automata**: canonicalizer (Transducer), analytics (Windowed, 1000-event sliding window), health (ScopeReconciler, per-component).

**Zero intelligence automata exist yet** (entity extractor, session detector, day summarizer). The SDK is complete. The intelligence layer is vacant.

### WindowedNode Example: Session Detector

```rust
// The highest-impact intelligence feature. Groups events by temporal proximity.
// Gap > 5 minutes = new session boundary.
struct SessionDetector;

impl WindowedNode for SessionDetector {
    type State = SessionAccumulator;
    type Input = JsonValue;         // All events
    type Output = JsonValue;        // activity.session.boundary

    fn window_policy(&self) -> WindowPolicy {
        WindowPolicy::EventCount(100)  // Or time-based
    }

    async fn accumulate(&mut self, state: &mut Self::State, input: Self::Input, ctx: &NodeEventContext)
        -> Result<WindowAction, NodeLogicError>
    {
        let ts = ctx.event_timestamp();
        if state.last_ts.map_or(false, |last| ts - last > Duration::minutes(5)) {
            return Ok(WindowAction::CloseAndEmit);  // Session boundary detected
        }
        state.events.push(input);
        state.last_ts = Some(ts);
        Ok(WindowAction::Continue)
    }

    async fn emit_window(&mut self, state: &mut Self::State) -> Result<Option<Self::Output>, NodeLogicError> {
        Ok(Some(json!({
            "start_time": state.start_ts,
            "end_time": state.last_ts,
            "event_count": state.events.len(),
            "sources": state.unique_sources(),
        })))
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
| xtask history | SQLite `~/.sinex/history.db` | Yes |
