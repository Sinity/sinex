## System Topology

### Data Flow

```
Nodes (Sources)            Nodes (Automata)           Clients
  fs, terminal,              canonicalizer,             CLI (sinexctl),
  desktop, system,           analytics, health          browser extension
  document                        |                         |
       |                          v                         |
       v                   Derived events                 |
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
    |   |
    |   +-- sinex-node-sdk    Node runtime + CLI: lifecycle, checkpoints, replay
    |           |
    |           +-- sinexd    Unified daemon
    |                   |
    |                   +-- sinexd::sources   Source-unit adapters
    |                   +-- sinexd::automata  All automata
    |                   +-- sinexd::event_engine  Persistence pipeline
    |                   +-- sinexd::api       API layer
    |                   +-- sinexd::supervisor  Orchestration

sinexctl                 Unified CLI (query, trace, telemetry, context, report, import)

xtask                    Build automation, sandbox test infra, dev-loop tooling
```

### NATS Subject Topology

```
Subjects:
  {env}.sinex.events.raw.>              Source event batches
  sinex.events.confirmed.>              Persistence confirmations
  sinex.events.dlq.>                    Dead-letter queue
  sinex.derived.invalidation            Scope invalidation (replay)
  sinex.telemetry                       Self-observation events
  sinex.control.nodes.{id}.scan         Replay scan commands
  sinex.control.replay.progress.{op}    Replay progress updates
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

Each derived event carries `node_model`, `temporal_policy`, and `semantics_version` — self-documenting provenance metadata.

**Current automata** (in `sinexd::automata`, deployed as per-automaton systemd services via `sinexd`):
- Command canonicalizer — Transducer, `command.canonical`
- Analytics — Windowed (250-event window), `analytics.insight`
- Health aggregator — ScopeReconciler, `health.aggregated_report`
- Session detector — Windowed, `activity.session.boundary` (enabled by NixOS module default)
- Hourly summarizer — Windowed, hourly rollups
- Daily summarizer — Windowed, daily rollups

Entity/relation shadow-lane automata are present in `sinexd::automata`; activation
as the main consumer substrate is tracked by #1087/#1346. Richer derivations
remain the open frontier.

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
        ctx: &AutomatonContext) -> Result<(), NodeLogicError>
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
