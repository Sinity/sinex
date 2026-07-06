## System Topology

### Data Flow

```
Sources                    Automata                   Clients
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
  |   Confirmed-events stream (bounded bus)     |           |
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
