## Data Flow

```
Nodes (Ingestors)          Nodes (Automata)           Clients
  fs, terminal,              analytics, search,         CLI, browser
  desktop, system            pkm, content               extension
       │                          │                         │
       ▼                          ▼                         │
  ┌────────────────────────────────────────────────────┐    │
  │              NATS JetStream                        │    │
  │         (Event Transport Layer)                    │    │
  └─────────────────────┬──────────────────────────────┘    │
                        │                                   │
                        ▼                                   │
              ┌─────────────────┐                           │
              │  sinex-ingestd  │ Batch writes, validation  │
              └────────┬────────┘                           │
                       │                                    │
                       ▼                                    │
              ┌─────────────────┐                           │
              │   PostgreSQL    │ TimescaleDB, pgvector     │
              │   + Extensions  │ pg_jsonschema, pg_trgm    │
              └────────┬────────┘                           │
                       │                                    │
                       ▼                                    │
              ┌─────────────────┐                           │
              │ sinex-gateway   │◄──────────────────────────┘
              │ RPC + Native    │ Auth, rate limits
              └─────────────────┘
```

---

## Dependency Hierarchy

```
sinex-primitives    ← Foundation: types (Uuid, Timestamp), validation, error handling, domain types, IDs
    │
    ├── sinex-schema      ← DB schema, migrations (library only, no binary)
    │
    └── sinex-db          ← Database pools, repositories, query helpers, typed ID persistence
            │
            ├── sinex-macros      ← #[derive(EventPayload)]
            │
            └── sinex-node-sdk    ← Node runtime + CLI: lifecycle, checkpoints, replay, entrypoint macro
                    │
                    └── All nodes (fs, terminal, desktop, system, automata)

sinex-services      ← Business logic: analytics, search, content, pkm
    │
    └── sinex-gateway     ← API layer

sinexctl            ← Unified CLI (uses sinex-primitives, sinex-db)
                       sinexctl trace: provenance chain walker
                       sinexctl query: event search with --has-lineage filter

xtask (sandbox)     ← Test infrastructure + migration runner + contract deployer
                       xtask status --summary: includes runtime metrics (ingestd health, lag, batch latency)
                       xtask doctor --runtime: runtime health checks
```

---

## Observability Model

Sinex uses **provenance chains** (not correlation IDs) for cross-component tracing:
- `source_event_ids[]` recursive CTE walks via `events.lineage` RPC
- `sinexctl trace <event-id>` walks and renders provenance chains
- Self-hosting: telemetry (batch stats, consumer lag, gauges) stored as events in Postgres
- `--log-format json` on both binaries for structured machine-parseable logging
- Optional `--tokio-console` (feature-gated) for async runtime debugging
