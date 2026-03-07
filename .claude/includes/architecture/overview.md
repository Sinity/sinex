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

xtask (sandbox)     ← Test infrastructure + migration runner + contract deployer
```
