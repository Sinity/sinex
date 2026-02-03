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
              │   + Extensions  │ pg_jsonschema, pgx_ulid   │
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
sinex-primitives    ← Foundation: types, validation, error handling, domain types, IDs
    │
    └── sinex-db          ← Database pools, repositories, query helpers

sinex-schema        ← DB schema, migrations, ULID conversions
    │
    ├── sinex-macros      ← #[with_context], #[derive(EventPayload)]
    │
    └── sinex-node-sdk    ← Node runtime: lifecycle, checkpoints, replay
            │
            ├── sinex-processor-runtime  ← CLI framework for nodes
            └── All nodes (fs, terminal, desktop, system, automata)

sinex-services      ← Business logic: analytics, search, content, pkm
    │
    └── sinex-gateway     ← API layer

sinexctl            ← Unified CLI (uses sinex-primitives, sinex-schema)

xtask (sandbox)     ← Test infrastructure (used via feature gate in test code)
```
