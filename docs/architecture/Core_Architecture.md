Status: canonical
# Core Architecture

This is the consolidated architecture overview. It links to and summarizes the canonical documents.

Mission
- Build a lifelong, local‑first “sentient archive” that externalizes working memory, preserves context, and enables powerful, privacy‑respecting augmentation.

Key Principles
- User sovereignty and local‑first operation
- Single writer + immutable event log with strict provenance
- Open, hackable architecture; graceful evolution via versioned migrations
- Observability by default (journald heartbeat; traceable command/response)

Flow
- Satellites → NATS JetStream → sinex-ingestd → Postgres (`core.events`) → Automata → Gateway (JSON‑RPC) → CLI.

Data Substrate
- Storage: PostgreSQL (+ TimescaleDB)
- IDs: ULIDs for ordering and distribution
- Event store: `core.events` with strict provenance
- Schema: see `crate/lib/sinex-schema/doc/overview.md` for table details

Streaming & Ingestion
- Messaging: NATS JetStream (subjects, durable consumers, explicit acks)
- Backpressure: bounded batches, ack timeouts, lag monitoring
- Ingestion: validation, persistence, idempotency, single writer
- See also: `docs/plan_v3.txt` for ingestion plan details

Security & Operations
- Security model, threat mitigation: `docs/architecture/security-architecture.md`
- Ops & integrity: backups, invariants, journald-based observability: `docs/architecture/SystemOperations_And_Integrity_Architecture.md`

Schema & Taxonomy
- Schema notes: `crate/lib/sinex-schema/doc/overview.md`
- Event taxonomy: `docs/architecture/event-taxonomy.md`

Implementation Guides
- Satellites SDK and patterns: `crate/lib/sinex-satellite-sdk/doc/overview.md`
- Gateway/CLI: see repository README and `./cli/exo.py`

See also: [Ingestion & Provenance Patterns](provenance.md) for sensor layering, Stage-as-you-go guidance, and timestamp taxonomy.
