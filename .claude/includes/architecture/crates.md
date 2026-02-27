## Crate Reference

### Core Libraries (`crate/lib/`)

| Crate | Purpose | Key Exports | Docs |
|-------|---------|-------------|------|
| **sinex-primitives** | Foundation types, validation, error handling | `prelude::*`, `SinexError`, `Event<T>`, `EventBuilder`, `Id<T>`, `EventSource`, `EventType` | `crate/lib/sinex-primitives/docs/` |
| **sinex-db** | Database pools, repositories, query helpers | `DbPoolExt`, `EventRepository`, `create_pool()`, `DbPool`, `PoolConfig`, `postgres_copy` | `crate/lib/sinex-db/docs/` |
| **sinex-node-sdk** | Node runtime + CLI framework | `NodeConfig`, `NodeArgs`, `NodeCli`, `NodeCliRunner`, `node_entrypoint!`, `CheckpointManager` | `crate/lib/sinex-node-sdk/docs/` |
| **sinex-services** | Business logic | `AnalyticsService`, `SearchService`, `ContentService`, `PkmService` | `crate/lib/sinex-services/docs/` |
| **sinex-schema** | DB schema + migrations | `Migrator`, `ulid_to_uuid()`, `UlidExt` | `crate/lib/sinex-schema/docs/` |
| **sinex-macros** | Proc macros | `#[derive(EventPayload)]` | `crate/lib/sinex-macros/docs/` |

### Binaries (`crate/core/`)

| Binary | Purpose | Key Config | Docs |
|--------|---------|------------|------|
| **sinex-ingestd** | Ingestion daemon: NATS → PostgreSQL | `DATABASE_URL`, `SINEX_NATS_URL`, `--batch-size` | `crate/core/sinex-ingestd/docs/` |
| **sinex-gateway** | API gateway: RPC + native messaging | `SINEX_GATEWAY_TCP_LISTEN`, bearer tokens | `crate/core/sinex-gateway/docs/` |

### Nodes (`crate/nodes/`)

| Node | Type | Events | Docs |
|------|------|--------|------|
| **sinex-fs-ingestor** | Ingestor | `file.created/modified/deleted` | `crate/nodes/sinex-fs-ingestor/docs/` |
| **sinex-terminal-ingestor** | Ingestor | `shell.command`, `shell.history` | `crate/nodes/sinex-terminal-ingestor/docs/` |
| **sinex-desktop-ingestor** | Ingestor | `window.focused/closed`, `clipboard.*` | `crate/nodes/sinex-desktop-ingestor/docs/` |
| **sinex-system-ingestor** | Ingestor | `systemd.*`, `device.*`, `login.*` | `crate/nodes/sinex-system-ingestor/docs/` |
| **sinex-document-ingestor** | Ingestor | `document.parsed`, `document.extracted` | `crate/nodes/sinex-document-ingestor/docs/` |
| **sinex-terminal-command-canonicalizer** | Automaton | `shell.command.canonical` | `crate/nodes/sinex-terminal-command-canonicalizer/docs/` |
| **sinex-analytics-automaton** | Automaton | `analytics.summary/trend` | `crate/nodes/sinex-analytics-automaton/docs/` |
| **sinex-health-automaton** | Automaton | `health.check`, `health.alert` | `crate/nodes/sinex-health-automaton/docs/` |

### CLI (`crate/cli/`)

| Binary | Purpose | Key Features | Docs |
|--------|---------|--------------|------|
| **sinexctl** | Unified CLI for sinex operations | Event queries, config management, TUI dashboard | `crate/cli/README.md`, `DESIGN.md` |
