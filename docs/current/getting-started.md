# Getting Started: Developer Onboarding

> **Purpose:** Get productive with the Sinex codebase in under an hour.

## Mental Model (5 minutes)

Sinex is an event-sourced observability system. Data flows one direction:

```
Nodes → NATS JetStream → sinex-ingestd → PostgreSQL → Automata → Gateway → CLI
```

- **Nodes** capture raw events (filesystem changes, terminal commands, window focus, system signals).
- **Ingestd** validates and persists events to `core.events`, publishing confirmations back to JetStream.
- **Automata** consume confirmed events and produce derived data (health reports, canonicalized commands).
- **Gateway** exposes a JSON-RPC interface for querying.

Everything is append-only. Events have ULIDs (time-ordered unique IDs) and immutable provenance.

## Crate Map

```
crate/
├── core/                      # Binaries (the "what runs")
│   ├── sinex-ingestd/         #   Consumes NATS, writes to Postgres
│   └── sinex-gateway/         #   JSON-RPC server
│
├── lib/                       # Libraries (the "what's shared")
│   ├── sinex-primitives/      #   Error types, IDs, validation, domain types
│   ├── sinex-schema/          #   Migrations, event taxonomy, JSON schemas
│   ├── sinex-db/              #   Connection pool, repository traits
│   ├── sinex-node-sdk/        #   Node lifecycle, streaming, checkpoints
│   ├── sinex-processor-runtime/  # CLI framework (clap integration)
│   ├── sinex-services/        #   Ingest service, health checks
│   └── sinex-macros/          #   Proc macros (EventPayload, with_context)
│
└── nodes/                     # Event nodes & automata
    ├── sinex-fs-ingestor/     #   Filesystem watcher
    ├── sinex-terminal-ingestor/   # Kitty/shell integration
    ├── sinex-desktop-ingestor/    # Clipboard, window focus
    ├── sinex-system-ingestor/     # D-Bus, journald, udev
    └── sinex-health-automaton/    # Health aggregation
```

**Rule of thumb:**
- Touch `crate/lib/` for shared types, traits, database patterns.
- Touch `crate/core/` for runtime behavior (ingestion logic, RPC handlers).
- Touch `crate/nodes/` to add or fix event capture.

## Development Loop

```bash
# 1. Enter the dev shell (required for all commands)
nix develop                     # or: direnv allow

# 2. Quick compile check
xtask check               # workspace-wide cargo check

# 3. Run tests
xtask test

# 4. Start services for manual testing
devenv up nats ingestd gateway
```

Database settings (`PGHOST`, `DATABASE_URL`, etc.) are auto-exported by the shell. SQLx validates queries against a live database during compilation—keep Postgres running.

## Common Tasks

### Adding a new event type

1. Define the payload struct in `crate/lib/sinex-schema/src/payloads/`
2. Add serde derives and register in the taxonomy
3. Run `xtask contracts generate` to regenerate JSON schemas
4. Commit the updated `schemas/` directory (CI enforces this)

### Creating a new node

1. Create `crate/nodes/sinex-<name>-ingestor/` (or `-automaton` for processors)
2. Implement `StatefulStreamProcessor` from `sinex-node-sdk`
3. Use `ProcessorCli` from `sinex-processor-runtime` for the CLI
4. Add to `Cargo.toml` workspace members and NixOS module

### Writing tests

- Use `#[sinex_test]` from `xtask::sandbox` for async tests
- Each test gets an isolated database via the parallel pool
- Nextest only: `xtask test` (not `cargo test`)
- See `TESTING.md` for test organization and flags

### Debugging ingestion

1. Check NATS subjects: `nats sub 'events.raw.>'`
2. Check DLQ: `nats sub 'events.dlq.>'`
3. Query recent events: `python3 cli/exo.py query --rpc-token "$SINEX_RPC_TOKEN"`

## Key Documentation

| Topic | Location |
|-------|----------|
| Architecture overview | `docs/current/architecture/Core_Architecture.md` |
| Event taxonomy | `crate/lib/sinex-schema/docs/event-taxonomy.md` |
| Node SDK patterns | `crate/lib/sinex-node-sdk/docs/overview.md` |
| Database patterns | `crate/lib/sinex-db/docs/README.md` |
| Type system patterns | `docs/current/architecture/type-system-patterns.md` |
| Testing guide | `TESTING.md` |
| Dev workflows | `CLAUDE.md` |

## Quick Reference

```bash
# Build everything
cargo build --workspace --all-targets

# Check for issues
xtask check

# Run all tests
xtask test

# Generate JSON schemas after payload changes
xtask contracts generate

# Apply database migrations
xtask db migrate

# Run a specific node in scanner mode
cargo run --bin sinex-fs-ingestor -- scan /path/to/scan
```

## Next Steps

1. Read `docs/current/architecture/Core_Architecture.md` for the full architecture diagram
2. Explore `crate/lib/sinex-node-sdk/docs/overview.md` if you're adding event capture
3. Check `crate/lib/sinex-schema/docs/event-taxonomy.md` for the event model
4. Run `devenv up` and experiment with the system
